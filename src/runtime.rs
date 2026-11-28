use crate::adapters::codex::{CodexAppServerClient, CodexInterruptHandle};
use crate::core::envelope::Envelope;
use crate::core::reducer::{Event, ReduceError, SideEffect};
use crate::core::state::LaneState;
use crate::core::state::{ReportLifecycle, WorkerState};
use crate::core::types::{LaneId, MessageId, RunId, TaskId, TurnId};
use crate::FileJournal;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Clone, Default)]
pub struct LaneDriverControl {
    inner: Arc<Mutex<LaneDriverControlState>>,
}

#[derive(Default)]
struct LaneDriverControlState {
    interrupt_handle: Option<CodexInterruptHandle>,
    thread_id: Option<String>,
    turn_id: Option<String>,
}

impl LaneDriverControl {
    fn attach(&self, handle: CodexInterruptHandle, thread_id: Option<String>) {
        if let Ok(mut state) = self.inner.lock() {
            state.interrupt_handle = Some(handle);
            state.thread_id = thread_id;
        }
    }

    fn set_active_turn(&self, turn_id: Option<String>) {
        if let Ok(mut state) = self.inner.lock() {
            state.turn_id = turn_id;
        }
    }

    pub fn interrupt(&self) -> Result<String, String> {
        let state = self.inner.lock().map_err(|e| e.to_string())?;
        let handle = state
            .interrupt_handle
            .as_ref()
            .ok_or("LaneDriver interrupt handle is not attached")?;
        let thread_id = state
            .thread_id
            .as_deref()
            .ok_or("Worker thread ID is not configured")?;
        let turn_id = state.turn_id.as_deref().ok_or("No active worker turn")?;
        handle
            .interrupt(thread_id, turn_id)
            .map_err(|e| e.to_string())?;
        Ok(turn_id.to_string())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LaneConfig {
    pub id: LaneId,
    pub repo_root: PathBuf,
    pub binding_epoch: u64,
    pub worker_thread_id: Option<String>,
}

pub struct RelayRuntime {
    state_dir: PathBuf,
    lanes: HashMap<LaneId, LaneConfig>,
    states: HashMap<LaneId, LaneState>,
    journals: HashMap<LaneId, FileJournal>,
}

impl RelayRuntime {
    pub fn new(state_dir: impl AsRef<Path>) -> Self {
        Self {
            state_dir: state_dir.as_ref().to_path_buf(),
            lanes: HashMap::new(),
            states: HashMap::new(),
            journals: HashMap::new(),
        }
    }

    pub fn register_lane(&mut self, config: LaneConfig) -> io::Result<()> {
        // Enforce normalized paths and prevent duplicate lane registration
        if self.lanes.contains_key(&config.id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Lane {} is already registered", config.id.as_str()),
            ));
        }

        // Validate repo root exists
        if !config.repo_root.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Repo root {:?} does not exist", config.repo_root),
            ));
        }

        let journal = FileJournal::open(&self.state_dir, config.id.clone(), config.binding_epoch)?;
        let replayed_state = journal.replay_state()?;
        self.states.insert(config.id.clone(), replayed_state);
        self.journals.insert(config.id.clone(), journal);
        self.lanes.insert(config.id.clone(), config);
        Ok(())
    }

    pub fn get_state(&self, lane_id: &LaneId) -> Option<&LaneState> {
        self.states.get(lane_id)
    }

    pub fn heartbeat(&mut self, lane_id: &LaneId) -> Result<(), String> {
        let state = self.states.get_mut(lane_id).ok_or("Lane not found")?;
        state.worker_liveness.last_heartbeat_at = Some(now_ms());
        Ok(())
    }

    pub fn mark_report_delivery_pending(&mut self, lane_id: &LaneId) -> Result<(), String> {
        let state = self.states.get_mut(lane_id).ok_or("Lane not found")?;
        state.report_lifecycle = ReportLifecycle::DeliveryPending;
        state.wakeup.send_attempted_at = Some(now_ms());
        state.wakeup.send_result = Some("DELIVERY_PENDING".to_string());
        Ok(())
    }

    pub fn mark_report_delivery_result(
        &mut self,
        lane_id: &LaneId,
        succeeded: bool,
        detail: impl Into<String>,
    ) -> Result<(), String> {
        let state = self.states.get_mut(lane_id).ok_or("Lane not found")?;
        state.report_lifecycle = if succeeded {
            ReportLifecycle::Ready
        } else {
            ReportLifecycle::DeliveryFailed
        };
        if succeeded {
            state.wakeup.send_succeeded_at = Some(now_ms());
        }
        state.wakeup.send_result = Some(detail.into());
        Ok(())
    }

    pub fn mark_waiting_external(
        &mut self,
        lane_id: &LaneId,
        reason: impl Into<String>,
    ) -> Result<(), String> {
        let state = self.states.get_mut(lane_id).ok_or("Lane not found")?;
        state.worker_liveness.state = WorkerState::WaitingExternal;
        state.worker_liveness.blocked_reason = Some(reason.into());
        state.worker_liveness.next_expected_action = "EXTERNAL_EVENT".to_string();
        state.worker_liveness.last_heartbeat_at = Some(now_ms());
        Ok(())
    }

    pub fn all_states(&self) -> &HashMap<LaneId, LaneState> {
        &self.states
    }

    pub fn lane_config(&self, lane_id: &LaneId) -> Option<&LaneConfig> {
        self.lanes.get(lane_id)
    }

    pub fn handle_event(
        &mut self,
        event: Event,
    ) -> Result<(LaneState, Vec<SideEffect>), ReduceError> {
        let observed_at = now_ms();
        let received = matches!(
            &event,
            Event::TaskReceived(_) | Event::DecisionResponseAccepted { .. }
        );
        let processing_started = matches!(&event, Event::WorkerTurnBound { .. });
        let processing_completed = matches!(
            &event,
            Event::WorkerTurnCompleted { .. } | Event::WorkerTurnFailed { .. }
        );
        let acknowledged = matches!(&event, Event::SupervisorAckReceived { .. });
        let lane_id = match &event {
            Event::StartRun { lane_id, .. } => lane_id.clone(),
            Event::TaskReceived(env) => env.lane_id.clone(),
            Event::DispatchIntentRecorded { lane_id, .. } => lane_id.clone(),
            Event::WorkerTurnBound { lane_id, .. } => lane_id.clone(),
            Event::DispatchUncertain { lane_id, .. } => lane_id.clone(),
            Event::WorkerTurnCompleted { lane_id, .. } => lane_id.clone(),
            Event::WorkerTurnFailed { lane_id, .. } => lane_id.clone(),
            Event::WorkerOutcomeUncertain { lane_id, .. } => lane_id.clone(),
            Event::SupervisorAckReceived { lane_id, .. } => lane_id.clone(),
            Event::DecisionResponseAccepted { lane_id, .. } => lane_id.clone(),
            Event::PauseRequested { lane_id } => lane_id.clone(),
            Event::ResumeRequested { lane_id } => lane_id.clone(),
            Event::StopRequested { lane_id } => lane_id.clone(),
            Event::Reconciled { lane_id, .. } => lane_id.clone(),
        };

        let current_state = self.states.get(&lane_id).ok_or(ReduceError::LaneMismatch)?;

        let (mut new_state, effects) = crate::core::reducer::reduce(current_state, event)?;
        new_state.worker_liveness.last_heartbeat_at = Some(observed_at);
        if received {
            new_state.wakeup.received_at = Some(observed_at);
        }
        if processing_started {
            new_state.wakeup.processing_started_at = Some(observed_at);
        }
        if processing_completed {
            new_state.wakeup.processing_completed_at = Some(observed_at);
        }
        if acknowledged {
            new_state.wakeup.acknowledged_at = Some(observed_at);
        }

        // Persist events before executing external effects
        if let Some(journal) = self.journals.get_mut(&lane_id) {
            for effect in &effects {
                if let SideEffect::PersistEvent(durable) = effect {
                    journal
                        .append(durable)
                        .map_err(|_| ReduceError::InvalidPhaseTransition {
                            current: new_state.phase,
                            attempted: "StorageFailure",
                        })?;
                }
            }
        }

        self.states.insert(lane_id, new_state.clone());
        Ok((new_state, effects))
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Inspects raw worker report text for explicit decision marker per TR-DD-001 §3.3.
/// Does not guess based on loose substrings. Looks for explicit JSON field `"status": "DECISION_REQUIRED"`
/// or exact uppercase token `DECISION_REQUIRED` at word boundaries/marker headers.
pub fn is_explicit_decision_required(report_text: &str) -> bool {
    // 1. Check if structured JSON with status == "DECISION_REQUIRED"
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(report_text) {
        if let Some(st) = v.get("status").and_then(|s| s.as_str()) {
            if st == "DECISION_REQUIRED" {
                return true;
            }
        }
    }
    // 2. Check for explicit marker tokens (e.g. [DECISION_REQUIRED] or DECISION_REQUIRED: / ^DECISION_REQUIRED$)
    for line in report_text.lines() {
        let trimmed = line.trim();
        if trimmed == "DECISION_REQUIRED"
            || trimmed.starts_with("DECISION_REQUIRED:")
            || trimmed.starts_with("[DECISION_REQUIRED]")
            || trimmed.starts_with("## DECISION_REQUIRED")
        {
            return true;
        }
    }
    false
}

impl RelayRuntime {
    /// Dispatches active accepted task up to turn_start without holding runtime lock across turn completion wait.
    pub fn begin_worker_turn(
        &mut self,
        lane_id: &LaneId,
        codex_client: &mut CodexAppServerClient,
    ) -> Result<(TurnId, String, Envelope, TaskId, u64, RunId), String> {
        let state = self.get_state(lane_id).ok_or("Lane not found")?.clone();
        let active_task = state.active_task.ok_or("No active task on lane")?;
        let task_env = state
            .active_task_envelope
            .ok_or("No active task envelope on lane")?;
        let run_id = state.run_id.ok_or("No active run")?;
        let epoch = state.binding_epoch;

        let thread_id = {
            let cfg = self.lane_config(lane_id).ok_or("Lane config not found")?;
            cfg.worker_thread_id
                .clone()
                .ok_or("No registered worker_thread_id for lane")?
        };

        // 1. Record dispatch intent
        self.handle_event(Event::DispatchIntentRecorded {
            lane_id: lane_id.clone(),
            binding_epoch: epoch,
            run_id: run_id.clone(),
            task_id: active_task.clone(),
        })
        .map_err(|e| format!("{:?}", e))?;

        // 2. Start turn on Codex
        let turn_id_str = match codex_client.turn_start(&thread_id, &task_env.payload_utf8) {
            Ok(tid) => tid,
            Err(e) => {
                let _ = self.handle_event(Event::DispatchUncertain {
                    lane_id: lane_id.clone(),
                    binding_epoch: epoch,
                    run_id: run_id.clone(),
                    task_id: active_task,
                    reason: format!("Failed to start turn on worker: {:?}", e),
                });
                return Err(format!("Codex turn_start error: {:?}", e));
            }
        };

        let turn_id = crate::core::types::TurnId::new(&turn_id_str)
            .map_err(|e| format!("Invalid turn ID from worker: {:?}", e))?;

        // 3. Bind worker turn in runtime
        self.handle_event(Event::WorkerTurnBound {
            lane_id: lane_id.clone(),
            binding_epoch: epoch,
            run_id: run_id.clone(),
            task_id: active_task.clone(),
            turn_id: turn_id.clone(),
        })
        .map_err(|e| format!("{:?}", e))?;

        Ok((turn_id, turn_id_str, task_env, active_task, epoch, run_id))
    }

    /// Complete worker turn after external RPC finished
    #[allow(clippy::too_many_arguments)]
    pub fn complete_worker_turn(
        &mut self,
        lane_id: &LaneId,
        turn_id: TurnId,
        turn_id_str: &str,
        task_env: &Envelope,
        active_task: TaskId,
        epoch: u64,
        run_id: RunId,
        report_text: &str,
    ) -> Result<(), String> {
        let report_msg_id = format!("rep-{}", turn_id_str);
        let is_decision = is_explicit_decision_required(report_text);
        let kind = if is_decision {
            crate::core::envelope::MessageKind::DecisionRequired
        } else {
            crate::core::envelope::MessageKind::Report
        };

        let rep_env = Envelope::new(
            lane_id.clone(),
            epoch,
            run_id.clone(),
            MessageId::new(&report_msg_id).map_err(|e| format!("{:?}", e))?,
            active_task,
            Some(task_env.message_id.clone()),
            kind,
            report_text,
        );

        self.handle_event(Event::WorkerTurnCompleted {
            lane_id: lane_id.clone(),
            binding_epoch: epoch,
            run_id,
            turn_id,
            report_envelope: rep_env,
        })
        .map_err(|e| format!("{:?}", e))?;

        Ok(())
    }

    /// Record worker turn uncertain outcome
    pub fn record_worker_uncertain(
        &mut self,
        lane_id: &LaneId,
        turn_id: TurnId,
        epoch: u64,
        run_id: RunId,
        reason: String,
    ) -> Result<(), String> {
        self.handle_event(Event::WorkerOutcomeUncertain {
            lane_id: lane_id.clone(),
            binding_epoch: epoch,
            run_id,
            turn_id,
            reason,
        })
        .map_err(|e| format!("{:?}", e))?;
        Ok(())
    }

    /// Record worker turn failed outcome
    #[allow(clippy::too_many_arguments)]
    pub fn record_worker_failed(
        &mut self,
        lane_id: &LaneId,
        turn_id: TurnId,
        turn_id_str: &str,
        task_env: &Envelope,
        active_task: TaskId,
        epoch: u64,
        run_id: RunId,
        err_msg: &str,
    ) -> Result<(), String> {
        let rep_env = Envelope::new(
            lane_id.clone(),
            epoch,
            run_id.clone(),
            MessageId::new(format!("rep-fail-{}", turn_id_str)).map_err(|e| format!("{:?}", e))?,
            active_task,
            Some(task_env.message_id.clone()),
            crate::core::envelope::MessageKind::Report,
            err_msg,
        );

        self.handle_event(Event::WorkerTurnFailed {
            lane_id: lane_id.clone(),
            binding_epoch: epoch,
            run_id,
            turn_id,
            failure_report_envelope: rep_env,
        })
        .map_err(|e| format!("{:?}", e))?;

        Ok(())
    }

    /// Process active dispatch side effect by invoking codex client turn using registered worker binding
    pub fn execute_dispatch_to_worker(
        &mut self,
        lane_id: &LaneId,
        codex_client: &mut CodexAppServerClient,
    ) -> Result<String, String> {
        let (turn_id, turn_id_str, task_env, active_task, epoch, run_id) =
            self.begin_worker_turn(lane_id, codex_client)?;

        // 4. Wait for turn completion
        let result = match codex_client.wait_turn_completion(&turn_id_str) {
            Ok(res) => res,
            Err(e) => {
                let _ = self.record_worker_uncertain(
                    lane_id,
                    turn_id,
                    epoch,
                    run_id,
                    format!("Worker turn completion uncertain: {:?}", e),
                );
                return Err(format!("Codex wait_turn_completion error: {:?}", e));
            }
        };

        self.complete_worker_turn(
            lane_id,
            turn_id,
            &turn_id_str,
            &task_env,
            active_task,
            epoch,
            run_id,
            &result.report_text,
        )?;

        Ok(result.report_text)
    }
}

/// Continuous Background Worker Driver for a registered Lane
pub struct LaneDriver {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl LaneDriver {
    pub fn spawn(
        runtime_arc: Arc<Mutex<RelayRuntime>>,
        lane_id: LaneId,
        codex_bin_path: String,
        control: LaneDriverControl,
    ) -> std::io::Result<Self> {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        // Check if thread_id is configured
        let target_thread_id = {
            let rt = runtime_arc
                .lock()
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            rt.lane_config(&lane_id)
                .and_then(|cfg| cfg.worker_thread_id.clone())
        };

        // Synchronously verify codex client spawn, initialization and thread_resume before returning READY
        let mut codex_client = CodexAppServerClient::spawn(&codex_bin_path).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Failed to spawn codex client: {:?}", e),
            )
        })?;

        codex_client.initialize().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("Failed to initialize codex client: {:?}", e),
            )
        })?;

        if let Some(ref tid) = target_thread_id {
            codex_client.thread_resume(tid).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Failed to resume worker thread {}: {:?}", tid, e),
                )
            })?;
        }

        control.attach(codex_client.interrupt_handle(), target_thread_id.clone());

        let handle = thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                let dispatch_setup = {
                    let mut rt = runtime_arc.lock().unwrap();
                    let _ = rt.heartbeat(&lane_id);
                    let should_dispatch = if let Some(st) = rt.get_state(&lane_id) {
                        st.phase == crate::core::state::Phase::Accepted && !st.pause_requested
                    } else {
                        false
                    };

                    if should_dispatch {
                        rt.begin_worker_turn(&lane_id, &mut codex_client).ok()
                    } else {
                        None
                    }
                };

                if let Some((turn_id, turn_id_str, task_env, active_task, epoch, run_id)) =
                    dispatch_setup
                {
                    control.set_active_turn(Some(turn_id_str.clone()));
                    // Wait for turn completion WITHOUT holding runtime_arc lock!
                    let result = codex_client.wait_turn_completion(&turn_id_str);
                    control.set_active_turn(None);

                    let mut rt = runtime_arc.lock().unwrap();
                    match result {
                        Ok(res) => {
                            let _ = rt.complete_worker_turn(
                                &lane_id,
                                turn_id,
                                &turn_id_str,
                                &task_env,
                                active_task,
                                epoch,
                                run_id,
                                &res.report_text,
                            );
                        }
                        Err(crate::adapters::codex::CodexAdapterError::TurnInterrupted {
                            ..
                        }) => {
                            let _ = rt.record_worker_failed(
                                &lane_id,
                                turn_id,
                                &turn_id_str,
                                &task_env,
                                active_task,
                                epoch,
                                run_id,
                                "{\"status\":\"interrupted\"}",
                            );
                        }
                        Err(crate::adapters::codex::CodexAdapterError::TurnFailed {
                            message,
                            ..
                        }) => {
                            let _ = rt.record_worker_failed(
                                &lane_id,
                                turn_id,
                                &turn_id_str,
                                &task_env,
                                active_task,
                                epoch,
                                run_id,
                                &format!("{{\"status\":\"failed\",\"error\":{:?}}}", message),
                            );
                        }
                        Err(e) => {
                            let _ = rt.record_worker_uncertain(
                                &lane_id,
                                turn_id,
                                epoch,
                                run_id,
                                format!("Worker turn completion uncertain: {:?}", e),
                            );
                        }
                    }
                } else {
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        });

        Ok(Self {
            running,
            handle: Some(handle),
        })
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for LaneDriver {
    fn drop(&mut self) {
        self.stop();
    }
}
