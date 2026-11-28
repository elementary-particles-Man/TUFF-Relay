use crate::core::envelope::Envelope;
use crate::core::state::{ReportLifecycle, WakeupObservation, WorkerLiveness};
use crate::core::types::{LaneId, MessageId, TaskId};
use crate::runtime::{LaneDriverControl, RelayRuntime};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// Minimum tools exposed to Supervisor according to TR-DD-001 §6.3:
/// submit, get_report, ack, finish, status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneStatusDto {
    pub lane_id: String,
    pub phase: String,
    pub binding_epoch: u64,
    pub active_task: Option<String>,
    pub pending_report: Option<String>,
    pub is_decision_required: bool,
    pub pause_requested: bool,
    pub worker_liveness: WorkerLiveness,
    pub heartbeat_age_ms: Option<u64>,
    pub report_lifecycle: ReportLifecycle,
    pub wakeup: WakeupObservation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitTaskRequest {
    pub lane_id: String,
    pub task_id: String,
    pub payload_json: String,
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitTaskResponse {
    pub accepted: bool,
    pub message_id: String,
    pub error: Option<String>,
    pub current_task_id: Option<String>,
    pub current_phase: String,
    pub rejection_reason: Option<String>,
    pub retry_when: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetReportResponse {
    pub report_available: bool,
    pub report_id: Option<String>,
    pub is_decision_required: bool,
    pub payload_utf8: Option<String>,
    pub phase: String,
    pub worker_liveness: WorkerLiveness,
    pub heartbeat_age_ms: Option<u64>,
    pub report_lifecycle: ReportLifecycle,
    pub wakeup: WakeupObservation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckReportRequest {
    pub lane_id: String,
    pub report_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckReportResponse {
    pub acked: bool,
    pub new_phase: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishRequest {
    pub lane_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishResponse {
    pub finished: bool,
    pub new_phase: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlRequest {
    pub lane_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlResponse {
    pub accepted: bool,
    pub operation: String,
    pub phase: String,
    pub worker_state: String,
    pub detail: Option<String>,
}

pub struct SupervisorAdapter {
    runtime: Arc<Mutex<RelayRuntime>>,
    allowed_lane_id: LaneId,
    driver_control: Option<LaneDriverControl>,
}

impl SupervisorAdapter {
    pub fn new(runtime: Arc<Mutex<RelayRuntime>>, allowed_lane_id: LaneId) -> Self {
        Self {
            runtime,
            allowed_lane_id,
            driver_control: None,
        }
    }

    pub fn new_with_control(
        runtime: Arc<Mutex<RelayRuntime>>,
        allowed_lane_id: LaneId,
        driver_control: LaneDriverControl,
    ) -> Self {
        Self {
            runtime,
            allowed_lane_id,
            driver_control: Some(driver_control),
        }
    }

    /// Check lane status (Read-only tool)
    pub fn status(&self, lane_id_str: &str) -> Result<LaneStatusDto, String> {
        if lane_id_str != self.allowed_lane_id.as_str() {
            return Err(format!("Unauthorized lane ID: {}", lane_id_str));
        }
        let rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let state = rt
            .get_state(&self.allowed_lane_id)
            .ok_or_else(|| "Lane not registered in runtime".to_string())?;

        Ok(LaneStatusDto {
            lane_id: state.lane_id.as_str().to_string(),
            phase: format!("{:?}", state.phase),
            binding_epoch: state.binding_epoch,
            active_task: state.active_task.as_ref().map(|t| t.as_str().to_string()),
            pending_report: state
                .pending_report
                .as_ref()
                .map(|r| r.message_id.as_str().to_string()),
            is_decision_required: state.last_report_was_decision_required,
            pause_requested: state.pause_requested,
            worker_liveness: state.worker_liveness.clone(),
            heartbeat_age_ms: heartbeat_age_ms(state.worker_liveness.last_heartbeat_at),
            report_lifecycle: state.report_lifecycle,
            wakeup: state.wakeup.clone(),
        })
    }

    /// Submit a task from supervisor (Write-capable tool)
    pub fn submit(&self, req: SubmitTaskRequest) -> Result<SubmitTaskResponse, String> {
        if req.lane_id != self.allowed_lane_id.as_str() {
            return Err(format!("Unauthorized lane ID: {}", req.lane_id));
        }

        // Validate payload JSON string
        if serde_json::from_str::<serde_json::Value>(&req.payload_json).is_err() {
            return Err("Invalid JSON payload".to_string());
        }

        let mut rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let state = rt
            .get_state(&self.allowed_lane_id)
            .ok_or_else(|| "Lane not registered".to_string())?;

        let run_id = state
            .run_id
            .clone()
            .ok_or_else(|| "Run not started on lane".to_string())?;

        // Deterministic stable message_id based on task_id and epoch to support idempotent re-submission
        let msg_id = format!("msg-{}-epoch-{}", req.task_id, state.binding_epoch);
        let in_reply_to = req
            .in_reply_to
            .as_deref()
            .and_then(|s| MessageId::new(s).ok());

        let env = Envelope::new(
            self.allowed_lane_id.clone(),
            state.binding_epoch,
            run_id.clone(),
            MessageId::new(&msg_id).map_err(|e| format!("{:?}", e))?,
            TaskId::new(&req.task_id).map_err(|e| format!("{:?}", e))?,
            in_reply_to,
            crate::core::envelope::MessageKind::Task,
            req.payload_json.as_str(),
        );

        let event = if state.phase == crate::core::state::Phase::WaitDecision {
            crate::core::reducer::Event::DecisionResponseAccepted {
                lane_id: self.allowed_lane_id.clone(),
                binding_epoch: state.binding_epoch,
                run_id,
                continuation_task: env,
            }
        } else {
            crate::core::reducer::Event::TaskReceived(env)
        };

        match rt.handle_event(event) {
            Ok((new_state, _)) => Ok(SubmitTaskResponse {
                accepted: true,
                message_id: msg_id,
                error: None,
                current_task_id: new_state
                    .active_task
                    .as_ref()
                    .map(|task| task.as_str().to_string()),
                current_phase: format!("{:?}", new_state.phase),
                rejection_reason: None,
                retry_when: None,
            }),
            Err(e) => {
                let current = rt
                    .get_state(&self.allowed_lane_id)
                    .ok_or_else(|| "Lane not registered".to_string())?;
                let reason = format!("{:?}", e);
                Ok(SubmitTaskResponse {
                    accepted: false,
                    message_id: msg_id,
                    error: Some(reason.clone()),
                    current_task_id: current
                        .active_task
                        .as_ref()
                        .map(|task| task.as_str().to_string()),
                    current_phase: format!("{:?}", current.phase),
                    rejection_reason: Some(reason),
                    retry_when: Some(retry_condition(current)),
                })
            }
        }
    }

    /// Retrieve the pending report if available
    pub fn get_report(&self, lane_id_str: &str) -> Result<GetReportResponse, String> {
        if lane_id_str != self.allowed_lane_id.as_str() {
            return Err(format!("Unauthorized lane ID: {}", lane_id_str));
        }
        let rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let state = rt
            .get_state(&self.allowed_lane_id)
            .ok_or_else(|| "Lane not registered".to_string())?;

        if let Some(rep) = &state.pending_report {
            Ok(GetReportResponse {
                report_available: true,
                report_id: Some(rep.message_id.as_str().to_string()),
                is_decision_required: state.last_report_was_decision_required,
                payload_utf8: Some(rep.payload_utf8.to_string()),
                phase: format!("{:?}", state.phase),
                worker_liveness: state.worker_liveness.clone(),
                heartbeat_age_ms: heartbeat_age_ms(state.worker_liveness.last_heartbeat_at),
                report_lifecycle: state.report_lifecycle,
                wakeup: state.wakeup.clone(),
            })
        } else {
            Ok(GetReportResponse {
                report_available: false,
                report_id: None,
                is_decision_required: false,
                payload_utf8: None,
                phase: format!("{:?}", state.phase),
                worker_liveness: state.worker_liveness.clone(),
                heartbeat_age_ms: heartbeat_age_ms(state.worker_liveness.last_heartbeat_at),
                report_lifecycle: state.report_lifecycle,
                wakeup: state.wakeup.clone(),
            })
        }
    }

    /// Ack report
    pub fn ack(&self, req: AckReportRequest) -> Result<AckReportResponse, String> {
        if req.lane_id != self.allowed_lane_id.as_str() {
            return Err(format!("Unauthorized lane ID: {}", req.lane_id));
        }
        let mut rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let (run_id, epoch) = {
            let state = rt
                .get_state(&self.allowed_lane_id)
                .ok_or_else(|| "Lane not registered".to_string())?;
            (
                state
                    .run_id
                    .clone()
                    .ok_or_else(|| "Run not started on lane".to_string())?,
                state.binding_epoch,
            )
        };
        let rep_id = MessageId::new(&req.report_id).map_err(|e| format!("{:?}", e))?;

        let (new_state, _) = rt
            .handle_event(crate::core::reducer::Event::SupervisorAckReceived {
                lane_id: self.allowed_lane_id.clone(),
                binding_epoch: epoch,
                run_id,
                report_id: rep_id,
            })
            .map_err(|e| format!("{:?}", e))?;

        Ok(AckReportResponse {
            acked: true,
            new_phase: format!("{:?}", new_state.phase),
        })
    }

    pub fn pause(&self, req: ControlRequest) -> Result<ControlResponse, String> {
        self.ensure_lane(&req.lane_id)?;
        let mut rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let (state, _) = rt
            .handle_event(crate::core::reducer::Event::PauseRequested {
                lane_id: self.allowed_lane_id.clone(),
            })
            .map_err(|e| format!("{:?}", e))?;
        Ok(control_response(true, "pause", &state, None))
    }

    pub fn resume(&self, req: ControlRequest) -> Result<ControlResponse, String> {
        self.ensure_lane(&req.lane_id)?;
        let mut rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let (state, _) = rt
            .handle_event(crate::core::reducer::Event::ResumeRequested {
                lane_id: self.allowed_lane_id.clone(),
            })
            .map_err(|e| format!("{:?}", e))?;
        Ok(control_response(true, "resume", &state, None))
    }

    pub fn reconcile(&self, req: ControlRequest) -> Result<ControlResponse, String> {
        self.ensure_lane(&req.lane_id)?;
        let rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let state = rt
            .get_state(&self.allowed_lane_id)
            .ok_or_else(|| "Lane not registered".to_string())?;
        Ok(control_response(
            false,
            "reconcile",
            state,
            Some(
                "BLOCKED: authoritative worker probe is required before applying Reconciled"
                    .to_string(),
            ),
        ))
    }

    pub fn interrupt(&self, req: ControlRequest) -> Result<ControlResponse, String> {
        self.ensure_lane(&req.lane_id)?;
        let control = self
            .driver_control
            .as_ref()
            .ok_or("LaneDriver interrupt control channel is not attached")?;
        let turn_id = control.interrupt()?;
        let rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let state = rt
            .get_state(&self.allowed_lane_id)
            .ok_or_else(|| "Lane not registered".to_string())?;
        Ok(control_response(
            true,
            "interrupt",
            state,
            Some(format!(
                "turn/interrupt requested for {}; awaiting authoritative turn/completed",
                turn_id
            )),
        ))
    }

    /// Graceful finish/stop run
    pub fn finish(&self, req: FinishRequest) -> Result<FinishResponse, String> {
        if req.lane_id != self.allowed_lane_id.as_str() {
            return Err(format!("Unauthorized lane ID: {}", req.lane_id));
        }
        let mut rt = self.runtime.lock().map_err(|e| e.to_string())?;
        let (new_state, _) = rt
            .handle_event(crate::core::reducer::Event::StopRequested {
                lane_id: self.allowed_lane_id.clone(),
            })
            .map_err(|e| format!("{:?}", e))?;

        Ok(FinishResponse {
            finished: true,
            new_phase: format!("{:?}", new_state.phase),
        })
    }

    fn ensure_lane(&self, lane_id: &str) -> Result<(), String> {
        if lane_id != self.allowed_lane_id.as_str() {
            return Err(format!("Unauthorized lane ID: {}", lane_id));
        }
        Ok(())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn heartbeat_age_ms(last_heartbeat_at: Option<u64>) -> Option<u64> {
    last_heartbeat_at.map(|at| now_ms().saturating_sub(at))
}

fn retry_condition(state: &crate::core::state::LaneState) -> String {
    match state.phase {
        crate::core::state::Phase::WaitTask => "NOW".to_string(),
        crate::core::state::Phase::WaitDecision => "AFTER_ACK_WITH_IN_REPLY_TO".to_string(),
        crate::core::state::Phase::ReportReady => "AFTER_REPORT_ACK".to_string(),
        crate::core::state::Phase::Reconcile => "AFTER_RECONCILIATION".to_string(),
        crate::core::state::Phase::Stopped => "AFTER_RUN_START".to_string(),
        _ => "AFTER_CURRENT_TASK_REACHES_REPORT_READY_AND_IS_ACKED".to_string(),
    }
}

fn control_response(
    accepted: bool,
    operation: &str,
    state: &crate::core::state::LaneState,
    detail: Option<String>,
) -> ControlResponse {
    ControlResponse {
        accepted,
        operation: operation.to_string(),
        phase: format!("{:?}", state.phase),
        worker_state: format!("{:?}", state.worker_liveness.state).to_uppercase(),
        detail,
    }
}
