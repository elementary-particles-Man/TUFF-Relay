use super::envelope::{Envelope, MessageKind};
use super::state::{LaneState, Phase, ReportLifecycle, WakeupObservation, WorkerState};
use super::types::{LaneId, MessageId, RunId, TaskId, TurnId, PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconcileOutcome {
    TaskNotStarted,
    TaskRunning { turn_id: TurnId },
    TaskCompleted { report: Envelope },
    TaskFailed { report: Envelope },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    StartRun {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
    },
    TaskReceived(Envelope),
    DispatchIntentRecorded {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        task_id: TaskId,
    },
    WorkerTurnBound {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        task_id: TaskId,
        turn_id: TurnId,
    },
    DispatchUncertain {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        task_id: TaskId,
        reason: String,
    },
    WorkerTurnCompleted {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        turn_id: TurnId,
        report_envelope: Envelope,
    },
    WorkerTurnFailed {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        turn_id: TurnId,
        failure_report_envelope: Envelope,
    },
    WorkerOutcomeUncertain {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        turn_id: TurnId,
        reason: String,
    },
    SupervisorAckReceived {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        report_id: MessageId,
    },
    DecisionResponseAccepted {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        continuation_task: Envelope,
    },
    PauseRequested {
        lane_id: LaneId,
    },
    ResumeRequested {
        lane_id: LaneId,
    },
    StopRequested {
        lane_id: LaneId,
    },
    Reconciled {
        lane_id: LaneId,
        binding_epoch: u64,
        run_id: RunId,
        outcome: ReconcileOutcome,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableEventBody {
    RunStarted { run_id: RunId },
    TaskAccepted { envelope: Envelope },
    DispatchIntent { task_id: TaskId },
    WorkerBound { task_id: TaskId, turn_id: TurnId },
    ReportStored { report: Envelope },
    DecisionReportStored { report: Envelope },
    WorkerFailed { failure_report: Envelope },
    Uncertain { reason: String },
    ReportAcked { report_id: MessageId },
    DecisionAccepted { continuation_task: Envelope },
    PauseRequested { current_phase: Phase },
    ResumeRequested { current_phase: Phase },
    RunStopped { run_id: Option<RunId> },
    Reconciled { outcome: ReconcileOutcome },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableJournalEvent {
    pub seq: u64,
    pub lane_id: LaneId,
    pub binding_epoch: u64,
    pub body: DurableEventBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SideEffect {
    PersistEvent(DurableJournalEvent),
    DispatchWorkerTask {
        task_id: TaskId,
        payload_utf8: std::sync::Arc<str>,
        payload_sha256: [u8; 32],
    },
    NotifySupervisorReport {
        report: Envelope,
    },
    NotifyOperator {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReduceError {
    LaneMismatch,
    EpochMismatch,
    RunMismatch,
    TaskMismatch,
    TurnMismatch,
    MessageKindMismatch,
    UnsupportedProtocolVersion,
    PayloadHashMismatch,
    InvalidJsonPayload,
    TaskConflictSameIdDifferentPayload,
    DuplicateTaskIgnored,
    DuplicateAckIgnored,
    ReportIdMismatch,
    InReplyToMismatch,
    InvalidPhaseTransition {
        current: Phase,
        attempted: &'static str,
    },
    PausedCannotDispatch,
    NotEnabled,
    InvalidReconciliationOutcome {
        current: Phase,
        attempted: &'static str,
    },
}

pub fn reduce(
    state: &LaneState,
    event: Event,
) -> Result<(LaneState, Vec<SideEffect>), ReduceError> {
    match event {
        Event::StartRun {
            lane_id,
            binding_epoch,
            run_id,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if state.phase != Phase::Idle && state.phase != Phase::Stopped {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "StartRun",
                });
            }
            let mut next = state.clone();
            next.phase = Phase::WaitTask;
            next.enabled = true;
            next.pause_requested = false;
            next.run_id = Some(run_id.clone());
            next.active_task = None;
            next.active_task_envelope = None;
            next.worker_turn = None;
            next.pending_report = None;
            next.worker_liveness.state = WorkerState::Waiting;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.current_task_id = None;
            next.worker_liveness.blocked_reason = None;
            next.worker_liveness.next_expected_action = "SUBMIT_TASK".to_string();
            next.report_lifecycle = ReportLifecycle::NotCreated;
            next.wakeup = WakeupObservation::default();
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::RunStarted { run_id },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }

        Event::TaskReceived(envelope) => {
            if envelope.protocol_version != PROTOCOL_VERSION {
                return Err(ReduceError::UnsupportedProtocolVersion);
            }
            if envelope.lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if envelope.binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&envelope.run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if envelope.kind != MessageKind::Task {
                return Err(ReduceError::MessageKindMismatch);
            }
            envelope
                .verify_integrity()
                .map_err(|_| ReduceError::PayloadHashMismatch)?;

            // Verify payload is valid JSON
            if serde_json::from_str::<serde_json::Value>(&envelope.payload_utf8).is_err() {
                return Err(ReduceError::InvalidJsonPayload);
            }

            // Check against task history (completed or previously accepted)
            if let Some(recorded_hash) = state.task_history.get(&envelope.task_id) {
                if recorded_hash == &envelope.payload_sha256 {
                    return Err(ReduceError::DuplicateTaskIgnored);
                } else {
                    return Err(ReduceError::TaskConflictSameIdDifferentPayload);
                }
            }

            // Check against currently active task
            if let Some(active_task_id) = &state.active_task {
                if active_task_id == &envelope.task_id {
                    let active_hash = state
                        .active_task_envelope
                        .as_ref()
                        .map(|e| e.payload_sha256);
                    if active_hash == Some(envelope.payload_sha256) {
                        return Err(ReduceError::DuplicateTaskIgnored);
                    } else {
                        return Err(ReduceError::TaskConflictSameIdDifferentPayload);
                    }
                }
            }

            if state.phase != Phase::WaitTask {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "TaskReceived",
                });
            }

            let mut next = state.clone();
            next.phase = Phase::Accepted;
            next.active_task = Some(envelope.task_id.clone());
            next.active_task_envelope = Some(envelope.clone());
            next.worker_liveness.state = WorkerState::Waiting;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.current_task_id = Some(envelope.task_id.as_str().to_string());
            next.worker_liveness.blocked_reason = None;
            next.worker_liveness.next_expected_action = "DISPATCH_WORKER".to_string();
            next.report_lifecycle = ReportLifecycle::NotCreated;
            next.wakeup = WakeupObservation {
                received_at: None,
                ..WakeupObservation::default()
            };
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::TaskAccepted {
                    envelope: envelope.clone(),
                },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }

        Event::DispatchIntentRecorded {
            lane_id,
            binding_epoch,
            run_id,
            task_id,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if state.phase != Phase::Accepted {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "DispatchIntentRecorded",
                });
            }
            if state.pause_requested || !state.enabled {
                return Err(ReduceError::PausedCannotDispatch);
            }
            if state.active_task.as_ref() != Some(&task_id) {
                return Err(ReduceError::TaskMismatch);
            }

            let active_envelope = match &state.active_task_envelope {
                Some(env) if env.task_id == task_id => env,
                _ => return Err(ReduceError::TaskMismatch),
            };

            let mut next = state.clone();
            next.phase = Phase::Dispatching;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.next_expected_action = "WORKER_TURN_START".to_string();
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::DispatchIntent {
                    task_id: task_id.clone(),
                },
            };

            let effects = vec![
                SideEffect::PersistEvent(journal_event),
                SideEffect::DispatchWorkerTask {
                    task_id,
                    payload_utf8: active_envelope.payload_utf8.clone(),
                    payload_sha256: active_envelope.payload_sha256,
                },
            ];
            Ok((next, effects))
        }

        Event::WorkerTurnBound {
            lane_id,
            binding_epoch,
            run_id,
            task_id,
            turn_id,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if state.phase != Phase::Dispatching {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "WorkerTurnBound",
                });
            }
            if state.active_task.as_ref() != Some(&task_id) {
                return Err(ReduceError::TaskMismatch);
            }

            let mut next = state.clone();
            next.phase = Phase::Running;
            next.worker_turn = Some(turn_id.clone());
            next.worker_liveness.state = WorkerState::Running;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.current_task_id = Some(task_id.as_str().to_string());
            next.worker_liveness.blocked_reason = None;
            next.worker_liveness.next_expected_action = "WORKER_COMPLETION".to_string();
            next.report_lifecycle = ReportLifecycle::Generating;
            next.wakeup.processing_started_at = None;
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::WorkerBound { task_id, turn_id },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }

        Event::DispatchUncertain {
            lane_id,
            binding_epoch,
            run_id,
            task_id,
            reason,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if state.active_task.as_ref() != Some(&task_id) {
                return Err(ReduceError::TaskMismatch);
            }
            if state.phase != Phase::Dispatching && state.phase != Phase::Accepted {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "DispatchUncertain",
                });
            }

            let mut next = state.clone();
            next.phase = Phase::Reconcile;
            next.worker_liveness.state = WorkerState::Failed;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.blocked_reason = Some(reason.clone());
            next.worker_liveness.next_expected_action = "RECONCILE".to_string();
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::Uncertain {
                    reason: reason.clone(),
                },
            };
            let effects = vec![
                SideEffect::PersistEvent(journal_event),
                SideEffect::NotifyOperator {
                    message: format!("Reconciliation required: {}", reason),
                },
            ];
            Ok((next, effects))
        }

        Event::WorkerTurnCompleted {
            lane_id,
            binding_epoch,
            run_id,
            turn_id,
            report_envelope,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if report_envelope.lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if report_envelope.binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&report_envelope.run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if state.phase != Phase::Running {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "WorkerTurnCompleted",
                });
            }
            if state.worker_turn.as_ref() != Some(&turn_id) {
                return Err(ReduceError::TurnMismatch);
            }
            if Some(&report_envelope.task_id) != state.active_task.as_ref() {
                return Err(ReduceError::TaskMismatch);
            }
            if report_envelope.kind != MessageKind::Report
                && report_envelope.kind != MessageKind::DecisionRequired
            {
                return Err(ReduceError::MessageKindMismatch);
            }
            report_envelope
                .verify_integrity()
                .map_err(|_| ReduceError::PayloadHashMismatch)?;

            let is_decision_required = report_envelope.kind == MessageKind::DecisionRequired;

            let mut next = state.clone();
            next.phase = Phase::ReportReady;
            next.pending_report = Some(report_envelope.clone());
            next.last_report_was_decision_required = is_decision_required;
            next.worker_liveness.state = if is_decision_required {
                WorkerState::ApprovalRequired
            } else {
                WorkerState::Waiting
            };
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.blocked_reason =
                is_decision_required.then_some("SUPERVISOR_APPROVAL_REQUIRED".to_string());
            next.worker_liveness.next_expected_action = "GET_REPORT_AND_ACK".to_string();
            next.report_lifecycle = ReportLifecycle::Ready;
            next.wakeup.processing_completed_at = None;
            next.journal_seq += 1;

            let journal_body = if is_decision_required {
                DurableEventBody::DecisionReportStored {
                    report: report_envelope.clone(),
                }
            } else {
                DurableEventBody::ReportStored {
                    report: report_envelope.clone(),
                }
            };
            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: journal_body,
            };

            let effects = vec![
                SideEffect::PersistEvent(journal_event),
                SideEffect::NotifySupervisorReport {
                    report: report_envelope,
                },
            ];
            Ok((next, effects))
        }

        Event::WorkerTurnFailed {
            lane_id,
            binding_epoch,
            run_id,
            turn_id,
            failure_report_envelope,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if failure_report_envelope.lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if failure_report_envelope.binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&failure_report_envelope.run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if state.phase != Phase::Running {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "WorkerTurnFailed",
                });
            }
            if state.worker_turn.as_ref() != Some(&turn_id) {
                return Err(ReduceError::TurnMismatch);
            }
            if Some(&failure_report_envelope.task_id) != state.active_task.as_ref() {
                return Err(ReduceError::TaskMismatch);
            }
            if failure_report_envelope.kind != MessageKind::Report {
                return Err(ReduceError::MessageKindMismatch);
            }
            failure_report_envelope
                .verify_integrity()
                .map_err(|_| ReduceError::PayloadHashMismatch)?;

            let mut next = state.clone();
            next.phase = Phase::ReportReady;
            next.pending_report = Some(failure_report_envelope.clone());
            next.last_report_was_decision_required = false;
            next.worker_liveness.state = WorkerState::Failed;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.blocked_reason = Some("WORKER_FAILED".to_string());
            next.worker_liveness.next_expected_action = "GET_FAILURE_REPORT_AND_ACK".to_string();
            next.report_lifecycle = ReportLifecycle::Ready;
            next.wakeup.processing_completed_at = None;
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::WorkerFailed {
                    failure_report: failure_report_envelope.clone(),
                },
            };
            let effects = vec![
                SideEffect::PersistEvent(journal_event),
                SideEffect::NotifySupervisorReport {
                    report: failure_report_envelope,
                },
            ];
            Ok((next, effects))
        }

        Event::WorkerOutcomeUncertain {
            lane_id,
            binding_epoch,
            run_id,
            turn_id,
            reason,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if state.phase != Phase::Running {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "WorkerOutcomeUncertain",
                });
            }
            if state.worker_turn.as_ref() != Some(&turn_id) {
                return Err(ReduceError::TurnMismatch);
            }

            let mut next = state.clone();
            next.phase = Phase::Reconcile;
            next.worker_liveness.state = WorkerState::Failed;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.blocked_reason = Some(reason.clone());
            next.worker_liveness.next_expected_action = "RECONCILE".to_string();
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::Uncertain {
                    reason: reason.clone(),
                },
            };
            let effects = vec![
                SideEffect::PersistEvent(journal_event),
                SideEffect::NotifyOperator {
                    message: format!("Worker turn outcome uncertain: {}", reason),
                },
            ];
            Ok((next, effects))
        }

        Event::SupervisorAckReceived {
            lane_id,
            binding_epoch,
            run_id,
            report_id,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }

            // Check idempotent duplicate ACK
            if state.last_acked_report.as_ref() == Some(&report_id) {
                return Err(ReduceError::DuplicateAckIgnored);
            }

            if state.phase != Phase::ReportReady {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "SupervisorAckReceived",
                });
            }

            let pending_msg_id = state.pending_report.as_ref().map(|e| e.message_id.clone());
            if pending_msg_id.as_ref() != Some(&report_id) {
                return Err(ReduceError::ReportIdMismatch);
            }

            let mut next = state.clone();
            next.last_acked_report = Some(report_id.clone());
            next.pending_report = None;
            next.report_lifecycle = ReportLifecycle::Acked;
            next.wakeup.acknowledged_at = None;

            // Record completed active task into task_history before clearing
            if let Some(active_task_id) = next.active_task.take() {
                if let Some(active_env) = next.active_task_envelope.take() {
                    next.task_history
                        .insert(active_task_id, active_env.payload_sha256);
                }
            }
            next.worker_turn = None;
            next.journal_seq += 1;

            if next.last_report_was_decision_required {
                next.phase = Phase::WaitDecision;
                next.worker_liveness.state = WorkerState::ApprovalRequired;
                next.worker_liveness.blocked_reason =
                    Some("SUPERVISOR_DECISION_REQUIRED".to_string());
                next.worker_liveness.next_expected_action = "SUBMIT_DECISION_RESPONSE".to_string();
            } else if next.pause_requested {
                next.phase = Phase::Stopped;
                next.worker_liveness.state = WorkerState::Paused;
                next.worker_liveness.next_expected_action = "RESUME".to_string();
            } else {
                next.phase = Phase::WaitTask;
                next.worker_liveness.state = WorkerState::Waiting;
                next.worker_liveness.current_task_id = None;
                next.worker_liveness.blocked_reason = None;
                next.worker_liveness.next_expected_action = "SUBMIT_TASK".to_string();
            }
            next.worker_liveness.last_heartbeat_at = None;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::ReportAcked { report_id },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }

        Event::DecisionResponseAccepted {
            lane_id,
            binding_epoch,
            run_id,
            continuation_task,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if continuation_task.protocol_version != PROTOCOL_VERSION {
                return Err(ReduceError::UnsupportedProtocolVersion);
            }
            if continuation_task.lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if continuation_task.binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&continuation_task.run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if continuation_task.kind != MessageKind::Task {
                return Err(ReduceError::MessageKindMismatch);
            }
            if state.phase != Phase::WaitDecision {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "DecisionResponseAccepted",
                });
            }
            // in_reply_to must match last_acked_report
            if continuation_task.in_reply_to.as_ref() != state.last_acked_report.as_ref() {
                return Err(ReduceError::InReplyToMismatch);
            }
            continuation_task
                .verify_integrity()
                .map_err(|_| ReduceError::PayloadHashMismatch)?;

            if serde_json::from_str::<serde_json::Value>(&continuation_task.payload_utf8).is_err() {
                return Err(ReduceError::InvalidJsonPayload);
            }

            // Check against history
            if let Some(recorded_hash) = state.task_history.get(&continuation_task.task_id) {
                if recorded_hash == &continuation_task.payload_sha256 {
                    return Err(ReduceError::DuplicateTaskIgnored);
                } else {
                    return Err(ReduceError::TaskConflictSameIdDifferentPayload);
                }
            }

            let mut next = state.clone();
            next.phase = Phase::Accepted;
            next.active_task = Some(continuation_task.task_id.clone());
            next.active_task_envelope = Some(continuation_task.clone());
            next.last_report_was_decision_required = false;
            next.worker_liveness.state = WorkerState::Waiting;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.current_task_id =
                Some(continuation_task.task_id.as_str().to_string());
            next.worker_liveness.blocked_reason = None;
            next.worker_liveness.next_expected_action = "DISPATCH_WORKER".to_string();
            next.report_lifecycle = ReportLifecycle::NotCreated;
            next.wakeup = WakeupObservation {
                received_at: None,
                ..WakeupObservation::default()
            };
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::DecisionAccepted { continuation_task },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }

        Event::PauseRequested { lane_id } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            let mut next = state.clone();
            next.pause_requested = true;
            next.worker_liveness.state = WorkerState::Paused;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.blocked_reason = Some("PAUSE_REQUESTED".to_string());
            next.worker_liveness.next_expected_action = "RESUME_OR_INTERRUPT".to_string();
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::PauseRequested {
                    current_phase: state.phase,
                },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }

        Event::ResumeRequested { lane_id } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            let mut next = state.clone();
            next.pause_requested = false;
            next.worker_liveness.state = match state.phase {
                Phase::Running => WorkerState::Running,
                Phase::WaitDecision => WorkerState::ApprovalRequired,
                Phase::Reconcile => WorkerState::Failed,
                Phase::Stopped => WorkerState::Stopped,
                _ => WorkerState::Waiting,
            };
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.blocked_reason = None;
            next.worker_liveness.next_expected_action = match state.phase {
                Phase::Running => "WORKER_COMPLETION",
                Phase::WaitDecision => "SUBMIT_DECISION_RESPONSE",
                Phase::Reconcile => "RECONCILE",
                Phase::Stopped => "START_RUN",
                _ => "SUBMIT_TASK",
            }
            .to_string();
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::ResumeRequested {
                    current_phase: state.phase,
                },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }

        Event::StopRequested { lane_id } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if state.phase != Phase::WaitTask
                && state.phase != Phase::Idle
                && state.phase != Phase::Stopped
            {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "StopRequestedWhileActive",
                });
            }

            let mut next = state.clone();
            next.phase = Phase::Stopped;
            next.enabled = false;
            next.worker_liveness.state = WorkerState::Stopped;
            next.worker_liveness.last_heartbeat_at = None;
            next.worker_liveness.blocked_reason = Some("RUN_STOPPED".to_string());
            next.worker_liveness.next_expected_action = "START_RUN".to_string();
            next.journal_seq += 1;

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::RunStopped {
                    run_id: next.run_id.clone(),
                },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }

        Event::Reconciled {
            lane_id,
            binding_epoch,
            run_id,
            outcome,
        } => {
            if lane_id != state.lane_id {
                return Err(ReduceError::LaneMismatch);
            }
            if binding_epoch != state.binding_epoch {
                return Err(ReduceError::EpochMismatch);
            }
            if Some(&run_id) != state.run_id.as_ref() {
                return Err(ReduceError::RunMismatch);
            }
            if state.phase != Phase::Reconcile {
                return Err(ReduceError::InvalidPhaseTransition {
                    current: state.phase,
                    attempted: "Reconciled",
                });
            }

            let mut next = state.clone();
            next.journal_seq += 1;

            match &outcome {
                ReconcileOutcome::TaskNotStarted => {
                    next.phase = Phase::Accepted;
                    next.worker_turn = None;
                }
                ReconcileOutcome::TaskRunning { turn_id } => {
                    next.phase = Phase::Running;
                    next.worker_turn = Some(turn_id.clone());
                }
                ReconcileOutcome::TaskCompleted { report } => {
                    if report.lane_id != state.lane_id
                        || report.binding_epoch != state.binding_epoch
                        || Some(&report.run_id) != state.run_id.as_ref()
                        || Some(&report.task_id) != state.active_task.as_ref()
                    {
                        return Err(ReduceError::InvalidReconciliationOutcome {
                            current: state.phase,
                            attempted: "TaskCompletedValidationFailed",
                        });
                    }
                    report
                        .verify_integrity()
                        .map_err(|_| ReduceError::PayloadHashMismatch)?;
                    next.phase = Phase::ReportReady;
                    next.last_report_was_decision_required =
                        report.kind == MessageKind::DecisionRequired;
                    next.pending_report = Some(report.clone());
                }
                ReconcileOutcome::TaskFailed { report } => {
                    if report.lane_id != state.lane_id
                        || report.binding_epoch != state.binding_epoch
                        || Some(&report.run_id) != state.run_id.as_ref()
                        || Some(&report.task_id) != state.active_task.as_ref()
                    {
                        return Err(ReduceError::InvalidReconciliationOutcome {
                            current: state.phase,
                            attempted: "TaskFailedValidationFailed",
                        });
                    }
                    report
                        .verify_integrity()
                        .map_err(|_| ReduceError::PayloadHashMismatch)?;
                    next.phase = Phase::ReportReady;
                    next.last_report_was_decision_required = false;
                    next.pending_report = Some(report.clone());
                }
            }

            let journal_event = DurableJournalEvent {
                seq: next.journal_seq,
                lane_id: next.lane_id.clone(),
                binding_epoch: next.binding_epoch,
                body: DurableEventBody::Reconciled { outcome },
            };
            let effects = vec![SideEffect::PersistEvent(journal_event)];
            Ok((next, effects))
        }
    }
}
