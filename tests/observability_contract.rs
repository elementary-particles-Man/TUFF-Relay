use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tuff_relay::adapters::supervisor::{SubmitTaskRequest, SupervisorAdapter};
use tuff_relay::core::*;
use tuff_relay::runtime::{LaneConfig, RelayRuntime};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

fn fixture() -> (LaneId, Arc<Mutex<RelayRuntime>>, SupervisorAdapter) {
    let suffix = NEXT_TEST_ID.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "cosm-observability-{}-{}",
        std::process::id(),
        suffix
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let lane = LaneId::new(format!("obs-lane-{}", suffix)).unwrap();
    let mut runtime = RelayRuntime::new(&dir);
    runtime
        .register_lane(LaneConfig {
            id: lane.clone(),
            repo_root: dir,
            binding_epoch: 1,
            worker_thread_id: None,
        })
        .unwrap();
    runtime
        .handle_event(Event::StartRun {
            lane_id: lane.clone(),
            binding_epoch: 1,
            run_id: RunId::new("run-observability").unwrap(),
        })
        .unwrap();
    let shared = Arc::new(Mutex::new(runtime));
    let adapter = SupervisorAdapter::new(shared.clone(), lane.clone());
    (lane, shared, adapter)
}

fn submit(adapter: &SupervisorAdapter, lane: &LaneId, task: &str) {
    let response = adapter
        .submit(SubmitTaskRequest {
            lane_id: lane.as_str().to_string(),
            task_id: task.to_string(),
            payload_json: r#"{"prompt":"observe"}"#.to_string(),
            in_reply_to: None,
        })
        .unwrap();
    assert!(response.accepted);
}

fn drive_running(runtime: &Arc<Mutex<RelayRuntime>>, lane: &LaneId, task: &str) {
    let mut runtime = runtime.lock().unwrap();
    runtime
        .handle_event(Event::DispatchIntentRecorded {
            lane_id: lane.clone(),
            binding_epoch: 1,
            run_id: RunId::new("run-observability").unwrap(),
            task_id: TaskId::new(task).unwrap(),
        })
        .unwrap();
    runtime
        .handle_event(Event::WorkerTurnBound {
            lane_id: lane.clone(),
            binding_epoch: 1,
            run_id: RunId::new("run-observability").unwrap(),
            task_id: TaskId::new(task).unwrap(),
            turn_id: TurnId::new(format!("turn-{}", task)).unwrap(),
        })
        .unwrap();
}

#[test]
fn waiting_state_is_explicit() {
    let (lane, _, adapter) = fixture();
    let status = adapter.status(lane.as_str()).unwrap();
    assert_eq!(status.phase, "WaitTask");
    assert_eq!(status.worker_liveness.state, WorkerState::Waiting);
    assert_eq!(status.report_lifecycle, ReportLifecycle::NotCreated);
    assert_eq!(status.worker_liveness.next_expected_action, "SUBMIT_TASK");
}

#[test]
fn running_and_generating_are_explicit() {
    let (lane, runtime, adapter) = fixture();
    submit(&adapter, &lane, "task-running");
    drive_running(&runtime, &lane, "task-running");
    let status = adapter.status(lane.as_str()).unwrap();
    assert_eq!(status.worker_liveness.state, WorkerState::Running);
    assert_eq!(status.report_lifecycle, ReportLifecycle::Generating);
    assert_eq!(
        status.worker_liveness.current_task_id.as_deref(),
        Some("task-running")
    );
    assert!(status.wakeup.processing_started_at.is_some());
}

#[test]
fn approval_required_is_not_hidden_as_running() {
    let (lane, runtime, adapter) = fixture();
    submit(&adapter, &lane, "task-approval");
    drive_running(&runtime, &lane, "task-approval");
    let report = Envelope::new(
        lane.clone(),
        1,
        RunId::new("run-observability").unwrap(),
        MessageId::new("rep-approval").unwrap(),
        TaskId::new("task-approval").unwrap(),
        None,
        MessageKind::DecisionRequired,
        r#"{"status":"DECISION_REQUIRED"}"#,
    );
    runtime
        .lock()
        .unwrap()
        .handle_event(Event::WorkerTurnCompleted {
            lane_id: lane.clone(),
            binding_epoch: 1,
            run_id: RunId::new("run-observability").unwrap(),
            turn_id: TurnId::new("turn-task-approval").unwrap(),
            report_envelope: report,
        })
        .unwrap();
    let status = adapter.status(lane.as_str()).unwrap();
    assert_eq!(status.worker_liveness.state, WorkerState::ApprovalRequired);
    assert_eq!(status.report_lifecycle, ReportLifecycle::Ready);
}

#[test]
fn delivery_pending_and_failed_are_distinct() {
    let (lane, runtime, adapter) = fixture();
    runtime
        .lock()
        .unwrap()
        .mark_report_delivery_pending(&lane)
        .unwrap();
    assert_eq!(
        adapter.status(lane.as_str()).unwrap().report_lifecycle,
        ReportLifecycle::DeliveryPending
    );
    runtime
        .lock()
        .unwrap()
        .mark_report_delivery_result(&lane, false, "addon unavailable")
        .unwrap();
    let status = adapter.status(lane.as_str()).unwrap();
    assert_eq!(status.report_lifecycle, ReportLifecycle::DeliveryFailed);
    assert_eq!(
        status.wakeup.send_result.as_deref(),
        Some("addon unavailable")
    );
}

#[test]
fn worker_failure_is_explicit() {
    let (lane, runtime, adapter) = fixture();
    submit(&adapter, &lane, "task-failed");
    drive_running(&runtime, &lane, "task-failed");
    let report = Envelope::new(
        lane.clone(),
        1,
        RunId::new("run-observability").unwrap(),
        MessageId::new("rep-failed").unwrap(),
        TaskId::new("task-failed").unwrap(),
        None,
        MessageKind::Report,
        r#"{"status":"failed"}"#,
    );
    runtime
        .lock()
        .unwrap()
        .handle_event(Event::WorkerTurnFailed {
            lane_id: lane.clone(),
            binding_epoch: 1,
            run_id: RunId::new("run-observability").unwrap(),
            turn_id: TurnId::new("turn-task-failed").unwrap(),
            failure_report_envelope: report,
        })
        .unwrap();
    let status = adapter.status(lane.as_str()).unwrap();
    assert_eq!(status.worker_liveness.state, WorkerState::Failed);
    assert_eq!(
        status.worker_liveness.blocked_reason.as_deref(),
        Some("WORKER_FAILED")
    );
}

#[test]
fn waiting_external_is_explicit() {
    let (lane, runtime, adapter) = fixture();
    runtime
        .lock()
        .unwrap()
        .mark_waiting_external(&lane, "approval service offline")
        .unwrap();
    let status = adapter.status(lane.as_str()).unwrap();
    assert_eq!(status.worker_liveness.state, WorkerState::WaitingExternal);
    assert_eq!(
        status.worker_liveness.blocked_reason.as_deref(),
        Some("approval service offline")
    );
    assert_eq!(
        status.worker_liveness.next_expected_action,
        "EXTERNAL_EVENT"
    );
}

#[test]
fn completed_report_and_ack_are_observable() {
    let (lane, runtime, adapter) = fixture();
    submit(&adapter, &lane, "task-complete");
    drive_running(&runtime, &lane, "task-complete");
    let report = Envelope::new(
        lane.clone(),
        1,
        RunId::new("run-observability").unwrap(),
        MessageId::new("rep-complete").unwrap(),
        TaskId::new("task-complete").unwrap(),
        None,
        MessageKind::Report,
        r#"{"status":"complete"}"#,
    );
    runtime
        .lock()
        .unwrap()
        .handle_event(Event::WorkerTurnCompleted {
            lane_id: lane.clone(),
            binding_epoch: 1,
            run_id: RunId::new("run-observability").unwrap(),
            turn_id: TurnId::new("turn-task-complete").unwrap(),
            report_envelope: report,
        })
        .unwrap();
    let ready = adapter.get_report(lane.as_str()).unwrap();
    assert!(ready.report_available);
    assert_eq!(ready.report_lifecycle, ReportLifecycle::Ready);
    adapter
        .ack(tuff_relay::adapters::supervisor::AckReportRequest {
            lane_id: lane.as_str().to_string(),
            report_id: "rep-complete".to_string(),
        })
        .unwrap();
    let acked = adapter.status(lane.as_str()).unwrap();
    assert_eq!(acked.report_lifecycle, ReportLifecycle::Acked);
    assert!(acked.wakeup.acknowledged_at.is_some());
}

#[test]
fn duplicate_rejection_explains_current_state_and_retry_condition() {
    let (lane, _, adapter) = fixture();
    submit(&adapter, &lane, "task-duplicate");
    let rejected = adapter
        .submit(SubmitTaskRequest {
            lane_id: lane.as_str().to_string(),
            task_id: "task-duplicate".to_string(),
            payload_json: r#"{"prompt":"observe"}"#.to_string(),
            in_reply_to: None,
        })
        .unwrap();
    assert!(!rejected.accepted);
    assert_eq!(rejected.current_task_id.as_deref(), Some("task-duplicate"));
    assert_eq!(rejected.current_phase, "Accepted");
    assert!(rejected.rejection_reason.is_some());
    assert!(rejected.retry_when.is_some());
}
