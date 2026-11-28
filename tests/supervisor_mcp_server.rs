use std::fs;
use std::sync::{Arc, Mutex};
use tuff_relay::adapters::mcp_server::{McpJsonRpcRequest, SupervisorMcpServer};
use tuff_relay::adapters::supervisor::SupervisorAdapter;
use tuff_relay::core::*;
use tuff_relay::runtime::{LaneConfig, RelayRuntime};

#[test]
fn test_supervisor_mcp_server_protocol() {
    let temp_dir = std::env::temp_dir().join(format!("tuff-mcp-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).unwrap();

    let lane_id = LaneId::new("lane-mcp-01").unwrap();
    let epoch = 1;

    let mut runtime = RelayRuntime::new(&temp_dir);
    runtime
        .register_lane(LaneConfig {
            id: lane_id.clone(),
            repo_root: temp_dir.clone(),
            binding_epoch: epoch,
            worker_thread_id: None,
        })
        .unwrap();

    let runtime_arc = Arc::new(Mutex::new(runtime));
    let adapter = Arc::new(SupervisorAdapter::new(runtime_arc.clone(), lane_id.clone()));
    let server = SupervisorMcpServer::new(adapter);

    // 1. Initialize
    let init_req = McpJsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(serde_json::json!(1)),
        method: "initialize".to_string(),
        params: None,
    };
    let init_resp = server.handle_request(init_req);
    assert!(init_resp.result.is_some());
    let server_name = init_resp
        .result
        .unwrap()
        .get("serverInfo")
        .and_then(|s| s.get("name"))
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    assert_eq!(server_name, "COSM-Relay-Supervisor-MCP");

    // 2. Tools list
    let list_req = McpJsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(serde_json::json!(2)),
        method: "tools/list".to_string(),
        params: None,
    };
    let list_resp = server.handle_request(list_req);
    let tools = list_resp
        .result
        .unwrap()
        .get("tools")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap();
    assert_eq!(tools.len(), 8);
    for required in [
        "status",
        "submit",
        "get_report",
        "ack",
        "pause",
        "resume",
        "interrupt",
        "reconcile",
    ] {
        assert!(tools
            .iter()
            .any(|tool| tool.get("name").and_then(|v| v.as_str()) == Some(required)));
    }

    // 3. Status tool call
    let status_req = McpJsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(serde_json::json!(3)),
        method: "tools/call".to_string(),
        params: Some(serde_json::json!({
            "name": "status",
            "arguments": {
                "lane_id": "lane-mcp-01"
            }
        })),
    };
    let status_resp = server.handle_request(status_req);
    assert!(status_resp.result.is_some());

    let _ = fs::remove_dir_all(&temp_dir);
}
