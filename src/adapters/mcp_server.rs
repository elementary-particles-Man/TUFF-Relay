use crate::adapters::supervisor::SupervisorAdapter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufRead, Write};
use std::sync::Arc;

fn lane_control_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "lane_id": { "type": "string" }
        },
        "required": ["lane_id"]
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct McpJsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct McpJsonRpcResponse {
    pub jsonrpc: String,
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpJsonRpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct McpJsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

pub struct SupervisorMcpServer {
    adapter: Arc<SupervisorAdapter>,
}

impl SupervisorMcpServer {
    pub fn new(adapter: Arc<SupervisorAdapter>) -> Self {
        Self { adapter }
    }

    pub fn handle_request(&self, req: McpJsonRpcRequest) -> McpJsonRpcResponse {
        let req_id = req.id.clone();
        match req.method.as_str() {
            "initialize" => {
                let result = serde_json::json!({
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {
                        "name": "COSM-Relay-Supervisor-MCP",
                        "version": "0.1.0"
                    },
                    "capabilities": {
                        "tools": {
                            "listChanged": false
                        }
                    }
                });
                McpJsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id: req_id,
                    result: Some(result),
                    error: None,
                }
            }
            "tools/list" => {
                let tools = serde_json::json!({
                    "tools": [
                        {
                            "name": "status",
                            "description": "Query current phase, epoch, active task, and pause status for a registered lane (Read-only).",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "lane_id": { "type": "string" }
                                },
                                "required": ["lane_id"]
                            }
                        },
                        {
                            "name": "submit",
                            "description": "Submit a task JSON payload to a registered lane (Write-capable).",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "lane_id": { "type": "string" },
                                    "task_id": { "type": "string" },
                                    "payload_json": { "type": "string" },
                                    "in_reply_to": { "type": "string" }
                                },
                                "required": ["lane_id", "task_id", "payload_json"]
                            }
                        },
                        {
                            "name": "get_report",
                            "description": "Retrieve the pending worker report for a registered lane (Read-only, preserves state).",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "lane_id": { "type": "string" }
                                },
                                "required": ["lane_id"]
                            }
                        },
                        {
                            "name": "ack",
                            "description": "Acknowledge receipt of a completed or decision-required report (Write-capable).",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "lane_id": { "type": "string" },
                                    "report_id": { "type": "string" }
                                },
                                "required": ["lane_id", "report_id"]
                            }
                        },
                        {
                            "name": "pause",
                            "description": "Pause new dispatch and expose PAUSED liveness (Write-capable).",
                            "inputSchema": lane_control_schema()
                        },
                        {
                            "name": "resume",
                            "description": "Resume dispatch after a pause (Write-capable).",
                            "inputSchema": lane_control_schema()
                        },
                        {
                            "name": "interrupt",
                            "description": "Request turn/interrupt for the active worker turn (Write-capable).",
                            "inputSchema": lane_control_schema()
                        },
                        {
                            "name": "reconcile",
                            "description": "Request authoritative reconciliation; returns BLOCKED until a worker probe is available (Write-capable).",
                            "inputSchema": lane_control_schema()
                        }
                    ]
                });
                McpJsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id: req_id,
                    result: Some(tools),
                    error: None,
                }
            }
            "tools/call" => {
                let params = req.params.unwrap_or(Value::Null);
                let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let args = params.get("arguments").cloned().unwrap_or(Value::Null);

                match tool_name {
                    "status" => {
                        let lane_id = args.get("lane_id").and_then(|v| v.as_str()).unwrap_or("");
                        match self.adapter.status(lane_id) {
                            Ok(res) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: Some(serde_json::json!({
                                    "content": [{
                                        "type": "text",
                                        "text": serde_json::to_string(&res).unwrap_or_default()
                                    }]
                                })),
                                error: None,
                            },
                            Err(e) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: None,
                                error: Some(McpJsonRpcError {
                                    code: -32000,
                                    message: e,
                                    data: None,
                                }),
                            },
                        }
                    }
                    "submit" => {
                        let lane_id = args.get("lane_id").and_then(|v| v.as_str()).unwrap_or("");
                        let task_id = args.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
                        let payload_json = args
                            .get("payload_json")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let in_reply_to = args
                            .get("in_reply_to")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());

                        match self
                            .adapter
                            .submit(crate::adapters::supervisor::SubmitTaskRequest {
                                lane_id: lane_id.to_string(),
                                task_id: task_id.to_string(),
                                payload_json: payload_json.to_string(),
                                in_reply_to,
                            }) {
                            Ok(res) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: Some(serde_json::json!({
                                    "content": [{
                                        "type": "text",
                                        "text": serde_json::to_string(&res).unwrap_or_default()
                                    }]
                                })),
                                error: None,
                            },
                            Err(e) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: None,
                                error: Some(McpJsonRpcError {
                                    code: -32000,
                                    message: e,
                                    data: None,
                                }),
                            },
                        }
                    }
                    "get_report" => {
                        let lane_id = args.get("lane_id").and_then(|v| v.as_str()).unwrap_or("");
                        match self.adapter.get_report(lane_id) {
                            Ok(res) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: Some(serde_json::json!({
                                    "content": [{
                                        "type": "text",
                                        "text": serde_json::to_string(&res).unwrap_or_default()
                                    }]
                                })),
                                error: None,
                            },
                            Err(e) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: None,
                                error: Some(McpJsonRpcError {
                                    code: -32000,
                                    message: e,
                                    data: None,
                                }),
                            },
                        }
                    }
                    "ack" => {
                        let lane_id = args.get("lane_id").and_then(|v| v.as_str()).unwrap_or("");
                        let report_id =
                            args.get("report_id").and_then(|v| v.as_str()).unwrap_or("");
                        match self
                            .adapter
                            .ack(crate::adapters::supervisor::AckReportRequest {
                                lane_id: lane_id.to_string(),
                                report_id: report_id.to_string(),
                            }) {
                            Ok(res) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: Some(serde_json::json!({
                                    "content": [{
                                        "type": "text",
                                        "text": serde_json::to_string(&res).unwrap_or_default()
                                    }]
                                })),
                                error: None,
                            },
                            Err(e) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: None,
                                error: Some(McpJsonRpcError {
                                    code: -32000,
                                    message: e,
                                    data: None,
                                }),
                            },
                        }
                    }
                    "pause" | "resume" | "interrupt" | "reconcile" => {
                        let lane_id = args.get("lane_id").and_then(|v| v.as_str()).unwrap_or("");
                        let req = crate::adapters::supervisor::ControlRequest {
                            lane_id: lane_id.to_string(),
                        };
                        let result = match tool_name {
                            "pause" => self.adapter.pause(req),
                            "resume" => self.adapter.resume(req),
                            "interrupt" => self.adapter.interrupt(req),
                            "reconcile" => self.adapter.reconcile(req),
                            _ => unreachable!(),
                        };
                        match result {
                            Ok(res) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: Some(serde_json::json!({
                                    "content": [{
                                        "type": "text",
                                        "text": serde_json::to_string(&res).unwrap_or_default()
                                    }]
                                })),
                                error: None,
                            },
                            Err(e) => McpJsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: req_id,
                                result: None,
                                error: Some(McpJsonRpcError {
                                    code: -32000,
                                    message: e,
                                    data: None,
                                }),
                            },
                        }
                    }
                    _ => McpJsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id: req_id,
                        result: None,
                        error: Some(McpJsonRpcError {
                            code: -32601,
                            message: format!("Method/tool not found: {}", tool_name),
                            data: None,
                        }),
                    },
                }
            }
            _ => McpJsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: req_id,
                result: None,
                error: Some(McpJsonRpcError {
                    code: -32601,
                    message: format!("Method not found: {}", req.method),
                    data: None,
                }),
            },
        }
    }

    /// Process stdio lines from MCP client
    pub fn serve_stdio<R: BufRead, W: Write>(
        &self,
        mut reader: R,
        mut writer: W,
    ) -> std::io::Result<()> {
        let mut line = String::new();
        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line)?;
            if bytes_read == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(req) = serde_json::from_str::<McpJsonRpcRequest>(trimmed) {
                let resp = self.handle_request(req);
                let out_line = serde_json::to_string(&resp).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                writer.write_all(out_line.as_bytes())?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }
        }
        Ok(())
    }
}
