use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TurnExecutionStatus {
    Completed,
    Interrupted,
    Failed,
    InProgress,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnResult {
    pub turn_id: String,
    pub status: TurnExecutionStatus,
    pub report_text: String,
    pub raw_response: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerRequestMessage {
    pub id: u64,
    pub method: String,
    pub params: Value,
}

#[derive(Debug)]
pub enum CodexAdapterError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Rpc { code: i64, message: String },
    ProcessTerminated,
    MissingField(String),
    TurnFailed { turn_id: String, message: String },
    TurnInterrupted { turn_id: String },
}

impl fmt::Display for CodexAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodexAdapterError::Io(e) => write!(f, "I/O error: {}", e),
            CodexAdapterError::Json(e) => write!(f, "JSON error: {}", e),
            CodexAdapterError::Rpc { code, message } => {
                write!(f, "RPC error (code {}): {}", code, message)
            }
            CodexAdapterError::ProcessTerminated => write!(f, "Process terminated prematurely"),
            CodexAdapterError::MissingField(field) => {
                write!(f, "Missing expected field in response: {}", field)
            }
            CodexAdapterError::TurnFailed { turn_id, message } => {
                write!(f, "Turn {} failed: {}", turn_id, message)
            }
            CodexAdapterError::TurnInterrupted { turn_id } => {
                write!(f, "Turn {} was interrupted", turn_id)
            }
        }
    }
}

impl std::error::Error for CodexAdapterError {}

impl From<std::io::Error> for CodexAdapterError {
    fn from(err: std::io::Error) -> Self {
        CodexAdapterError::Io(err)
    }
}

impl From<serde_json::Error> for CodexAdapterError {
    fn from(err: serde_json::Error) -> Self {
        CodexAdapterError::Json(err)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcRequest<T> {
    jsonrpc: String,
    id: u64,
    method: String,
    params: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcNotification<T> {
    jsonrpc: String,
    method: String,
    params: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcResponse<T> {
    #[serde(default)]
    jsonrpc: Option<String>,
    id: Option<u64>,
    result: Option<T>,
    error: Option<JsonRpcErrorObj>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcErrorObj {
    code: i64,
    message: String,
    data: Option<Value>,
}

pub struct CodexAppServerClient {
    child: Child,
    stdin: Arc<Mutex<ChildStdin>>,
    reader: BufReader<ChildStdout>,
    next_id: Arc<AtomicU64>,
    buffered_server_requests: Vec<ServerRequestMessage>,
}

#[derive(Clone)]
pub struct CodexInterruptHandle {
    stdin: Arc<Mutex<ChildStdin>>,
    next_id: Arc<AtomicU64>,
}

impl CodexInterruptHandle {
    pub fn interrupt(&self, thread_id: &str, turn_id: &str) -> Result<(), CodexAdapterError> {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: self.next_id.fetch_add(1, Ordering::SeqCst),
            method: "turn/interrupt".to_string(),
            params: serde_json::json!({
                "threadId": thread_id,
                "turnId": turn_id
            }),
        };
        let line = serde_json::to_string(&req)?;
        let mut stdin = self
            .stdin
            .lock()
            .map_err(|_| std::io::Error::other("Codex stdin lock poisoned"))?;
        stdin.write_all(line.as_bytes())?;
        stdin.write_all(b"\n")?;
        stdin.flush()?;
        Ok(())
    }
}

impl CodexAppServerClient {
    pub fn spawn(executable_path: &str) -> Result<Self, CodexAdapterError> {
        let mut child = Command::new(executable_path)
            .arg("app-server")
            .arg("--stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdin = child.stdin.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Failed to capture stdin")
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Failed to capture stdout")
        })?;
        let reader = BufReader::new(stdout);

        Ok(Self {
            child,
            stdin: Arc::new(Mutex::new(stdin)),
            reader,
            next_id: Arc::new(AtomicU64::new(1)),
            buffered_server_requests: Vec::new(),
        })
    }

    fn next_request_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn interrupt_handle(&self) -> CodexInterruptHandle {
        CodexInterruptHandle {
            stdin: self.stdin.clone(),
            next_id: self.next_id.clone(),
        }
    }

    pub fn pop_server_request(&mut self) -> Option<ServerRequestMessage> {
        self.buffered_server_requests.pop()
    }

    pub fn send_request<P: Serialize, R: for<'de> Deserialize<'de>>(
        &mut self,
        method: &str,
        params: P,
    ) -> Result<R, CodexAdapterError> {
        let req_id = self.next_request_id();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: req_id,
            method: method.to_string(),
            params,
        };
        let line = serde_json::to_string(&req)?;
        {
            let mut stdin = self
                .stdin
                .lock()
                .map_err(|_| std::io::Error::other("Codex stdin lock poisoned"))?;
            stdin.write_all(line.as_bytes())?;
            stdin.write_all(b"\n")?;
            stdin.flush()?;
        }

        // Wait for response matching req_id
        let mut resp_line = String::new();
        loop {
            resp_line.clear();
            let bytes_read = self.reader.read_line(&mut resp_line)?;
            if bytes_read == 0 {
                return Err(CodexAdapterError::ProcessTerminated);
            }
            let trimmed = resp_line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let val: Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(_) => continue, // non-json stdout or log lines ignored
            };

            // Check if server issued a request/approval requirement
            if let (Some(id), Some(meth)) = (
                val.get("id").and_then(|v| v.as_u64()),
                val.get("method").and_then(|v| v.as_str()),
            ) {
                self.buffered_server_requests.push(ServerRequestMessage {
                    id,
                    method: meth.to_string(),
                    params: val.get("params").cloned().unwrap_or(Value::Null),
                });
                continue;
            }

            if let Some(id) = val.get("id").and_then(|v| v.as_u64()) {
                if id == req_id {
                    let resp: JsonRpcResponse<R> = serde_json::from_value(val)?;
                    if let Some(err) = resp.error {
                        return Err(CodexAdapterError::Rpc {
                            code: err.code,
                            message: err.message,
                        });
                    }
                    if let Some(res) = resp.result {
                        return Ok(res);
                    }
                    return Err(CodexAdapterError::MissingField("result".to_string()));
                }
            }
        }
    }

    pub fn send_notification<P: Serialize>(
        &mut self,
        method: &str,
        params: P,
    ) -> Result<(), CodexAdapterError> {
        let notif = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
        };
        let line = serde_json::to_string(&notif)?;
        let mut stdin = self
            .stdin
            .lock()
            .map_err(|_| std::io::Error::other("Codex stdin lock poisoned"))?;
        stdin.write_all(line.as_bytes())?;
        stdin.write_all(b"\n")?;
        stdin.flush()?;
        Ok(())
    }

    pub fn initialize(&mut self) -> Result<Value, CodexAdapterError> {
        let res: Value = self.send_request(
            "initialize",
            serde_json::json!({
                "clientInfo": {
                    "name": "TUFF-Relay",
                    "version": "0.1.0"
                },
                "capabilities": {}
            }),
        )?;
        self.send_notification("initialized", serde_json::json!({}))?;
        Ok(res)
    }

    pub fn thread_start(&mut self, cwd: &str) -> Result<String, CodexAdapterError> {
        let res: Value = self.send_request(
            "thread/start",
            serde_json::json!({
                "cwd": cwd
            }),
        )?;
        let thread_id = res
            .get("thread")
            .and_then(|t| t.get("id"))
            .and_then(|v| v.as_str())
            .or_else(|| res.get("threadId").and_then(|v| v.as_str()))
            .ok_or_else(|| CodexAdapterError::MissingField("thread.id / threadId".to_string()))?;
        Ok(thread_id.to_string())
    }

    pub fn thread_resume(&mut self, thread_id: &str) -> Result<Value, CodexAdapterError> {
        let res: Value = self.send_request(
            "thread/resume",
            serde_json::json!({
                "threadId": thread_id,
                "excludeTurns": true
            }),
        )?;
        Ok(res)
    }

    pub fn thread_read(&mut self, thread_id: &str) -> Result<Value, CodexAdapterError> {
        let res: Value = self.send_request(
            "thread/read",
            serde_json::json!({
                "threadId": thread_id,
                "includeTurns": false
            }),
        )?;
        Ok(res)
    }

    pub fn turn_start(
        &mut self,
        thread_id: &str,
        prompt: &str,
    ) -> Result<String, CodexAdapterError> {
        let res: Value = self.send_request(
            "turn/start",
            serde_json::json!({
                "threadId": thread_id,
                "input": [{
                    "type": "text",
                    "text": prompt
                }]
            }),
        )?;
        let turn_id = res
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(|v| v.as_str())
            .or_else(|| res.get("turnId").and_then(|v| v.as_str()))
            .ok_or_else(|| CodexAdapterError::MissingField("turn.id / turnId".to_string()))?;
        Ok(turn_id.to_string())
    }

    pub fn turn_interrupt(
        &mut self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<(), CodexAdapterError> {
        let _: Value = self.send_request(
            "turn/interrupt",
            serde_json::json!({
                "threadId": thread_id,
                "turnId": turn_id
            }),
        )?;
        Ok(())
    }

    /// Reads events until `turn/completed` for target `turn_id` is encountered.
    /// Distinguishes between completed, failed, and interrupted status.
    /// Extracts the final assistant message text and returns a structured `TurnResult`.
    pub fn wait_turn_completion(
        &mut self,
        target_turn_id: &str,
    ) -> Result<TurnResult, CodexAdapterError> {
        let mut collected_text = String::new();
        let mut resp_line = String::new();

        loop {
            resp_line.clear();
            let bytes_read = self.reader.read_line(&mut resp_line)?;
            if bytes_read == 0 {
                return Err(CodexAdapterError::ProcessTerminated);
            }
            let trimmed = resp_line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let val: Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Capture server requests (e.g. approval) while waiting for turn
            if let (Some(id), Some(meth)) = (
                val.get("id").and_then(|v| v.as_u64()),
                val.get("method").and_then(|v| v.as_str()),
            ) {
                self.buffered_server_requests.push(ServerRequestMessage {
                    id,
                    method: meth.to_string(),
                    params: val.get("params").cloned().unwrap_or(Value::Null),
                });
                continue;
            }

            if let Some(method) = val.get("method").and_then(|v| v.as_str()) {
                if method == "item/added" || method == "item/completed" {
                    if let Some(params) = val.get("params") {
                        if let Some(item) = params.get("item") {
                            let item_type = item.get("type").and_then(|v| v.as_str());
                            if item_type == Some("agent_message")
                                || item_type == Some("agentMessage")
                            {
                                if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                                    if !text.is_empty() {
                                        collected_text = text.to_string();
                                    }
                                }
                            }
                        }
                    }
                } else if method == "turn/completed" {
                    if let Some(params) = val.get("params") {
                        let turn_obj = params.get("turn");
                        let turn_id = turn_obj
                            .and_then(|t| t.get("id"))
                            .and_then(|v| v.as_str())
                            .or_else(|| params.get("turnId").and_then(|v| v.as_str()));

                        if turn_id == Some(target_turn_id) {
                            let status_str = turn_obj
                                .and_then(|t| t.get("status"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("completed");

                            let status = match status_str {
                                "completed" => TurnExecutionStatus::Completed,
                                "interrupted" => TurnExecutionStatus::Interrupted,
                                "failed" => TurnExecutionStatus::Failed,
                                "inProgress" => TurnExecutionStatus::InProgress,
                                other => TurnExecutionStatus::Unknown(other.to_string()),
                            };

                            // Check if turn ended in failure or interrupt
                            match status {
                                TurnExecutionStatus::Failed => {
                                    let err_msg = turn_obj
                                        .and_then(|t| t.get("error"))
                                        .and_then(|e| e.get("message"))
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("Unknown error");
                                    return Err(CodexAdapterError::TurnFailed {
                                        turn_id: target_turn_id.to_string(),
                                        message: err_msg.to_string(),
                                    });
                                }
                                TurnExecutionStatus::Interrupted => {
                                    return Err(CodexAdapterError::TurnInterrupted {
                                        turn_id: target_turn_id.to_string(),
                                    });
                                }
                                _ => {
                                    return Ok(TurnResult {
                                        turn_id: target_turn_id.to_string(),
                                        status,
                                        report_text: collected_text,
                                        raw_response: val,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Drop for CodexAppServerClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
