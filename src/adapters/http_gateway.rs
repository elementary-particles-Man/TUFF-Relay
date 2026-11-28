use crate::adapters::supervisor::{
    AckReportRequest, ControlRequest, FinishRequest, SubmitTaskRequest, SupervisorAdapter,
};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub const MAX_REQUEST_BODY_SIZE: usize = 10 * 1024 * 1024; // 10 MiB limit
pub const MAX_REQUEST_LINE_LENGTH: usize = 8 * 1024; // 8 KiB limit
pub const MAX_HEADER_COUNT: usize = 64; // Max 64 headers
pub const MAX_HEADER_LINE_LENGTH: usize = 8 * 1024; // 8 KiB per header line
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

pub struct HttpGateway {
    addr: String,
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl HttpGateway {
    pub fn start(
        bind_addr: &str,
        adapter: Arc<SupervisorAdapter>,
        auth_token: Option<String>,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?.to_string();

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let handle = thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let ad = adapter.clone();
                        let token = auth_token.clone();
                        thread::spawn(move || {
                            let _ = handle_http_connection(stream, ad, token);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        Ok(Self {
            addr: local_addr,
            running,
            handle: Some(handle),
        })
    }

    pub fn local_addr(&self) -> &str {
        &self.addr
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for HttpGateway {
    fn drop(&mut self) {
        self.stop();
    }
}

fn handle_http_connection(
    stream: TcpStream,
    adapter: Arc<SupervisorAdapter>,
    expected_token: Option<String>,
) -> std::io::Result<()> {
    // Fail-closed check: if expected_token is None or empty, reject immediately with 401
    if expected_token
        .as_ref()
        .map(|t| t.trim().is_empty())
        .unwrap_or(true)
    {
        let mut writer = &stream;
        let resp = "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"Server authentication token not configured or empty\"}";
        let _ = writer.write_all(resp.as_bytes());
        return Ok(());
    }
    let token = expected_token.unwrap();

    // Enforce 30s connection timeouts
    let _ = stream.set_read_timeout(Some(HTTP_TIMEOUT));
    let _ = stream.set_write_timeout(Some(HTTP_TIMEOUT));

    let mut reader = BufReader::new(&stream);
    let mut request_line = String::new();
    let n = reader.read_line(&mut request_line)?;
    if n == 0 || request_line.len() > MAX_REQUEST_LINE_LENGTH {
        let mut writer = &stream;
        let resp = "HTTP/1.1 414 URI Too Long\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"Request line too long\"}";
        let _ = writer.write_all(resp.as_bytes());
        return Ok(());
    }

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(());
    }
    let method = parts[0];
    let path = parts[1];

    let mut headers = std::collections::HashMap::new();
    let mut content_length: usize = 0;
    let mut header_count = 0;

    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        if line.len() > MAX_HEADER_LINE_LENGTH {
            let mut writer = &stream;
            let resp = "HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"Header line too long\"}";
            let _ = writer.write_all(resp.as_bytes());
            return Ok(());
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }

        header_count += 1;
        if header_count > MAX_HEADER_COUNT {
            let mut writer = &stream;
            let resp = "HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"Too many header fields\"}";
            let _ = writer.write_all(resp.as_bytes());
            return Ok(());
        }

        if let Some((k, v)) = trimmed.split_once(':') {
            let k_lower = k.trim().to_lowercase();
            let v_trim = v.trim().to_string();
            if k_lower == "content-length" {
                if let Ok(cl) = v_trim.parse::<usize>() {
                    content_length = cl;
                }
            }
            headers.insert(k_lower, v_trim);
        }
    }

    let mut writer = &stream;

    // Body limit enforcement
    if content_length > MAX_REQUEST_BODY_SIZE {
        let resp = "HTTP/1.1 413 Payload Too Large\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"Payload too large\"}";
        let _ = writer.write_all(resp.as_bytes());
        return Ok(());
    }

    // Verify Bearer Token Auth (Fail-closed)
    let auth_hdr = headers
        .get("authorization")
        .map(|s| s.as_str())
        .unwrap_or("");
    let expected_hdr = format!("Bearer {}", token);
    if auth_hdr != expected_hdr {
        let resp = "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"Unauthorized\"}";
        let _ = writer.write_all(resp.as_bytes());
        return Ok(());
    }

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    // Strict UTF-8 validation
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            let resp = "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"Invalid UTF-8 body\"}";
            let _ = writer.write_all(resp.as_bytes());
            return Ok(());
        }
    };

    let (status_code, body_resp) = match (method, path) {
        ("GET", p) if p.starts_with("/status") => {
            let lane_id = parse_query_param(p, "lane_id").unwrap_or("tuff-relay");
            match adapter.status(lane_id) {
                Ok(st) => ("200 OK", serde_json::to_string(&st).unwrap_or_default()),
                Err(e) => (
                    "400 Bad Request",
                    serde_json::json!({ "error": e }).to_string(),
                ),
            }
        }
        ("POST", "/submit") => match serde_json::from_str::<SubmitTaskRequest>(body_str) {
            Ok(req) => match adapter.submit(req) {
                Ok(resp) => ("200 OK", serde_json::to_string(&resp).unwrap_or_default()),
                Err(e) => (
                    "400 Bad Request",
                    serde_json::json!({ "error": e }).to_string(),
                ),
            },
            Err(e) => (
                "400 Bad Request",
                serde_json::json!({ "error": e.to_string() }).to_string(),
            ),
        },
        ("GET", p) if p.starts_with("/report") => {
            let lane_id = parse_query_param(p, "lane_id").unwrap_or("tuff-relay");
            match adapter.get_report(lane_id) {
                Ok(rep) => ("200 OK", serde_json::to_string(&rep).unwrap_or_default()),
                Err(e) => (
                    "400 Bad Request",
                    serde_json::json!({ "error": e }).to_string(),
                ),
            }
        }
        ("POST", "/ack") => match serde_json::from_str::<AckReportRequest>(body_str) {
            Ok(req) => match adapter.ack(req) {
                Ok(resp) => ("200 OK", serde_json::to_string(&resp).unwrap_or_default()),
                Err(e) => (
                    "400 Bad Request",
                    serde_json::json!({ "error": e }).to_string(),
                ),
            },
            Err(e) => (
                "400 Bad Request",
                serde_json::json!({ "error": e.to_string() }).to_string(),
            ),
        },
        ("POST", "/pause") => control_http(body_str, |req| adapter.pause(req)),
        ("POST", "/resume") => control_http(body_str, |req| adapter.resume(req)),
        ("POST", "/interrupt") => control_http(body_str, |req| adapter.interrupt(req)),
        ("POST", "/reconcile") => control_http(body_str, |req| adapter.reconcile(req)),
        ("POST", "/finish") => match serde_json::from_str::<FinishRequest>(body_str) {
            Ok(req) => match adapter.finish(req) {
                Ok(resp) => ("200 OK", serde_json::to_string(&resp).unwrap_or_default()),
                Err(e) => (
                    "400 Bad Request",
                    serde_json::json!({ "error": e }).to_string(),
                ),
            },
            Err(e) => (
                "400 Bad Request",
                serde_json::json!({ "error": e.to_string() }).to_string(),
            ),
        },
        _ => (
            "404 Not Found",
            serde_json::json!({ "error": "Endpoint not found" }).to_string(),
        ),
    };

    let full_resp = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status_code,
        body_resp.len(),
        body_resp
    );
    let _ = writer.write_all(full_resp.as_bytes());
    let _ = writer.flush();
    Ok(())
}

fn control_http<F>(body: &str, action: F) -> (&'static str, String)
where
    F: FnOnce(ControlRequest) -> Result<crate::adapters::supervisor::ControlResponse, String>,
{
    match serde_json::from_str::<ControlRequest>(body) {
        Ok(req) => match action(req) {
            Ok(resp) => ("200 OK", serde_json::to_string(&resp).unwrap_or_default()),
            Err(error) => (
                "400 Bad Request",
                serde_json::json!({ "error": error }).to_string(),
            ),
        },
        Err(error) => (
            "400 Bad Request",
            serde_json::json!({ "error": error.to_string() }).to_string(),
        ),
    }
}

fn parse_query_param<'a>(path: &'a str, key: &str) -> Option<&'a str> {
    path.split('?').nth(1).and_then(|q| {
        q.split('&').find_map(|pair| {
            let mut s = pair.split('=');
            if s.next() == Some(key) {
                s.next()
            } else {
                None
            }
        })
    })
}
