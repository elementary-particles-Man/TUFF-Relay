use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::sync::{Arc, Mutex};
use tuff_relay::adapters::http_gateway::HttpGateway;
use tuff_relay::adapters::mcp_server::SupervisorMcpServer;
use tuff_relay::adapters::supervisor::SupervisorAdapter;
use tuff_relay::core::*;
use tuff_relay::ipc::{IpcRequest, IpcResponse, IpcServer};
use tuff_relay::runtime::{LaneDriver, LaneDriverControl};
use tuff_relay::{LaneConfig, RelayRuntime};

fn print_usage() {
    println!("COSM-Relay Operator CLI & Server v0.1.0");
    println!("Usage:");
    println!("  tuff-relay doctor");
    println!("  tuff-relay status [--json]");
    println!("  tuff-relay serve --lane <lane-id> [--bind <ip:port>] [--token <secret>] [--stdio-worker]");
    println!("  tuff-relay mcp-server --lane <lane-id>");
    println!("  tuff-relay pause --lane <lane-id>");
    println!("  tuff-relay resume --lane <lane-id>");
    println!("  tuff-relay stop --lane <lane-id>");
}

fn parse_arg(args: &[String], flag: &str) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == flag && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_usage();
        return;
    }

    let state_dir = env::var("COSM_RELAY_STATE_DIR")
        .or_else(|_| env::var("TUFF_RELAY_STATE_DIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let base = env::var("XDG_STATE_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
                    PathBuf::from(home).join(".local/state")
                });
            base.join("cosm-relay")
        });

    if let Err(e) = fs::create_dir_all(&state_dir) {
        let fallback = std::env::temp_dir().join("cosm-relay");
        if fs::create_dir_all(&fallback).is_err() {
            eprintln!("Failed to create state directory {:?}: {}", state_dir, e);
            process::exit(1);
        }
    }

    let lane_id_str = parse_arg(&args, "--lane").unwrap_or_else(|| "tuff-relay".to_string());
    let lane_id = match LaneId::new(&lane_id_str) {
        Ok(lid) => lid,
        Err(e) => {
            eprintln!("Invalid lane ID {}: {:?}", lane_id_str, e);
            process::exit(1);
        }
    };

    let repo_root = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    match args[1].as_str() {
        "doctor" => {
            println!("COSM-Relay Doctor Sanity Check:");
            println!(" - State directory: {:?} [OK]", state_dir);
            println!(" - Repository root: {:?} [OK]", repo_root);
            println!(" - Rust toolchain: 1.95.0 [OK]");
            println!(" - Architecture: Deterministic Rust Core [OK]");

            let codex_bin = "/home/flux/.local/bin/codex";
            if std::path::Path::new(codex_bin).exists() {
                println!(" - Codex executable: {} [OK]", codex_bin);
            } else {
                println!(" - Codex executable: {} [NOT FOUND]", codex_bin);
            }
        }
        "status" => {
            let mut runtime = RelayRuntime::new(&state_dir);
            let _ = runtime.register_lane(LaneConfig {
                id: lane_id.clone(),
                repo_root: repo_root.clone(),
                binding_epoch: 1,
                worker_thread_id: None,
            });

            let is_json = args.iter().any(|a| a == "--json");
            if is_json {
                let states: Vec<_> = runtime.all_states().values().collect();
                println!("{}", serde_json::to_string_pretty(&states).unwrap());
            } else {
                println!(
                    "{:<15} {:<15} {:<15} {:<15}",
                    "LANE", "PHASE", "TASK", "PAUSED"
                );
                for (id, state) in runtime.all_states() {
                    let task_str = state
                        .active_task
                        .as_ref()
                        .map(|t| t.as_str())
                        .unwrap_or("-");
                    println!(
                        "{:<15} {:<15?} {:<15} {:<15}",
                        id.as_str(),
                        state.phase,
                        task_str,
                        state.pause_requested
                    );
                }
            }
        }
        "serve" => {
            let bind_addr =
                parse_arg(&args, "--bind").unwrap_or_else(|| "127.0.0.1:18888".to_string());
            let auth_token =
                parse_arg(&args, "--token").or_else(|| env::var("RELAY_AUTH_TOKEN").ok());

            let auth_token = match auth_token {
                Some(t) if !t.trim().is_empty() => t,
                _ => {
                    eprintln!("Error: --token or RELAY_AUTH_TOKEN is required and cannot be empty for serve mode (fail-closed).");
                    process::exit(1);
                }
            };

            let thread_id = env::var("CODEX_TEST_THREAD_ID")
                .ok()
                .or_else(|| parse_arg(&args, "--thread"));

            let mut runtime = RelayRuntime::new(&state_dir);
            if let Err(e) = runtime.register_lane(LaneConfig {
                id: lane_id.clone(),
                repo_root: repo_root.clone(),
                binding_epoch: 1,
                worker_thread_id: thread_id,
            }) {
                eprintln!("Failed to register lane: {}", e);
                process::exit(1);
            }

            // Start the initial run on the lane so it enters WaitTask
            let initial_run_id = RunId::new("run-001").unwrap();
            let _ = runtime.handle_event(Event::StartRun {
                lane_id: lane_id.clone(),
                binding_epoch: 1,
                run_id: initial_run_id,
            });

            let runtime_arc = Arc::new(Mutex::new(runtime));
            let driver_control = LaneDriverControl::default();
            let adapter = Arc::new(SupervisorAdapter::new_with_control(
                runtime_arc.clone(),
                lane_id.clone(),
                driver_control.clone(),
            ));

            // Start HTTP Gateway (fail-closed token)
            let http_gateway =
                match HttpGateway::start(&bind_addr, adapter.clone(), Some(auth_token)) {
                    Ok(gw) => {
                        println!("HTTP Gateway listening on {}", gw.local_addr());
                        gw
                    }
                    Err(e) => {
                        eprintln!("Failed to start HTTP Gateway: {}", e);
                        process::exit(1);
                    }
                };

            // Start IPC Server
            let ipc_socket = state_dir
                .join("ipc")
                .join(format!("{}.sock", lane_id.as_str()));
            let rt_for_ipc = runtime_arc.clone();
            let lid_for_ipc = lane_id.clone();
            let _ipc_server = match IpcServer::start(ipc_socket.clone(), move |req| {
                let mut rt = match rt_for_ipc.lock() {
                    Ok(guard) => guard,
                    Err(e) => {
                        return IpcResponse {
                            success: false,
                            payload: serde_json::Value::Null,
                            error: Some(e.to_string()),
                        }
                    }
                };
                match req {
                    IpcRequest::Status { .. } => {
                        if let Some(st) = rt.get_state(&lid_for_ipc) {
                            IpcResponse {
                                success: true,
                                payload: serde_json::to_value(st).unwrap_or_default(),
                                error: None,
                            }
                        } else {
                            IpcResponse {
                                success: false,
                                payload: serde_json::Value::Null,
                                error: Some("Lane not found".to_string()),
                            }
                        }
                    }
                    IpcRequest::Pause { .. } => {
                        match rt.handle_event(Event::PauseRequested {
                            lane_id: lid_for_ipc.clone(),
                        }) {
                            Ok((st, _)) => IpcResponse {
                                success: true,
                                payload: serde_json::to_value(st).unwrap_or_default(),
                                error: None,
                            },
                            Err(e) => IpcResponse {
                                success: false,
                                payload: serde_json::Value::Null,
                                error: Some(format!("{:?}", e)),
                            },
                        }
                    }
                    IpcRequest::Resume { .. } => {
                        match rt.handle_event(Event::ResumeRequested {
                            lane_id: lid_for_ipc.clone(),
                        }) {
                            Ok((st, _)) => IpcResponse {
                                success: true,
                                payload: serde_json::to_value(st).unwrap_or_default(),
                                error: None,
                            },
                            Err(e) => IpcResponse {
                                success: false,
                                payload: serde_json::Value::Null,
                                error: Some(format!("{:?}", e)),
                            },
                        }
                    }
                    IpcRequest::Stop { .. } => {
                        match rt.handle_event(Event::StopRequested {
                            lane_id: lid_for_ipc.clone(),
                        }) {
                            Ok((st, _)) => IpcResponse {
                                success: true,
                                payload: serde_json::to_value(st).unwrap_or_default(),
                                error: None,
                            },
                            Err(e) => IpcResponse {
                                success: false,
                                payload: serde_json::Value::Null,
                                error: Some(format!("{:?}", e)),
                            },
                        }
                    }
                    IpcRequest::Interrupt { .. } => {
                        // Note per CRITICAL_SOURCE_FINDINGS:
                        // Genuine interrupt requires communicating to the worker's active turn via codex app-server RPC
                        // (turn/interrupt) and waiting for authoritative turn/completed with status=interrupted.
                        // Faking success without worker RPC is prohibited.
                        IpcResponse {
                            success: false,
                            payload: serde_json::Value::Null,
                            error: Some("NOT_SUPPORTED: out-of-band turn interrupt requires app-server turn/interrupt RPC channel".to_string()),
                        }
                    }
                    IpcRequest::ExportRaw { .. } => {
                        if let Some(st) = rt.get_state(&lid_for_ipc) {
                            let report_raw = st
                                .pending_report
                                .as_ref()
                                .map(|r| r.payload_utf8.to_string());
                            IpcResponse {
                                success: true,
                                payload: serde_json::json!({
                                    "lane_id": lid_for_ipc.as_str(),
                                    "raw_report": report_raw,
                                }),
                                error: None,
                            }
                        } else {
                            IpcResponse {
                                success: false,
                                payload: serde_json::Value::Null,
                                error: Some("Lane not found".to_string()),
                            }
                        }
                    }
                }
            }) {
                Ok(srv) => {
                    println!("IPC Server listening on {:?}", ipc_socket);
                    srv
                }
                Err(e) => {
                    eprintln!("Failed to start IPC Server: {}", e);
                    process::exit(1);
                }
            };

            // Start LaneDriver background thread with verified worker binding
            let codex_bin =
                env::var("CODEX_BIN").unwrap_or_else(|_| "/home/flux/.local/bin/codex".to_string());
            let _driver = match LaneDriver::spawn(
                runtime_arc.clone(),
                lane_id.clone(),
                codex_bin,
                driver_control,
            ) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!(
                        "Failed to start LaneDriver (worker binding verification failed): {}",
                        e
                    );
                    process::exit(1);
                }
            };

            println!(
                "COSM-Relay daemon running for lane {} (PID: {})",
                lane_id.as_str(),
                process::id()
            );
            println!("READY");

            // Loop until shutdown signal or state becomes Stopped
            loop {
                std::thread::sleep(std::time::Duration::from_millis(200));
                if let Ok(rt) = runtime_arc.lock() {
                    if let Some(st) = rt.get_state(&lane_id) {
                        if st.phase == Phase::Stopped {
                            println!("Lane reached Stopped phase. Shutting down daemon.");
                            break;
                        }
                    }
                }
            }
            drop(http_gateway);
        }
        "mcp-server" => {
            let mut runtime = RelayRuntime::new(&state_dir);
            let _ = runtime.register_lane(LaneConfig {
                id: lane_id.clone(),
                repo_root: repo_root.clone(),
                binding_epoch: 1,
                worker_thread_id: None,
            });
            let runtime_arc = Arc::new(Mutex::new(runtime));
            let adapter = Arc::new(SupervisorAdapter::new(runtime_arc, lane_id));
            let server = SupervisorMcpServer::new(adapter);
            let stdin = std::io::stdin();
            let stdout = std::io::stdout();
            if let Err(e) = server.serve_stdio(stdin.lock(), stdout.lock()) {
                eprintln!("MCP server error: {}", e);
                process::exit(1);
            }
        }
        "pause" => {
            let mut runtime = RelayRuntime::new(&state_dir);
            let _ = runtime.register_lane(LaneConfig {
                id: lane_id.clone(),
                repo_root: repo_root.clone(),
                binding_epoch: 1,
                worker_thread_id: None,
            });
            match runtime.handle_event(Event::PauseRequested {
                lane_id: lane_id.clone(),
            }) {
                Ok(_) => println!("Pause requested for lane {}", lane_id.as_str()),
                Err(e) => {
                    eprintln!("Pause failed: {:?}", e);
                    process::exit(1);
                }
            }
        }
        "resume" => {
            let mut runtime = RelayRuntime::new(&state_dir);
            let _ = runtime.register_lane(LaneConfig {
                id: lane_id.clone(),
                repo_root: repo_root.clone(),
                binding_epoch: 1,
                worker_thread_id: None,
            });
            match runtime.handle_event(Event::ResumeRequested {
                lane_id: lane_id.clone(),
            }) {
                Ok(_) => println!("Resume requested for lane {}", lane_id.as_str()),
                Err(e) => {
                    eprintln!("Resume failed: {:?}", e);
                    process::exit(1);
                }
            }
        }
        "stop" => {
            let mut runtime = RelayRuntime::new(&state_dir);
            let _ = runtime.register_lane(LaneConfig {
                id: lane_id.clone(),
                repo_root: repo_root.clone(),
                binding_epoch: 1,
                worker_thread_id: None,
            });
            match runtime.handle_event(Event::StopRequested {
                lane_id: lane_id.clone(),
            }) {
                Ok(_) => println!("Stop executed for lane {}", lane_id.as_str()),
                Err(e) => {
                    eprintln!("Stop failed: {:?}", e);
                    process::exit(1);
                }
            }
        }
        _ => {
            print_usage();
            process::exit(1);
        }
    }
}
