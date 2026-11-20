//! A radio's control socket: one JSON request line in, one JSON answer line
//! out. The socket is root's alone (0600 in a 0700 directory), because a
//! request may carry a network key; the console API reaches it through
//! `pulzr-edge radio <radio id> <op>` with the request on stdin.
//!
//! Request: `{"radio": "<id>", "op": "<op>", "params": {...}}`.
//! Answer: `{"ok": true, "result": {...}}` or
//! `{"ok": false, "error": "<for a person>", "code": "bad-request" | "refused" | "busy" | "no-module"}`.

use super::{RadioError, RadioModule, OP_TIMEOUT};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{watch, Mutex};

/// A request line is capped: a Z-Wave NVM backup in a `restore` is the
/// largest thing that comes this way (up to 256 KiB, base64).
const MAX_REQUEST: usize = 1 << 20;

/// Ops that are the gateway's (`pulzr-radios`), never the module's.
const GATEWAY_OPS: &[&str] = &["use", "release"];

/// Serves `socket` until `stop`. One op at a time: the console is one person
/// at a time, and a stack's own exchanges are serial anyway.
pub async fn listen(
    socket: PathBuf,
    radio: String,
    module: Arc<dyn RadioModule>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let listener = bind(&socket)?;
    let one = Arc::new(Mutex::new(()));
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            conn = listener.accept() => {
                let Ok((stream, _)) = conn else { continue };
                let (module, radio, one) = (module.clone(), radio.clone(), one.clone());
                tokio::spawn(async move {
                    if let Err(e) = serve_one(stream, &radio, module, one).await {
                        tracing::warn!(error = %e, "control connection");
                    }
                });
            }
        }
    }
    let _ = std::fs::remove_file(&socket);
    Ok(())
}

fn bind(socket: &Path) -> Result<UnixListener> {
    let dir = socket.parent().context("socket has no directory")?;
    std::fs::create_dir_all(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    // A socket left by a process that died is in the way; nothing else is
    // ever at this path.
    let _ = std::fs::remove_file(socket);
    let listener =
        UnixListener::bind(socket).with_context(|| format!("binding {}", socket.display()))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

async fn serve_one(
    stream: UnixStream,
    radio: &str,
    module: Arc<dyn RadioModule>,
    one: Arc<Mutex<()>>,
) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut line = Vec::new();
    let mut reader = BufReader::new(read).take(MAX_REQUEST as u64 + 1);
    reader.read_until(b'\n', &mut line).await?;
    let answer = if line.len() > MAX_REQUEST {
        refusal("bad-request", "The request is too large.")
    } else {
        answer(&line, radio, module, one).await
    };
    let mut out = serde_json::to_vec(&answer)?;
    out.push(b'\n');
    write.write_all(&out).await?;
    write.shutdown().await?;
    Ok(())
}

async fn answer(
    line: &[u8],
    radio: &str,
    module: Arc<dyn RadioModule>,
    one: Arc<Mutex<()>>,
) -> Value {
    let Ok(req) = serde_json::from_slice::<Value>(line) else {
        return refusal("bad-request", "The request is not JSON.");
    };
    if req["radio"] != radio {
        return refusal("bad-request", "This socket is another radio's.");
    }
    let Some(op) = req["op"].as_str().filter(|o| is_op_name(o)) else {
        return refusal("bad-request", "op is a lower-case name.");
    };
    if GATEWAY_OPS.contains(&op) {
        return refusal(
            "bad-request",
            "use and release are the gateway's, not the module's.",
        );
    }
    let params = match &req["params"] {
        Value::Null => json!({}),
        p @ Value::Object(_) => p.clone(),
        _ => return refusal("bad-request", "params is an object."),
    };
    let Ok(_turn) = one.try_lock() else {
        return refusal("busy", "Another change to this radio is still running.");
    };
    match tokio::time::timeout(OP_TIMEOUT, module.call(op, params)).await {
        Ok(Ok(result)) => json!({ "ok": true, "result": result }),
        Ok(Err(e)) => refusal(e.code(), e.message()),
        Err(_) => refusal(
            RadioError::Busy(String::new()).code(),
            "The radio did not answer in time.",
        ),
    }
}

fn is_op_name(op: &str) -> bool {
    (1..=32).contains(&op.len()) && op.bytes().all(|b| b.is_ascii_lowercase() || b == b'-')
}

fn refusal(code: &str, error: &str) -> Value {
    json!({ "ok": false, "error": error, "code": code })
}

/// The client side, for `pulzr-edge radio <radio id> <op>`: the answer, or
/// `no-module` when nothing listens (the module is not running on that radio).
pub async fn ask(socket: &Path, radio: &str, op: &str, params: Value) -> Value {
    let stream = match UnixStream::connect(socket).await {
        Ok(s) => s,
        Err(_) => return refusal("no-module", "No radio module is running on that radio."),
    };
    let (read, mut write) = stream.into_split();
    let mut req = serde_json::to_vec(&json!({ "radio": radio, "op": op, "params": params }))
        .expect("serialisable");
    req.push(b'\n');
    if write.write_all(&req).await.is_err() {
        return refusal("no-module", "The radio module closed the connection.");
    }
    let mut line = Vec::new();
    // The module answers within OP_TIMEOUT, or the server says it did not.
    let read = tokio::time::timeout(
        OP_TIMEOUT + std::time::Duration::from_secs(5),
        BufReader::new(read).read_until(b'\n', &mut line),
    )
    .await;
    match read {
        Ok(Ok(n)) if n > 0 => serde_json::from_slice(&line).unwrap_or_else(|_| {
            refusal(
                "refused",
                "The radio module answered something that is not JSON.",
            )
        }),
        _ => refusal("busy", "The radio module did not answer."),
    }
}
