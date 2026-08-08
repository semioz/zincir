use async_trait::async_trait;
use serde_json::{json, Value};
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::types::{ToolCall, ToolResult};

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult>;
}

/// Writes `{call_id}\n` to a file. The file is the observable side effect —
/// counting its lines proves "the tool ran exactly once" across crash-resume.
///
/// If `ZINCIR_PAUSE_TOOL_MS` is set, sleeps before writing to widen the crash
/// window (intent-recorded, side-effect-not-yet-written) for the fuzz test.
pub struct FileAppendExecutor {
    pub path: PathBuf,
}

#[async_trait]
impl ToolExecutor for FileAppendExecutor {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult> {
        if let Ok(ms) = std::env::var("ZINCIR_PAUSE_TOOL_MS") {
            if let Ok(ms) = ms.parse::<u64>() {
                if ms > 0 {
                    tracing::info!(call_id = %call.id, ms, "pausing before tool side effect");
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                }
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|e| Error::Tool(format!("open {}: {e}", self.path.display())))?;

        file.write_all(format!("{}\n", call.id).as_bytes())
            .await
            .map_err(|e| Error::Tool(format!("write: {e}")))?;
        file.flush()
            .await
            .map_err(|e| Error::Tool(format!("flush: {e}")))?;

        tracing::info!(call_id = %call.id, path = %self.path.display(), "tool side effect applied");

        Ok(ToolResult {
            call_id: call.id,
            content: json!({ "ok": true }),
        })
    }
}
