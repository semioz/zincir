use async_trait::async_trait;
use serde_json::{json, Value};

use crate::error::Result;
use crate::types::{ToolCall, ToolResult};

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult>;
}

/// Noop — returns an error result. The stub provider never issues tool
/// calls so this never runs in the v0.1 demo, but the runtime needs one.
pub struct NoopToolExecutor;

#[async_trait]
impl ToolExecutor for NoopToolExecutor {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult> {
        Ok(ToolResult {
            call_id: call.id,
            content: json!({ "error": "no tools configured" }),
        })
    }
}
