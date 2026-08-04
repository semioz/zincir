use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::Result;
use crate::types::ToolCall;

/// One conversation turn — role + content + optional tool calls / tool ref.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMessage {
    pub role: String,
    pub content: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl LlmMessage {
    pub fn system(content: &str) -> Self {
        Self {
            role: "system".into(),
            content: json!(content),
            tool_calls: vec![],
            tool_call_id: None,
        }
    }

    pub fn user(content: &str) -> Self {
        Self {
            role: "user".into(),
            content: json!(content),
            tool_calls: vec![],
            tool_call_id: None,
        }
    }

    pub fn assistant(content: &Value, tool_calls: &[ToolCall]) -> Self {
        Self {
            role: "assistant".into(),
            content: content.clone(),
            tool_calls: tool_calls.to_vec(),
            tool_call_id: None,
        }
    }

    pub fn tool(call_id: &str, content: &Value) -> Self {
        Self {
            role: "tool".into(),
            content: content.clone(),
            tool_calls: vec![],
            tool_call_id: Some(call_id.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSchema {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub content: Value,
    pub tool_calls: Vec<ToolCall>,
    pub stop_reason: String,
}

#[async_trait]
pub trait LLMProvider: Send + Sync {
    async fn complete(
        &self,
        messages: Vec<LlmMessage>,
        tools: Vec<ToolSchema>,
    ) -> Result<Response>;
}

// ---------------------------------------------------------------------------
// Stub — returns a canned response, no tool calls. Lets the runtime loop
// run end-to-end without an API key.
// ---------------------------------------------------------------------------

pub struct StubProvider;

#[async_trait]
impl LLMProvider for StubProvider {
    async fn complete(
        &self,
        _messages: Vec<LlmMessage>,
        _tools: Vec<ToolSchema>,
    ) -> Result<Response> {
        Ok(Response {
            content: json!("Hello from the stub provider."),
            tool_calls: vec![],
            stop_reason: "stop".into(),
        })
    }
}
