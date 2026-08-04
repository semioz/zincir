use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// RunStatus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl std::str::FromStr for RunStatus {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(Error::InvalidState(format!("unknown run status: {other}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// EventType
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    LlmCall,
    ToolCall,
    ToolResult,
    StateTransition,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LlmCall => "llm_call",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::StateTransition => "state_transition",
        }
    }
}

impl std::str::FromStr for EventType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "llm_call" => Ok(Self::LlmCall),
            "tool_call" => Ok(Self::ToolCall),
            "tool_result" => Ok(Self::ToolResult),
            "state_transition" => Ok(Self::StateTransition),
            other => Err(Error::InvalidState(format!("unknown event type: {other}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// sqlx text-mapping for the two enums above.
// Lets AgentRun / Event derive FromRow without manual column handling.
// ---------------------------------------------------------------------------

macro_rules! impl_sqlx_text_enum {
    ($t:ty) => {
        impl sqlx::Type<sqlx::Postgres> for $t {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <&str as sqlx::Type<sqlx::Postgres>>::type_info()
            }
        }
        impl<'q> sqlx::Encode<'q, sqlx::Postgres> for $t {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> std::result::Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                self.as_str().encode_by_ref(buf)
            }
        }
        impl<'r> sqlx::Decode<'r, sqlx::Postgres> for $t {
            fn decode(
                value: sqlx::postgres::PgValueRef<'r>,
            ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                let s = <&str as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
                s.parse().map_err(|e: Error| -> Box<dyn std::error::Error + Sync + Send> {
                    Box::new(e)
                })
            }
        }
    };
}

impl_sqlx_text_enum!(RunStatus);
impl_sqlx_text_enum!(EventType);

// ---------------------------------------------------------------------------
// Row structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct AgentRun {
    pub id: Uuid,
    pub parent_run_id: Option<Uuid>,
    pub role: String,
    pub status: RunStatus,
    pub provider: String,
    pub config: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Event {
    pub id: i64,
    pub run_id: Uuid,
    pub seq: i32,
    pub event_type: EventType,
    pub payload: Value,
    pub idempotency_key: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Message {
    pub id: i64,
    pub from_run_id: Uuid,
    pub to_run_id: Uuid,
    pub payload: Value,
    pub delivered: bool,
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Tool primitives — shared by provider and tool modules
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub args: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub call_id: String,
    pub content: Value,
}

// ---------------------------------------------------------------------------
// Per-run config — stored in agent_runs.config JSONB, drives replay.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    pub model: String,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    pub system: String,
    pub input: String,
    #[serde(default)]
    pub tools: Vec<String>,
}
