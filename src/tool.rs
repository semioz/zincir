#[cfg(not(unix))]
compile_error!("Zincir's idempotent file executor currently requires Unix");

use async_trait::async_trait;
use serde_json::json;
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::types::{ToolCall, ToolResult};

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult>;
}

/// Stores one atomic result file per tool-call ID. A retry reads the existing
/// result instead of applying the effect again.
pub struct IdempotentFileExecutor {
    pub directory: PathBuf,
}

#[async_trait]
impl ToolExecutor for IdempotentFileExecutor {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult> {
        validate_call_id(&call.id)?;
        fs::create_dir_all(&self.directory)
            .await
            .map_err(|e| tool_io_error("create directory", &self.directory, e))?;

        let result_path = self.directory.join(format!("{}.json", call.id));
        if let Some(result) = read_result(&result_path, &call.id).await? {
            tracing::info!(call_id = %call.id, path = %result_path.display(), "reusing tool result");
            return Ok(result);
        }

        pause("ZINCIR_PAUSE_BEFORE_TOOL_MS", &call.id).await;

        let result = ToolResult {
            call_id: call.id.clone(),
            content: json!({
                "ok": true,
                "content": call.args.get("content").cloned().unwrap_or_default()
            }),
        };
        let bytes = serde_json::to_vec(&result)?;
        let temporary_path = self
            .directory
            .join(format!(".{}.{}.tmp", call.id, Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .await
            .map_err(|e| tool_io_error("open temporary result", &temporary_path, e))?;
        file.write_all(&bytes)
            .await
            .map_err(|e| tool_io_error("write temporary result", &temporary_path, e))?;
        file.sync_all()
            .await
            .map_err(|e| tool_io_error("sync temporary result", &temporary_path, e))?;
        let published = publish_result(&temporary_path, &result_path, result).await?;

        // ponytail: atomic hard-link publication covers process crashes; sync
        // the directory when power-loss durability becomes a requirement.
        tracing::info!(call_id = %call.id, path = %result_path.display(), "tool side effect applied");
        pause("ZINCIR_PAUSE_AFTER_TOOL_MS", &call.id).await;

        Ok(published)
    }
}

fn validate_call_id(call_id: &str) -> Result<()> {
    if !call_id.is_empty()
        && call_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Ok(());
    }
    Err(Error::Tool(format!("unsafe tool call id: {call_id:?}")))
}

async fn publish_result(
    temporary_path: &Path,
    result_path: &Path,
    proposed: ToolResult,
) -> Result<ToolResult> {
    let call_id = proposed.call_id.clone();
    let published = match fs::hard_link(temporary_path, result_path).await {
        Ok(()) => Ok(proposed),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_result(result_path, &call_id).await?.ok_or_else(|| {
                Error::Tool(format!("result disappeared: {}", result_path.display()))
            })
        }
        Err(error) => Err(tool_io_error("publish result", result_path, error)),
    };

    if let Err(error) = fs::remove_file(temporary_path).await {
        tracing::warn!(path = %temporary_path.display(), %error, "failed to remove temporary result");
    }
    published
}

async fn read_result(path: &Path, call_id: &str) -> Result<Option<ToolResult>> {
    let mut file = match OpenOptions::new().read(true).open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(tool_io_error("open result", path, error)),
    };
    let opened_metadata = file
        .metadata()
        .await
        .map_err(|error| tool_io_error("inspect opened result", path, error))?;
    let path_metadata = fs::symlink_metadata(path)
        .await
        .map_err(|error| tool_io_error("inspect result path", path, error))?;
    if !path_metadata.file_type().is_file() || !same_file(&opened_metadata, &path_metadata) {
        return Err(Error::Tool(format!(
            "cached result path changed while opening: {}",
            path.display()
        )));
    }

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .await
        .map_err(|error| tool_io_error("read result", path, error))?;
    let result: ToolResult = serde_json::from_slice(&bytes)?;
    if result.call_id != call_id {
        return Err(Error::Tool(format!(
            "cached result {} belongs to tool call {:?}",
            path.display(),
            result.call_id
        )));
    }
    Ok(Some(result))
}

#[cfg(unix)]
fn same_file(opened: &std::fs::Metadata, current: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    opened.dev() == current.dev() && opened.ino() == current.ino()
}

async fn pause(variable: &str, call_id: &str) {
    let Ok(milliseconds) = std::env::var(variable) else {
        return;
    };
    let Ok(milliseconds) = milliseconds.parse::<u64>() else {
        return;
    };
    if milliseconds == 0 {
        return;
    }

    tracing::info!(%call_id, milliseconds, variable, "pausing tool execution");
    tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
}

fn tool_io_error(operation: &str, path: &Path, error: std::io::Error) -> Error {
    Error::Tool(format!("{operation} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn repeated_call_reuses_one_effect_file() {
        let directory = std::env::temp_dir().join(format!("zincir-tool-{}", Uuid::new_v4()));
        let executor = IdempotentFileExecutor {
            directory: directory.clone(),
        };
        let call = ToolCall {
            id: "call_1".into(),
            name: "write_file".into(),
            args: json!({ "content": "hello" }),
        };

        let first = executor.execute(call.clone()).await.unwrap();
        let second = executor
            .execute(ToolCall {
                args: json!({ "content": "changed after first execution" }),
                ..call
            })
            .await
            .unwrap();
        let persisted: ToolResult =
            serde_json::from_slice(&std::fs::read(directory.join("call_1.json")).unwrap()).unwrap();

        assert_eq!(first.content, second.content);
        assert_eq!(first.content, persisted.content);
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn concurrent_publication_keeps_one_winner() {
        let directory = std::env::temp_dir().join(format!("zincir-tool-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let final_path = directory.join("call_1.json");
        let first_path = directory.join("first.tmp");
        let second_path = directory.join("second.tmp");
        let first = ToolResult {
            call_id: "call_1".into(),
            content: json!({ "winner": 1 }),
        };
        let second = ToolResult {
            call_id: "call_1".into(),
            content: json!({ "winner": 2 }),
        };
        std::fs::write(&first_path, serde_json::to_vec(&first).unwrap()).unwrap();
        std::fs::write(&second_path, serde_json::to_vec(&second).unwrap()).unwrap();

        let (first_result, second_result) = tokio::join!(
            publish_result(&first_path, &final_path, first),
            publish_result(&second_path, &final_path, second),
        );
        let first_result = first_result.unwrap();
        let second_result = second_result.unwrap();

        assert_eq!(first_result.content, second_result.content);
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn rejects_call_ids_that_can_escape_the_output_directory() {
        let directory = std::env::temp_dir().join(format!("zincir-tool-{}", Uuid::new_v4()));
        let executor = IdempotentFileExecutor { directory };
        let call = ToolCall {
            id: "../escape".into(),
            name: "write_file".into(),
            args: json!({}),
        };

        let error = executor.execute(call).await.unwrap_err();

        assert!(matches!(error, Error::Tool(_)));
    }

    #[tokio::test]
    async fn rejects_cached_result_for_a_different_call_id() {
        let directory = std::env::temp_dir().join(format!("zincir-tool-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("call_1.json"),
            serde_json::to_vec(&ToolResult {
                call_id: "other_call".into(),
                content: json!({ "ok": true }),
            })
            .unwrap(),
        )
        .unwrap();

        let executor = IdempotentFileExecutor {
            directory: directory.clone(),
        };
        let error = executor
            .execute(ToolCall {
                id: "call_1".into(),
                name: "write_file".into(),
                args: json!({}),
            })
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Tool(_)));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cached_result_does_not_follow_a_symlink() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!("zincir-tool-{}", Uuid::new_v4()));
        let directory = root.join("results");
        let victim = root.join("victim.json");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            &victim,
            serde_json::to_vec(&ToolResult {
                call_id: "call_1".into(),
                content: json!({ "spoofed": true }),
            })
            .unwrap(),
        )
        .unwrap();
        symlink(&victim, directory.join("call_1.json")).unwrap();

        let executor = IdempotentFileExecutor { directory };
        let error = executor
            .execute(ToolCall {
                id: "call_1".into(),
                name: "write_file".into(),
                args: json!({ "content": "hello" }),
            })
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Tool(_)));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn temporary_result_does_not_follow_a_precreated_symlink() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!("zincir-tool-{}", Uuid::new_v4()));
        let directory = root.join("results");
        let victim = root.join("victim.txt");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(&victim, "do not overwrite").unwrap();
        symlink(&victim, directory.join(".call_1.tmp")).unwrap();

        let executor = IdempotentFileExecutor {
            directory: directory.clone(),
        };
        executor
            .execute(ToolCall {
                id: "call_1".into(),
                name: "write_file".into(),
                args: json!({ "content": "hello" }),
            })
            .await
            .unwrap();

        assert_eq!(std::fs::read_to_string(victim).unwrap(), "do not overwrite");
        std::fs::remove_dir_all(root).unwrap();
    }
}
