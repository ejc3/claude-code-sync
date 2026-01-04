use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// Represents a single line/entry in the JSONL conversation file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationEntry {
    /// The type of this entry (e.g., "user", "assistant", "file-history-snapshot")
    ///
    /// This field identifies what kind of entry this is in the conversation.
    /// Common types include user messages, assistant responses, and system events.
    #[serde(rename = "type")]
    pub entry_type: String,

    /// Unique identifier for this conversation entry
    ///
    /// Each entry may have its own UUID to uniquely identify it within the conversation.
    /// Not all entry types require a UUID, hence this is optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,

    /// UUID of the parent entry in the conversation thread
    ///
    /// This links entries together in a conversation tree, allowing for branching
    /// and threading of messages. If present, it references the UUID of the entry
    /// that this entry is responding to or following from.
    #[serde(rename = "parentUuid", skip_serializing_if = "Option::is_none")]
    pub parent_uuid: Option<String>,

    /// Session identifier grouping related conversation entries together
    ///
    /// All entries within a single conversation session share the same session ID.
    /// This is used to associate entries across multiple files or to reconstruct
    /// conversation context. If not present in the entry, the filename may be used.
    #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,

    /// ISO 8601 timestamp indicating when this entry was created
    ///
    /// Format is typically "YYYY-MM-DDTHH:MM:SS.sssZ" (e.g., "2025-01-01T00:00:00.000Z").
    /// Used for sorting entries chronologically and determining the latest activity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,

    /// The actual message content as a JSON value
    ///
    /// Contains the text and structured data of the user or assistant message.
    /// Stored as a generic JSON value to accommodate different message formats
    /// and structures without strict schema requirements.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<Value>,

    /// Current working directory at the time this entry was created
    ///
    /// Stores the filesystem path of the working directory, providing context
    /// about where the conversation or command was executed. Useful for
    /// reproducing environments and understanding file references.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,

    /// Version string of the Claude Code CLI that created this entry
    ///
    /// Records which version of the tool generated this conversation entry,
    /// helpful for debugging compatibility issues and tracking feature support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    /// Git branch name active when this entry was created
    ///
    /// Captures the current git branch context, allowing conversation entries
    /// to be associated with specific branches in version control. Useful for
    /// tracking which branch work was performed on.
    #[serde(rename = "gitBranch", skip_serializing_if = "Option::is_none")]
    pub git_branch: Option<String>,

    /// Catch-all field for additional JSON properties not explicitly defined
    ///
    /// Preserves any extra fields in the JSON that aren't part of the explicit schema.
    /// This allows forward compatibility - newer versions can add fields without breaking
    /// older parsers. The flattened serde attribute merges these fields at the same level
    /// as the named fields when serializing/deserializing.
    #[serde(flatten)]
    pub extra: Value,
}

/// Represents a complete conversation session
#[derive(Debug, Clone)]
pub struct ConversationSession {
    /// Unique identifier for this conversation session
    ///
    /// Either extracted from the first entry that contains a sessionId field,
    /// or derived from the filename (without extension) if no entries contain
    /// a session ID. Used to group related conversation entries together.
    pub session_id: String,

    /// All conversation entries in chronological order
    ///
    /// Contains the complete sequence of entries from the JSONL file, including
    /// user messages, assistant responses, and system events like file history
    /// snapshots. Preserves the original order from the file.
    pub entries: Vec<ConversationEntry>,

    /// Path to the JSONL file this session was loaded from
    ///
    /// Stores the filesystem path of the source file, used for tracking the
    /// origin of the conversation data and for potential file operations like
    /// rewriting or updating the session.
    pub file_path: String,
}

impl ConversationSession {
    /// Parse a JSONL file into a ConversationSession
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let file =
            File::open(path).with_context(|| format!("Failed to open file: {}", path.display()))?;

        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut session_id = None;

        for (line_num, line) in reader.lines().enumerate() {
            let line = line.with_context(|| {
                format!("Failed to read line {} in {}", line_num + 1, path.display())
            })?;

            if line.trim().is_empty() {
                continue;
            }

            let entry: ConversationEntry = serde_json::from_str(&line).with_context(|| {
                format!(
                    "Failed to parse JSON at line {} in {}",
                    line_num + 1,
                    path.display()
                )
            })?;

            // Extract session ID from first entry that has one
            if session_id.is_none() {
                if let Some(ref sid) = entry.session_id {
                    session_id = Some(sid.clone());
                }
            }

            entries.push(entry);
        }

        // If no session ID in entries, use filename (without extension) as session ID
        let session_id = session_id
            .or_else(|| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .with_context(|| {
                format!(
                    "No session ID found in file or filename: {}",
                    path.display()
                )
            })?;

        Ok(ConversationSession {
            session_id,
            entries,
            file_path: path.to_string_lossy().to_string(),
        })
    }

    /// Write the conversation session to a JSONL file
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        let mut file = File::create(path)
            .with_context(|| format!("Failed to create file: {}", path.display()))?;

        for entry in &self.entries {
            let json =
                serde_json::to_string(entry).context("Failed to serialize conversation entry")?;
            writeln!(file, "{json}")
                .with_context(|| format!("Failed to write to file: {}", path.display()))?;
        }

        Ok(())
    }

    /// Get the latest timestamp from the conversation
    pub fn latest_timestamp(&self) -> Option<String> {
        self.entries
            .iter()
            .filter_map(|e| e.timestamp.clone())
            .max()
    }

    /// Get the number of messages (user + assistant) in the conversation
    pub fn message_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.entry_type == "user" || e.entry_type == "assistant")
            .count()
    }

    /// Calculate a stable hash of the conversation content
    /// Uses xxhash for cross-platform stability (same result on ARM and x86)
    pub fn content_hash(&self) -> String {
        let mut combined = String::new();
        for entry in &self.entries {
            if let Ok(json) = serde_json::to_string(entry) {
                combined.push_str(&json);
                combined.push('\n');
            }
        }
        format!("{:016x}", xxhash_rust::xxh3::xxh3_64(combined.as_bytes()))
    }
}

/// Append entries to a JSONL file without rewriting existing content.
///
/// This is safe for concurrent access - existing entries are never modified.
/// Only new entries are appended to the end of the file. Data is flushed to
/// disk before returning to ensure durability.
///
/// # Arguments
/// * `path` - Path to the JSONL file
/// * `entries` - Entries to append
///
/// # Safety
/// - Existing file content is never modified
/// - Uses `sync_all()` to ensure data reaches disk before returning
/// - Partial writes during a crash are possible but won't corrupt existing data
pub fn append_entries_to_file<P: AsRef<Path>>(path: P, entries: &[ConversationEntry]) -> Result<()> {
    let path = path.as_ref();

    // Create parent directories if they don't exist
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open file for appending: {}", path.display()))?;

    for entry in entries {
        let json = serde_json::to_string(entry).context("Failed to serialize conversation entry")?;
        writeln!(file, "{json}")
            .with_context(|| format!("Failed to append to file: {}", path.display()))?;
    }

    // Ensure data is flushed to disk for durability
    file.sync_all()
        .with_context(|| format!("Failed to sync file to disk: {}", path.display()))?;

    Ok(())
}

/// Generate a deduplication key for entries without UUIDs.
///
/// For entries like `file-history-snapshot` that don't have UUIDs, we use
/// a combination of (type, timestamp, content_hash) for deduplication.
///
/// Uses xxhash for cross-platform stability (same result on ARM and x86).
pub fn make_content_key(entry: &ConversationEntry) -> String {
    let ts = entry.timestamp.as_deref().unwrap_or("");
    let content_hash = entry
        .message
        .as_ref()
        .map(|m| {
            let json = serde_json::to_string(m).unwrap_or_default();
            xxhash_rust::xxh3::xxh3_64(json.as_bytes())
        })
        .unwrap_or(0);
    format!("{}:{}:{:016x}", entry.entry_type, ts, content_hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_conversation_entry() {
        let json =
            r#"{"type":"user","uuid":"123","sessionId":"abc","timestamp":"2025-01-01T00:00:00Z"}"#;
        let entry: ConversationEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.entry_type, "user");
        assert_eq!(entry.uuid.unwrap(), "123");
    }

    #[test]
    fn test_read_write_session() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{"type":"user","sessionId":"test-123","uuid":"1","timestamp":"2025-01-01T00:00:00Z"}}"#).unwrap();
        writeln!(temp_file, r#"{{"type":"assistant","sessionId":"test-123","uuid":"2","timestamp":"2025-01-01T00:01:00Z"}}"#).unwrap();

        let session = ConversationSession::from_file(temp_file.path()).unwrap();
        assert_eq!(session.session_id, "test-123");
        assert_eq!(session.entries.len(), 2);
        assert_eq!(session.message_count(), 2);

        // Test write
        let output_temp = NamedTempFile::new().unwrap();
        session.write_to_file(output_temp.path()).unwrap();

        let reloaded = ConversationSession::from_file(output_temp.path()).unwrap();
        assert_eq!(reloaded.session_id, session.session_id);
        assert_eq!(reloaded.entries.len(), session.entries.len());
    }

    #[test]
    fn test_session_id_from_filename() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let session_file = temp_dir
            .path()
            .join("248a0cdf-1466-48a7-b3d0-00f9e8e6e4ee.jsonl");

        // Create file with entries that don't have sessionId field
        let mut file = File::create(&session_file).unwrap();
        writeln!(file, r#"{{"type":"file-history-snapshot","messageId":"abc","timestamp":"2025-01-01T00:00:00Z"}}"#).unwrap();
        writeln!(file, r#"{{"type":"file-history-snapshot","messageId":"def","timestamp":"2025-01-01T00:01:00Z"}}"#).unwrap();

        // Parse should succeed using filename as session ID
        let session = ConversationSession::from_file(&session_file).unwrap();
        assert_eq!(session.session_id, "248a0cdf-1466-48a7-b3d0-00f9e8e6e4ee");
        assert_eq!(session.entries.len(), 2);
    }

    #[test]
    fn test_session_id_from_entry_preferred() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let session_file = temp_dir.path().join("filename-uuid.jsonl");

        // Create file with sessionId in entries
        let mut file = File::create(&session_file).unwrap();
        writeln!(file, r#"{{"type":"user","sessionId":"entry-uuid","uuid":"1","timestamp":"2025-01-01T00:00:00Z"}}"#).unwrap();

        // Should prefer sessionId from entry over filename
        let session = ConversationSession::from_file(&session_file).unwrap();
        assert_eq!(session.session_id, "entry-uuid");
    }

    #[test]
    fn test_mixed_entries_with_and_without_session_id() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let session_file = temp_dir.path().join("test-session.jsonl");

        // Create file with mix of entries
        let mut file = File::create(&session_file).unwrap();
        writeln!(file, r#"{{"type":"file-history-snapshot","messageId":"abc","timestamp":"2025-01-01T00:00:00Z"}}"#).unwrap();
        writeln!(file, r#"{{"type":"user","sessionId":"test-123","uuid":"1","timestamp":"2025-01-01T00:01:00Z"}}"#).unwrap();

        // Should use sessionId from the entry that has it
        let session = ConversationSession::from_file(&session_file).unwrap();
        assert_eq!(session.session_id, "test-123");
        assert_eq!(session.entries.len(), 2);
    }

    // =========================================================================
    // Tests for append_entries_to_file
    // =========================================================================

    fn create_test_entry(uuid: &str, entry_type: &str, timestamp: &str) -> ConversationEntry {
        ConversationEntry {
            entry_type: entry_type.to_string(),
            uuid: Some(uuid.to_string()),
            parent_uuid: None,
            session_id: Some("test-session".to_string()),
            timestamp: Some(timestamp.to_string()),
            message: Some(serde_json::json!({"text": format!("Message {}", uuid)})),
            cwd: None,
            version: None,
            git_branch: None,
            extra: serde_json::Value::Null,
        }
    }

    #[test]
    fn test_append_entries_creates_new_file() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("new_session.jsonl");

        // File doesn't exist yet
        assert!(!file_path.exists());

        let entries = vec![
            create_test_entry("1", "user", "2025-01-01T00:00:00Z"),
            create_test_entry("2", "assistant", "2025-01-01T00:01:00Z"),
        ];

        append_entries_to_file(&file_path, &entries).unwrap();

        // File should now exist with 2 entries
        assert!(file_path.exists());
        let session = ConversationSession::from_file(&file_path).unwrap();
        assert_eq!(session.entries.len(), 2);
        assert_eq!(session.entries[0].uuid, Some("1".to_string()));
        assert_eq!(session.entries[1].uuid, Some("2".to_string()));
    }

    #[test]
    fn test_append_entries_preserves_existing() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("existing_session.jsonl");

        // Create initial file with 2 entries
        let initial_entries = vec![
            create_test_entry("1", "user", "2025-01-01T00:00:00Z"),
            create_test_entry("2", "assistant", "2025-01-01T00:01:00Z"),
        ];
        let initial_session = ConversationSession {
            session_id: "test-session".to_string(),
            entries: initial_entries,
            file_path: file_path.to_string_lossy().to_string(),
        };
        initial_session.write_to_file(&file_path).unwrap();

        // Read original content for comparison
        let original_content = fs::read_to_string(&file_path).unwrap();
        let original_lines: Vec<&str> = original_content.lines().collect();
        assert_eq!(original_lines.len(), 2);

        // Append 2 more entries
        let new_entries = vec![
            create_test_entry("3", "user", "2025-01-01T00:02:00Z"),
            create_test_entry("4", "assistant", "2025-01-01T00:03:00Z"),
        ];
        append_entries_to_file(&file_path, &new_entries).unwrap();

        // Read new content
        let new_content = fs::read_to_string(&file_path).unwrap();
        let new_lines: Vec<&str> = new_content.lines().collect();

        // Should have 4 entries total
        assert_eq!(new_lines.len(), 4);

        // First 2 lines should be EXACTLY the same as before (byte-for-byte)
        assert_eq!(new_lines[0], original_lines[0], "First entry was modified!");
        assert_eq!(new_lines[1], original_lines[1], "Second entry was modified!");

        // Verify all entries via parsing
        let session = ConversationSession::from_file(&file_path).unwrap();
        assert_eq!(session.entries.len(), 4);
        assert_eq!(session.entries[0].uuid, Some("1".to_string()));
        assert_eq!(session.entries[1].uuid, Some("2".to_string()));
        assert_eq!(session.entries[2].uuid, Some("3".to_string()));
        assert_eq!(session.entries[3].uuid, Some("4".to_string()));
    }

    // =========================================================================
    // Tests for make_content_key
    // =========================================================================

    #[test]
    fn test_make_content_key_uniqueness() {
        // Same type and timestamp but different message should produce different keys
        let entry1 = ConversationEntry {
            entry_type: "file-history-snapshot".to_string(),
            uuid: None,
            parent_uuid: None,
            session_id: None,
            timestamp: Some("2025-01-01T00:00:00Z".to_string()),
            message: Some(serde_json::json!({"file": "a.txt", "content": "hello"})),
            cwd: None,
            version: None,
            git_branch: None,
            extra: serde_json::Value::Null,
        };

        let entry2 = ConversationEntry {
            entry_type: "file-history-snapshot".to_string(),
            uuid: None,
            parent_uuid: None,
            session_id: None,
            timestamp: Some("2025-01-01T00:00:00Z".to_string()),
            message: Some(serde_json::json!({"file": "b.txt", "content": "world"})),
            cwd: None,
            version: None,
            git_branch: None,
            extra: serde_json::Value::Null,
        };

        let key1 = make_content_key(&entry1);
        let key2 = make_content_key(&entry2);

        assert_ne!(key1, key2, "Different messages should produce different keys");
    }

    #[test]
    fn test_make_content_key_same_content_same_key() {
        // Identical entries should produce identical keys
        let entry1 = ConversationEntry {
            entry_type: "file-history-snapshot".to_string(),
            uuid: None,
            parent_uuid: None,
            session_id: None,
            timestamp: Some("2025-01-01T00:00:00Z".to_string()),
            message: Some(serde_json::json!({"file": "test.txt"})),
            cwd: None,
            version: None,
            git_branch: None,
            extra: serde_json::Value::Null,
        };

        let entry2 = ConversationEntry {
            entry_type: "file-history-snapshot".to_string(),
            uuid: None,
            parent_uuid: None,
            session_id: None,
            timestamp: Some("2025-01-01T00:00:00Z".to_string()),
            message: Some(serde_json::json!({"file": "test.txt"})),
            cwd: None,
            version: None,
            git_branch: None,
            extra: serde_json::Value::Null,
        };

        assert_eq!(make_content_key(&entry1), make_content_key(&entry2));
    }
}
