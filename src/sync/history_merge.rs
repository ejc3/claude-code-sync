//! History.jsonl merge utilities
//!
//! Provides functions to merge history.jsonl files from different sources,
//! deduplicating entries by (sessionId, timestamp) tuple.

use anyhow::Result;
use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// Represents a parsed history.jsonl entry with its deduplication key
#[derive(Debug, Clone)]
struct HistoryEntry {
    /// The raw JSON line
    line: String,
    /// Session ID (required for valid entries)
    session_id: String,
    /// Timestamp in milliseconds (required for valid entries)
    timestamp: i64,
    /// Display text (for logging/debugging)
    display: String,
}

impl HistoryEntry {
    /// Parse a JSON line into a HistoryEntry
    /// Returns None if the entry is invalid (missing sessionId or timestamp)
    fn parse(line: &str) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_str(line).ok()?;

        let session_id = value.get("sessionId").and_then(|v| v.as_str())?;
        let timestamp = value.get("timestamp").and_then(|v| v.as_i64())?;
        let display = value
            .get("display")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Reject entries with missing required fields
        if session_id.is_empty() {
            log::warn!("Skipping history entry with empty sessionId");
            return None;
        }
        if timestamp == 0 {
            log::warn!("Skipping history entry with zero timestamp for session {}", session_id);
            return None;
        }

        Some(Self {
            line: line.to_string(),
            session_id: session_id.to_string(),
            timestamp,
            display,
        })
    }

    /// Get the deduplication key for this entry
    fn dedup_key(&self) -> (String, i64) {
        (self.session_id.clone(), self.timestamp)
    }
}

/// Priority for merge operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergePriority {
    /// Source entries take priority (used when pushing local to sync repo)
    SourceFirst,
    /// Target entries take priority (used when pulling to local)
    TargetFirst,
}

/// Merge two history.jsonl files, deduplicating by (sessionId, timestamp)
///
/// # Arguments
/// * `source_path` - Path to the source history.jsonl file
/// * `target_path` - Path to the target history.jsonl file (will be overwritten)
/// * `priority` - Which file's entries take priority when both exist
///
/// # Returns
/// A tuple of (total_entries, entries_added_from_source)
pub fn merge_history_files(
    source_path: &Path,
    target_path: &Path,
    priority: MergePriority,
) -> Result<(usize, usize)> {
    let mut seen: HashSet<(String, i64)> = HashSet::new();
    let mut entries: Vec<HistoryEntry> = Vec::new();

    // Determine read order based on priority
    // The first file read has priority (its entries are kept when there's a conflict)
    let (first_path, second_path) = match priority {
        MergePriority::TargetFirst => (target_path, source_path),
        MergePriority::SourceFirst => (source_path, target_path),
    };

    // Read first file (priority)
    let mut first_count = 0;
    if first_path.exists() {
        let file = fs::File::open(first_path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Some(entry) = HistoryEntry::parse(&line) {
                let key = entry.dedup_key();
                if !seen.contains(&key) {
                    seen.insert(key);
                    entries.push(entry);
                    first_count += 1;
                }
            } else {
                log::debug!("Skipping invalid history entry: {}", &line[..line.len().min(100)]);
            }
        }
    }

    // Read second file (add entries not in first)
    let mut second_added = 0;
    if second_path.exists() {
        let file = fs::File::open(second_path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Some(entry) = HistoryEntry::parse(&line) {
                let key = entry.dedup_key();
                if !seen.contains(&key) {
                    seen.insert(key);
                    entries.push(entry);
                    second_added += 1;
                }
            }
        }
    }

    // Sort by timestamp (entries already have parsed timestamps - no re-parsing needed)
    entries.sort_by_key(|e| e.timestamp);

    // Write merged result
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(target_path)?;
    for entry in &entries {
        writeln!(file, "{}", entry.line)?;
    }

    let total = entries.len();
    let added_from_source = match priority {
        MergePriority::SourceFirst => first_count,
        MergePriority::TargetFirst => second_added,
    };

    log::info!(
        "Merged history.jsonl: {} total entries, {} from source",
        total,
        added_from_source
    );

    Ok((total, added_from_source))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_history_file(path: &Path, entries: &[&str]) {
        let mut file = fs::File::create(path).unwrap();
        for entry in entries {
            writeln!(file, "{}", entry).unwrap();
        }
    }

    #[test]
    fn test_parse_valid_entry() {
        let line = r#"{"sessionId":"abc-123","timestamp":1234567890,"display":"test"}"#;
        let entry = HistoryEntry::parse(line).unwrap();
        assert_eq!(entry.session_id, "abc-123");
        assert_eq!(entry.timestamp, 1234567890);
        assert_eq!(entry.display, "test");
    }

    #[test]
    fn test_parse_missing_session_id() {
        let line = r#"{"timestamp":1234567890,"display":"test"}"#;
        assert!(HistoryEntry::parse(line).is_none());
    }

    #[test]
    fn test_parse_zero_timestamp() {
        let line = r#"{"sessionId":"abc","timestamp":0,"display":"test"}"#;
        assert!(HistoryEntry::parse(line).is_none());
    }

    #[test]
    fn test_merge_deduplication() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.jsonl");
        let target = dir.path().join("target.jsonl");

        write_history_file(&source, &[
            r#"{"sessionId":"a","timestamp":1000,"display":"source1"}"#,
            r#"{"sessionId":"a","timestamp":2000,"display":"source2"}"#,
        ]);
        write_history_file(&target, &[
            r#"{"sessionId":"a","timestamp":1000,"display":"target1"}"#,
            r#"{"sessionId":"b","timestamp":3000,"display":"target3"}"#,
        ]);

        // Target first - target's version of duplicate should win
        let (total, added) = merge_history_files(&source, &target, MergePriority::TargetFirst).unwrap();
        assert_eq!(total, 3); // a@1000, a@2000, b@3000
        assert_eq!(added, 1); // Only a@2000 added from source

        // Read back and verify
        let content = fs::read_to_string(&target).unwrap();
        assert!(content.contains("target1")); // Target's version kept
        assert!(content.contains("source2")); // Unique from source
        assert!(content.contains("target3")); // Unique from target
    }

    #[test]
    fn test_merge_sorted_by_timestamp() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.jsonl");
        let target = dir.path().join("target.jsonl");

        write_history_file(&source, &[
            r#"{"sessionId":"a","timestamp":3000,"display":"third"}"#,
            r#"{"sessionId":"a","timestamp":1000,"display":"first"}"#,
        ]);
        write_history_file(&target, &[
            r#"{"sessionId":"a","timestamp":2000,"display":"second"}"#,
        ]);

        merge_history_files(&source, &target, MergePriority::TargetFirst).unwrap();

        let content = fs::read_to_string(&target).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("first"));
        assert!(lines[1].contains("second"));
        assert!(lines[2].contains("third"));
    }
}
