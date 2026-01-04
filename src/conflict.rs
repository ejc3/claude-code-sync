use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::merge;
use crate::parser::ConversationSession;

/// Represents a conflict between local and remote versions of the same conversation session.
///
/// A `Conflict` is detected when both local and remote filesystems contain a conversation
/// with the same session ID but different content (as determined by content hashes). This
/// typically occurs when the same conversation has been modified on different machines or
/// when changes haven't been synchronized properly.
///
/// The conflict contains metadata about both versions to help users make informed decisions
/// about how to resolve the discrepancy.
///
/// # Examples
///
/// ```ignore
/// use claude_code_sync::conflict::Conflict;
/// use claude_code_sync::parser::ConversationSession;
///
/// let conflict = Conflict::new(local, remote);
///
/// if conflict.is_real_conflict() {
///     println!("{}", conflict.description());
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    /// The unique identifier for the conversation session that has conflicted.
    ///
    /// This ID is shared between both the local and remote versions, as they represent
    /// different states of the same conversation.
    pub session_id: String,

    /// The file path to the local version of the conversation.
    ///
    /// This points to the conversation file in the local filesystem, typically in the
    /// user's local conversation storage directory.
    pub local_file: PathBuf,

    /// The file path to the remote version of the conversation.
    ///
    /// This points to the conversation file from the remote source (e.g., synced from
    /// another machine or cloud storage).
    pub remote_file: PathBuf,

    /// The timestamp of the most recent message in the local version.
    ///
    /// This is `None` if the local conversation has no messages with timestamps.
    /// The timestamp helps users understand which version is more recent.
    pub local_timestamp: Option<String>,

    /// The timestamp of the most recent message in the remote version.
    ///
    /// This is `None` if the remote conversation has no messages with timestamps.
    /// The timestamp helps users understand which version is more recent.
    pub remote_timestamp: Option<String>,

    /// The total number of messages in the local version of the conversation.
    ///
    /// This count includes all conversation entries (user messages, assistant responses, etc.)
    /// and helps users compare the relative completeness of each version.
    pub local_message_count: usize,

    /// The total number of messages in the remote version of the conversation.
    ///
    /// This count includes all conversation entries (user messages, assistant responses, etc.)
    /// and helps users compare the relative completeness of each version.
    pub remote_message_count: usize,

    /// A hash of the local conversation's content.
    ///
    /// This hash is used to detect whether the local and remote versions are truly different.
    /// If the hashes match, the conversations are identical despite any metadata differences.
    pub local_hash: String,

    /// A hash of the remote conversation's content.
    ///
    /// This hash is used to detect whether the local and remote versions are truly different.
    /// If the hashes match, the conversations are identical despite any metadata differences.
    pub remote_hash: String,

    /// The current resolution status of the conflict.
    ///
    /// Initially set to `ConflictResolution::Pending` when a conflict is detected.
    /// Updated to one of the other variants once the user or system decides how to
    /// resolve the conflict.
    pub resolution: ConflictResolution,
}

/// Represents the resolution strategy for a conversation conflict.
///
/// When a conflict is detected between local and remote versions of the same conversation,
/// the user or system must choose how to resolve it. This enum captures the different
/// resolution strategies available.
///
/// # Resolution Strategies
///
/// - **SmartMerge**: Intelligently combines both versions by merging non-conflicting changes
/// - **KeepBoth**: Preserves both versions by renaming the remote file to avoid overwriting
/// - **KeepLocal**: Discards the remote version and keeps only the local version
/// - **KeepRemote**: Discards the local version and keeps only the remote version
/// - **Pending**: No resolution has been chosen yet (default state for new conflicts)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Intelligently merge both versions into a single conversation.
    ///
    /// This strategy attempts to combine messages from both local and remote versions
    /// by analyzing message UUIDs, parent relationships, and timestamps. It can handle:
    /// - Non-overlapping messages (simple merge)
    /// - Edited messages (resolved by timestamp)
    /// - Conversation branches (all branches preserved)
    /// - Entries without UUIDs (merged by timestamp)
    ///
    /// If smart merge fails (e.g., due to corrupted data or circular references),
    /// the system will fall back to another resolution strategy.
    ///
    /// # Fields
    ///
    /// * `merged_entries` - The result of merging both conversations
    /// * `stats` - Statistics about the merge operation
    SmartMerge {
        /// The merged conversation entries
        merged_entries: Vec<crate::parser::ConversationEntry>,
        /// Statistics about the merge operation
        stats: merge::MergeStats,
    },

    /// Keep both versions by renaming the remote file with a conflict suffix.
    ///
    /// This strategy preserves both the local and remote versions of the conversation.
    /// The local file retains its original name and location, while the remote file
    /// is renamed to include a conflict marker (typically a timestamp-based suffix)
    /// to prevent overwriting the local version.
    ///
    /// # Fields
    ///
    /// * `renamed_remote_file` - The new path where the remote file will be saved.
    ///   This path includes a conflict suffix to distinguish it from the local version.
    ///
    /// # Example
    ///
    /// If the local file is `conversation.jsonl` and a conflict is detected,
    /// the remote version might be saved as `conversation-conflict-20250122-143000.jsonl`.
    KeepBoth {
        /// The destination path for the renamed remote file, including the conflict suffix.
        renamed_remote_file: PathBuf,
    },

    /// Keep only the local version and discard the remote version.
    ///
    /// This strategy assumes the local version is correct and the remote version
    /// should be ignored. The local file remains unchanged, and the remote version
    /// is not saved to disk.
    KeepLocal,

    /// Keep only the remote version and discard the local version.
    ///
    /// This strategy assumes the remote version is correct and should replace the
    /// local version. The local file will be overwritten with the remote content.
    KeepRemote,

    /// The conflict has not yet been resolved.
    ///
    /// This is the default state for newly detected conflicts. The user must choose
    /// one of the other resolution strategies before the conflict can be resolved.
    Pending,
}

impl Conflict {
    /// Creates a new `Conflict` by comparing local and remote conversation sessions.
    ///
    /// This function constructs a conflict record from two versions of the same conversation
    /// session (identified by matching session IDs). It extracts and stores relevant metadata
    /// from both versions, including timestamps, message counts, and content hashes.
    ///
    /// The conflict is initialized with a `Pending` resolution status, indicating that no
    /// resolution strategy has been chosen yet.
    ///
    /// # Arguments
    ///
    /// * `local` - A reference to the local version of the conversation session
    /// * `remote` - A reference to the remote version of the conversation session
    ///
    /// # Returns
    ///
    /// A new `Conflict` instance containing metadata from both conversation versions,
    /// with the resolution status set to `Pending`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use claude_code_sync::conflict::Conflict;
    /// use claude_code_sync::parser::ConversationSession;
    ///
    /// let conflict = Conflict::new(local_session, remote_session);
    ///
    /// println!("Conflict in session: {}", conflict.session_id);
    /// println!("Local messages: {}", conflict.local_message_count);
    /// println!("Remote messages: {}", conflict.remote_message_count);
    /// ```
    pub fn new(local: &ConversationSession, remote: &ConversationSession) -> Self {
        Conflict {
            session_id: local.session_id.clone(),
            local_file: PathBuf::from(&local.file_path),
            remote_file: PathBuf::from(&remote.file_path),
            local_timestamp: local.latest_timestamp(),
            remote_timestamp: remote.latest_timestamp(),
            local_message_count: local.message_count(),
            remote_message_count: remote.message_count(),
            local_hash: local.content_hash(),
            remote_hash: remote.content_hash(),
            resolution: ConflictResolution::Pending,
        }
    }

    /// Attempts to resolve the conflict using smart merge
    ///
    /// This method tries to intelligently combine local and remote versions
    /// by analyzing message UUIDs, timestamps, and parent relationships.
    ///
    /// # Arguments
    ///
    /// * `local_session` - The local conversation session
    /// * `remote_session` - The remote conversation session
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the smart merge succeeds, or an error if it fails.
    /// On success, the conflict resolution is set to `SmartMerge` with the merged entries.
    pub fn try_smart_merge(
        &mut self,
        local_session: &ConversationSession,
        remote_session: &ConversationSession,
    ) -> Result<()> {
        let merge_result = merge::merge_conversations(local_session, remote_session)?;

        self.resolution = ConflictResolution::SmartMerge {
            merged_entries: merge_result.merged_entries,
            stats: merge_result.stats,
        };

        Ok(())
    }

    /// Resolve the conflict by keeping both versions
    pub fn resolve_keep_both(&mut self, conflict_suffix: &str) -> Result<PathBuf> {
        let remote_file_name = self
            .remote_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        let remote_file_ext = self
            .remote_file
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("jsonl");

        let parent = self.remote_file.parent().unwrap_or_else(|| Path::new("."));

        let new_name = format!(
            "{remote_file_name}-{conflict_suffix}.{remote_file_ext}"
        );
        let renamed_path = parent.join(new_name);

        self.resolution = ConflictResolution::KeepBoth {
            renamed_remote_file: renamed_path.clone(),
        };

        Ok(renamed_path)
    }

    /// Get a human-readable description of the conflict
    pub fn description(&self) -> String {
        format!(
            "Session {} has diverged:\n  Local: {} messages, last update: {}\n  Remote: {} messages, last update: {}",
            self.session_id,
            self.local_message_count,
            self.local_timestamp.as_deref().unwrap_or("unknown"),
            self.remote_message_count,
            self.remote_timestamp.as_deref().unwrap_or("unknown")
        )
    }

    /// Determine if this is a real conflict (different content)
    pub fn is_real_conflict(&self) -> bool {
        self.local_hash != self.remote_hash
    }
}

/// Conflict detector for conversation sessions
pub struct ConflictDetector {
    conflicts: Vec<Conflict>,
}

impl ConflictDetector {
    /// Creates a new `ConflictDetector` with an empty conflict list.
    ///
    /// The conflict detector is used to identify and manage conflicts between local and remote
    /// conversation sessions. It maintains a list of detected conflicts and provides methods
    /// to resolve them according to different strategies.
    ///
    /// The detector starts with no conflicts; conflicts are added by calling the [`detect`]
    /// method with local and remote conversation sessions.
    ///
    /// [`detect`]: ConflictDetector::detect
    ///
    /// # Returns
    ///
    /// A new `ConflictDetector` instance with an empty internal conflict list, ready to
    /// detect and manage conflicts between conversation sessions.
    ///
    /// # Examples
    ///
    /// ```
    /// # use claude_code_sync::conflict::ConflictDetector;
    /// # use claude_code_sync::parser::ConversationSession;
    /// # fn example(local_sessions: Vec<ConversationSession>, remote_sessions: Vec<ConversationSession>) {
    /// let mut detector = ConflictDetector::new();
    ///
    /// // Detect conflicts between local and remote sessions
    /// detector.detect(&local_sessions, &remote_sessions);
    ///
    /// if detector.has_conflicts() {
    ///     println!("Found {} conflicts", detector.conflict_count());
    /// }
    /// # }
    /// ```
    ///
    /// # See Also
    ///
    /// * [`detect`] - Method to scan for conflicts between local and remote sessions
    /// * [`has_conflicts`] - Check if any conflicts have been detected
    /// * [`conflict_count`] - Get the number of detected conflicts
    ///
    /// [`detect`]: ConflictDetector::detect
    /// [`has_conflicts`]: ConflictDetector::has_conflicts
    /// [`conflict_count`]: ConflictDetector::conflict_count
    pub fn new() -> Self {
        ConflictDetector {
            conflicts: Vec::new(),
        }
    }

    /// Compare local and remote sessions and detect conflicts
    ///
    /// Only reports TRUE conflicts where both sides have diverged.
    /// Simple extensions (one side has more messages) are NOT conflicts.
    pub fn detect(
        &mut self,
        local_sessions: &[ConversationSession],
        remote_sessions: &[ConversationSession],
    ) {
        // Build a map of session_id -> local session
        let local_map: std::collections::HashMap<_, _> = local_sessions
            .iter()
            .map(|s| (s.session_id.clone(), s))
            .collect();

        // Check each remote session against local
        for remote in remote_sessions {
            if let Some(local) = local_map.get(&remote.session_id) {
                // Session exists in both - analyze relationship
                let relationship = analyze_session_relationship(local, remote);

                match relationship {
                    SessionRelationship::Identical => {
                        // No action needed - sessions are the same
                    }
                    SessionRelationship::LocalIsPrefix => {
                        // Remote has more messages - NOT a conflict
                        // This will be handled as a normal "Modified" copy in pull
                        log::debug!(
                            "Session {} is extended in remote ({} -> {} entries)",
                            local.session_id,
                            local.entries.len(),
                            remote.entries.len()
                        );
                    }
                    SessionRelationship::RemoteIsPrefix => {
                        // Local has more messages - NOT a conflict
                        // Keep local, no action needed during pull
                        log::debug!(
                            "Session {} is extended locally ({} -> {} entries), keeping local",
                            local.session_id,
                            remote.entries.len(),
                            local.entries.len()
                        );
                    }
                    SessionRelationship::Diverged => {
                        // TRUE conflict - both have unique entries
                        let conflict = Conflict::new(local, remote);
                        self.conflicts.push(conflict);
                        log::info!(
                            "True conflict detected in session {} (local: {}, remote: {} entries)",
                            local.session_id,
                            local.entries.len(),
                            remote.entries.len()
                        );
                    }
                }
            }
        }
    }

    /// Resolve all conflicts using the "keep both" strategy
    #[allow(dead_code)]
    pub fn resolve_all_keep_both(&mut self) -> Result<Vec<(PathBuf, PathBuf)>> {
        let mut renames = Vec::new();

        for conflict in &mut self.conflicts {
            let timestamp = chrono::Utc::now().format("%Y%m%d-%H%M%S");
            let conflict_suffix = format!("conflict-{timestamp}");

            let renamed_path = conflict.resolve_keep_both(&conflict_suffix)?;
            renames.push((conflict.remote_file.clone(), renamed_path));
        }

        Ok(renames)
    }

    /// Get all detected conflicts
    pub fn conflicts(&self) -> &[Conflict] {
        &self.conflicts
    }

    /// Get mutable reference to all detected conflicts
    pub fn conflicts_mut(&mut self) -> &mut [Conflict] {
        &mut self.conflicts
    }

    /// Check if any conflicts were detected
    pub fn has_conflicts(&self) -> bool {
        !self.conflicts.is_empty()
    }

    /// Get count of conflicts
    pub fn conflict_count(&self) -> usize {
        self.conflicts.len()
    }
}

impl Default for ConflictDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Relationship between two sessions with the same ID
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionRelationship {
    /// Sessions are identical (same content hash)
    Identical,
    /// Local is a prefix of remote (remote has more messages, all local messages exist in remote)
    LocalIsPrefix,
    /// Remote is a prefix of local (local has more messages, all remote messages exist in local)
    RemoteIsPrefix,
    /// True divergence - both have unique messages not in the other (actual conflict)
    Diverged,
}

/// Analyzes the relationship between two sessions to determine if they truly conflict
/// or if one is simply an extension of the other.
///
/// This is crucial for avoiding false "conflicts" when:
/// - Machine A has session with 40 messages
/// - Machine B has the same session with 50 messages (continued the conversation)
/// - This is NOT a conflict - Machine B just has more messages
///
/// True conflicts only occur when BOTH sides have added different messages.
pub fn analyze_session_relationship(
    local: &ConversationSession,
    remote: &ConversationSession,
) -> SessionRelationship {
    // Fast path: identical hashes
    if local.content_hash() == remote.content_hash() {
        return SessionRelationship::Identical;
    }

    // Build sets of UUIDs from each session
    let local_uuids: HashSet<String> = local
        .entries
        .iter()
        .filter_map(|e| e.uuid.clone())
        .collect();

    let remote_uuids: HashSet<String> = remote
        .entries
        .iter()
        .filter_map(|e| e.uuid.clone())
        .collect();

    // Check for entries unique to each side
    let local_only: HashSet<_> = local_uuids.difference(&remote_uuids).collect();
    let remote_only: HashSet<_> = remote_uuids.difference(&local_uuids).collect();

    // If local has no unique entries, local is a prefix of remote
    if local_only.is_empty() && !remote_only.is_empty() {
        // Verify common entries are identical
        if verify_common_entries_identical(local, remote) {
            return SessionRelationship::LocalIsPrefix;
        }
    }

    // If remote has no unique entries, remote is a prefix of local
    if remote_only.is_empty() && !local_only.is_empty() {
        // Verify common entries are identical
        if verify_common_entries_identical(local, remote) {
            return SessionRelationship::RemoteIsPrefix;
        }
    }

    // Both have unique entries - true divergence
    SessionRelationship::Diverged
}

/// Verifies that entries with the same UUID have identical content
fn verify_common_entries_identical(
    local: &ConversationSession,
    remote: &ConversationSession,
) -> bool {
    use std::collections::HashMap;

    // Build map of UUID -> serialized entry for local
    let local_map: HashMap<String, String> = local
        .entries
        .iter()
        .filter_map(|e| {
            e.uuid.as_ref().and_then(|uuid| {
                serde_json::to_string(e).ok().map(|json| (uuid.clone(), json))
            })
        })
        .collect();

    // Check each remote entry with a UUID
    for entry in &remote.entries {
        if let Some(uuid) = &entry.uuid {
            if let Some(local_json) = local_map.get(uuid) {
                // This UUID exists in both - check if content is identical
                if let Ok(remote_json) = serde_json::to_string(entry) {
                    if &remote_json != local_json {
                        // Same UUID but different content - entries were modified
                        return false;
                    }
                }
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ConversationEntry;

    fn create_test_session(session_id: &str, message_count: usize) -> ConversationSession {
        let mut entries = Vec::new();

        for i in 0..message_count {
            entries.push(ConversationEntry {
                entry_type: if i % 2 == 0 { "user" } else { "assistant" }.to_string(),
                uuid: Some(format!("uuid-{i}")),
                parent_uuid: if i > 0 {
                    Some(format!("uuid-{}", i - 1))
                } else {
                    None
                },
                session_id: Some(session_id.to_string()),
                timestamp: Some(format!("2025-01-01T{i:02}:00:00Z")),
                message: None,
                cwd: None,
                version: None,
                git_branch: None,
                extra: serde_json::Value::Null,
            });
        }

        ConversationSession {
            session_id: session_id.to_string(),
            entries,
            file_path: format!("/test/{session_id}.jsonl"),
        }
    }

    /// Creates a diverged session where both sides have unique messages
    fn create_diverged_sessions(session_id: &str) -> (ConversationSession, ConversationSession) {
        // Common base: messages 0-4
        let mut local_entries = Vec::new();
        let mut remote_entries = Vec::new();

        for i in 0..5 {
            let entry = ConversationEntry {
                entry_type: if i % 2 == 0 { "user" } else { "assistant" }.to_string(),
                uuid: Some(format!("uuid-{i}")),
                parent_uuid: if i > 0 {
                    Some(format!("uuid-{}", i - 1))
                } else {
                    None
                },
                session_id: Some(session_id.to_string()),
                timestamp: Some(format!("2025-01-01T{i:02}:00:00Z")),
                message: None,
                cwd: None,
                version: None,
                git_branch: None,
                extra: serde_json::Value::Null,
            };
            local_entries.push(entry.clone());
            remote_entries.push(entry);
        }

        // Local adds message 5-local (unique to local)
        local_entries.push(ConversationEntry {
            entry_type: "user".to_string(),
            uuid: Some("uuid-5-local".to_string()),
            parent_uuid: Some("uuid-4".to_string()),
            session_id: Some(session_id.to_string()),
            timestamp: Some("2025-01-01T05:00:00Z".to_string()),
            message: None,
            cwd: None,
            version: None,
            git_branch: None,
            extra: serde_json::Value::Null,
        });

        // Remote adds message 5-remote (unique to remote)
        remote_entries.push(ConversationEntry {
            entry_type: "user".to_string(),
            uuid: Some("uuid-5-remote".to_string()),
            parent_uuid: Some("uuid-4".to_string()),
            session_id: Some(session_id.to_string()),
            timestamp: Some("2025-01-01T05:30:00Z".to_string()),
            message: None,
            cwd: None,
            version: None,
            git_branch: None,
            extra: serde_json::Value::Null,
        });

        let local = ConversationSession {
            session_id: session_id.to_string(),
            entries: local_entries,
            file_path: format!("/test/{session_id}.jsonl"),
        };

        let remote = ConversationSession {
            session_id: session_id.to_string(),
            entries: remote_entries,
            file_path: format!("/sync/{session_id}.jsonl"),
        };

        (local, remote)
    }

    #[test]
    fn test_session_relationship_identical() {
        let local = create_test_session("session-1", 5);
        let remote = create_test_session("session-1", 5);

        let relationship = analyze_session_relationship(&local, &remote);
        assert_eq!(relationship, SessionRelationship::Identical);
    }

    #[test]
    fn test_session_relationship_local_is_prefix() {
        // Local has 5 messages, remote has 10 (same first 5)
        let local = create_test_session("session-1", 5);
        let remote = create_test_session("session-1", 10);

        let relationship = analyze_session_relationship(&local, &remote);
        assert_eq!(relationship, SessionRelationship::LocalIsPrefix);
    }

    #[test]
    fn test_session_relationship_remote_is_prefix() {
        // Local has 10 messages, remote has 5 (same first 5)
        let local = create_test_session("session-1", 10);
        let remote = create_test_session("session-1", 5);

        let relationship = analyze_session_relationship(&local, &remote);
        assert_eq!(relationship, SessionRelationship::RemoteIsPrefix);
    }

    #[test]
    fn test_session_relationship_diverged() {
        let (local, remote) = create_diverged_sessions("session-1");

        let relationship = analyze_session_relationship(&local, &remote);
        assert_eq!(relationship, SessionRelationship::Diverged);
    }

    #[test]
    fn test_conflict_detection_only_diverged() {
        // This is the KEY test: extensions should NOT be conflicts
        let local_5 = create_test_session("session-ext", 5);
        let remote_10 = create_test_session("session-ext", 10);

        let mut detector = ConflictDetector::new();
        detector.detect(&[local_5], &[remote_10]);

        // Extension should NOT create a conflict
        assert!(
            !detector.has_conflicts(),
            "Extension (local prefix of remote) should NOT be a conflict"
        );
    }

    #[test]
    fn test_conflict_detection_diverged_creates_conflict() {
        let (local, remote) = create_diverged_sessions("session-div");

        let mut detector = ConflictDetector::new();
        detector.detect(&[local], &[remote]);

        // True divergence SHOULD create a conflict
        assert!(
            detector.has_conflicts(),
            "Diverged sessions SHOULD be a conflict"
        );
        assert_eq!(detector.conflict_count(), 1);
    }

    #[test]
    fn test_no_conflict_same_content() {
        let local_session = create_test_session("session-1", 5);
        let remote_session = create_test_session("session-1", 5);

        let mut detector = ConflictDetector::new();
        detector.detect(&[local_session], &[remote_session]);

        assert!(!detector.has_conflicts());
    }
}
