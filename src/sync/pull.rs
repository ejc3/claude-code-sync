use anyhow::{Context, Result};
use colored::Colorize;
use inquire::Confirm;
use std::collections::HashMap;
use std::path::Path;

use crate::conflict::{analyze_session_relationship, ConflictDetector, SessionRelationship};
use crate::filter::FilterConfig;
use crate::history::{
    ConversationSummary, OperationHistory, OperationRecord, OperationType, SyncOperation,
};
use crate::interactive_conflict;
use crate::parser::ConversationSession;
use crate::report::{save_conflict_report, ConflictReport};
use crate::scm;

use super::discovery::{claude_projects_dir, discover_sessions};
use super::state::SyncState;
use super::MAX_CONVERSATIONS_TO_DISPLAY;

/// Generate a unique temp branch name with timestamp
fn generate_temp_branch_name() -> String {
    let timestamp = chrono::Utc::now().format("%Y%m%d-%H%M%S");
    format!("sync-local-{}", timestamp)
}

/// Pull and merge history from sync repository
///
/// Safe workflow:
/// 1. Create temp branch from current state
/// 2. Copy local .claude sessions to sync repo and commit to temp branch
/// 3. Push temp branch to remote (preserves local work - SAFETY NET)
/// 4. Checkout main/master and pull from remote
/// 5. Merge temp branch into main (smart conflict resolution)
/// 6. Copy merged result to .claude
/// 7. Delete temp branch (local + remote)
pub fn pull_history(
    fetch_remote: bool,
    branch: Option<&str>,
    interactive: bool,
    verbosity: crate::VerbosityLevel,
) -> Result<()> {
    use crate::VerbosityLevel;

    if verbosity != VerbosityLevel::Quiet {
        println!("{}", "Pulling Claude Code history...".cyan().bold());
    }

    let state = SyncState::load()?;
    let repo = scm::open(&state.sync_repo_path)?;
    let filter = FilterConfig::load()?;
    let claude_dir = claude_projects_dir()?;

    // Get the main branch name
    let main_branch = branch
        .map(|s| s.to_string())
        .or_else(|| repo.current_branch().ok())
        .unwrap_or_else(|| "main".to_string());

    // ============================================================================
    // STEP 1: Create temp branch and save local state
    // ============================================================================
    let temp_branch = generate_temp_branch_name();

    if verbosity != VerbosityLevel::Quiet {
        println!("  {} temp branch '{}'...", "Creating".cyan(), temp_branch);
    }

    // Create the temp branch from current HEAD
    repo.create_branch(&temp_branch)
        .context("Failed to create temp branch")?;
    repo.checkout(&temp_branch)
        .context("Failed to checkout temp branch")?;

    // ============================================================================
    // STEP 2: Copy local .claude sessions to sync repo on temp branch
    // ============================================================================
    if verbosity != VerbosityLevel::Quiet {
        println!("  {} local sessions to temp branch...", "Saving".cyan());
    }

    let local_sessions = discover_sessions(&claude_dir, &filter)?;
    let projects_dir = state.sync_repo_path.join(&filter.sync_subdirectory);
    std::fs::create_dir_all(&projects_dir)?;

    let mut local_session_count = 0;
    for session in &local_sessions {
        let relative_path = Path::new(&session.file_path)
            .strip_prefix(&claude_dir)
            .unwrap_or(Path::new(&session.file_path));
        let dest_path = projects_dir.join(relative_path);
        session.write_to_file(&dest_path)?;
        local_session_count += 1;
    }

    // Commit local state to temp branch
    repo.stage_all()?;
    if repo.has_changes()? {
        let commit_msg = format!(
            "Save local state before pull ({})",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        );
        repo.commit(&commit_msg)?;

        if verbosity != VerbosityLevel::Quiet {
            println!(
                "  {} Saved {} local sessions to temp branch",
                "✓".green(),
                local_session_count
            );
        }
    } else if verbosity != VerbosityLevel::Quiet {
        println!("  {} No local changes to save", "✓".green());
    }

    // ============================================================================
    // STEP 3: Push temp branch to remote (SAFETY NET - never lose work)
    // ============================================================================
    if fetch_remote && state.has_remote {
        if verbosity != VerbosityLevel::Quiet {
            println!("  {} temp branch to remote...", "Pushing".cyan());
        }

        match repo.push("origin", &temp_branch) {
            Ok(_) => {
                if verbosity != VerbosityLevel::Quiet {
                    println!("  {} Pushed temp branch to origin/{}", "✓".green(), temp_branch);
                }
            }
            Err(e) => {
                log::warn!("Failed to push temp branch: {}", e);
                log::info!("Continuing - local temp branch still preserves your work");
            }
        }
    }

    // ============================================================================
    // STEP 4: Checkout main and pull from remote
    // ============================================================================
    if verbosity != VerbosityLevel::Quiet {
        println!("  {} to main branch...", "Switching".cyan());
    }

    repo.checkout(&main_branch)
        .context("Failed to checkout main branch")?;

    if fetch_remote && state.has_remote {
        if verbosity != VerbosityLevel::Quiet {
            println!("  {} from remote...", "Pulling".cyan());
        }

        // First fetch to see what's on remote
        match repo.fetch("origin") {
            Ok(_) => {
                if verbosity != VerbosityLevel::Quiet {
                    println!("  {} Fetched from origin", "✓".green());
                }
            }
            Err(e) => {
                log::warn!("Failed to fetch: {}", e);
            }
        }

        // Now pull (which will fast-forward if possible)
        match repo.pull("origin", &main_branch) {
            Ok(_) => {
                if verbosity != VerbosityLevel::Quiet {
                    println!("  {} Pulled origin/{}", "✓".green(), main_branch);
                }
            }
            Err(e) => {
                log::warn!("Failed to pull: {}", e);
                log::info!("Continuing with local state...");
            }
        }
    }

    // ============================================================================
    // STEP 5: Merge temp branch into main (smart merge)
    // ============================================================================
    if verbosity != VerbosityLevel::Quiet {
        println!("  {} temp branch into main...", "Merging".cyan());
    }

    // Discover sessions from both branches
    // - main branch now has remote changes
    // - temp branch has our local changes
    let remote_sessions = discover_sessions(&projects_dir, &filter)?;

    // We need to get the local sessions from the temp branch
    // Switch to temp branch, read sessions, switch back
    repo.checkout(&temp_branch)?;
    let temp_branch_sessions = discover_sessions(&projects_dir, &filter)?;
    repo.checkout(&main_branch)?;

    if verbosity != VerbosityLevel::Quiet {
        println!(
            "  {} {} sessions from remote, {} from local",
            "Found".green(),
            remote_sessions.len(),
            temp_branch_sessions.len()
        );
    }

    // ============================================================================
    // CONFLICT DETECTION
    // ============================================================================
    if verbosity != VerbosityLevel::Quiet {
        println!("  {} conflicts...", "Detecting".cyan());
    }

    // Build maps for comparison
    let remote_map: HashMap<_, _> = remote_sessions
        .iter()
        .map(|s| (s.session_id.clone(), s))
        .collect();

    let local_map: HashMap<_, _> = temp_branch_sessions
        .iter()
        .map(|s| (s.session_id.clone(), s))
        .collect();

    // Find sessions that exist in both and may have conflicts
    let mut detector = ConflictDetector::new();
    detector.detect(&temp_branch_sessions, &remote_sessions);

    // ============================================================================
    // INTERACTIVE CONFIRMATION
    // ============================================================================
    if verbosity != VerbosityLevel::Quiet {
        println!();
        println!("{}", "Pull Summary:".bold().cyan());
        println!("  {} Local sessions: {}", "•".cyan(), temp_branch_sessions.len());
        println!("  {} Remote sessions: {}", "•".cyan(), remote_sessions.len());
        println!("  {} Conflicts: {}", "•".yellow(), detector.conflict_count());
        println!();
    }

    if interactive && interactive_conflict::is_interactive() {
        let confirm = Confirm::new("Do you want to proceed with merging these changes?")
            .with_default(true)
            .with_help_message("This will merge remote sessions with your local changes")
            .prompt()
            .context("Failed to get confirmation")?;

        if !confirm {
            // Clean up temp branch before exiting
            cleanup_temp_branch(repo.as_ref(), &temp_branch, fetch_remote && state.has_remote, verbosity)?;
            println!("\n{}", "Pull cancelled.".yellow());
            return Ok(());
        }
    }

    // ============================================================================
    // SMART MERGE AND APPLY TO SYNC REPO
    // ============================================================================
    let mut affected_conversations: Vec<ConversationSummary> = Vec::new();
    let mut merged_count = 0;
    let mut added_count = 0;
    let mut modified_count = 0;
    let mut unchanged_count = 0;
    let mut skipped_local_newer = 0;

    // Handle conflicts with smart merge
    if detector.has_conflicts() {
        if verbosity != VerbosityLevel::Quiet {
            println!(
                "  {} {} diverged sessions detected (will create forks)",
                "!".yellow(),
                detector.conflict_count()
            );
            println!("  {} branches (fork-aware merge)...", "Combining".cyan());
        }

        let mut smart_merge_success_count = 0;
        let mut smart_merge_failed_conflicts = Vec::new();

        for conflict in detector.conflicts_mut() {
            if let (Some(local_session), Some(remote_session)) = (
                local_map.get(&conflict.session_id),
                remote_map.get(&conflict.session_id),
            ) {
                match conflict.try_smart_merge(local_session, remote_session) {
                    Ok(()) => {
                        smart_merge_success_count += 1;
                        if let crate::conflict::ConflictResolution::SmartMerge {
                            ref merged_entries,
                            ref stats,
                        } = conflict.resolution
                        {
                            let merged_session = ConversationSession {
                                session_id: conflict.session_id.clone(),
                                entries: merged_entries.clone(),
                                file_path: conflict.local_file.to_string_lossy().to_string(),
                            };

                            // Write to sync repo (main branch)
                            let dest_path = projects_dir.join(
                                Path::new(&local_session.file_path)
                                    .strip_prefix(&claude_dir)
                                    .unwrap_or(Path::new(&local_session.file_path))
                            );
                            if let Err(e) = merged_session.write_to_file(&dest_path) {
                                log::warn!("Failed to write merged session: {}", e);
                                smart_merge_failed_conflicts.push(conflict.clone());
                            } else if verbosity != VerbosityLevel::Quiet {
                                println!(
                                    "  {} Forked {} ({} local + {} remote = {} combined)",
                                    "✓".green(),
                                    conflict.session_id,
                                    stats.local_messages,
                                    stats.remote_messages,
                                    stats.merged_messages,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("Smart merge failed for {}: {}", conflict.session_id, e);
                        smart_merge_failed_conflicts.push(conflict.clone());
                    }
                }
            }
        }

        if verbosity != VerbosityLevel::Quiet {
            println!(
                "  {} Successfully merged {}/{} diverged sessions",
                "✓".green(),
                smart_merge_success_count,
                detector.conflict_count()
            );
        }

        // Handle failed smart merges
        if !smart_merge_failed_conflicts.is_empty() {
            if verbosity != VerbosityLevel::Quiet {
                println!(
                    "  {} {} conflicts require manual resolution",
                    "!".yellow(),
                    smart_merge_failed_conflicts.len()
                );
            }

            if crate::interactive_conflict::is_interactive() {
                let resolution_result = crate::interactive_conflict::resolve_conflicts_interactive(
                    &mut smart_merge_failed_conflicts,
                )?;

                let _renames = crate::interactive_conflict::apply_resolutions(
                    &resolution_result,
                    &remote_sessions,
                    &claude_dir,
                    &projects_dir,
                )?;
            } else {
                // Non-interactive: keep both versions
                for conflict in &smart_merge_failed_conflicts {
                    let timestamp = chrono::Utc::now().format("%Y%m%d-%H%M%S");
                    let conflict_suffix = format!("conflict-{timestamp}");

                    if let Ok(renamed_path) = conflict.clone().resolve_keep_both(&conflict_suffix) {
                        if let Some(session) = remote_sessions
                            .iter()
                            .find(|s| s.session_id == conflict.session_id)
                        {
                            session.write_to_file(&renamed_path)?;
                        }
                    }
                }
            }

            let report = ConflictReport::from_conflicts(detector.conflicts());
            save_conflict_report(&report)?;
        }
    }

    // ============================================================================
    // MERGE NON-CONFLICTING SESSIONS
    // ============================================================================
    if verbosity != VerbosityLevel::Quiet {
        println!("  {} non-conflicting sessions...", "Merging".cyan());
    }

    // All sessions from temp branch (local) that aren't conflicts
    for local_session in &temp_branch_sessions {
        if detector
            .conflicts()
            .iter()
            .any(|c| c.session_id == local_session.session_id)
        {
            continue; // Already handled above
        }

        let relative_path = Path::new(&local_session.file_path)
            .strip_prefix(&claude_dir)
            .ok()
            .unwrap_or_else(|| Path::new(&local_session.file_path));

        let dest_path = projects_dir.join(relative_path);

        let (operation, should_copy) = if let Some(remote) = remote_map.get(&local_session.session_id) {
            let relationship = analyze_session_relationship(local_session, remote);

            match relationship {
                SessionRelationship::Identical => {
                    unchanged_count += 1;
                    (SyncOperation::Unchanged, false)
                }
                SessionRelationship::LocalIsPrefix => {
                    // Remote has more - use remote
                    modified_count += 1;
                    // Remote is already in main branch, just track it
                    (SyncOperation::Modified, false)
                }
                SessionRelationship::RemoteIsPrefix => {
                    // Local has more - use local
                    skipped_local_newer += 1;
                    (SyncOperation::Modified, true)
                }
                SessionRelationship::Diverged => {
                    // Should have been caught as conflict, but handle anyway
                    modified_count += 1;
                    (SyncOperation::Modified, true)
                }
            }
        } else {
            // Local-only session
            added_count += 1;
            (SyncOperation::Added, true)
        };

        if should_copy {
            local_session.write_to_file(&dest_path)?;
            merged_count += 1;
        }

        let relative_path_str = relative_path.to_string_lossy().to_string();
        if let Ok(summary) = ConversationSummary::new(
            local_session.session_id.clone(),
            relative_path_str,
            local_session.latest_timestamp(),
            local_session.message_count(),
            operation,
        ) {
            affected_conversations.push(summary);
        }
    }

    // Also track remote-only sessions (new from remote)
    for remote_session in &remote_sessions {
        if local_map.contains_key(&remote_session.session_id) {
            continue; // Already handled above
        }

        let relative_path = Path::new(&remote_session.file_path)
            .strip_prefix(&projects_dir)
            .ok()
            .unwrap_or_else(|| Path::new(&remote_session.file_path));

        added_count += 1;

        let relative_path_str = relative_path.to_string_lossy().to_string();
        if let Ok(summary) = ConversationSummary::new(
            remote_session.session_id.clone(),
            relative_path_str,
            remote_session.latest_timestamp(),
            remote_session.message_count(),
            SyncOperation::Added,
        ) {
            affected_conversations.push(summary);
        }
    }

    // Commit the merged result to main branch
    repo.stage_all()?;
    if repo.has_changes()? {
        let commit_msg = format!(
            "Merge local changes from {} ({})",
            temp_branch,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        );
        repo.commit(&commit_msg)?;
    }

    if verbosity != VerbosityLevel::Quiet {
        println!("  {} Merged {} sessions", "✓".green(), merged_count);
        if skipped_local_newer > 0 {
            println!(
                "  {} Kept {} local sessions (already ahead of remote)",
                "✓".green(),
                skipped_local_newer
            );
        }
    }

    // ============================================================================
    // STEP 6: Copy merged result to .claude
    // ============================================================================
    if verbosity != VerbosityLevel::Quiet {
        println!("  {} merged sessions to .claude...", "Copying".cyan());
    }

    let final_sessions = discover_sessions(&projects_dir, &filter)?;
    for session in &final_sessions {
        let relative_path = Path::new(&session.file_path)
            .strip_prefix(&projects_dir)
            .unwrap_or(Path::new(&session.file_path));
        let dest_path = claude_dir.join(relative_path);
        session.write_to_file(&dest_path)?;
    }

    if verbosity != VerbosityLevel::Quiet {
        println!("  {} Updated {} sessions in .claude", "✓".green(), final_sessions.len());
    }

    // ============================================================================
    // STEP 7: Clean up temp branch
    // ============================================================================
    cleanup_temp_branch(repo.as_ref(), &temp_branch, fetch_remote && state.has_remote, verbosity)?;

    // ============================================================================
    // CREATE AND SAVE OPERATION RECORD
    // ============================================================================
    let operation_record = OperationRecord::new(
        OperationType::Pull,
        Some(main_branch.clone()),
        affected_conversations.clone(),
    );

    let mut history = match OperationHistory::load() {
        Ok(h) => h,
        Err(e) => {
            log::warn!("Failed to load operation history: {}", e);
            OperationHistory::default()
        }
    };

    if let Err(e) = history.add_operation(operation_record) {
        log::warn!("Failed to save operation to history: {}", e);
    }

    // ============================================================================
    // DISPLAY SUMMARY
    // ============================================================================
    if verbosity != VerbosityLevel::Quiet {
        println!("\n{}", "=== Pull Summary ===".bold().cyan());

        let fork_count = detector.conflict_count();
        println!(
            "  {} Added    {} Modified    {} Forked    {} Unchanged",
            format!("{added_count}").green(),
            format!("{modified_count}").cyan(),
            format!("{fork_count}").yellow(),
            format!("{unchanged_count}").dimmed(),
        );

        if skipped_local_newer > 0 {
            println!(
                "  (Kept {} sessions where local was ahead of remote)",
                skipped_local_newer
            );
        }
        println!();

        // Group by project
        let mut by_project: HashMap<String, Vec<&ConversationSummary>> = HashMap::new();
        for conv in &affected_conversations {
            if conv.operation == SyncOperation::Unchanged {
                continue;
            }
            let project = conv
                .project_path
                .split('/')
                .next()
                .unwrap_or("unknown")
                .to_string();
            by_project.entry(project).or_default().push(conv);
        }

        if !by_project.is_empty() {
            println!("{}", "Affected Conversations:".bold());

            let mut projects: Vec<_> = by_project.keys().collect();
            projects.sort();

            for project in projects {
                let conversations = &by_project[project];
                println!("\n  {} {}/", "Project:".bold(), project.cyan());

                for conv in conversations.iter().take(MAX_CONVERSATIONS_TO_DISPLAY) {
                    let operation_str = match conv.operation {
                        SyncOperation::Added => "ADD".green(),
                        SyncOperation::Modified => "MOD".cyan(),
                        SyncOperation::Conflict => "FORK".yellow(),
                        SyncOperation::Unchanged => "---".dimmed(),
                    };

                    let timestamp_str = conv
                        .timestamp
                        .as_ref()
                        .and_then(|t| t.split('T').next())
                        .unwrap_or("unknown");

                    println!(
                        "    {} {} ({}msg, {})",
                        operation_str,
                        conv.project_path,
                        conv.message_count,
                        timestamp_str.dimmed()
                    );
                }

                if conversations.len() > MAX_CONVERSATIONS_TO_DISPLAY {
                    println!(
                        "    {} ... and {} more conversations",
                        "...".dimmed(),
                        conversations.len() - MAX_CONVERSATIONS_TO_DISPLAY
                    );
                }
            }
        }

        println!("\n{}", "Pull complete!".green().bold());
    }

    Ok(())
}

/// Clean up the temporary branch (local and optionally remote)
fn cleanup_temp_branch(
    repo: &dyn scm::Scm,
    temp_branch: &str,
    has_remote: bool,
    verbosity: crate::VerbosityLevel,
) -> Result<()> {
    use crate::VerbosityLevel;

    if verbosity != VerbosityLevel::Quiet {
        println!("  {} temp branch...", "Cleaning up".cyan());
    }

    // Delete remote branch first (if it exists)
    if has_remote {
        match repo.delete_remote_branch("origin", temp_branch) {
            Ok(_) => {
                if verbosity != VerbosityLevel::Quiet {
                    println!("  {} Deleted origin/{}", "✓".green(), temp_branch);
                }
            }
            Err(e) => {
                log::debug!("Failed to delete remote branch (may not exist): {}", e);
            }
        }
    }

    // Delete local branch
    match repo.delete_branch(temp_branch) {
        Ok(_) => {
            if verbosity != VerbosityLevel::Quiet {
                println!("  {} Deleted local branch {}", "✓".green(), temp_branch);
            }
        }
        Err(e) => {
            log::warn!("Failed to delete local branch: {}", e);
        }
    }

    Ok(())
}
