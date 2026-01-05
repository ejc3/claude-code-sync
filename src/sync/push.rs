use anyhow::{Context, Result};
use colored::Colorize;
use inquire::Confirm;

use crate::filter::FilterConfig;
use crate::history::{OperationHistory, OperationRecord, OperationType};
use crate::interactive_conflict;
use crate::lock::SyncLock;
use crate::scm;

use super::state::SyncState;

/// Push sync repository to remote
///
/// Simple workflow:
/// 1. Stage any uncommitted changes in sync repo
/// 2. Commit if there are changes
/// 3. Push to remote (fail on conflict - user must pull first)
///
/// Note: Local ~/.claude sessions are captured during `pull`, not here.
/// Push just pushes whatever is already in the sync repo.
pub fn push_history(
    commit_message: Option<&str>,
    push_remote: bool,
    branch: Option<&str>,
    _exclude_attachments: bool,
    interactive: bool,
    verbosity: crate::VerbosityLevel,
) -> Result<()> {
    use crate::VerbosityLevel;

    // Acquire exclusive lock to prevent concurrent sync operations
    let _lock = SyncLock::acquire()?;

    if verbosity != VerbosityLevel::Quiet {
        println!("{}", "Pushing Claude Code history...".cyan().bold());
    }

    let state = SyncState::load()?;
    let repo = scm::open(&state.sync_repo_path)?;
    let filter = FilterConfig::load()?;

    // Set up LFS if enabled
    if filter.enable_lfs {
        if verbosity != VerbosityLevel::Quiet {
            println!("  {} Git LFS...", "Configuring".cyan());
        }
        scm::lfs::setup(&state.sync_repo_path, &filter.lfs_patterns)
            .context("Failed to set up Git LFS")?;
    }

    // Get the current branch name
    let branch_name = branch
        .map(|s| s.to_string())
        .or_else(|| repo.current_branch().ok())
        .unwrap_or_else(|| "main".to_string());

    // Stage any uncommitted changes
    repo.stage_all()?;

    let has_changes = repo.has_changes()?;
    let commit_before_push = repo.current_commit_hash().ok();

    if has_changes {
        // Show what will be committed
        if verbosity != VerbosityLevel::Quiet {
            println!("  {} Changes staged for commit", "✓".green());
        }

        // Interactive confirmation
        if interactive && interactive_conflict::is_interactive() {
            let confirm = Confirm::new("Do you want to proceed with pushing these changes?")
                .with_default(true)
                .with_help_message("This will commit and push to the sync repository")
                .prompt()
                .context("Failed to get confirmation")?;

            if !confirm {
                println!("\n{}", "Push cancelled.".yellow());
                return Ok(());
            }
        }

        // Commit
        let default_message = format!(
            "Sync at {}",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        );
        let message = commit_message.unwrap_or(&default_message);

        if verbosity != VerbosityLevel::Quiet {
            println!("  {} changes...", "Committing".cyan());
        }
        repo.commit(message)?;
        if verbosity != VerbosityLevel::Quiet {
            println!("  {} Committed: {}", "✓".green(), message);
        }
    } else if verbosity != VerbosityLevel::Quiet {
        println!("  {} No new changes to commit", "✓".green());
    }

    // Push to remote if configured
    if push_remote && state.has_remote {
        if verbosity != VerbosityLevel::Quiet {
            println!("  {} to remote...", "Pushing".cyan());
        }

        match repo.push("origin", &branch_name) {
            Ok(_) => {
                if verbosity != VerbosityLevel::Quiet {
                    println!("  {} Pushed to origin/{}", "✓".green(), branch_name);
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("non-fast-forward")
                    || error_msg.contains("fetch first")
                    || error_msg.contains("rejected")
                    || error_msg.contains("failed to push")
                {
                    println!(
                        "\n{} Remote has changes that aren't in your local repository.",
                        "!".yellow().bold()
                    );
                    println!(
                        "{} Run {} first to merge remote changes, then push again.",
                        "→".cyan(),
                        "claude-code-sync pull".bold()
                    );
                    return Err(anyhow::anyhow!(
                        "Push rejected: remote has new commits. Run 'claude-code-sync pull' first."
                    ));
                } else {
                    return Err(e.context("Failed to push to remote"));
                }
            }
        }
    } else if !has_changes {
        // No remote and no local changes - nothing to do
        if verbosity != VerbosityLevel::Quiet {
            println!("  {} No changes to push", "✓".green());
        }
        return Ok(());
    }

    // Record operation in history
    let mut operation_record = OperationRecord::new(
        OperationType::Push,
        Some(branch_name.clone()),
        Vec::new(), // No detailed conversation tracking in simplified push
    );
    operation_record.commit_hash = commit_before_push;

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

    if verbosity == VerbosityLevel::Quiet {
        println!("Push complete");
    } else {
        println!("\n{}", "Push complete!".green().bold());
    }

    Ok(())
}
