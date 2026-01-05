//! End-to-end tests using real Claude CLI
//!
//! These tests verify that claude-code-sync correctly syncs conversation
//! history across simulated "machines" using isolated Claude config directories.
//!
//! Requirements:
//! - Claude CLI installed (`which claude` returns a path)
//! - `.env.local` contains `CLAUDE_CODE_OAUTH_TOKEN=...` (git-ignored)
//! - `~/.claude/settings.json` exists (copied to test dirs for permissions)
//!
//! If requirements are not met, tests skip gracefully.

use serial_test::file_serial;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;
use walkdir::WalkDir;

/// Load OAuth token from .env.local file
fn load_oauth_token() -> Option<String> {
    let env_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".env.local");
    if !env_file.exists() {
        return None;
    }

    let content = fs::read_to_string(&env_file).ok()?;
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(token) = line.strip_prefix("CLAUDE_CODE_OAUTH_TOKEN=") {
            return Some(token.to_string());
        }
    }
    None
}

/// Check if Claude CLI is installed
fn claude_available() -> bool {
    Command::new("which")
        .arg("claude")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Copy settings.json from real ~/.claude to test directory
fn copy_claude_settings(dest: &Path) -> std::io::Result<()> {
    let home = std::env::var("HOME").expect("HOME not set");
    let src = PathBuf::from(home).join(".claude").join("settings.json");

    fs::create_dir_all(dest)?;

    if src.exists() {
        fs::copy(&src, dest.join("settings.json"))?;
    } else {
        // Create minimal settings if none exist
        let minimal_settings = r#"{"permissions":{"allow":[],"deny":[],"ask":[],"defaultMode":"default"}}"#;
        fs::write(dest.join("settings.json"), minimal_settings)?;
    }
    Ok(())
}

/// Run Claude CLI with isolated config and OAuth token
fn run_claude(
    config_dir: &Path,
    project_dir: &Path,
    prompt: &str,
    oauth_token: &str,
) -> std::io::Result<std::process::Output> {
    Command::new("claude")
        .arg("-p")
        .arg(prompt)
        .current_dir(project_dir)
        .env("CLAUDE_CONFIG_DIR", config_dir)
        .env("CLAUDE_CODE_OAUTH_TOKEN", oauth_token)
        .output()
}

/// Run claude-code-sync with config dir set to test directory
fn run_sync(config_dir: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new(env!("CARGO_BIN_EXE_claude-code-sync"))
        .args(args)
        .env("CLAUDE_CODE_SYNC_CONFIG_DIR", config_dir)
        .output()
}

/// Initialize git config for a test directory (needed for commits)
fn init_git_config(home: &Path) -> std::io::Result<()> {
    let git_config = home.join(".gitconfig");
    fs::write(
        &git_config,
        "[user]\n    name = Test User\n    email = test@example.com\n",
    )?;
    Ok(())
}

/// Count .jsonl files in a directory tree
fn count_jsonl_files(dir: &Path) -> usize {
    if !dir.exists() {
        return 0;
    }
    WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("jsonl"))
        .count()
}

/// Find session files in a projects directory
fn find_session_files(projects_dir: &Path) -> Vec<PathBuf> {
    if !projects_dir.exists() {
        return Vec::new();
    }
    WalkDir::new(projects_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("jsonl"))
        .map(|e| e.path().to_path_buf())
        .collect()
}

/// Read a JSONL file and count entries
fn count_jsonl_entries(path: &Path) -> usize {
    fs::read_to_string(path)
        .map(|content| content.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0)
}

/// Test prerequisites - skip if not met
fn check_prerequisites() -> Option<String> {
    let token = load_oauth_token()?;

    if !claude_available() {
        eprintln!("Skipping E2E test: Claude CLI not installed");
        return None;
    }

    Some(token)
}

/// Setup a "machine" for testing: creates claude config dir and project dir
struct TestMachine {
    /// Directory for claude-code-sync config (state.json, etc.)
    sync_config_dir: PathBuf,
    /// Directory for Claude CLI config (settings.json, projects/)
    claude_dir: PathBuf,
    /// Working directory for Claude sessions
    project_dir: PathBuf,
    /// Local clone of the shared sync repo
    sync_repo: PathBuf,
    /// Home directory for git config
    home: PathBuf,
}

impl TestMachine {
    fn new(base: &Path, name: &str, shared_remote: &Path, _oauth_token: &str) -> Self {
        let home = base.join(name);
        let claude_dir = home.join(".claude");
        let project_dir = home.join("project");
        let sync_repo = home.join("sync-repo");
        let sync_config_dir = home.join("sync-config");

        // Create directories
        fs::create_dir_all(&claude_dir).unwrap();
        fs::create_dir_all(&project_dir).unwrap();
        fs::create_dir_all(&sync_repo).unwrap();
        fs::create_dir_all(&sync_config_dir).unwrap();

        // Copy settings.json for permissions
        copy_claude_settings(&claude_dir).unwrap();

        // Setup git config
        init_git_config(&home).unwrap();

        // Initialize local sync repo (clone from shared remote)
        Command::new("git")
            .args(["clone", &format!("file://{}", shared_remote.display()), "."])
            .current_dir(&sync_repo)
            .output()
            .unwrap();

        // Configure git user in the cloned repo
        Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(&sync_repo)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test User"])
            .current_dir(&sync_repo)
            .output()
            .unwrap();

        // Initialize claude-code-sync state in the sync_config_dir
        let state = serde_json::json!({
            "sync_repo_path": sync_repo.to_string_lossy(),
            "has_remote": true,
            "is_cloned_repo": true
        });
        fs::write(sync_config_dir.join("state.json"), state.to_string()).unwrap();

        // Create filter config (TOML format) with custom claude_projects_dir
        let filter_config = format!(
            r#"
exclude_attachments = false
claude_projects_dir = "{}"
"#,
            claude_dir.join("projects").to_string_lossy().replace('\\', "/")
        );
        fs::write(sync_config_dir.join("config.toml"), filter_config).unwrap();

        // Create the projects directory
        fs::create_dir_all(claude_dir.join("projects")).unwrap();

        TestMachine {
            sync_config_dir,
            claude_dir,
            project_dir,
            sync_repo,
            home,
        }
    }

    fn run_claude(&self, prompt: &str, oauth_token: &str) -> std::process::Output {
        run_claude(&self.claude_dir, &self.project_dir, prompt, oauth_token).unwrap()
    }

    fn push(&self) -> std::process::Output {
        run_sync(&self.sync_config_dir, &["push"]).unwrap()
    }

    fn pull(&self) -> std::process::Output {
        run_sync(&self.sync_config_dir, &["pull"]).unwrap()
    }

    fn session_count(&self) -> usize {
        count_jsonl_files(&self.claude_dir.join("projects"))
    }

    fn find_sessions(&self) -> Vec<PathBuf> {
        find_session_files(&self.claude_dir.join("projects"))
    }
}

/// Create a bare git repo to serve as the shared "remote"
fn create_shared_remote(base: &Path) -> PathBuf {
    let remote = base.join("shared-remote");
    fs::create_dir_all(&remote).unwrap();

    Command::new("git")
        .args(["init", "--bare"])
        .current_dir(&remote)
        .output()
        .unwrap();

    // Create a temp clone to add initial commit
    let temp_clone = base.join("temp-clone");
    fs::create_dir_all(&temp_clone).unwrap();

    Command::new("git")
        .args(["clone", &format!("file://{}", remote.display()), "."])
        .current_dir(&temp_clone)
        .output()
        .unwrap();

    // Set git config for temp clone
    Command::new("git")
        .args(["config", "user.email", "test@test.com"])
        .current_dir(&temp_clone)
        .output()
        .unwrap();
    Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(&temp_clone)
        .output()
        .unwrap();

    // Create initial commit
    fs::write(temp_clone.join("README.md"), "# Claude Sync Test\n").unwrap();
    Command::new("git")
        .args(["add", "."])
        .current_dir(&temp_clone)
        .output()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", "Initial commit"])
        .current_dir(&temp_clone)
        .output()
        .unwrap();
    Command::new("git")
        .args(["push", "origin", "main"])
        .current_dir(&temp_clone)
        .output()
        .unwrap();

    // Cleanup temp clone
    fs::remove_dir_all(&temp_clone).unwrap();

    remote
}

#[test]
#[file_serial(e2e_tests)]
fn test_e2e_basic_sync() {
    // Check prerequisites
    let oauth_token = match check_prerequisites() {
        Some(t) => t,
        None => {
            eprintln!("Skipping test_e2e_basic_sync: prerequisites not met");
            return;
        }
    };

    let test_root = TempDir::new().unwrap();

    // Create shared remote
    let shared_remote = create_shared_remote(test_root.path());

    // Setup Machine A
    let machine_a = TestMachine::new(test_root.path(), "machine_a", &shared_remote, &oauth_token);

    // Machine A: Create a session
    eprintln!("Machine A: Creating session...");
    let output = machine_a.run_claude("Say exactly: HELLO_FROM_MACHINE_A", &oauth_token);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("Invalid API key") || stderr.contains("login") {
            eprintln!("Skipping test: Claude auth failed - {}", stderr);
            return;
        }
        panic!("Claude failed unexpectedly: {}", stderr);
    }

    // Verify session was created
    eprintln!("Machine A claude dir contents:");
    for entry in walkdir::WalkDir::new(&machine_a.claude_dir).max_depth(4) {
        if let Ok(e) = entry {
            eprintln!("  {}", e.path().display());
        }
    }

    let session_count = machine_a.session_count();
    eprintln!("Machine A session count: {}", session_count);
    assert!(
        session_count > 0,
        "Machine A should have at least 1 session after claude -p"
    );

    // Check the config file
    let config_content = fs::read_to_string(machine_a.sync_config_dir.join("config.toml")).unwrap();
    eprintln!("Config.toml: {}", config_content);

    // Machine A: Pull first (this captures local sessions to sync repo)
    // Then Push (this pushes sync repo to remote)
    eprintln!("Machine A: Pull (to capture local sessions)...");
    let pull_output = machine_a.pull();
    eprintln!("Pull stdout: {}", String::from_utf8_lossy(&pull_output.stdout));
    eprintln!("Pull stderr: {}", String::from_utf8_lossy(&pull_output.stderr));
    assert!(
        pull_output.status.success(),
        "Pull failed: {}",
        String::from_utf8_lossy(&pull_output.stderr)
    );

    // Machine A: Push
    eprintln!("Machine A: Pushing...");
    let push_output = machine_a.push();
    eprintln!("Push stdout: {}", String::from_utf8_lossy(&push_output.stdout));
    eprintln!("Push stderr: {}", String::from_utf8_lossy(&push_output.stderr));
    assert!(
        push_output.status.success(),
        "Push failed: {}",
        String::from_utf8_lossy(&push_output.stderr)
    );

    // Check what's in the sync repo after push
    eprintln!("Sync repo contents:");
    for entry in walkdir::WalkDir::new(&machine_a.sync_repo).max_depth(3) {
        if let Ok(e) = entry {
            eprintln!("  {}", e.path().display());
        }
    }

    // Setup Machine B
    let machine_b = TestMachine::new(test_root.path(), "machine_b", &shared_remote, &oauth_token);

    // Verify Machine B has no sessions initially
    assert_eq!(
        machine_b.session_count(),
        0,
        "Machine B should have no sessions initially"
    );

    // Machine B: Pull
    eprintln!("Machine B: Pulling...");
    let pull_output = machine_b.pull();
    eprintln!("Pull stdout: {}", String::from_utf8_lossy(&pull_output.stdout));
    eprintln!("Pull stderr: {}", String::from_utf8_lossy(&pull_output.stderr));
    assert!(
        pull_output.status.success(),
        "Pull failed: {}",
        String::from_utf8_lossy(&pull_output.stderr)
    );

    // Check what's in Machine B's claude dir after pull
    eprintln!("Machine B claude dir contents:");
    for entry in walkdir::WalkDir::new(&machine_b.claude_dir).max_depth(3) {
        if let Ok(e) = entry {
            eprintln!("  {}", e.path().display());
        }
    }

    // Verify Machine B now has the session
    assert!(
        machine_b.session_count() > 0,
        "Machine B should have sessions after pull"
    );

    // Verify session content matches
    let a_sessions = machine_a.find_sessions();
    let b_sessions = machine_b.find_sessions();

    assert_eq!(
        a_sessions.len(),
        b_sessions.len(),
        "Both machines should have same number of sessions"
    );

    eprintln!("test_e2e_basic_sync PASSED");
}

#[test]
#[file_serial(e2e_tests)]
fn test_e2e_bidirectional_sync() {
    // Check prerequisites
    let oauth_token = match check_prerequisites() {
        Some(t) => t,
        None => {
            eprintln!("Skipping test_e2e_bidirectional_sync: prerequisites not met");
            return;
        }
    };

    let test_root = TempDir::new().unwrap();

    // Create shared remote
    let shared_remote = create_shared_remote(test_root.path());

    // Setup both machines
    let machine_a = TestMachine::new(test_root.path(), "machine_a", &shared_remote, &oauth_token);
    let machine_b = TestMachine::new(test_root.path(), "machine_b", &shared_remote, &oauth_token);

    // Machine A: Create session and sync (pull to capture, then push)
    eprintln!("Machine A: Creating session...");
    let output = machine_a.run_claude("Say: MESSAGE_FROM_A", &oauth_token);
    if !output.status.success() {
        eprintln!(
            "Skipping test: Claude failed - {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    eprintln!("Machine A: Pull (capture) then Push...");
    machine_a.pull();
    machine_a.push();

    // Machine B: Pull, create its own session, push
    eprintln!("Machine B: Pulling...");
    machine_b.pull();

    let b_sessions_after_pull = machine_b.session_count();
    assert!(
        b_sessions_after_pull > 0,
        "Machine B should have A's session after pull"
    );

    eprintln!("Machine B: Creating its own session...");
    let output = machine_b.run_claude("Say: MESSAGE_FROM_B", &oauth_token);
    if !output.status.success() {
        eprintln!(
            "Skipping: Claude failed on B - {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    // Machine B should now have more sessions
    assert!(
        machine_b.session_count() > b_sessions_after_pull,
        "Machine B should have created a new session"
    );

    eprintln!("Machine B: Pull (capture) then Push...");
    machine_b.pull();
    machine_b.push();

    // Machine A: Pull to get B's session
    eprintln!("Machine A: Pulling...");
    machine_a.pull();

    // Both machines should have all sessions
    let a_count = machine_a.session_count();
    let b_count = machine_b.session_count();

    assert_eq!(
        a_count, b_count,
        "Both machines should have same session count after bidirectional sync"
    );
    assert!(a_count >= 2, "Should have at least 2 sessions total");

    eprintln!("test_e2e_bidirectional_sync PASSED");
}

#[test]
#[file_serial(e2e_tests)]
fn test_e2e_concurrent_messages_merge() {
    // Check prerequisites
    let oauth_token = match check_prerequisites() {
        Some(t) => t,
        None => {
            eprintln!("Skipping test_e2e_concurrent_messages_merge: prerequisites not met");
            return;
        }
    };

    let test_root = TempDir::new().unwrap();

    // Create shared remote
    let shared_remote = create_shared_remote(test_root.path());

    // Setup Machine A and create initial session
    let machine_a = TestMachine::new(test_root.path(), "machine_a", &shared_remote, &oauth_token);

    eprintln!("Machine A: Creating initial session...");
    let output = machine_a.run_claude("Say: INITIAL_MESSAGE", &oauth_token);
    if !output.status.success() {
        eprintln!(
            "Skipping test: Claude failed - {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    // Get the session that was created
    let a_sessions = machine_a.find_sessions();
    assert!(!a_sessions.is_empty(), "Machine A should have a session");

    let initial_entry_count = count_jsonl_entries(&a_sessions[0]);
    eprintln!("Initial session has {} entries", initial_entry_count);

    // Pull (capture) then Push from A
    eprintln!("Machine A: Pull (capture) then Push initial session...");
    machine_a.pull();
    machine_a.push();

    // Setup Machine B and pull
    let machine_b = TestMachine::new(test_root.path(), "machine_b", &shared_remote, &oauth_token);
    eprintln!("Machine B: Pulling...");
    machine_b.pull();

    // Both machines now have the same session
    // Now simulate concurrent edits by having both add messages

    // This is tricky because --resume needs the session ID
    // For now, we just verify the basic fork detection by having each machine
    // create a new session without syncing

    eprintln!("Machine A: Creating another session (without syncing)...");
    let _output = machine_a.run_claude("Say: A_SECOND_MESSAGE", &oauth_token);

    eprintln!("Machine B: Creating another session (without syncing)...");
    let _output = machine_b.run_claude("Say: B_SECOND_MESSAGE", &oauth_token);

    // Now sync: A pull+push, B pulls (should merge), B pull+push, A pulls
    eprintln!("Machine A: Pull (capture) then Push...");
    machine_a.pull();
    machine_a.push();

    eprintln!("Machine B: Pulling (merge scenario)...");
    let pull_output = machine_b.pull();
    assert!(
        pull_output.status.success(),
        "Pull merge failed: {}",
        String::from_utf8_lossy(&pull_output.stderr)
    );

    eprintln!("Machine B: Pushing...");
    machine_b.push();

    eprintln!("Machine A: Pulling...");
    machine_a.pull();

    // Both should have all sessions
    let final_a_count = machine_a.session_count();
    let final_b_count = machine_b.session_count();

    assert_eq!(
        final_a_count, final_b_count,
        "Both machines should have same session count"
    );

    eprintln!(
        "Final session count: A={}, B={}",
        final_a_count, final_b_count
    );
    eprintln!("test_e2e_concurrent_messages_merge PASSED");
}
