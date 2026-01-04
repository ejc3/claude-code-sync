//! Verify session sync between two .claude directories
//!
//! Compares session files to ensure they're identical or one is a prefix of the other
//! (same entries, just one has more recent messages appended).

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
struct SessionInfo {
    path: PathBuf,
    relative_path: String,
    entry_count: usize,
    /// UUIDs in order - used to detect prefix relationships
    uuids: Vec<String>,
}

#[derive(Debug, Default)]
struct ComparisonStats {
    identical: usize,
    host1_ahead: usize,
    host2_ahead: usize,
    diverged: usize,
    host1_only: usize,
    host2_only: usize,
}

fn discover_sessions(base_path: &Path) -> Result<HashMap<String, SessionInfo>> {
    let mut sessions = HashMap::new();

    for entry in WalkDir::new(base_path)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }

        let relative_path = path
            .strip_prefix(base_path)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();

        match parse_session_uuids(path) {
            Ok((entry_count, uuids)) => {
                sessions.insert(
                    relative_path.clone(),
                    SessionInfo {
                        path: path.to_path_buf(),
                        relative_path,
                        entry_count,
                        uuids,
                    },
                );
            }
            Err(e) => {
                eprintln!("Warning: Failed to parse {}: {}", path.display(), e);
            }
        }
    }

    Ok(sessions)
}

fn parse_session_uuids(path: &Path) -> Result<(usize, Vec<String>)> {
    let content = fs::read_to_string(path).context("Failed to read file")?;
    let mut uuids = Vec::new();
    let mut entry_count = 0;

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        entry_count += 1;

        // Parse JSON and extract uuid
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(uuid) = value.get("uuid").and_then(|v| v.as_str()) {
                uuids.push(uuid.to_string());
            }
        }
    }

    Ok((entry_count, uuids))
}

/// Check if vec1 is a prefix of vec2
fn is_prefix(shorter: &[String], longer: &[String]) -> bool {
    if shorter.len() > longer.len() {
        return false;
    }
    shorter.iter().zip(longer.iter()).all(|(a, b)| a == b)
}

fn compare_sessions(
    host1_sessions: &HashMap<String, SessionInfo>,
    host2_sessions: &HashMap<String, SessionInfo>,
    host1_name: &str,
    host2_name: &str,
) -> (ComparisonStats, Vec<(String, SessionInfo, SessionInfo)>) {
    let mut stats = ComparisonStats::default();
    let mut diverged_sessions = Vec::new();

    // Get all unique paths
    let all_paths: HashSet<_> = host1_sessions
        .keys()
        .chain(host2_sessions.keys())
        .collect();

    for path in all_paths {
        let host1_info = host1_sessions.get(path);
        let host2_info = host2_sessions.get(path);

        match (host1_info, host2_info) {
            (None, Some(_)) => {
                stats.host2_only += 1;
            }
            (Some(_), None) => {
                stats.host1_only += 1;
            }
            (Some(h1), Some(h2)) => {
                if h1.uuids == h2.uuids {
                    stats.identical += 1;
                } else if is_prefix(&h1.uuids, &h2.uuids) {
                    // host1 is prefix of host2 - host2 is ahead
                    stats.host2_ahead += 1;
                } else if is_prefix(&h2.uuids, &h1.uuids) {
                    // host2 is prefix of host1 - host1 is ahead
                    stats.host1_ahead += 1;
                } else {
                    // Diverged
                    stats.diverged += 1;
                    diverged_sessions.push((path.clone(), h1.clone(), h2.clone()));
                }
            }
            (None, None) => unreachable!(),
        }
    }

    (stats, diverged_sessions)
}

fn find_divergence_point(uuids1: &[String], uuids2: &[String]) -> usize {
    uuids1
        .iter()
        .zip(uuids2.iter())
        .position(|(a, b)| a != b)
        .unwrap_or(uuids1.len().min(uuids2.len()))
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: verify-sync <path1> <path2>");
        eprintln!();
        eprintln!("Compares two .claude/projects directories to verify sync status.");
        eprintln!("Sessions should be identical or one should be a prefix of the other.");
        eprintln!();
        eprintln!("Example:");
        eprintln!("  verify-sync /tmp/arm-claude /tmp/x86-claude");
        std::process::exit(1);
    }

    let path1 = PathBuf::from(&args[1]);
    let path2 = PathBuf::from(&args[2]);

    // Extract host names from paths for display
    let host1_name = path1
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "host1".to_string());
    let host2_name = path2
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "host2".to_string());

    println!("=== Claude Code Session Sync Verification ===");
    println!();

    println!("Scanning {}...", path1.display());
    let host1_sessions = discover_sessions(&path1)?;
    println!("  Found {} sessions", host1_sessions.len());

    println!("Scanning {}...", path2.display());
    let host2_sessions = discover_sessions(&path2)?;
    println!("  Found {} sessions", host2_sessions.len());

    println!();
    println!("=== Comparing Sessions ===");

    let (stats, diverged) =
        compare_sessions(&host1_sessions, &host2_sessions, &host1_name, &host2_name);

    println!();
    println!("Results:");
    println!("  ✓ Identical:      {}", stats.identical);
    println!("  → {} ahead:  {}", host1_name, stats.host1_ahead);
    println!("  ← {} ahead:  {}", host2_name, stats.host2_ahead);
    println!("  ✗ Diverged:       {}", stats.diverged);
    println!("  ◦ {} only:   {}", host1_name, stats.host1_only);
    println!("  ◦ {} only:   {}", host2_name, stats.host2_only);
    println!();

    let total_shared = stats.identical + stats.host1_ahead + stats.host2_ahead + stats.diverged;

    if stats.diverged == 0 {
        println!(
            "✅ All {} shared sessions are in sync (one is prefix of other)",
            total_shared
        );
    } else {
        println!(
            "⚠️  {} sessions have diverged histories!",
            stats.diverged
        );
        println!();
        println!("=== Diverged Session Details ===");

        for (path, h1, h2) in diverged.iter().take(10) {
            let diverge_point = find_divergence_point(&h1.uuids, &h2.uuids);

            println!();
            println!("Session: {}", path);
            println!(
                "  {} entries: {}, {} entries: {}",
                host1_name, h1.entry_count, host2_name, h2.entry_count
            );
            println!(
                "  Divergence at entry {} (0-indexed)",
                diverge_point
            );

            if diverge_point < h1.uuids.len() && diverge_point < h2.uuids.len() {
                println!(
                    "  {} UUID at divergence: {}",
                    host1_name,
                    &h1.uuids[diverge_point]
                );
                println!(
                    "  {} UUID at divergence: {}",
                    host2_name,
                    &h2.uuids[diverge_point]
                );
            }

            // Show context: entries before divergence
            if diverge_point > 0 {
                println!(
                    "  Last common UUID: {}",
                    &h1.uuids[diverge_point - 1]
                );
            }
        }

        if diverged.len() > 10 {
            println!();
            println!("... and {} more diverged sessions", diverged.len() - 10);
        }
    }

    // Exit with error code if there are diverged sessions
    if stats.diverged > 0 {
        std::process::exit(1);
    }

    Ok(())
}
