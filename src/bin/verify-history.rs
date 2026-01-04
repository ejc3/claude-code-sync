//! Verify history.jsonl sync between two .claude directories
//!
//! Compares history.jsonl files to ensure they contain the same entries
//! (same sessionId + timestamp pairs).

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(Debug, Clone)]
struct HistoryEntry {
    session_id: String,
    timestamp: i64,
    display: String,
    project: String,
}

#[derive(Debug, Default)]
struct ComparisonStats {
    identical: usize,
    host1_only: usize,
    host2_only: usize,
}

fn parse_history_file(path: &Path) -> Result<Vec<HistoryEntry>> {
    let file = fs::File::open(path).context("Failed to open history.jsonl")?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
            let session_id = value
                .get("sessionId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let timestamp = value.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
            let display = value
                .get("display")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let project = value
                .get("project")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if !session_id.is_empty() {
                entries.push(HistoryEntry {
                    session_id,
                    timestamp,
                    display,
                    project,
                });
            }
        }
    }

    Ok(entries)
}

fn compare_histories(
    host1_entries: &[HistoryEntry],
    host2_entries: &[HistoryEntry],
    host1_name: &str,
    host2_name: &str,
) -> (ComparisonStats, Vec<HistoryEntry>, Vec<HistoryEntry>) {
    let mut stats = ComparisonStats::default();

    // Build sets of (sessionId, timestamp) tuples
    let host1_set: HashSet<(String, i64)> = host1_entries
        .iter()
        .map(|e| (e.session_id.clone(), e.timestamp))
        .collect();

    let host2_set: HashSet<(String, i64)> = host2_entries
        .iter()
        .map(|e| (e.session_id.clone(), e.timestamp))
        .collect();

    // Build lookup maps for details
    let host1_map: HashMap<(String, i64), &HistoryEntry> = host1_entries
        .iter()
        .map(|e| ((e.session_id.clone(), e.timestamp), e))
        .collect();

    let host2_map: HashMap<(String, i64), &HistoryEntry> = host2_entries
        .iter()
        .map(|e| ((e.session_id.clone(), e.timestamp), e))
        .collect();

    let mut host1_only_entries = Vec::new();
    let mut host2_only_entries = Vec::new();

    // Find entries in both
    for key in host1_set.intersection(&host2_set) {
        stats.identical += 1;
    }

    // Find entries only in host1
    for key in host1_set.difference(&host2_set) {
        stats.host1_only += 1;
        if let Some(entry) = host1_map.get(key) {
            host1_only_entries.push((*entry).clone());
        }
    }

    // Find entries only in host2
    for key in host2_set.difference(&host1_set) {
        stats.host2_only += 1;
        if let Some(entry) = host2_map.get(key) {
            host2_only_entries.push((*entry).clone());
        }
    }

    (stats, host1_only_entries, host2_only_entries)
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: verify-history <path1/history.jsonl> <path2/history.jsonl>");
        eprintln!();
        eprintln!("Compares two history.jsonl files to verify sync status.");
        eprintln!("Entries are matched by (sessionId, timestamp) tuple.");
        eprintln!();
        eprintln!("Example:");
        eprintln!("  verify-history /tmp/arm-history.jsonl /tmp/x86-history.jsonl");
        std::process::exit(1);
    }

    let path1 = Path::new(&args[1]);
    let path2 = Path::new(&args[2]);

    // Extract names from paths
    let host1_name = path1
        .file_stem()
        .or_else(|| path1.parent().and_then(|p| p.file_name()))
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "host1".to_string());
    let host2_name = path2
        .file_stem()
        .or_else(|| path2.parent().and_then(|p| p.file_name()))
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "host2".to_string());

    println!("=== Claude Code History.jsonl Verification ===");
    println!();

    println!("Loading {}...", path1.display());
    let host1_entries = parse_history_file(path1)?;
    let host1_sessions: HashSet<_> = host1_entries.iter().map(|e| &e.session_id).collect();
    println!(
        "  {} entries, {} unique sessions",
        host1_entries.len(),
        host1_sessions.len()
    );

    println!("Loading {}...", path2.display());
    let host2_entries = parse_history_file(path2)?;
    let host2_sessions: HashSet<_> = host2_entries.iter().map(|e| &e.session_id).collect();
    println!(
        "  {} entries, {} unique sessions",
        host2_entries.len(),
        host2_sessions.len()
    );

    println!();
    println!("=== Comparing Entries ===");

    let (stats, host1_only, host2_only) =
        compare_histories(&host1_entries, &host2_entries, &host1_name, &host2_name);

    println!();
    println!("Results:");
    println!("  ✓ Identical:    {}", stats.identical);
    println!("  ◦ {} only: {}", host1_name, stats.host1_only);
    println!("  ◦ {} only: {}", host2_name, stats.host2_only);
    println!();

    if stats.host1_only == 0 && stats.host2_only == 0 {
        println!(
            "✅ All {} entries are identical between both hosts",
            stats.identical
        );
    } else {
        println!(
            "⚠️  {} entries differ between hosts",
            stats.host1_only + stats.host2_only
        );

        if !host1_only.is_empty() {
            println!();
            println!("=== Entries only in {} (first 10) ===", host1_name);
            for entry in host1_only.iter().take(10) {
                let display_truncated: String = entry.display.chars().take(50).collect();
                println!(
                    "  {} | {} | {}",
                    &entry.session_id[..8],
                    entry.timestamp,
                    display_truncated
                );
            }
            if host1_only.len() > 10 {
                println!("  ... and {} more", host1_only.len() - 10);
            }
        }

        if !host2_only.is_empty() {
            println!();
            println!("=== Entries only in {} (first 10) ===", host2_name);
            for entry in host2_only.iter().take(10) {
                let display_truncated: String = entry.display.chars().take(50).collect();
                println!(
                    "  {} | {} | {}",
                    &entry.session_id[..8],
                    entry.timestamp,
                    display_truncated
                );
            }
            if host2_only.len() > 10 {
                println!("  ... and {} more", host2_only.len() - 10);
            }
        }

        // Show session-level summary
        println!();
        println!("=== Session Summary ===");

        let host1_session_set: HashSet<_> = host1_only.iter().map(|e| &e.session_id).collect();
        let host2_session_set: HashSet<_> = host2_only.iter().map(|e| &e.session_id).collect();

        println!(
            "  Sessions only in {}: {}",
            host1_name,
            host1_session_set.len()
        );
        println!(
            "  Sessions only in {}: {}",
            host2_name,
            host2_session_set.len()
        );
    }

    // Exit with error if there are differences
    if stats.host1_only > 0 || stats.host2_only > 0 {
        std::process::exit(1);
    }

    Ok(())
}
