use anyhow::{Context, Result};
use log::LevelFilter;
use std::fs::OpenOptions;
use std::io::Write;

use crate::config::ConfigManager;

/// Initialize the logging system
///
/// Sets up logging to both console and a log file in the config directory.
///
/// **Console logging** can be controlled via the `RUST_LOG` environment variable:
/// - `RUST_LOG=error` - Only errors
/// - `RUST_LOG=warn` - Warnings and errors
/// - `RUST_LOG=info` - Info, warnings, and errors (default)
/// - `RUST_LOG=debug` - Debug and above
/// - `RUST_LOG=trace` - Everything
///
/// **File logging** always captures all levels and is stored at:
/// - Linux: ~/.config/claude-code-sync/claude-code-sync.log or $XDG_CONFIG_HOME/claude-code-sync/claude-code-sync.log
/// - macOS: ~/Library/Application Support/claude-code-sync/claude-code-sync.log
/// - Windows: %APPDATA%\claude-code-sync\claude-code-sync.log
///
/// ## Examples
///
/// ```bash
/// # Show all debug messages on console
/// RUST_LOG=debug claude-code-sync sync
///
/// # Only show errors on console
/// RUST_LOG=error claude-code-sync push
///
/// # No console output (file logging continues)
/// RUST_LOG=off claude-code-sync pull
/// ```
pub fn init_logger() -> Result<()> {
    // Ensure config directory exists
    ConfigManager::ensure_config_dir()?;

    // Determine if console logging should be enabled
    // By default, use Info level unless RUST_LOG is set
    let default_level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|s| s.parse::<LevelFilter>().ok())
        .unwrap_or(LevelFilter::Info);

    // Initialize env_logger with custom format
    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{:5}] {}",
                chrono::Local::now().format("%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter_level(default_level)
        .target(env_logger::Target::Stdout)
        .try_init()
        .ok(); // Ignore error if logger is already initialized

    // Also log initialization to file
    log_to_file(&format!(
        "Logger initialized with level: {default_level:?}"
    ))?;

    Ok(())
}

/// Log to file only (useful for background operations or detailed logging)
pub fn log_to_file(message: &str) -> Result<()> {
    let log_path = ConfigManager::log_file_path()?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("Failed to open log file: {}", log_path.display()))?;

    writeln!(
        file,
        "[{}] {}",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
        message
    )?;

    Ok(())
}

/// Rotate log file if it exceeds the size limit (default: 10MB)
pub fn rotate_log_if_needed() -> Result<()> {
    const MAX_LOG_SIZE: u64 = 10 * 1024 * 1024; // 10MB

    let log_path = ConfigManager::log_file_path()?;

    // Check if log file exists and its size
    if log_path.exists() {
        let metadata = std::fs::metadata(&log_path)?;

        if metadata.len() > MAX_LOG_SIZE {
            // Rotate: rename current log to .old and start fresh
            let old_log_path = log_path.with_extension("log.old");

            // Remove old backup if it exists
            if old_log_path.exists() {
                std::fs::remove_file(&old_log_path)?;
            }

            // Rename current log to .old
            std::fs::rename(&log_path, &old_log_path)?;

            log::info!("Log file rotated to {}", old_log_path.display());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::file_serial;
    use std::fs::File;

    #[test]
    #[file_serial]
    fn test_init_logger_succeeds() {
        // Set up isolated test environment using HOME
        let temp_dir = tempfile::TempDir::new().unwrap();
        let original_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", temp_dir.path());

        // Should not panic - may fail if logger already initialized in process
        let result = init_logger();
        // Either succeeds or fails with "already initialized" which is fine
        if let Err(e) = &result {
            let err_str = e.to_string();
            // env_logger fails with SetLoggerError if already initialized
            assert!(
                result.is_ok() || err_str.contains("logger") || err_str.contains("initialized"),
                "Unexpected error: {}",
                err_str
            );
        }

        // Restore HOME
        if let Some(home) = original_home {
            std::env::set_var("HOME", home);
        } else {
            std::env::remove_var("HOME");
        }
    }

    #[test]
    #[file_serial]
    fn test_log_to_file() -> Result<()> {
        // Set up isolated test environment using HOME to isolate on all platforms
        let temp_dir = tempfile::TempDir::new()?;
        let original_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", temp_dir.path());

        // Ensure config directory exists
        ConfigManager::ensure_config_dir()?;

        log_to_file("Test log message")?;

        let log_path = ConfigManager::log_file_path()?;
        assert!(log_path.exists());

        let contents = std::fs::read_to_string(&log_path)?;
        assert!(contents.contains("Test log message"));

        // Restore HOME
        if let Some(home) = original_home {
            std::env::set_var("HOME", home);
        } else {
            std::env::remove_var("HOME");
        }

        Ok(())
    }

    #[test]
    #[file_serial]
    fn test_rotate_log_creates_backup() -> Result<()> {
        // Set up isolated test environment using HOME to isolate on all platforms
        let temp_dir = tempfile::TempDir::new()?;
        let original_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", temp_dir.path());

        // Ensure config directory exists first
        ConfigManager::ensure_config_dir()?;

        // Get the log path after setting env var
        let log_path = ConfigManager::log_file_path()?;
        let mut file = File::create(&log_path)?;

        // Write 11MB of data
        let data = vec![b'a'; 11 * 1024 * 1024];
        file.write_all(&data)?;
        file.sync_all()?;
        drop(file);

        // Verify file exists and is large before rotation
        assert!(log_path.exists(), "Log file should exist before rotation");
        let size = std::fs::metadata(&log_path)?.len();
        assert!(size > 10 * 1024 * 1024, "Log file should be > 10MB");

        // Rotate
        rotate_log_if_needed()?;

        // Check that .old file was created
        let old_log_path = log_path.with_extension("log.old");
        assert!(old_log_path.exists(), "Old log file should exist after rotation");

        // Original log should be fresh (or not exist)
        if log_path.exists() {
            let metadata = std::fs::metadata(&log_path)?;
            assert!(metadata.len() < 11 * 1024 * 1024);
        }

        // Restore HOME
        if let Some(home) = original_home {
            std::env::set_var("HOME", home);
        } else {
            std::env::remove_var("HOME");
        }

        Ok(())
    }
}
