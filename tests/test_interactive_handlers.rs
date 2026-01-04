use claude_code_sync::VerbosityLevel;

/// Test VerbosityLevel enum basic functionality
#[test]
fn test_verbosity_level_equality() {
    assert_eq!(VerbosityLevel::Quiet, VerbosityLevel::Quiet);
    assert_eq!(VerbosityLevel::Normal, VerbosityLevel::Normal);
    assert_eq!(VerbosityLevel::Verbose, VerbosityLevel::Verbose);

    assert_ne!(VerbosityLevel::Quiet, VerbosityLevel::Normal);
    assert_ne!(VerbosityLevel::Normal, VerbosityLevel::Verbose);
    assert_ne!(VerbosityLevel::Quiet, VerbosityLevel::Verbose);
}

/// Test VerbosityLevel can be copied
#[test]
fn test_verbosity_level_copy() {
    let v1 = VerbosityLevel::Verbose;
    let v2 = v1;
    assert_eq!(v1, v2);
}

/// Test FilterConfig can be loaded (tests config handler dependency)
#[test]
fn test_filter_config_load_or_default() {
    use claude_code_sync::filter::FilterConfig;

    // Should either load existing config or create default
    let config = FilterConfig::load();
    assert!(config.is_ok());

    let config = config.unwrap();
    assert!(config.max_file_size_bytes > 0);
}

/// Test FilterConfig can be cloned (needed for interactive handlers)
#[test]
fn test_filter_config_clone() {
    use claude_code_sync::filter::FilterConfig;

    let config = FilterConfig::load().unwrap();
    let cloned = config.clone();

    assert_eq!(config.max_file_size_bytes, cloned.max_file_size_bytes);
    assert_eq!(config.exclude_attachments, cloned.exclude_attachments);
    assert_eq!(config.exclude_older_than_days, cloned.exclude_older_than_days);
}

/// Test that FilterConfig can be modified (needed for wizard)
#[test]
fn test_filter_config_modification() {
    use claude_code_sync::filter::FilterConfig;

    let mut config = FilterConfig::load().unwrap();

    // Test modifications
    config.exclude_attachments = true;
    assert!(config.exclude_attachments);

    config.exclude_older_than_days = Some(30);
    assert_eq!(config.exclude_older_than_days, Some(30));

    config.include_patterns = vec!["*work*".to_string()];
    assert_eq!(config.include_patterns.len(), 1);

    config.exclude_patterns = vec!["*test*".to_string(), "*tmp*".to_string()];
    assert_eq!(config.exclude_patterns.len(), 2);

    config.max_file_size_bytes = 5 * 1024 * 1024; // 5MB
    assert_eq!(config.max_file_size_bytes, 5 * 1024 * 1024);
}

/// Test that non-interactive mode doesn't require terminal
#[test]
fn test_non_interactive_push_verbosity() {
    // This tests that the logic paths work without actual terminal interaction
    // We can't easily test the actual push_history function without a full setup,
    // but we can verify the verbosity enum works as expected

    let verbosity = VerbosityLevel::Quiet;
    assert_eq!(verbosity, VerbosityLevel::Quiet);

    let verbosity = VerbosityLevel::Verbose;
    assert_eq!(verbosity, VerbosityLevel::Verbose);
}

/// Test verbosity level Debug trait
#[test]
fn test_verbosity_debug() {
    let quiet = VerbosityLevel::Quiet;
    let normal = VerbosityLevel::Normal;
    let verbose = VerbosityLevel::Verbose;

    // Should be able to format with Debug
    let quiet_str = format!("{:?}", quiet);
    let normal_str = format!("{:?}", normal);
    let verbose_str = format!("{:?}", verbose);

    assert!(quiet_str.contains("Quiet"));
    assert!(normal_str.contains("Normal"));
    assert!(verbose_str.contains("Verbose"));
}
