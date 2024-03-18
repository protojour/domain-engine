use test_log::tracing_subscriber::FmtSubscriber;

pub fn init_test_tracing() {
    let __internal_event_filter = {
        use ::test_log::tracing_subscriber::fmt::format::FmtSpan;
        match ::std::env::var_os("RUST_LOG_SPAN_EVENTS") {
            Some(mut value) => {
                value.make_ascii_lowercase();
                let value = value
                    .to_str()
                    .expect("test-log: RUST_LOG_SPAN_EVENTS must be valid UTF-8");
                value
                    .split(',')
                    .map(|filter| match filter.trim() {
                        "new" => FmtSpan::NEW,
                        "enter" => FmtSpan::ENTER,
                        "exit" => FmtSpan::EXIT,
                        "close" => FmtSpan::CLOSE,
                        "active" => FmtSpan::ACTIVE,
                        "full" => FmtSpan::FULL,
                        _ => panic!(
                            "test-log: RUST_LOG_SPAN_EVENTS must contain filters separated by `,`
                            For example: `active` or `new,close`
                            Supported filters: new, enter, exit, close, active, full
                            Got: {}",
                            value
                        ),
                    })
                    .fold(FmtSpan::NONE, |acc, filter| filter | acc)
            }
            None => FmtSpan::NONE,
        }
    };
    let _ = FmtSubscriber::builder()
        .with_env_filter(::test_log::tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(__internal_event_filter)
        .with_test_writer()
        .try_init();
}
