use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

pub trait SystemAPI {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc>;
}

/// A system API for use in tests with a fake clock.
pub struct TestSystem;

impl SystemAPI for TestSystem {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        DateTime::from_naive_utc_and_offset(
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            ),
            Utc,
        )
    }
}
