use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

pub trait SystemAPI {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc>;
}

/// A system API for use in tests with a fake clock.
pub struct TestSystem {
    year_counter: Arc<Mutex<i32>>,
}

impl Default for TestSystem {
    fn default() -> Self {
        Self {
            year_counter: Arc::new(Mutex::new(1970)),
        }
    }
}

impl SystemAPI for TestSystem {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        let year = {
            let mut lock = self.year_counter.lock().unwrap();
            let year = *lock;
            *lock += 1;
            year
        };

        DateTime::from_naive_utc_and_offset(
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(year, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            ),
            Utc,
        )
    }
}
