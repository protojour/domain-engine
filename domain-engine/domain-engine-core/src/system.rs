use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

/// Combination of system-specific functionality and configuration for the domain engine.
pub trait SystemAPI: Sync {
    /// Get the system's current time.
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    /// Generate a new [uuid::Uuid].
    ///
    /// The default implementation uses UUID V7 (SortRand),
    /// because this is used in a database context.
    fn generate_uuid(&self) -> uuid::Uuid {
        uuid::Uuid::now_v7()
    }

    /// Get the configured default limit of returned elements from a data store query.
    fn default_query_limit(&self) -> usize {
        20
    }
}

/// A [SystemAPI] in an [Arc].
pub type ArcSystemApi = Arc<dyn SystemAPI + Send + Sync>;

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
