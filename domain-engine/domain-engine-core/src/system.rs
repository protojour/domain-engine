use std::sync::Arc;

use chrono::Utc;

use crate::{DomainResult, Session};

/// Combination of system-specific functionality and configuration for the domain engine.
#[cfg_attr(feature = "unimock", unimock::unimock(api = SystemApiMock))]
#[async_trait::async_trait]
pub trait SystemAPI: Sync {
    /// Get the system's current time.
    fn current_time(&self) -> chrono::DateTime<chrono::Utc>;

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

    /// Call a HTTP JSON hook.
    /// It should return a JSON value encoded as Vec<u8>.
    async fn call_http_json_hook(
        &self,
        url: &str,
        session: Session,
        input: Vec<u8>,
    ) -> DomainResult<Vec<u8>>;
}

/// A [SystemAPI] in an [Arc].
pub type ArcSystemApi = Arc<dyn SystemAPI + Send + Sync>;

pub fn current_time() -> chrono::DateTime<chrono::Utc> {
    Utc::now()
}
