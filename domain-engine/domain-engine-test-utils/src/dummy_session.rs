use std::convert::Infallible;

use axum::extract::FromRequestParts;
use domain_engine_core::Session;

pub struct DummySession;

impl FromRequestParts<()> for DummySession {
    type Rejection = Infallible;

    async fn from_request_parts(
        _req: &mut axum::http::request::Parts,
        _state: &(),
    ) -> Result<Self, Self::Rejection> {
        Ok(Self)
    }
}

impl From<DummySession> for Session {
    fn from(_value: DummySession) -> Self {
        Session::default()
    }
}
