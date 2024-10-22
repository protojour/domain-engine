use axum::body::Body;
use axum::response::IntoResponse;
use http::{header::CONTENT_TYPE, StatusCode};

#[derive(Clone, Copy, Debug)]
pub enum JsonContentType {
    Json,
    JsonLines,
}

impl JsonContentType {
    pub fn parse(req: &http::Request<Body>) -> Result<Self, MissingJsonContentType> {
        let content_type = if let Some(content_type) = req.headers().get(CONTENT_TYPE) {
            content_type
        } else {
            return Err(MissingJsonContentType);
        };
        let content_type = if let Ok(content_type) = content_type.to_str() {
            content_type
        } else {
            return Err(MissingJsonContentType);
        };
        let mime = if let Ok(mime) = content_type.parse::<mime::Mime>() {
            mime
        } else {
            return Err(MissingJsonContentType);
        };

        if mime.type_() != "application" {
            return Err(MissingJsonContentType);
        }

        match Self::from_subtype(mime.subtype().as_str()) {
            Ok(t) => Ok(t),
            Err(err) => {
                if let Some(suffix) = mime.suffix() {
                    Self::from_subtype(suffix.as_str())
                } else {
                    Err(err)
                }
            }
        }
    }

    fn from_subtype(subtype: &str) -> Result<Self, MissingJsonContentType> {
        match subtype {
            "json" => Ok(Self::Json),
            "json-lines" => Ok(Self::JsonLines),
            _ => Err(MissingJsonContentType),
        }
    }
}

pub struct MissingJsonContentType;

impl MissingJsonContentType {
    pub fn into_response(self) -> axum::response::Response {
        (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Expected a json content type",
        )
            .into_response()
    }
}
