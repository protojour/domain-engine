use std::{fmt::Display, sync::Arc};

use url::Url;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct DomainUrl(Url);

#[async_trait::async_trait]
pub trait DomainUrlResolver: Send + Sync {
    async fn resolve_domain_url(&self, url: &DomainUrl) -> Option<Arc<String>>;
}

pub struct DomainUrlParser {
    base_url: url::Url,
}

pub enum DomainUrlError {
    InvalidDomainReference,
}

impl DomainUrl {
    pub fn new(url: Url) -> Self {
        Self(url)
    }

    pub fn parse(name: &str) -> Self {
        DomainUrlParser::default()
            .parse(name)
            .unwrap_or_else(|_| panic!())
    }

    pub fn short_name(&self) -> &str {
        if let Some(mut segments) = self.0.path_segments() {
            if let Some(last) = segments.next_back() {
                return last;
            }
        }

        "<none>"
    }

    pub fn url(&self) -> &Url {
        &self.0
    }

    pub fn join(&self, other: &DomainUrl) -> Self {
        match other.0.scheme() {
            "file" => {
                let mut next_url = self.0.clone();

                {
                    let mut segments = next_url.path_segments_mut().unwrap();

                    segments.pop();

                    if let Some(mut orig_segments) = other.0.path_segments() {
                        if let Some(yo) = orig_segments.next_back() {
                            segments.push(yo);
                        }
                    }
                }

                Self(next_url)
            }
            _ => other.clone(),
        }
    }

    /// Ontol Log url of this URL ("./ontol.log")
    pub fn log_url(&self) -> Option<Url> {
        let mut segments = self.0.path_segments()?.peekable();
        let mut log_path = vec![];

        while let Some(segment) = segments.next() {
            if segments.peek().is_none() {
                break;
            }
            log_path.push(segment);
        }

        log_path.push("ontol.log");

        let mut log_url = self.0.clone();
        log_url.set_path(&log_path.join("/"));

        Some(log_url)
    }

    pub fn is_log_url(&self) -> bool {
        self.0
            .path_segments()
            .and_then(Iterator::last)
            .map(|last_segment| last_segment == "ontol.log")
            .unwrap_or(false)
    }
}

impl From<Url> for DomainUrl {
    fn from(value: Url) -> Self {
        Self(value)
    }
}

impl From<DomainUrl> for Url {
    fn from(value: DomainUrl) -> Self {
        value.0
    }
}

impl Display for DomainUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for DomainUrlParser {
    fn default() -> Self {
        Self {
            base_url: url::Url::parse("file://").unwrap(),
        }
    }
}

impl DomainUrlParser {
    pub fn parse(&self, uri: &str) -> Result<DomainUrl, DomainUrlError> {
        let url = url::Url::options()
            .base_url(Some(&self.base_url))
            .parse(uri)
            .map_err(|_| DomainUrlError::InvalidDomainReference)?;

        Ok(DomainUrl(url))
    }
}

#[async_trait::async_trait]
impl DomainUrlResolver for Vec<Box<dyn DomainUrlResolver>> {
    async fn resolve_domain_url(&self, url: &DomainUrl) -> Option<Arc<String>> {
        for resolver in self.iter() {
            if let Some(source) = resolver.resolve_domain_url(url).await {
                return Some(source);
            }
        }

        None
    }
}
