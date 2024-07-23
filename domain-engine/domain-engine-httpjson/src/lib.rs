use std::sync::Arc;

use domain_engine_core::DomainEngine;
use ontol_runtime::{interface::DomainInterface, PackageId};

pub fn create_httpjson_router(
    engine: Arc<DomainEngine>,
    package_id: PackageId,
) -> Option<axum::Router> {
    let _httpjson = engine
        .ontology_owned()
        .domain_interfaces(package_id)
        .iter()
        .filter_map(|interface| match interface {
            DomainInterface::HttpJson(httpjson) => Some(httpjson),
            _ => None,
        })
        .next()?;

    None
}
