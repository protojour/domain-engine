use http::{header::CONTENT_TYPE, Request, StatusCode};
use ontol_macros::datastore_test;
use ontol_test_utils::{
    examples::stix::{stix_bundle, STIX},
    TestCompile,
};
use serde_json::json;
use tower::ServiceExt;
use tracing::info;

use crate::{assert_status, json_body, jsonlines_body, make_domain_engine, MakeTestRouter};

#[datastore_test(tokio::test)]
async fn test_httpjson_stix(ds: &str) {
    let test = stix_bundle().compile();
    let engine = make_domain_engine(test.ontology_owned(), ds).await;
    let router = test.make_test_router(engine, STIX.0);

    info!("test json");
    let put_json_response = router
        .clone()
        .oneshot(
            Request::put("/stix-object")
                .header(CONTENT_TYPE, "application/json")
                .body(json_body(testdata::course_of_action()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_status(put_json_response, StatusCode::OK)
        .await
        .unwrap();

    info!("test jsonlines");
    let put_jsonlines_response = router
        .oneshot(
            Request::put("/stix-object")
                .header(CONTENT_TYPE, "application/jsonlines")
                .body(jsonlines_body(vec![
                    testdata::course_of_action(),
                    testdata::url(),
                ]))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_status(put_jsonlines_response, StatusCode::OK)
        .await
        .unwrap();
}

mod testdata {
    use super::*;

    pub fn course_of_action() -> serde_json::Value {
        json!({
            "type": "course-of-action",
            "id": "course-of-action--0beabf44-e8d8-4ae4-9122-ef56369a2564",
            "created_by_ref": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
            "created": "2017-10-25T14:48:51.657Z",
            "modified": "2018-01-17T12:56:55.080Z",
            "name": "Use Recent OS Version",
            "description": "New mobile operating system versions bring not only patches against discovered vulnerabilities but also often bring security architecture improvements that provide resilience against potential vulnerabilities or weaknesses that have not yet been discovered. They may also bring improvements that block use of observed adversary techniques.",
            "external_references": [
                {
                    "source_name": "mitre-attack-mobile",
                    "url": "https://attack.mitre.org/mobile/index.php/Mitigation/MOB-M1006",
                    "external_id": "MOB-M1006"
                }
            ],
            "object_marking_refs": ["marking-definition--fa42a846-8d90-4e51-bc29-71d5b4802168"],
            "spec_version": "2.1",
        })
    }

    pub fn url() -> serde_json::Value {
        json!({
            "id": "url--13164076-ab7e-4cb1-8747-0624c5361edc",
            "type": "url",
            "value": "http://jøkkagnork",
            "defanged": true,
            "object_marking_refs": [],
            "granular_markings": [],
        })
    }
}
