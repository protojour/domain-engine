use http::{header::CONTENT_TYPE, Request, StatusCode};
use ontol_macros::datastore_test;
use ontol_test_utils::{
    examples::stix::{stix_bundle, STIX},
    TestCompile,
};
use serde_json::json;
use tower::ServiceExt;
use tracing::info;

use crate::{
    fetch_body_assert_status, json_body, jsonlines_stream, make_domain_engine, streaming_axum_body,
    MakeTestRouter,
};

#[datastore_test(tokio::test)]
async fn test_httpjson_stix(ds: &str) {
    let test = stix_bundle().compile();
    let engine = make_domain_engine(test.ontology_owned(), ds).await;
    let router = test.make_test_router(engine, STIX.0);

    async fn put_one_url(router: &axum::Router) {
        let put_json_response = router
            .clone()
            .oneshot(
                Request::put("/stix-object")
                    .header(CONTENT_TYPE, "application/json")
                    .body(json_body(testdata::url1()))
                    .unwrap(),
            )
            .await
            .unwrap();

        fetch_body_assert_status(put_json_response, StatusCode::OK)
            .await
            .unwrap();
    }

    async fn put_jsonlines_bundle(router: &axum::Router) {
        let put_jsonlines_response = router
            .clone()
            .oneshot(
                Request::put("/stix-object")
                    .header(CONTENT_TYPE, "application/json-lines")
                    .body(streaming_axum_body(jsonlines_stream(vec![
                        testdata::identity(),
                        testdata::marking_definition(),
                        testdata::course_of_action(),
                        testdata::url2(),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        fetch_body_assert_status(put_jsonlines_response, StatusCode::OK)
            .await
            .unwrap();
    }

    info!("test json");
    put_one_url(&router).await;

    info!("test json UPDATE (UPSERT) over last PUT");
    put_one_url(&router).await;

    info!("test jsonlines");
    put_jsonlines_bundle(&router).await;

    info!("test jsonlines UPDATE UPSERT) over last PUT");
    put_jsonlines_bundle(&router).await;
}

#[datastore_test(tokio::test)]
async fn test_httpjson_stix_jsonlines_unresolved_foreign_key(ds: &str) {
    let test = stix_bundle().compile();
    let engine = make_domain_engine(test.ontology_owned(), ds).await;
    let router = test.make_test_router(engine, STIX.0);
    let put_jsonlines_response = router
        .oneshot(
            Request::put("/stix-object")
                .header(CONTENT_TYPE, "application/json-lines")
                .body(streaming_axum_body(jsonlines_stream(vec![
                    testdata::identity(),
                    testdata::course_of_action(),
                ])))
                .unwrap(),
        )
        .await
        .unwrap();

    let message =
        fetch_body_assert_status(put_jsonlines_response, StatusCode::UNPROCESSABLE_ENTITY)
            .await
            .unwrap();
    assert_eq!("{\"message\":\"unresolved foreign key: \\\"marking-definition--fa42a846-8d90-4e51-bc29-71d5b4802168\\\"\"}", message);
}

mod testdata {
    use super::*;

    pub fn identity() -> serde_json::Value {
        json!({
            "id": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
            "modified": "2017-06-01T00:00:00.000Z",
            "object_marking_refs": [
                "marking-definition--fa42a846-8d90-4e51-bc29-71d5b4802168"
            ],
            "name": "The MITRE Corporation",
            "created": "2017-06-01T00:00:00.000Z",
            "type": "identity",
            "identity_class": "organization",
            "spec_version": "2.1",
            // "x_mitre_attack_spec_version": "2.1.0",
            // "x_mitre_domains": [
            //     "mobile-attack"
            // ],
            // "x_mitre_version": "1.0"
        })
    }

    pub fn marking_definition() -> serde_json::Value {
        json!({
            "definition": {
                "statement": "Copyright 2015-2021, The MITRE Corporation. MITRE ATT&CK and ATT&CK are registered trademarks of The MITRE Corporation."
            },
            "id": "marking-definition--fa42a846-8d90-4e51-bc29-71d5b4802168",
            "definition_type": "statement",
            "created_by_ref": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
            "created": "2017-06-01T00:00:00Z",
            "type": "marking-definition",
            "spec_version": "2.1",
            // "x_mitre_attack_spec_version": "2.1.0",
            // "x_mitre_domains": [
            //     "mobile-attack"
            // ]
        })
    }

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

    pub fn url1() -> serde_json::Value {
        json!({
            "id": "url--13164076-ab7e-4cb1-8747-0624c5361edc",
            "type": "url",
            "value": "http://jøkkagnork",
            "defanged": true,
            "object_marking_refs": [],
            "granular_markings": [],
        })
    }

    pub fn url2() -> serde_json::Value {
        json!({
            "id": "url--43477621-084b-4151-8834-50986efbf51b",
            "type": "url",
            "value": "http://jøkkagnork2",
            "defanged": true,
            "object_marking_refs": [],
            "granular_markings": [],
        })
    }
}
