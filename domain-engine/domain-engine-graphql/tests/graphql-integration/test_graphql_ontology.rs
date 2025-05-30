use domain_engine_core::Session;
use domain_engine_graphql::{
    domain::context::ServiceCtx,
    gql_scalar::GqlScalar,
    ontology::{OntologyCtx, OntologySchema},
};
use domain_engine_test_utils::graphql_test_utils::{Exec, TestCompileSchema, ValueExt};
use juniper::{ScalarValue, graphql_value};
use ontol_examples::stix::stix_bundle;
use ontol_macros::datastore_test;
use ontol_test_utils::{TestCompile, expect_eq};
use tracing::info;

use crate::{mk_engine_default, mk_engine_with_mock};

#[derive(Default)]
pub struct OntologyParams {
    pub def_id: Option<String>,
}

impl From<OntologyParams> for juniper::Variables<GqlScalar> {
    fn from(value: OntologyParams) -> Self {
        let mut variables = juniper::Variables::default();
        if let Some(def_id) = value.def_id {
            variables.insert("defId".to_string(), def_id.to_string().into());
        }
        variables
    }
}

fn expected_defs() -> juniper::Value<GqlScalar> {
    #[derive(Default)]
    struct Builder {
        next_tag: usize,
        list: Vec<juniper::Value<GqlScalar>>,
    }

    impl Builder {
        fn add(&mut self, ident: &str, kind: &str, persistent: bool) {
            let mut obj = juniper::Object::with_capacity(2);
            obj.add_field(
                "id",
                format!(
                    "{}01GNYFZP30ED0EZ1579TH0D55P§0:{}",
                    if persistent { "" } else { "~" },
                    self.next_tag
                )
                .into(),
            );
            obj.add_field("ident", ident.into());
            obj.add_field("kind", kind.into());

            self.next_tag += 1;
            self.list.push(juniper::Value::Object(obj));
        }

        fn add_persistent(&mut self, ident: &str, kind: &str) {
            self.add(ident, kind, true);
        }

        fn add_transient(&mut self, ident: &str, kind: &str) {
            self.add(ident, kind, false);
        }

        fn skip(&mut self, n: usize) {
            self.next_tag += n;
        }

        fn reset(&mut self) {
            self.next_tag = 0;
        }
    }

    let mut b = Builder::default();

    b.add_persistent("ontol", "DOMAIN");
    b.skip(1);
    b.add_persistent("false", "DATA");
    b.add_persistent("true", "DATA");
    b.add_persistent("boolean", "DATA");
    b.skip(2);
    b.add_persistent("number", "DATA");
    b.add_persistent("integer", "DATA");
    b.add_persistent("i64", "DATA");
    b.add_persistent("float", "DATA");
    b.add_persistent("f32", "DATA");
    b.add_persistent("f64", "DATA");
    b.add_persistent("serial", "DATA");
    b.add_persistent("text", "DATA");
    b.add_persistent("octets", "DATA");
    b.add_persistent("vertex", "DATA");
    b.add_persistent("uuid", "DATA");
    b.add_persistent("ulid", "DATA");
    b.add_persistent("datetime", "DATA");
    b.skip(3);
    b.add_persistent("is", "RELATION");
    b.skip(3);
    b.add_persistent("store_key", "RELATION");
    b.add_persistent("min", "RELATION");
    b.add_persistent("max", "RELATION");
    b.add_persistent("default", "RELATION");
    b.add_persistent("gen", "RELATION");
    b.add_persistent("order", "RELATION");
    b.add_persistent("direction", "RELATION");
    b.add_persistent("example", "RELATION");
    b.skip(2);
    b.add_persistent("ascending", "DATA");
    b.add_persistent("descending", "DATA");
    b.add_persistent("auto", "GENERATOR");
    b.add_persistent("create_time", "GENERATOR");
    b.add_persistent("update_time", "GENERATOR");
    b.add_persistent("format", "DATA");
    b.skip(2);
    b.add_persistent("repr", "RELATION");
    b.add_persistent("crdt", "GENERATOR");

    b.reset();
    b.skip(6);
    b.add_transient("+", "FUNCTION");
    b.add_transient("-", "FUNCTION");
    b.add_transient("*", "FUNCTION");
    b.add_transient("/", "FUNCTION");
    b.add_transient("append", "FUNCTION");

    juniper::Value::List(b.list)
}

#[datastore_test(tokio::test)]
async fn test_ontology_ontol(ds: &str) {
    let test = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ (
        rel. name: 'test'
    )
    "
    .compile();
    let engine = mk_engine_with_mock(test.ontology_owned(), (), ds).await;
    let ontology_ctx = OntologyCtx::new(engine, Session::default());
    let ontology_schema = OntologySchema::new_with_scalar_value(
        Default::default(),
        Default::default(),
        Default::default(),
    );

    expect_eq!(
        actual = r"{ domains { id name } }"
            .exec([], &ontology_schema, &ontology_ctx)
            .await,
        expected = Ok(graphql_value!({
            "domains": [
                {
                    "id": "01GNYFZP30ED0EZ1579TH0D55P§0",
                    "name": "ontol",
                },
                {
                    "id": "7ZZZZZZZZZZTESTZZZZZZZZZZZ§0",
                    "name": "test"
                }
            ]
        }))
    );

    expect_eq!(
        actual = r#"
            {
                domain(id: "01GNYFZP30ED0EZ1579TH0D55P§0") {
                    defs {
                        id
                        ident
                        kind
                    }
                }
            }
        "#
        .exec([], &ontology_schema, &ontology_ctx)
        .await,
        expected = Ok(graphql_value!({
            "domain": {
                "defs": expected_defs()
            }
        }))
    );
}

#[datastore_test(tokio::test)]
async fn test_ontology_stix(ds: &str) {
    let (test, [stix_schema]) = stix_bundle().compile_schemas(["stix"]);
    let [identity, url] = test.bind(["identity", "url"]);

    let engine = mk_engine_default(test.ontology_owned(), ds).await;
    let domain_ctx: ServiceCtx = engine.clone().into();
    let ontology_ctx = OntologyCtx::new(engine, Session::default());
    let ontology_schema = OntologySchema::new_with_scalar_value(
        Default::default(),
        Default::default(),
        Default::default(),
    );

    // insert Stix "identity" through domain schema
    r#"mutation {
        identity(create:[
            {
                id: "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                modified: "2017-06-01T00:00:00.000Z",
                object_marking_refs: [],
                name: "The MITRE Corporation",
                created: "2017-06-01T00:00:00.000Z",
                type: "identity",
                identity_class: "organization",
                spec_version: "2.1",
                roles: ["roleA", "roleB"],
                external_references: [
                    {
                        source_name: "mitre-mobile-attack",
                        url: "https://attack.mitre.org/mobile/index.php/Technique/MOB-T1056",
                        external_id: "MOB-T1056"
                    },
                ],
            }
        ]) {
            node { id }
        }
    }"#
    .exec([], &stix_schema, &domain_ctx)
    .await
    .unwrap();

    r#"mutation {
        url(create:[
            {
                type: "url"
                id: "url--6bd10f1e-0c23-4278-8349-19d27c46817c"
                value: "http://first"
                defanged: true
                object_marking_refs: []
                granular_markings: []
            },
            {
                type: "url"
                id: "url--c28dfe9f-aa02-4dc5-81ee-b69da50f2cc9"
                value: "http://second"
                defanged: true
                object_marking_refs: []
                granular_markings: []
            }
        ]) {
            node { defanged }
        }
    }"#
    .exec([], &stix_schema, &domain_ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r"{ domains { id name } }"
            .exec([], &ontology_schema, &ontology_ctx)
            .await,
        expected = Ok(graphql_value!({
            "domains": [
                {
                    "id": "01GNYFZP30ED0EZ1579TH0D55P§0",
                    "name": "ontol",
                },
                {
                    "id": "01GZ13EAM0RY693MSJ75XZARHY§0",
                    "name": "stix",
                },
                {
                    "id": "01J5C5GB45Q3C0E4YYRPP8SN4R§0",
                    "name": "stix_common",
                },
                {
                    "id": "01J6SACJ2A3P8FNEQ2S20M3DV7§0",
                    "name": "stix_edges",
                },
                {
                    "id": "01H7ZN6PJ0NHEFMHW2PBN4FEPZ§0",
                    "name": "stix_interface",
                },
                {
                    "id": "01J5C5JJKM7XWR56TTYH9B7VRN§0",
                    "name": "stix_open_vocab",
                },
                {
                    "id": "01H7ZZ5KMG538CXJMNJQVTMP9K§0",
                    "name": "SI",
                },
            ]
        }))
    );

    expect_eq!(
        actual = r"
            query def($defId: DefId!) {
                def(defId: $defId) {
                    id
                    kind
                    dataRelationships {
                        propId
                        name
                    }
                }
            }
        "
        .exec(
            OntologyParams {
                def_id: Some(identity.graphql_def_id())
            },
            &ontology_schema,
            &ontology_ctx
        )
        .await,
        expected = Ok(graphql_value!({
            "def": {
                "id": "01GZ13EAM0RY693MSJ75XZARHY§0:54",
                "kind": "ENTITY",
                "dataRelationships": [
                    { "propId": "p@1:54:0", "name": "type" },
                    { "propId": "p@1:54:1", "name": "id" },
                    { "propId": "p@1:54:2", "name": "spec_version" },
                    { "propId": "p@1:54:3", "name": "created" },
                    { "propId": "p@1:54:4", "name": "modified" },
                    { "propId": "p@1:54:5", "name": "confidence" },
                    { "propId": "p@1:54:6", "name": "revoked" },
                    { "propId": "p@1:54:7", "name": "labels" },
                    { "propId": "p@1:54:8", "name": "lang" },
                    { "propId": "p@1:54:9", "name": "external_references" },
                    { "propId": "p@1:54:10", "name": "created_by_ref" },
                    { "propId": "p@1:54:11", "name": "object_marking_refs" },
                    { "propId": "p@1:54:12", "name": "granular_markings" },
                    { "propId": "p@1:54:13", "name": "name" },
                    { "propId": "p@1:54:14", "name": "description" },
                    { "propId": "p@1:54:15", "name": "roles" },
                    { "propId": "p@1:54:16", "name": "identity_class" },
                    { "propId": "p@1:54:17", "name": "sectors" },
                    { "propId": "p@1:54:18", "name": "contact_information" },
                ]
            }
        }))
    );

    info!("vertices");
    expect_eq!(
        actual = r#"
            query vertices($defId: DefId!) {
                vertices(
                    defId: $defId,
                    first: 100,
                    withAddress: false,
                    withDefId: false
                ) {
                    elements
                }
            }
            "#
        .exec(
            OntologyParams {
                def_id: Some(identity.graphql_def_id()),
            },
            &ontology_schema,
            &ontology_ctx,
        )
        .await,
        expected = Ok(graphql_value!({
            "vertices": {
                "elements": [
                    {
                        "type": "struct",
                        "attrs": [
                            {
                                "propId": "p@1:54:0",
                                "attr": "unit",
                                "type": "text",
                                "value": "identity",
                            },
                            {
                                "propId": "p@1:54:1",
                                "attr": "unit",
                                "type": "text",
                                "value": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                            },
                            {
                                "propId": "p@1:54:2",
                                "attr": "unit",
                                "type": "text",
                                "value": "2.1",
                            },
                            {
                                "propId": "p@1:54:3",
                                "attr": "unit",
                                "type": "datetime",
                                "value": "2017-06-01T00:00:00Z",
                            },
                            {
                                "propId": "p@1:54:4",
                                "attr": "unit",
                                "type": "datetime",
                                "value": "2017-06-01T00:00:00Z",
                            },
                            {
                                "propId": "p@1:54:9",
                                "attr": "matrix",
                                "columns": [
                                    [
                                        {
                                            "type": "struct",
                                            "attrs": [
                                                {
                                                    "propId": "p@2:11:0",
                                                    "attr": "unit",
                                                    "type": "text",
                                                    "value": "mitre-mobile-attack"
                                                },
                                                {
                                                    "propId": "p@2:11:2",
                                                    "attr": "unit",
                                                    "type": "text",
                                                    "value": "https://attack.mitre.org/mobile/index.php/Technique/MOB-T1056"
                                                },
                                                {
                                                    "propId": "p@2:11:4",
                                                    "attr": "unit",
                                                    "type": "text",
                                                    "value": "MOB-T1056"
                                                },
                                            ]
                                        }
                                    ]
                                ]
                            },
                            {
                                "propId": "p@1:54:13",
                                "attr": "unit",
                                "type": "text",
                                "value": "The MITRE Corporation"
                            },
                            {
                                "propId": "p@1:54:15",
                                "attr": "matrix",
                                "columns": [
                                    [
                                        { "type": "text", "value": "roleA" },
                                        { "type": "text", "value": "roleB" },
                                    ]
                                ]
                            },
                            {
                                "propId": "p@1:54:16",
                                "attr": "unit",
                                "type": "text",
                                "value": "organization"
                            },
                        ]
                    }
                ]
            }
        }))
    );

    info!("vertices (identity) with address");
    {
        let vertices_with_address = r#"
            query vertices($defId: DefId!) {
                vertices(defId: $defId, first: 100) {
                    elements
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            "#
        .exec(
            OntologyParams {
                def_id: Some(identity.graphql_def_id()),
            },
            &ontology_schema,
            &ontology_ctx,
        )
        .await
        .unwrap();

        let address = vertices_with_address
            .field("vertices")
            .field("elements")
            .element(0)
            .field("address");
        let juniper::Value::Scalar(GqlScalar::String(address)) = address else {
            panic!("address was not a string: {address:?}");
        };
        info!("address is `{address}`");
    }

    info!("vertices (url) ordered by updated");
    // FIXME: Sort by update timestamp not implemented for inmemory yet
    if ds != "inmemory" {
        let update_time_prop_id = {
            let ontol_update_time = r#"
            {
                domain(id: "01GNYFZP30ED0EZ1579TH0D55P§0") {
                    defs(ident: "update_time") {
                        propId
                    }
                }
            }
            "#
            .exec([], &ontology_schema, &ontology_ctx)
            .await
            .unwrap();

            ontol_update_time
                .field("domain")
                .field("defs")
                .element(0)
                .field("propId")
                .as_scalar_value()
                .unwrap()
                .to_string()
        };

        expect_eq!(
            actual = format!(
                r#"
                query vertices($defId: DefId!) {{
                    vertices(
                        defId: $defId,
                        first: 100,
                        withAddress: false,
                        withDefId: false,
                        order: [{{
                            fieldPaths: ["{update_time_prop_id}"],
                            direction: DESCENDING
                        }}]
                    ) {{
                        elements
                    }}
                }}
                "#
            )
            .exec(
                OntologyParams {
                    def_id: Some(url.graphql_def_id()),
                },
                &ontology_schema,
                &ontology_ctx,
            )
            .await,
            expected = Ok(graphql_value!({
                "vertices": {
                    "elements": [
                        {
                            "type": "struct",
                            "attrs": [
                                { "propId": "p@1:83:0", "attr": "unit", "type": "text", "value": "url" },
                                { "propId": "p@1:83:1", "attr": "unit", "type": "text", "value": "url--c28dfe9f-aa02-4dc5-81ee-b69da50f2cc9" },
                                { "propId": "p@1:83:2", "attr": "unit", "type": "i64", "value": "1" },
                                { "propId": "p@1:83:5", "attr": "unit", "type": "text", "value": "http://second" },
                            ]
                        },
                        {
                            "type": "struct",
                            "attrs": [
                                { "propId": "p@1:83:0", "attr": "unit", "type": "text", "value": "url" },
                                { "propId": "p@1:83:1", "attr": "unit", "type": "text", "value": "url--6bd10f1e-0c23-4278-8349-19d27c46817c" },
                                { "propId": "p@1:83:2", "attr": "unit", "type": "i64", "value": "1" },
                                { "propId": "p@1:83:5", "attr": "unit", "type": "text", "value": "http://first" },
                            ]
                        },
                    ]
                }
            }))
        );

        info!("fetch with addresses");
        let with_address = r#"
            query vertices($defId: DefId!) {
                vertices(
                    defId: $defId,
                    first: 100,
                    withAddress: true,
                    withDefId: false,
                    withAttrs: false,
                ) {
                    elements
                }
            }
            "#
        .exec(
            OntologyParams {
                def_id: Some(url.graphql_def_id()),
            },
            &ontology_schema,
            &ontology_ctx,
        )
        .await
        .unwrap();

        let first_address = with_address
            .field("vertices")
            .field("elements")
            .element(0)
            .field("address")
            .scalar()
            .as_string()
            .unwrap();

        info!("filter by address");
        let filtered_by_address = format!(
            r#"
            query vertices($defId: DefId!) {{
                vertices(
                    defId: $defId,
                    first: 100,
                    addresses: ["{first_address}"],
                    withAddress: true,
                    withDefId: false,
                    withAttrs: false,
                ) {{
                    elements
                }}
            }}
            "#
        )
        .exec(
            OntologyParams {
                def_id: Some(url.graphql_def_id()),
            },
            &ontology_schema,
            &ontology_ctx,
        )
        .await
        .unwrap();

        let vertices = filtered_by_address
            .field("vertices")
            .field("elements")
            .as_list_value()
            .unwrap();
        assert_eq!(vertices.len(), 1);
    }

    info!("vertices (url) with updated timestamp");
    // FIXME: Domain-external properties and standard timestamps not implemented for inmemory yet
    if ds != "inmemory" {
        expect_eq!(
            actual = r#"
                query vertices($defId: DefId!) {
                    vertices(
                        defId: $defId,
                        first: 100,
                        withAddress: false,
                        withDefId: false,
                        withUpdateTime: true,
                    ) {
                        elements
                    }
                }
            "#
            .exec(
                OntologyParams {
                    def_id: Some(url.graphql_def_id()),
                },
                &ontology_schema,
                &ontology_ctx,
            )
            .await,
            expected = Ok(graphql_value!({
                "vertices": {
                    "elements": [
                        {
                            "update_time": "1974-01-01T00:00:00Z",
                            "type": "struct",
                            "attrs": [
                                { "propId": "p@1:83:0", "attr": "unit", "type": "text", "value": "url" },
                                { "propId": "p@1:83:1", "attr": "unit", "type": "text", "value": "url--6bd10f1e-0c23-4278-8349-19d27c46817c" },
                                { "propId": "p@1:83:2", "attr": "unit", "type": "i64", "value": "1" },
                                { "propId": "p@1:83:5", "attr": "unit", "type": "text", "value": "http://first" },
                            ]
                        },
                        {
                            "update_time": "1975-01-01T00:00:00Z",
                            "type": "struct",
                            "attrs": [
                                { "propId": "p@1:83:0", "attr": "unit", "type": "text", "value": "url" },
                                { "propId": "p@1:83:1", "attr": "unit", "type": "text", "value": "url--c28dfe9f-aa02-4dc5-81ee-b69da50f2cc9" },
                                { "propId": "p@1:83:2", "attr": "unit", "type": "i64", "value": "1" },
                                { "propId": "p@1:83:5", "attr": "unit", "type": "text", "value": "http://second" },
                            ]
                        },
                    ]
                }
            }))
        );
    }

    info!("filter updated after");
    // FIXME: Domain-external properties and standard timestamps not implemented for inmemory yet
    if ds != "inmemory" {
        expect_eq!(
            actual = r#"
                query vertices($defId: DefId!) {
                    vertices(
                        defId: $defId,
                        first: 100,
                        withAddress: false,
                        withDefId: false,
                        withUpdateTime: true,
                        updatedAfter: "1974-01-01T00:00:00Z"
                    ) {
                        elements
                    }
                }
            "#
            .exec(
                OntologyParams {
                    def_id: Some(url.graphql_def_id()),
                },
                &ontology_schema,
                &ontology_ctx,
            )
            .await,
            expected = Ok(graphql_value!({
                "vertices": {
                    "elements": [
                        {
                            "update_time": "1975-01-01T00:00:00Z",
                            "type": "struct",
                            "attrs": [
                                { "propId": "p@1:83:0", "attr": "unit", "type": "text", "value": "url" },
                                { "propId": "p@1:83:1", "attr": "unit", "type": "text", "value": "url--c28dfe9f-aa02-4dc5-81ee-b69da50f2cc9" },
                                { "propId": "p@1:83:2", "attr": "unit", "type": "i64", "value": "1" },
                                { "propId": "p@1:83:5", "attr": "unit", "type": "text", "value": "http://second" },
                            ]
                        },
                    ]
                }
            }))
        );
    }
}
