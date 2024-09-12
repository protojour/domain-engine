use ontol_examples::{demo, gitmesh};
use ontol_macros::test;
use ontol_runtime::interface::serde::{
    operator::{SerdeOperator, SerdeStructFlags},
    processor::ProcessorMode,
};
use ontol_test_utils::TestCompile;
use serde_json::json;

#[test]
fn test_demo() {
    let test = demo().1.compile();
    test.mapper().assert_map_eq(
        ("archive_group", "ArchiveCollection"),
        json!({
            "id": "archive_group/a27214f3-1bde-492f-9dc0-a47b57b4b6d3",
            "created": "1970-01-01T00:00:00.000Z",
            "updated": "1970-01-01T00:00:00.000Z",
            "archived": "1970-01-01T00:00:00.000Z",
            "name": "Lovecraft Collection",
            "description": "",
            "start_date": "1970-01-01T00:00:00.000Z",
            "end_date": "1970-01-01T00:00:00.000Z",
            "archive_items": [
                {
                    "id": "archive_item/bd2ea9f1-eb0d-4803-989c-0d29b2937ca8",
                    "created": "1970-01-01T00:00:00.000Z",
                    "updated": "1970-01-01T00:00:00.000Z",
                    "archived": "1970-01-01T00:00:00.000Z",
                    "name": "Necronomicon",
                    "description": "Ghastly book. DO NOT OPEN!!",
                    "asset_score": 5,
                    "type": "ancient treasure"
                },
                {
                    "id": "archive_item/3daff378-42dc-44ec-bf9c-b2b2c5444640",
                    "created": "1970-01-01T00:00:00.000Z",
                    "updated": "1970-01-01T00:00:00.000Z",
                    "archived": "1970-01-01T00:00:00.000Z",
                    "name": "Octopus-bat(?) bas-relief",
                    "description": "A pulpy, tentacled head surmounted a grotesque and scaly body with rudimentary wings",
                    "asset_score": 9,
                    "type": "alien artifact"
                }
            ]
        }),
        json!({
            "id": "archive_group/a27214f3-1bde-492f-9dc0-a47b57b4b6d3",
            "description": "",
            "dates": {
                "archivedAt": "1970-01-01T00:00:00Z",
                "createdAt": "1970-01-01T00:00:00Z",
                "updatedAt": "1970-01-01T00:00:00Z",
            },
            "endDate": "1970-01-01T00:00:00Z",
            "name": "Lovecraft Collection",
            "startDate": "1970-01-01T00:00:00Z",
            "archiveObjects": [
                {
                    "assetScoreTimesTen": 50,
                    "dates": {
                        "archivedAt": "1970-01-01T00:00:00Z",
                        "createdAt": "1970-01-01T00:00:00Z",
                        "updatedAt": "1970-01-01T00:00:00Z",
                    },
                    "description": "Ghastly book. DO NOT OPEN!!",
                    "id": "archive_item/bd2ea9f1-eb0d-4803-989c-0d29b2937ca8",
                    "kind": "ancient treasure",
                    "name": "Necronomicon",
                },
                {
                    "assetScoreTimesTen": 90,
                    "dates": {
                        "archivedAt": "1970-01-01T00:00:00Z",
                        "createdAt": "1970-01-01T00:00:00Z",
                        "updatedAt": "1970-01-01T00:00:00Z",
                    },
                    "description": "A pulpy, tentacled head surmounted a grotesque and scaly body with rudimentary wings",
                    "id": "archive_item/3daff378-42dc-44ec-bf9c-b2b2c5444640",
                    "kind": "alien artifact",
                    "name": "Octopus-bat(?) bas-relief",
                },
            ],
        }),
    );
}

#[test]
fn test_gitmesh() {
    let test = gitmesh().1.compile();

    let [repository, user, organization] = test.bind(["Repository", "User", "Organization"]);
    let repository_entity = repository.def.entity().unwrap();
    assert!(!repository_entity.is_self_identifying);

    let user_entity = user.def.entity().unwrap();
    assert!(!user_entity.is_self_identifying);

    let organization_entity = organization.def.entity().unwrap();
    assert!(organization_entity.is_self_identifying);

    let org_op = test
        .ontology()
        .new_serde_processor(organization.serde_operator_addr(), ProcessorMode::Create);
    let SerdeOperator::Struct(org_op) = org_op.value_operator else {
        panic!();
    };
    assert!(!org_op.flags.contains(SerdeStructFlags::PROPER_ENTITY));
}
