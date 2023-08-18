use ontol_test_utils::{examples::DEMO, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn test_demo() {
    DEMO.1.compile_ok(|test| {
        test.assert_domain_map(
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
                    "archivedAt": "1970-01-01T00:00:00+00:00",
                    "createdAt": "1970-01-01T00:00:00+00:00",
                    "updatedAt": "1970-01-01T00:00:00+00:00",
                },
                "endDate": "1970-01-01T00:00:00+00:00",
                "name": "Lovecraft Collection",
                "startDate": "1970-01-01T00:00:00+00:00",
                "archiveObjects": [
                    {
                        "assetScoreTimesTen": 50,
                        "dates": {
                            "archivedAt": "1970-01-01T00:00:00+00:00",
                            "createdAt": "1970-01-01T00:00:00+00:00",
                            "updatedAt": "1970-01-01T00:00:00+00:00",
                        },
                        "description": "Ghastly book. DO NOT OPEN!!",
                        "id": "archive_item/bd2ea9f1-eb0d-4803-989c-0d29b2937ca8",
                        "kind": "ancient treasure",
                        "name": "Necronomicon",
                    },
                    {
                        "assetScoreTimesTen": 90,
                        "dates": {
                            "archivedAt": "1970-01-01T00:00:00+00:00",
                            "createdAt": "1970-01-01T00:00:00+00:00",
                            "updatedAt": "1970-01-01T00:00:00+00:00",
                        },
                        "description": "A pulpy, tentacled head surmounted a grotesque and scaly body with rudimentary wings",
                        "id": "archive_item/3daff378-42dc-44ec-bf9c-b2b2c5444640",
                        "kind": "alien artifact",
                        "name": "Octopus-bat(?) bas-relief",
                    },
                ],
            }),
        );
    });
}
