use ontol_examples::workspaces;
use ontol_runtime::{debug::OntolDebug, ontology::domain::DefRepr};
use ontol_test_utils::{TestCompile, assert_json_io_matches, serde_helper::serde_create};
use serde_json::json;

#[test]
fn test_workspaces_serde() {
    let test = workspaces().1.compile();
    let [workspace, file] = test.bind(["Workspace", "File"]);

    assert_json_io_matches!(serde_create(&workspace), {
        "data": {
            "title": "untitled",
            "files": [
                {
                    "path": "README.md",
                    "contents": "# Hello workspace"
                },
                {
                    "path": "src/lib.rs",
                    "contents": "fn main() {}"
                }
            ]
        }
    });

    let invalid_path = serde_create(&workspace).to_value(json!({
        "data": {
            "title": "untitled",
            "files": [
                {
                    "path": "/invalid/path",
                    "contents": ""
                },
            ]
        }
    }));
    // FIXME: The error message for this is currently awful
    assert!(invalid_path.is_err());

    let (_, path_rel_info) = file
        .def
        .data_relationship_by_name("path", test.ontology())
        .unwrap();
    let path_def = test.ontology().def(path_rel_info.target.def_id());
    let Some(DefRepr::Text) = path_def.repr() else {
        panic!("invalid repr: {:?}", path_def.repr().debug(test.ontology()));
    };
}
