//! Tests for important Atlas domains
//!
use ontol_examples::{filemeta, findings};
use ontol_macros::test;
use ontol_runtime::ontology::domain::{DataRelationshipKind, DefKind, EdgeCardinalFlags};
use ontol_test_utils::TestCompile;

#[test]
fn test_filemeta() {
    let _test = filemeta().1.compile();
}

#[test]
fn test_findings() {
    let test = findings().1.compile();

    let [session, finding] = test.bind(["FindingSession", "Finding"]);
    let DefKind::Edge(edge_info) = &finding.def.kind else {
        panic!()
    };

    {
        let (_prop_id, rel_info) = session
            .def
            .data_relationship_by_name("findings", test.ontology())
            .unwrap();

        let DataRelationshipKind::Edge(_) = rel_info.kind else {
            panic!(
                "should be an edge data relationship, but was: {:?}",
                rel_info.kind
            );
        };
    }

    {
        let session_cardinal = &edge_info.cardinals[0];
        let finding_cardinal = &edge_info.cardinals[1];

        assert_eq!(session_cardinal.flags, EdgeCardinalFlags::ENTITY);
        assert_eq!(finding_cardinal.flags, EdgeCardinalFlags::ENTITY);
    }
}
