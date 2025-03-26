use ontol_log::{
    analyzer::DomainAnalyzer,
    apply::apply_events,
    log_model::EKind,
    logfile::ser::to_string,
    snapshot_model::build_snapshot,
    tables::GlobalTables,
    tag::{LogRef, TagAllocator},
};
use ontol_macros::test;
use ontol_parser::cst::grammar;
use ontol_syntax::parse_green;
use ulid::Ulid;

use crate::verify_log;

fn analyze(src: &str) -> String {
    let (root, errors) = parse_green(src, grammar::ontol, None);
    assert!(errors.is_empty());

    let mut allocator = TagAllocator::default();

    let log_ref = LogRef(0);
    let domain_tag = allocator.next_tag.bump();

    let mut log = vec![];
    log.push(EKind::Start(Box::new(Ulid::new())));

    let mut gtables = GlobalTables::default();

    // first run
    {
        let domain_view = gtables.domain_view(log_ref, domain_tag);
        let mut analyzer = DomainAnalyzer::new(domain_tag, domain_view, &mut allocator);
        analyzer
            .ontol(root.clone(), parse_green("", grammar::ontol, None).0)
            .unwrap();
        let (events, sym_updates) = analyzer.finish();
        gtables.update(log_ref, domain_tag, sym_updates);
        log.extend(apply_events(
            events,
            gtables.domain_tables_mut(log_ref, domain_tag),
        ));
    }

    // second run - no changes
    {
        let domain_view = gtables.domain_view(log_ref, domain_tag);
        let mut analyzer = DomainAnalyzer::new(domain_tag, domain_view, &mut allocator);
        analyzer.ontol(root.clone(), root.clone()).unwrap();
        let (events, sym_updates) = analyzer.finish();

        assert!(events.is_empty());
        assert!(sym_updates.is_empty());
    }

    let _snapshot = build_snapshot(log.iter()).unwrap();

    verify_log(&log);

    to_string(&log).unwrap()
}

#[test]
fn test_artist_and_instrument() {
    analyze(&ontol_examples::artist_and_instrument().1);
}

#[test]
fn test_conduit_db() {
    analyze(&ontol_examples::conduit::conduit_db().1);
}

#[test]
fn test_blog_post_public() {
    analyze(&ontol_examples::conduit::blog_post_public().1);
}

#[test]
fn test_geojson() {
    analyze(&ontol_examples::geojson().1);
}
