use ontol_core::LogRef;
use ontol_log::{
    analyzer::DomainAnalyzer,
    log_model::{EKind, Log},
    logfile::ser::to_string,
    sem_model::GlobalSemModel,
};
use ontol_macros::test;
use ontol_parser::cst::grammar;
use ontol_syntax::{parse_syntax, syntax_view::View};
use thin_vec::thin_vec;
use tracing::info;
use ulid::Ulid;

use crate::verify_log;

fn analyze(src: &str) -> String {
    let (root, errors) = parse_syntax(src, grammar::ontol, None);
    assert!(errors.is_empty());

    let mut global_sem = GlobalSemModel::default();

    let log_ref = LogRef(0);
    let domain = global_sem.new_subdomain(log_ref);

    let mut log = Log::default();
    log.stage(EKind::Start(Box::new(Ulid::new())));
    log.stage(EKind::SubAdd(domain, thin_vec![]));
    log.lock_stage();

    // first run
    {
        let mut analyzer = DomainAnalyzer::new(global_sem.project(log_ref, domain), &mut log);
        analyzer.ontol(root.clone().view()).unwrap();
        global_sem = analyzer.finish().unproject();
    }

    log.lock_stage();

    info!("SECOND RUN");

    // second run - no changes
    {
        let mut analyzer = DomainAnalyzer::new(global_sem.project(log_ref, domain), &mut log);
        analyzer.ontol(root.view()).unwrap();
        analyzer.finish().unproject();
    }

    assert_eq!(log.staged(), []);

    verify_log(log.all());

    to_string(log.all()).unwrap()
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
#[ignore]
fn test_geojson() {
    analyze(&ontol_examples::geojson().1);
}
