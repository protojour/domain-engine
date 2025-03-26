use indoc::indoc;
use ontol_log::{
    analyzer::DomainAnalyzer,
    log_model::{EKind, Log},
    logfile::ser::to_string,
    sem_model::GlobalSemModel,
    tag::LogRef,
};
use ontol_macros::test;
use ontol_parser::cst::grammar;
use ontol_syntax::{parse_syntax, syntax_view::View};
use pretty_assertions::assert_eq;
use thin_vec::thin_vec;
use tracing::info;
use ulid::Ulid;

use crate::verify_log;

struct Original(&'static str);

impl Original {
    pub fn diff(self, new: &str) -> String {
        let (old, errors) = parse_syntax(self.0, grammar::ontol, None);
        assert!(errors.is_empty());

        let (new, errors) = parse_syntax(new, grammar::ontol, None);
        assert!(errors.is_empty());

        let mut global_sem = GlobalSemModel::default();

        let log_ref = LogRef(0);
        let domain = global_sem.new_domain(log_ref);

        let mut log = Log::default();
        log.stage(EKind::Start(Box::new(Ulid::new())));
        log.stage(EKind::DomainAdd(domain, thin_vec![]));
        log.lock_stage();

        // "old" run
        {
            let mut analyzer = DomainAnalyzer::new(global_sem.project(log_ref, domain), &mut log);
            analyzer.ontol(old.view()).unwrap();
            global_sem = analyzer.finish().unproject();
        }

        log.lock_stage();
        verify_log(log.all());

        info!("diff first");

        // "new" run
        {
            let mut analyzer = DomainAnalyzer::new(global_sem.project(log_ref, domain), &mut log);
            analyzer.ontol(new.clone().view()).unwrap();
            global_sem = analyzer.finish().unproject();
        };

        let log_diff: Vec<EKind> = log.staged().into_iter().cloned().collect();
        log.lock_stage();

        info!("diff second");

        // "new" rerun - expect no changes
        {
            let mut analyzer = DomainAnalyzer::new(global_sem.project(log_ref, domain), &mut log);
            analyzer.ontol(new.view()).unwrap();
            analyzer.finish();
        }

        assert_eq!(log.staged(), []);

        verify_log(&log_diff);

        to_string(&log_diff).unwrap()
    }
}

#[test]
fn test_diff_def_add() {
    let log = Original("def foo ()").diff("def bar () def foo ()");
    assert_eq!(
        indoc! {
            r#"
            (def-add #02 ((domain #00) (ident "bar")))
            "#
        },
        log,
    );
}

#[test]
fn test_diff_def_rm() {
    let log = Original("def foo () def bar ()").diff("def foo ()");
    assert_eq!(
        indoc! {
            r#"
            (def-remove #02)
            "#
        },
        log,
    );
}

#[test]
fn test_diff_def_ident1() {
    let log = Original("def old ()").diff("def new ()");
    assert_eq!(
        indoc! {
            r#"
            (def-change #01 ((ident "new")))
            "#
        },
        log,
    );
}

#[test]
fn test_diff_def_ident2() {
    let log = Original("def old ()").diff("def new ()");
    assert_eq!(
        indoc! {
            r#"
            (def-change #01 ((ident "new")))
            "#
        },
        log,
    );
}

#[test]
fn test_diff_rel_add() {
    let log = Original("def foo (rel* 'foo': text) def noise ()")
        .diff("def noise () def foo ( rel* 'bar': text rel* 'foo': text )");
    assert_eq!(
        indoc! {
            r#"
            (rel-add #04 ((domain #00) (rel (text "bar")) (subj-crd set) (subj-add (path (local #01))) (obj-crd unit) (obj-add (path (ontol text)))))
            "#
        },
        log,
    );
}

#[test]
fn test_diff_rel_rm() {
    let log = Original("def foo (rel* 'foo': text rel* 'bar': text) def noise ()")
        .diff("def noise () def foo ( rel* 'foo': text )");
    assert_eq!(
        indoc! {
            r#"
            (rel-remove #04)
            "#
        },
        log,
    );
}

#[test]
fn test_diff_rel_ident1() {
    let log = Original("def foo (rel* 'old': text) def noise ()")
        .diff("def noise () def foo ( rel* 'new': text )");
    assert_eq!(
        indoc! {
            r#"
            (rel-change #03 ((rel (text "new"))))
            "#
        },
        log,
    );
}

#[test]
fn test_diff_rel_cardinality1() {
    let log = Original("def foo (rel. 'r': text) def noise ()")
        .diff("def noise () def foo ( rel* 'r': text )");
    assert_eq!(
        indoc! {
            r#"
            (rel-change #03 ((subj-crd set)))
            "#
        },
        log,
    );
}

#[test]
fn test_diff_rel_cardinality2() {
    let log = Original("def foo (rel* 'r': text) def noise ()")
        .diff("def noise () def foo ( rel* 'r': {text} )");
    assert_eq!(
        indoc! {
            r#"
            (rel-change #03 ((obj-crd set)))
            "#
        },
        log,
    );
}

#[test]
fn test_diff_rel_objtype1() {
    let log = Original("def foo (rel* 'foo': text) def noise ()")
        .diff("def noise () def foo (rel* 'foo': i64)");
    assert_eq!(
        indoc! {
            r#"
            (rel-change #03 ((obj-rm (path (ontol text))) (obj-add (path (ontol i64)))))
            "#
        },
        log,
    );
}
