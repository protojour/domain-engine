use indoc::indoc;
use ontol_log::{
    analyzer::DomainAnalyzer,
    apply::apply_events,
    log_model::EKind,
    logfile::ser::to_string,
    tables::GlobalTables,
    tag::{LogRef, TagAllocator},
};
use ontol_macros::test;
use ontol_parser::cst::grammar;
use ontol_syntax::parse_green;
use pretty_assertions::assert_eq;
use ulid::Ulid;

use crate::verify_log;

struct Original(&'static str);

impl Original {
    pub fn diff(self, new: &str) -> String {
        let (old, errors) = parse_green(self.0, grammar::ontol, None);
        assert!(errors.is_empty());

        let (new, errors) = parse_green(new, grammar::ontol, None);
        assert!(errors.is_empty());

        let mut allocator = TagAllocator::default();

        let log_ref = LogRef(0);
        let domain_tag = allocator.next_tag.bump();

        let mut log = vec![];
        log.push(EKind::Start(Box::new(Ulid::new())));

        let mut gtables = GlobalTables::default();

        // "old" run
        {
            let domain_view = gtables.domain_view(log_ref, domain_tag);
            let mut analyzer = DomainAnalyzer::new(domain_tag, domain_view, &mut allocator);
            analyzer
                .ontol(old.clone(), parse_green("", grammar::ontol, None).0)
                .unwrap();
            let (events, sym_updates) = analyzer.finish();
            gtables.update(log_ref, domain_tag, sym_updates);
            log.extend(apply_events(
                events,
                gtables.domain_tables_mut(log_ref, domain_tag),
            ));
        }

        verify_log(&log);

        // "new" run
        let log_diff = {
            let domain_view = gtables.domain_view(log_ref, domain_tag);
            let mut analyzer = DomainAnalyzer::new(domain_tag, domain_view, &mut allocator);
            if let Err(err) = analyzer.ontol(new.clone(), old.clone()) {
                panic!("diff analyzer error: {err:#?}");
            }
            let (events, sym_updates) = analyzer.finish();
            gtables.update(log_ref, domain_tag, sym_updates);

            apply_events(events, gtables.domain_tables_mut(log_ref, domain_tag))
        };

        // "new" rerun - expect no changes
        {
            let domain_view = gtables.domain_view(log_ref, domain_tag);
            let mut analyzer = DomainAnalyzer::new(domain_tag, domain_view, &mut allocator);
            analyzer.ontol(new.clone(), new.clone()).unwrap();
            let (events, sym_updates) = analyzer.finish();

            assert!(events.is_empty());
            assert!(sym_updates.is_empty());
        }

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
            (def-add #02 ((ident "bar")))
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
            (def-change #01 ((change-ident "new" "old")))
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
            (def-change #01 ((change-ident "new" "old")))
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
            (rel-add #04 ((rel (text "bar")) (subj-add (path (local #01))) (subj-crd set) (obj-add (path (ontol text))) (obj-crd unit)))
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
