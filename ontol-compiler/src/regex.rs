use regex_syntax::hir::{
    Class, ClassUnicode, ClassUnicodeRange, Hir, Literal, Repetition, RepetitionKind,
    RepetitionRange,
};

pub fn uuid_regex() -> Hir {
    let hex = Hir::class(Class::Unicode(ClassUnicode::new([
        ClassUnicodeRange::new('0', '9'),
        ClassUnicodeRange::new('a', 'f'),
        ClassUnicodeRange::new('A', 'F'),
    ])));
    let opt_dash = Hir::repetition(Repetition {
        kind: RepetitionKind::ZeroOrOne,
        greedy: true,
        hir: Box::new(Hir::literal(Literal::Unicode('-'))),
    });

    fn repeat_exact(hir: Hir, n: u32) -> Hir {
        Hir::repetition(Repetition {
            kind: RepetitionKind::Range(RepetitionRange::Exactly(n)),
            greedy: true,
            hir: Box::new(hir),
        })
    }

    Hir::concat(vec![
        repeat_exact(hex.clone(), 8),
        opt_dash.clone(),
        repeat_exact(hex.clone(), 4),
        opt_dash.clone(),
        repeat_exact(hex.clone(), 4),
        opt_dash.clone(),
        repeat_exact(hex.clone(), 4),
        opt_dash,
        repeat_exact(hex, 12),
    ])
}
