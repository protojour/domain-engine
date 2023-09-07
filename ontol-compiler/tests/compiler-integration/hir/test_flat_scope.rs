use indoc::indoc;
use pretty_assertions::assert_eq;

use ontol_compiler::hir_unify::test_api::mk_flat_scope;

#[test]
fn test_flat_basic_struct1() {
    let output = mk_flat_scope(
        "
        (struct ($a)
            (prop $a S:0:0 (#u $b))
        )
        ",
    );
    let expected = indoc! {
        "
        $a: {} - Struct {}
        $c: {$a} - PropVariant(opt=f, Var(a), S:0:0) {$b}
        $d: {$c} - PropValue {$b}
        $b: {$d} - Var {$b}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_arithmetic_prop() {
    let output = mk_flat_scope(
        "
        (struct ($a)
            (prop $a S:0:0 (#u (- $b (+ 7 3))))
        )
        ",
    );
    let expected = indoc! {
        "
        $a: {} - Struct {}
        $c: {$a} - PropVariant(opt=f, Var(a), S:0:0) {$b}
        $d: {$c} - PropValue {$b}
        $e: {$d} - Call(Sub) {$b}
        $b: {$e} - Var {$b}
        $f: {$e} - Const((+ 7 3)) {}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_arithmetic_prop_dependency() {
    let output = mk_flat_scope(
        "
        (struct ($a)
            (prop $a O:0:0 (#u (+ $b $c)))
            (prop $a O:0:1 (#u (+ $b 20)))
        )
        ",
    );
    // BUG: Here the algorithm should distribute $b and $c to each PropVariant,
    // and not do a union in the first arm.
    // In the unifier it needs this behaviour to be able to place an assignment/seed.
    let expected = indoc! {
        "
        $a: {} - Struct {}
        $d: {$a} - PropVariant(opt=f, Var(a), O:0:0) {$b, $c}
        $e: {$d} - PropValue {$b, $c}
        $f: {$a} - PropVariant(opt=f, Var(a), O:0:1) {$b}
        $g: {$f} - PropValue {$b}
        $h: {$e} - Call(Add) {$b, $c}
        $i: {$g} - Call(Add) {$b}
        $b: {$h} - Var {$b}
        $c: {$h} - Var {$c}
        $b: {$i} - Var {$b}
        $j: {$i} - Const(20) {}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_regex() {
    let output = mk_flat_scope(
        "
        (struct ($a)
            (prop $a S:0:0
                (#u
                    (regex def@0:0 ((1 $b)))
                )
            )
        )
        ",
    );
    let expected = indoc! {
        "
        $a: {} - Struct {}
        $c: {$a} - PropVariant(opt=f, Var(a), S:0:0) {$b}
        $d: {$c} - PropValue {$b}
        $e: {$d} - Regex(def@0:0) {$b}
        $f: {$e} - RegexAlternation {$b}
        $b: {$f} - RegexCapture(1) {$b}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_seq() {
    let output = mk_flat_scope(
        "
        (struct ($b)
            (prop $b S:0:0
                (seq (@f)
                    (iter
                        #u
                        (struct ($c)
                            (prop $c S:1:1
                                (seq (@g)
                                    (iter #u (map $a))
                                )
                            )
                        )
                    )
                )
            )
        )",
    );
    let expected = indoc! {
        "
        $b: {} - Struct {}
        $f: {$b} - SeqPropVariant(opt=f, Var(b), S:0:0) {$f}
        $h: {$f} - IterElement {}
        $i: {$h} - PropValue {}
        $c: {$i} - Struct {}
        $g: {$c} - SeqPropVariant(opt=f, Var(c), S:1:1) {$g}
        $j: {$g} - IterElement {}
        $k: {$j} - PropValue {$a}
        $a: {$k} - Var {$a}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_seq_mix() {
    let output = mk_flat_scope(
        "
        (struct ($c)
            (prop $c S:1:1
                (seq (@d) (#u $a) (iter #u $b) (#u $c))
            )
        )",
    );
    // BUG: I think $a and $c should be grouped under ItemElement or something like that.
    let expected = indoc! {
        "
        $c: {} - Struct {}
        $d: {$c} - SeqPropVariant(TypedLabel { label: Label(d), ty: Error }, opt=f, Var(c), S:1:1) {$a, $c, $d}
        $e: {$d} - PropValue {$a}
        $f: {$d} - IterElement {}
        $g: {$f} - PropValue {$b}
        $h: {$d} - PropValue {$c}
        $a: {$e} - Var {$a}
        $b: {$g} - Var {$b}
        $c: {$h} - Var {$c}
        "
    };
    assert_eq!(expected, output);
}
