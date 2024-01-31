use indoc::indoc;
use pretty_assertions::assert_eq;

use ontol_compiler::hir_unify::test_api::mk_flat_scope;

#[test]
fn test_flat_basic_struct1() {
    let output = mk_flat_scope(
        "
        (struct ($a)
            (prop! $a S:1:0 (#u $b))
        )
        ",
    );
    let expected = indoc! {
        "
        $a: {} - Struct def{$a} cns{}
        $c: {$a} - PropVariant(d=0, PropFlags(0x0), $a, S:1:0) def{$b} cns{}
        $d: {$c} - PropValue def{$b} cns{}
        $b: {$d} - Var def{$b} cns{}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_arithmetic_prop() {
    let output = mk_flat_scope(
        "
        (struct ($a)
            (prop! $a S:1:0 (#u (- $b (+ 7 3))))
        )
        ",
    );
    let expected = indoc! {
        "
        $a: {} - Struct def{$a} cns{}
        $c: {$a} - PropVariant(d=0, PropFlags(0x0), $a, S:1:0) def{$b} cns{}
        $d: {$c} - PropValue def{$b} cns{}
        $e: {$d} - Call(Sub) def{$b} cns{}
        $b: {$e} - Var def{$b} cns{}
        $f: {$e} - Const((+ 7 3)) def{} cns{}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_arithmetic_prop_dependency() {
    let output = mk_flat_scope(
        "
        (struct ($a)
            (prop! $a O:1:0 (#u (+ $b $c)))
            (prop! $a O:1:1 (#u (+ $b 20)))
        )
        ",
    );
    // BUG: Here the algorithm should distribute $b and $c to each PropVariant,
    // and not do a union in the first arm.
    // In the unifier it needs this behaviour to be able to place an assignment/seed.
    let expected = indoc! {
        "
        $a: {} - Struct def{$a} cns{}
        $d: {$a} - PropVariant(d=0, PropFlags(0x0), $a, O:1:0) def{$b, $c} cns{}
        $e: {$d} - PropValue def{$b, $c} cns{}
        $f: {$a} - PropVariant(d=0, PropFlags(0x0), $a, O:1:1) def{$b} cns{}
        $g: {$f} - PropValue def{$b} cns{}
        $h: {$e} - Call(Add) def{$b, $c} cns{}
        $i: {$g} - Call(Add) def{$b} cns{}
        $b: {$h} - Var def{$b} cns{}
        $c: {$h} - Var def{$c} cns{}
        $b: {$i} - Var def{$b} cns{}
        $j: {$i} - Const(20) def{} cns{}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_regex() {
    let output = mk_flat_scope(
        "
        (struct ($a)
            (prop! $a S:1:0
                (#u
                    (regex def@0:0 ((1 $b)))
                )
            )
        )
        ",
    );
    let expected = indoc! {
        "
        $a: {} - Struct def{$a} cns{}
        $c: {$a} - PropVariant(d=0, PropFlags(0x0), $a, S:1:0) def{} cns{}
        $d: {$c} - PropValue def{} cns{}
        $e: {$d} - Regex(None, def@0:0) def{} cns{}
        $f: {$e} - RegexAlternation def{$b} cns{}
        $b: {$f} - RegexCapture(1) def{$b} cns{}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_multi_prop() {
    let output = mk_flat_scope(
        "
        (struct ($b)
            (prop! $b S:1:0
                (.. (@f)
                    (iter
                        #u
                        (struct ($c)
                            (prop! $c S:2:1
                                (.. (@g)
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
        $b: {} - Struct def{$b} cns{}
        $f: {$b} - SeqPropVariant(Data(Label(f), Error), OutputVar($h), PropFlags(0x0), HasDefault(false), $b, S:1:0) def{$f} cns{}
        $i: {$f} - IterElement(Label(f), OutputVar($h)) def{} cns{}
        $j: {$i} - PropValue def{$c} cns{}
        $c: {$j} - Struct def{$c} cns{}
        $g: {$c} - SeqPropVariant(Data(Label(g), Error), OutputVar($k), PropFlags(0x0), HasDefault(false), $c, S:2:1) def{$g} cns{}
        $l: {$g} - IterElement(Label(g), OutputVar($k)) def{} cns{}
        $m: {$l} - PropValue def{$a} cns{}
        $a: {$m} - Var def{$a} cns{}
        "
    };
    assert_eq!(expected, output);
}

#[test]
fn test_flat_seq_mix() {
    let output = mk_flat_scope(
        "
        (struct ($c)
            (prop! $c S:1:1
                (.. (@d) (#u $a) (iter #u $b) (#u $c))
            )
        )",
    );
    // FIXME: I think $a and $c should be grouped under ItemElement or something like that,
    // to clearly distinguish from IterElement..
    let expected = indoc! {
        "
        $c: {} - Struct def{$c} cns{}
        $d: {$c} - SeqPropVariant(Data(Label(d), Error), OutputVar($e), PropFlags(0x0), HasDefault(false), $c, S:1:1) def{$a, $c, $d} cns{}
        $f: {$d} - PropValue def{$a} cns{}
        $g: {$d} - IterElement(Label(d), OutputVar($e)) def{} cns{}
        $h: {$g} - PropValue def{$b} cns{}
        $i: {$d} - PropValue def{$c} cns{}
        $a: {$f} - Var def{$a} cns{}
        $b: {$h} - Var def{$b} cns{}
        $c: {$i} - Var def{$c} cns{}
        "
    };
    assert_eq!(expected, output);
}
