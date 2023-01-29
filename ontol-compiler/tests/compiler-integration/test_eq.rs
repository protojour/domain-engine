use crate::{util::TypeBinding, TestCompile};

#[test]
fn test_eq_simple() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) f (string))
    (rel! (bar) b (string))
    (eq! (:x)
        (obj! foo
            (f :x)
        )
        (obj! bar
            (b :x)
        )
    )
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
    })
}
