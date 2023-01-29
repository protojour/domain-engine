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

#[test]
fn test_meters_tmp() {
    "
    (type! meters)
    (type! millimeters)
    (rel! (meters) _ (number))
    (rel! (millimeters) _ (number))
    (eq! (:x)
        (obj! meters
            (_ :x)
        )
        (obj! millimeters
            (_ (* :x 1000)) ;; ERROR mismatched type
        )
    )
    "
    .compile_fail()
}
