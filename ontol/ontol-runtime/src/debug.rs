pub use ontol_core::debug::*;

#[cfg(test)]
mod tests {
    #![expect(dead_code)]

    use crate::ontology::Ontology;
    use ontol_core::debug::{Fmt, OntolDebug, OntolFormatter};
    use ontol_macros::OntolDebug;

    struct Custom;

    impl OntolDebug for Custom {
        fn fmt(
            &self,
            _of: &dyn OntolFormatter,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "Custom!")
        }
    }

    #[derive(OntolDebug)]
    struct Unit;

    #[derive(OntolDebug)]
    struct Tuple(Unit, Custom);

    #[derive(OntolDebug)]
    struct Named {
        tuple: Tuple,
        int: usize,
    }

    #[derive(OntolDebug)]
    enum Enum {
        A { f: Named },
        B,
        C(usize),
    }

    #[test]
    fn test_debug() {
        let value = Enum::A {
            f: Named {
                tuple: Tuple(Unit, Custom),
                int: 3,
            },
        };
        let ontology = Ontology::builder().build();
        let str = format!("{:?}", Fmt(&ontology, &value));

        assert_eq!(
            str,
            "A { f: Named { tuple: Tuple(Unit, Custom!), int: 3 } }"
        );
    }
}
