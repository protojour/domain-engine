use arcstr::ArcStr;

/// Trait for extracting Rustdoc from Rust data types.
pub trait RustDoc {
    type Key: ?Sized;

    fn get_field_rustdoc(key: &Self::Key) -> Option<ArcStr>;
}

#[cfg(test)]
mod tests {
    use ontol_macros::RustDoc;
    use pretty_assertions::assert_eq;

    use super::RustDoc;

    #[derive(RustDoc)]
    enum PrimitiveEnum {
        /// Variant
        /// ```ontol
        /// def a (
        ///     rel* 'prop': B
        /// )
        /// ```
        Variant,
    }

    #[derive(RustDoc)]
    enum DataEnum {
        /// Doc
        #[allow(unused)]
        Variant(u8),
    }

    #[derive(RustDoc)]
    struct Struct {
        /// Doc
        #[allow(unused)]
        field: u8,
    }

    #[test]
    fn indents_correctly() {
        let doc = PrimitiveEnum::get_field_rustdoc(&PrimitiveEnum::Variant).unwrap();
        assert_eq!(
            "Variant\n```ontol\ndef a (\n    rel* 'prop': B\n)\n```",
            doc
        );
    }

    #[test]
    fn data_enum() {
        let doc = DataEnum::get_field_rustdoc("Variant").unwrap();
        assert_eq!("Doc", doc);
    }

    #[test]
    fn struct_() {
        let doc = Struct::get_field_rustdoc("field").unwrap();
        assert_eq!("Doc", doc);
    }
}
