//! Extension traits for testing purposes

pub mod serde {
    use ontol_runtime::debug::OntolDebug;
    use ontol_runtime::interface::serde::operator::{SerdeOperator, StructOperator};

    pub trait SerdeOperatorExt {
        fn struct_op(&self) -> &StructOperator;
    }

    impl SerdeOperatorExt for SerdeOperator {
        fn struct_op(&self) -> &StructOperator {
            let Self::Struct(struct_op) = &self else {
                panic!("Not a struct operator: {:?}", self.debug(&()));
            };
            struct_op
        }
    }
}

pub mod graphql {
    use ontol_runtime::interface::graphql::data::{
        NativeScalarRef, NodeData, ObjectData, ObjectKind, ScalarData, TypeAddr, TypeData,
        TypeKind, UnitTypeRef,
    };

    pub trait TypeDataExt {
        fn object_data(&self) -> &ObjectData;
        fn custom_scalar(&self) -> &ScalarData;
    }

    pub trait ObjectDataExt {
        fn node_data(&self) -> &NodeData;
        fn field_names(&self) -> Vec<&str>;
    }

    pub trait UnitTypeRefExt {
        fn addr(&self) -> TypeAddr;
        fn native_scalar(&self) -> &NativeScalarRef;
    }

    impl TypeDataExt for TypeData {
        #[track_caller]
        fn object_data(&self) -> &ObjectData {
            let TypeKind::Object(object_data) = &self.kind else {
                panic!("not an object");
            };
            object_data
        }

        #[track_caller]
        fn custom_scalar(&self) -> &ScalarData {
            let TypeKind::CustomScalar(scalar_data) = &self.kind else {
                panic!("not an custom_scalar type");
            };
            scalar_data
        }
    }

    impl ObjectDataExt for ObjectData {
        fn node_data(&self) -> &NodeData {
            let ObjectKind::Node(node_data) = &self.kind else {
                panic!("ObjectData is not a Node");
            };
            node_data
        }

        fn field_names(&self) -> Vec<&str> {
            self.fields
                .iter()
                .map(|(key, _)| key.arc_str().as_str())
                .collect()
        }
    }

    impl UnitTypeRefExt for UnitTypeRef {
        #[track_caller]
        fn addr(&self) -> TypeAddr {
            let UnitTypeRef::Addr(type_addr) = self else {
                panic!("not an addressed type: {self:?}");
            };
            *type_addr
        }

        #[track_caller]
        fn native_scalar(&self) -> &NativeScalarRef {
            let Self::NativeScalar(native_scalar_ref) = self else {
                panic!("not a native scalar: {self:?}");
            };
            native_scalar_ref
        }
    }
}
