//! Extension traits for testing purposes

pub mod serde {
    use ontol_runtime::interface::serde::operator::{SerdeOperator, StructOperator};

    pub trait SerdeOperatorExt {
        fn struct_op(&self) -> &StructOperator;
    }

    impl SerdeOperatorExt for SerdeOperator {
        fn struct_op(&self) -> &StructOperator {
            let Self::Struct(struct_op) = &self else {
                panic!("Not a struct operator: {self:?}");
            };
            struct_op
        }
    }
}

pub mod graphql {
    use ontol_runtime::interface::graphql::data::{
        NativeScalarRef, NodeData, ObjectData, ObjectKind, ScalarData, TypeData, TypeIndex,
        TypeKind, UnitTypeRef,
    };

    pub trait TypeDataExt {
        fn object_data(&self) -> &ObjectData;
        fn custom_scalar(&self) -> &ScalarData;
    }

    pub trait ObjectDataExt {
        fn node_data(&self) -> &NodeData;
    }

    pub trait UnitTypeRefExt {
        fn indexed(&self) -> TypeIndex;
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
    }

    impl UnitTypeRefExt for UnitTypeRef {
        #[track_caller]
        fn indexed(&self) -> TypeIndex {
            let UnitTypeRef::Indexed(index) = self else {
                panic!("not an indexed type: {self:?}");
            };
            *index
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
