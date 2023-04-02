//! The core domain

use ontol_runtime::{proc::BuiltinProc, string_types::StringLikeType, DefId};

use crate::{
    def::{DefKind, TypeDef},
    mem::Intern,
    namespace::Space,
    package::CORE_PKG,
    patterns::StringPatternSegment,
    regex_util,
    relation::Constructor,
    types::{Type, TypeRef},
    Compiler, SourceSpan,
};

impl<'m> Compiler<'m> {
    pub fn with_core(mut self) -> Self {
        self.define_package(CORE_PKG);

        // fundamental types
        self.register_type(self.primitives.unit, Type::Unit);
        self.register_type(self.primitives.empty_sequence, Type::EmptySequence);

        self.register_named_type(self.primitives.is_relation, "is", |_| Type::BuiltinRelation);
        self.register_named_type(self.primitives.identifies_relation, "identifies", |_| {
            Type::BuiltinRelation
        });

        let int_ty = self.register_named_type(self.primitives.int, "int", Type::Int);
        let _ = self.register_named_type(self.primitives.number, "number", Type::Number);
        let string_ty = self.register_named_type(self.primitives.string, "string", Type::String);

        let int_int_ty = self.types.intern([int_ty, int_ty]);
        let string_string_ty = self.types.intern([string_ty, string_ty]);

        let int_int_to_int = self.types.intern(Type::Function {
            params: int_int_ty,
            output: int_ty,
        });
        let string_string_to_string = self.types.intern(Type::Function {
            params: string_string_ty,
            output: string_ty,
        });

        // Built-in functions
        // arithmetic
        self.def_core_proc("+", DefKind::CoreFn(BuiltinProc::Add), int_int_to_int);
        self.def_core_proc("-", DefKind::CoreFn(BuiltinProc::Sub), int_int_to_int);
        self.def_core_proc("*", DefKind::CoreFn(BuiltinProc::Mul), int_int_to_int);
        self.def_core_proc("/", DefKind::CoreFn(BuiltinProc::Div), int_int_to_int);

        // string manipulation
        self.def_core_proc(
            "append",
            DefKind::CoreFn(BuiltinProc::Append),
            string_string_to_string,
        );

        self.def_uuid();
        self.def_datetime();

        self
    }

    fn def_uuid(&mut self) {
        let (uuid, _) = self.define_domain_type("uuid", Type::DateTime);
        self.relations.properties_by_type_mut(uuid).constructor =
            Constructor::StringPattern(StringPatternSegment::Regex(regex_util::uuid()));
        self.defs
            .string_like_types
            .insert(uuid, StringLikeType::Uuid);
    }

    fn def_datetime(&mut self) {
        let (datetime, _) = self.define_domain_type("datetime", Type::DateTime);
        self.relations.properties_by_type_mut(datetime).constructor =
            Constructor::StringPattern(StringPatternSegment::Regex(regex_util::datetime_rfc3339()));
        self.defs
            .string_like_types
            .insert(datetime, StringLikeType::DateTime);
    }

    /// Define a core _domain_ type, i.e. not a primitive
    fn define_domain_type(
        &mut self,
        ident: &'static str,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> (DefId, TypeRef<'m>) {
        let def_id = self.defs.add_def(
            DefKind::Type(TypeDef {
                public: true,
                ident: Some(ident),
                params: None,
            }),
            CORE_PKG,
            SourceSpan::none(),
        );
        let type_ref = self.register_named_type(def_id, ident, ty_fn);
        (def_id, type_ref)
    }

    fn register_type(&mut self, def_id: DefId, ty_fn: impl Fn(DefId) -> Type<'m>) -> TypeRef<'m> {
        let ty = self.types.intern(ty_fn(def_id));
        self.def_types.map.insert(def_id, ty);
        ty
    }

    fn register_named_type(
        &mut self,
        def_id: DefId,
        ident: &str,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> TypeRef<'m> {
        let ty = self.types.intern(ty_fn(def_id));
        self.namespaces
            .get_namespace_mut(CORE_PKG, Space::Type)
            .insert(ident.into(), def_id);
        self.def_types.map.insert(def_id, ty);
        ty
    }

    fn def_core_proc(&mut self, ident: &str, def_kind: DefKind<'m>, ty: TypeRef<'m>) -> DefId {
        let def_id = self.add_named_def(ident, Space::Type, def_kind, CORE_PKG, SourceSpan::none());
        self.def_types.map.insert(def_id, ty);

        def_id
    }
}
