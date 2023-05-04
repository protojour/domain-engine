//! The core domain

use ontol_runtime::{proc::BuiltinProc, string_types::StringLikeType, DefId};

use crate::{
    def::{DefKind, TypeDef},
    mem::Intern,
    namespace::Space,
    package::CORE_PKG,
    patterns::{store_string_pattern_segment, StringPatternSegment},
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

        self.register_named_builtin_relation(self.primitives.is_relation, "is");
        self.register_named_builtin_relation(self.primitives.identifies_relation, "identifies");
        self.register_named_builtin_relation(self.primitives.min_relation, "min");
        self.register_named_builtin_relation(self.primitives.max_relation, "max");
        self.register_named_builtin_relation(self.primitives.default_relation, "default");
        self.register_named_builtin_relation(self.primitives.route_relation, "route");

        self.register_named_type(self.primitives.false_value, "false", Type::Bool);
        self.register_named_type(self.primitives.true_value, "true", Type::Bool);
        self.register_named_type(self.primitives.bool, "bool", Type::Bool);

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
        let (uuid, _) = self.define_domain_type("uuid", |def_id| {
            Type::StringLike(def_id, StringLikeType::Uuid)
        });
        let segment = StringPatternSegment::Regex(regex_util::uuid());
        store_string_pattern_segment(&mut self.patterns, uuid, &segment);
        self.relations.properties_by_type_mut(uuid).constructor = Constructor::StringFmt(segment);
        self.defs
            .string_like_types
            .insert(uuid, StringLikeType::Uuid);
    }

    fn def_datetime(&mut self) {
        let (datetime, _) = self.define_domain_type("datetime", |def_id| {
            Type::StringLike(def_id, StringLikeType::DateTime)
        });
        let segment = StringPatternSegment::Regex(regex_util::datetime_rfc3339());
        store_string_pattern_segment(&mut self.patterns, datetime, &segment);
        self.relations.properties_by_type_mut(datetime).constructor =
            Constructor::StringFmt(segment.clone());
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

    fn register_named_builtin_relation(&mut self, def_id: DefId, ident: &str) -> TypeRef<'m> {
        self.register_named_type(def_id, ident, |_| Type::BuiltinRelation)
    }

    fn def_core_proc(&mut self, ident: &str, def_kind: DefKind<'m>, ty: TypeRef<'m>) -> DefId {
        let def_id = self.add_named_def(ident, Space::Type, def_kind, CORE_PKG, SourceSpan::none());
        self.def_types.map.insert(def_id, ty);

        def_id
    }
}
