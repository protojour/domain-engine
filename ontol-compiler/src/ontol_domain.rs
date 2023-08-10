//! The ontol (builtin-in) domain

use ontol_runtime::{string_types::StringLikeType, vm::proc::BuiltinProc, DefId};

use crate::{
    def::{DefKind, TypeDef},
    mem::Intern,
    namespace::Space,
    package::ONTOL_PKG,
    patterns::{store_string_pattern_segment, StringPatternSegment},
    primitive::PrimitiveKind,
    regex_util,
    relation::{Constructor, Is, TypeRelation},
    types::{Type, TypeRef},
    Compiler, NO_SPAN,
};

impl<'m> Compiler<'m> {
    pub fn with_ontol(mut self) -> Self {
        self.define_package(ONTOL_PKG);

        // fundamental types
        self.register_type(self.primitives.empty_sequence, Type::EmptySequence);

        for (def_id, ident, kind) in self.primitives.list_primitives() {
            self.register_primitive(def_id, ident, kind);
        }
        for (def_id, ident) in self.primitives.list_relations() {
            self.register_named_builtin_relation(def_id, ident);
        }

        // bools
        self.is(self.primitives.true_value, self.primitives.bool);
        self.is(self.primitives.false_value, self.primitives.bool);

        // integers
        self.is(self.primitives.int, self.primitives.number);
        self.is(self.primitives.i64, self.primitives.int);

        // floats
        self.is(self.primitives.float, self.primitives.number);

        let i64_ty = *self.def_types.table.get(&self.primitives.i64).unwrap();

        let i64_i64_ty = self.types.intern([i64_ty, i64_ty]);

        let string_ty = *self.def_types.table.get(&self.primitives.string).unwrap();
        let string_string_ty = self.types.intern([string_ty, string_ty]);

        let i64_i64_to_i64 = self.types.intern(Type::Function {
            params: i64_i64_ty,
            output: i64_ty,
        });
        let string_string_to_string = self.types.intern(Type::Function {
            params: string_string_ty,
            output: string_ty,
        });

        // Built-in functions
        // arithmetic
        self.def_proc("+", DefKind::Fn(BuiltinProc::Add), i64_i64_to_i64);
        self.def_proc("-", DefKind::Fn(BuiltinProc::Sub), i64_i64_to_i64);
        self.def_proc("*", DefKind::Fn(BuiltinProc::Mul), i64_i64_to_i64);
        self.def_proc("/", DefKind::Fn(BuiltinProc::Div), i64_i64_to_i64);

        // string manipulation
        self.def_proc(
            "append",
            DefKind::Fn(BuiltinProc::Append),
            string_string_to_string,
        );

        self.def_uuid();
        self.def_datetime();

        self.register_named_type(
            self.primitives.generators.auto,
            "auto",
            Type::ValueGenerator,
        );
        self.register_named_type(
            self.primitives.generators.create_time,
            "create_time",
            Type::ValueGenerator,
        );
        self.register_named_type(
            self.primitives.generators.update_time,
            "update_time",
            Type::ValueGenerator,
        );

        self.seal_domain(ONTOL_PKG);

        self
    }

    fn def_uuid(&mut self) {
        let (uuid, _) = self.define_concrete_domain_type("uuid", |def_id| {
            Type::StringLike(def_id, StringLikeType::Uuid)
        });
        let segment = StringPatternSegment::Regex(regex_util::uuid());
        store_string_pattern_segment(&mut self.patterns, uuid, &segment);
        self.relations.properties_by_def_id_mut(uuid).constructor = Constructor::StringFmt(segment);
        self.defs
            .string_like_types
            .insert(uuid, StringLikeType::Uuid);
    }

    fn def_datetime(&mut self) {
        let (datetime, _) = self.define_concrete_domain_type("datetime", |def_id| {
            Type::StringLike(def_id, StringLikeType::DateTime)
        });
        let segment = StringPatternSegment::Regex(regex_util::datetime_rfc3339());
        store_string_pattern_segment(&mut self.patterns, datetime, &segment);
        self.relations
            .properties_by_def_id_mut(datetime)
            .constructor = Constructor::StringFmt(segment.clone());
        self.defs
            .string_like_types
            .insert(datetime, StringLikeType::DateTime);
    }

    /// Define an ontol _domain_ type, i.e. not a primitive
    fn define_concrete_domain_type(
        &mut self,
        ident: &'static str,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> (DefId, TypeRef<'m>) {
        let def_id = self.defs.add_def(
            DefKind::Type(TypeDef {
                public: true,
                ident: Some(ident),
                params: None,
                rel_type_for: None,
                concrete: true,
            }),
            ONTOL_PKG,
            NO_SPAN,
        );
        let type_ref = self.register_named_type(def_id, ident, ty_fn);
        (def_id, type_ref)
    }

    fn register_type(&mut self, def_id: DefId, ty_fn: impl Fn(DefId) -> Type<'m>) -> TypeRef<'m> {
        let ty = self.types.intern(ty_fn(def_id));
        self.def_types.table.insert(def_id, ty);
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
            .get_namespace_mut(ONTOL_PKG, Space::Type)
            .insert(ident.into(), def_id);
        self.def_types.table.insert(def_id, ty);
        ty
    }

    fn register_primitive(
        &mut self,
        def_id: DefId,
        ident: Option<&str>,
        kind: PrimitiveKind,
    ) -> TypeRef<'m> {
        let ty = self.types.intern(Type::Primitive(kind, def_id));
        if let Some(ident) = ident {
            self.namespaces
                .get_namespace_mut(ONTOL_PKG, Space::Type)
                .insert(ident.into(), def_id);
        }
        self.def_types.table.insert(def_id, ty);
        ty
    }

    fn register_named_builtin_relation(&mut self, def_id: DefId, ident: &str) -> TypeRef<'m> {
        self.register_named_type(def_id, ident, |_| Type::BuiltinRelation)
    }

    fn def_proc(&mut self, ident: &str, def_kind: DefKind<'m>, ty: TypeRef<'m>) -> DefId {
        let def_id = self.add_named_def(ident, Space::Type, def_kind, ONTOL_PKG, NO_SPAN);
        self.def_types.table.insert(def_id, ty);

        def_id
    }

    fn is(&mut self, sub_def_id: DefId, super_def_id: DefId) {
        self.relations
            .ontology_mesh
            .entry(sub_def_id)
            .or_default()
            .insert(
                Is {
                    def_id: super_def_id,
                    rel: TypeRelation::Super,
                    is_ontol_alias: true,
                },
                NO_SPAN,
            );
        self.relations
            .ontology_mesh
            .entry(super_def_id)
            .or_default()
            .insert(
                Is {
                    def_id: sub_def_id,
                    rel: TypeRelation::Sub,
                    is_ontol_alias: true,
                },
                NO_SPAN,
            );
    }
}
