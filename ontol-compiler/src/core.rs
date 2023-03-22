//! The core domain

use ontol_runtime::{proc::BuiltinProc, string_types::StringLikeType, DefId};

use crate::{
    def::{DefKind, Primitive},
    mem::Intern,
    namespace::Space,
    package::CORE_PKG,
    patterns::StringPatternSegment,
    regex::uuid_regex,
    relation::Constructor,
    types::{Type, TypeRef},
    Compiler, SourceSpan,
};

impl<'m> Compiler<'m> {
    pub fn with_core(mut self) -> Self {
        self.define_package(CORE_PKG);

        // fundamental types
        let _ = self.def_core_type(self.defs.unit(), Type::Unit);
        let _ = self.def_core_type(self.defs.empty_sequence(), Type::EmptySequence);
        let _ = self.def_core_type_name(self.defs.id_relation(), "id", |_| Type::BuiltinRelation);
        let int_ty = self.def_core_type_name(self.defs.int(), "int", Type::Int);
        let _ = self.def_core_type_name(self.defs.number(), "number", Type::Number);
        let string_ty = self.def_core_type_name(self.defs.string(), "string", Type::String);

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

        self
    }

    fn def_uuid(&mut self) {
        let uuid = self.defs.add_primitive(Primitive::Uuid);
        let _ = self.def_core_type_name(uuid, "uuid", Type::Uuid);
        self.relations.properties_by_type_mut(uuid).constructor =
            Constructor::StringPattern(StringPatternSegment::Regex(uuid_regex()));
        self.defs
            .string_like_types
            .insert(uuid, StringLikeType::Uuid);
    }

    fn def_core_type(
        &mut self,
        def_id: impl DefIdSource,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> TypeRef<'m> {
        let def_id = def_id.get(self);
        let ty = self.types.intern(ty_fn(def_id));
        self.def_types.map.insert(def_id, ty);
        ty
    }

    fn def_core_type_name(
        &mut self,
        def_id: impl DefIdSource,
        ident: &str,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> TypeRef<'m> {
        let def_id = def_id.get(self);
        let ty = self.types.intern(ty_fn(def_id));
        self.namespaces
            .get_mut(CORE_PKG, Space::Type)
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

trait DefIdSource {
    fn get(self, compiler: &mut Compiler) -> DefId;
}

impl DefIdSource for DefId {
    fn get(self, _: &mut Compiler) -> DefId {
        self
    }
}

impl DefIdSource for () {
    fn get(self, compiler: &mut Compiler) -> DefId {
        compiler.defs.alloc_def_id(CORE_PKG)
    }
}
