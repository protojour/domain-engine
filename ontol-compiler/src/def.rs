use std::{borrow::Cow, collections::HashMap, ops::Range};

use ontol_runtime::{proc::BuiltinProc, DefId, PackageId, RelationId};
use smallvec::SmallVec;

use crate::{
    compiler::Compiler,
    expr::ExprId,
    mem::{Intern, Mem},
    namespace::Space,
    pattern::StringPatternSegment,
    regex::uuid_regex,
    relation::{Constructor, RelationshipId},
    source::{Package, SourceSpan, CORE_PKG},
    types::{Type, TypeRef},
};

/// A definition in some package
#[derive(Debug)]
pub struct Def<'m> {
    pub id: DefId,
    pub package: PackageId,
    pub kind: DefKind<'m>,
    pub span: SourceSpan,
}

#[derive(Debug)]
pub enum DefKind<'m> {
    Primitive(Primitive),
    StringLiteral(&'m str),
    EmptySequence,
    DomainType(Option<&'m str>),
    DomainEntity(&'m str),
    Relation(Relation<'m>),
    Relationship(Relationship),
    // FIXME: This should not be builtin proc directly.
    // we may find the _actual_ builtin proc to call during type check,
    // if there are different variants per type.
    CoreFn(BuiltinProc),
    Equation(Variables, ExprId, ExprId),
}

impl<'m> DefKind<'m> {
    pub fn opt_identifier(&self) -> Option<Cow<str>> {
        match self {
            Self::Primitive(Primitive::Unit) => Some("unit".into()),
            Self::Primitive(Primitive::Int) => Some("int".into()),
            Self::Primitive(Primitive::Number) => Some("number".into()),
            Self::Primitive(Primitive::String) => Some("string".into()),
            Self::Primitive(Primitive::Uuid) => Some("uuid".into()),
            Self::StringLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::EmptySequence => None,
            Self::CoreFn(_) => None,
            Self::DomainType(opt_ident) => opt_ident.map(|ident| ident.into()),
            Self::DomainEntity(ident) => Some((*ident).into()),
            Self::Relation(_) => None,
            Self::Relationship(_) => None,
            Self::Equation(_, _, _) => None,
        }
    }
}

#[derive(Debug)]
pub struct Variables(pub SmallVec<[(ExprId, SourceSpan); 2]>);

#[derive(Debug)]
pub enum Primitive {
    /// The unit data type which contains no information
    Unit,
    /// All the integers
    Int,
    /// All numbers (realistically all rational numbers as all computer numbers are rational)
    Number,
    String,
    Uuid,
}

/// This definition expresses that a relation _exists_
#[derive(Debug)]
pub struct Relation<'m> {
    pub ident: RelationIdent,
    pub subject_prop: Option<&'m str>,
    pub object_prop: Option<&'m str>,
}

impl<'m> Relation<'m> {
    pub fn ident_def(&self) -> Option<DefId> {
        match self.ident {
            RelationIdent::Named(def_id) | RelationIdent::Typed(def_id) => Some(def_id),
            _ => None,
        }
    }

    pub fn named_ident(&self, defs: &'m Defs) -> Option<&'m str> {
        match self.ident {
            RelationIdent::Named(def_id) => match defs.get_def_kind(def_id) {
                Some(DefKind::StringLiteral(lit)) => Some(lit),
                _ => panic!(),
            },
            _ => None,
        }
    }

    pub fn subject_prop(&self, defs: &'m Defs) -> Option<&'m str> {
        self.subject_prop.or_else(|| self.named_ident(defs))
    }

    pub fn object_prop(&self, defs: &'m Defs) -> Option<&'m str> {
        self.object_prop.or_else(|| self.named_ident(defs))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RelationIdent {
    Named(DefId),
    Typed(DefId),
    Indexed,
}

/// This definition expresses that a relation is a relationship between a subject and an object
#[derive(Debug)]
pub struct Relationship {
    pub relation_id: RelationId,

    pub subject: (DefId, SourceSpan),
    /// The cardinality of the relationship, i.e. how many objects are related to the subject
    pub subject_cardinality: Cardinality,

    pub object: (DefId, SourceSpan),
    /// How many subjects are related to the object
    pub object_cardinality: Cardinality,

    pub rel_params: RelParams,
}

#[derive(Debug)]
pub enum RelParams {
    Unit,
    Type(DefId),
    IndexRange(Range<Option<u16>>),
}

pub type Cardinality = (PropertyCardinality, ValueCardinality);

#[derive(Clone, Copy, Debug)]
pub enum PropertyCardinality {
    Optional,
    Mandatory,
}

impl PropertyCardinality {
    pub fn is_mandatory(&self) -> bool {
        matches!(self, Self::Mandatory)
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ValueCardinality {
    One,
    Many,
    // ManyInRange(Option<u16>, Option<u16>),
}

#[derive(Debug)]
pub struct Defs<'m> {
    pub(crate) mem: &'m Mem,
    next_def_id: DefId,
    next_expr_id: ExprId,
    unit: DefId,
    indexed_relation: DefId,
    empty_sequence: DefId,
    empty_string: DefId,
    int: DefId,
    number: DefId,
    string: DefId,
    uuid: DefId,
    pub(crate) map: HashMap<DefId, &'m Def<'m>>,
    pub(crate) string_literals: HashMap<&'m str, DefId>,
}

impl<'m> Defs<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        let mut defs = Self {
            mem,
            next_def_id: DefId(0),
            next_expr_id: ExprId(0),
            unit: DefId(0),
            indexed_relation: DefId(0),
            empty_sequence: DefId(0),
            empty_string: DefId(0),
            int: DefId(0),
            number: DefId(0),
            string: DefId(0),
            uuid: DefId(0),
            map: Default::default(),
            string_literals: Default::default(),
        };

        defs.unit = defs.add_primitive(Primitive::Unit);
        assert_eq!(DefId::unit(), defs.unit);

        // Add some extremely fundamental definitions here already.
        // These are even independent from CORE being defined.

        defs.indexed_relation = defs.add_def(
            DefKind::Relation(Relation {
                ident: RelationIdent::Indexed,
                subject_prop: None,
                object_prop: None,
            }),
            CORE_PKG,
            SourceSpan::none(),
        );
        defs.empty_sequence = defs.add_def(DefKind::EmptySequence, CORE_PKG, SourceSpan::none());
        defs.empty_string = defs.add_def(DefKind::StringLiteral(""), CORE_PKG, SourceSpan::none());

        defs.int = defs.add_primitive(Primitive::Int);
        defs.number = defs.add_primitive(Primitive::Number);
        defs.string = defs.add_primitive(Primitive::String);
        defs.uuid = defs.add_primitive(Primitive::Uuid);

        defs
    }

    pub fn unit(&self) -> DefId {
        self.unit
    }

    pub fn indexed_relation(&self) -> DefId {
        self.indexed_relation
    }

    pub fn empty_sequence(&self) -> DefId {
        self.empty_sequence
    }

    pub fn empty_string(&self) -> DefId {
        self.empty_string
    }

    pub fn int(&self) -> DefId {
        self.int
    }

    pub fn number(&self) -> DefId {
        self.number
    }

    pub fn string(&self) -> DefId {
        self.string
    }

    pub fn uuid(&self) -> DefId {
        self.uuid
    }

    pub fn get_def_kind(&self, def_id: DefId) -> Option<&'m DefKind<'m>> {
        self.map.get(&def_id).map(|def| &def.kind)
    }

    pub fn get_relationship_defs(
        &self,
        relationship_id: RelationshipId,
    ) -> Result<(&'m Relationship, &'m Relation<'m>), ()> {
        let DefKind::Relationship(relationship) = self.get_def_kind(relationship_id.0).ok_or(())? else {
            return Err(());
        };
        let DefKind::Relation(relation) = self.get_def_kind(relationship.relation_id.0).ok_or(())? else {
            return Err(());
        };
        Ok((relationship, relation))
    }

    pub fn alloc_def_id(&mut self) -> DefId {
        let id = self.next_def_id;
        self.next_def_id.0 += 1;
        id
    }

    pub fn alloc_expr_id(&mut self) -> ExprId {
        let id = self.next_expr_id;
        self.next_expr_id.0 += 1;
        id
    }

    pub fn add_def(&mut self, kind: DefKind<'m>, package: PackageId, span: SourceSpan) -> DefId {
        let def_id = self.alloc_def_id();
        self.map.insert(
            def_id,
            self.mem.bump.alloc(Def {
                id: def_id,
                package,
                span,
                kind,
            }),
        );

        def_id
    }

    pub fn def_string_literal(&mut self, lit: &'m str) -> DefId {
        match self.string_literals.get(&lit) {
            Some(def_id) => *def_id,
            None => {
                let def_id =
                    self.add_def(DefKind::StringLiteral(lit), CORE_PKG, SourceSpan::none());
                self.string_literals.insert(lit, def_id);
                def_id
            }
        }
    }

    pub fn get_string_literal(&self, def_id: DefId) -> &str {
        match self.get_def_kind(def_id) {
            Some(DefKind::StringLiteral(lit)) => lit,
            kind => panic!("BUG: not a string literal: {kind:?}"),
        }
    }

    fn add_primitive(&mut self, primitive: Primitive) -> DefId {
        self.add_def(DefKind::Primitive(primitive), CORE_PKG, SourceSpan::none())
    }
}

impl<'m> Compiler<'m> {
    pub fn add_named_def(
        &mut self,
        name: &str,
        space: Space,
        kind: DefKind<'m>,
        package: PackageId,
        span: SourceSpan,
    ) -> DefId {
        let def_id = self.defs.alloc_def_id();
        self.namespaces
            .get_mut(package, space)
            .insert(name.into(), def_id);
        self.defs.map.insert(
            def_id,
            self.defs.mem.bump.alloc(Def {
                id: def_id,
                package,
                span,
                kind,
            }),
        );

        def_id
    }

    pub fn with_core(mut self) -> Self {
        self.packages.insert(
            CORE_PKG,
            Package {
                name: "core".into(),
            },
        );

        // fundamental types
        let _ = self.def_core_type(self.defs.unit, Type::Unit);
        let _ = self.def_core_type(self.defs.empty_sequence, Type::EmptySequence);
        let int_ty = self.def_core_type_name(self.defs.int, "int", Type::Int);
        let _ = self.def_core_type_name(self.defs.number, "number", Type::Number);
        let string_ty = self.def_core_type_name(self.defs.string, "string", Type::String);
        let _ = self.def_core_type_name(self.defs.uuid, "uuid", Type::Uuid);

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

        // string-like types
        self.relations
            .properties_by_type_mut(self.defs.uuid)
            .constructor = Constructor::StringPattern(StringPatternSegment::Regex(uuid_regex()));

        self
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
        compiler.defs.alloc_def_id()
    }
}
