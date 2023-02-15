use std::{borrow::Cow, collections::HashMap, ops::Range};

use ontol_runtime::{proc::BuiltinProc, DefId, PackageId, RelationId};
use smallvec::SmallVec;

use crate::{
    compiler::Compiler,
    expr::ExprId,
    mem::{Intern, Mem},
    namespace::Space,
    relation::RelationshipId,
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
    DomainType(&'m str),
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
            Self::StringLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::EmptySequence => None,
            Self::CoreFn(_) => None,
            Self::DomainType(ident) => Some((*ident).into()),
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
}

/// This definition expresses that a relation _exists_
#[derive(Debug)]
pub struct Relation<'m> {
    pub kind: RelationKind,
    pub subject_prop: Option<&'m str>,
    pub object_prop: Option<&'m str>,
}

impl<'m> Relation<'m> {
    pub fn named_ident(&self, defs: &'m Defs) -> Option<&'m str> {
        match self.kind {
            RelationKind::Named(def_id) => match defs.get_def_kind(def_id) {
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

#[derive(Clone, Debug)]
pub enum RelationKind {
    Named(DefId),
    Typed(DefId),
    Indexed,
    Anonymous,
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
    anonymous_relation: DefId,
    indexed_relation: DefId,
    empty_sequence: DefId,
    empty_string: DefId,
    int: DefId,
    number: DefId,
    string: DefId,
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
            anonymous_relation: DefId(0),
            indexed_relation: DefId(0),
            empty_sequence: DefId(0),
            empty_string: DefId(0),
            int: DefId(0),
            number: DefId(0),
            string: DefId(0),
            map: Default::default(),
            string_literals: Default::default(),
        };

        defs.unit = defs.add_primitive(Primitive::Unit);
        assert_eq!(DefId::unit(), defs.unit);

        // Add some extremely fundamental definitions here already.
        // These are even independent from CORE being defined.

        // The anonymous / "manifested-as" relation
        defs.anonymous_relation = defs.add_def(
            DefKind::Relation(Relation {
                kind: RelationKind::Anonymous,
                subject_prop: None,
                object_prop: None,
            }),
            CORE_PKG,
            SourceSpan::none(),
        );
        defs.indexed_relation = defs.add_def(
            DefKind::Relation(Relation {
                kind: RelationKind::Indexed,
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

        defs
    }

    pub fn unit(&self) -> DefId {
        self.unit
    }

    pub fn anonymous_relation(&self) -> DefId {
        self.anonymous_relation
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
        let unit_ty = self.types.intern(Type::Unit(self.defs.unit));
        let empty_sequence_ty = self
            .types
            .intern(Type::EmptySequence(self.defs.empty_sequence));
        let int_ty = self.types.intern(Type::Int(self.defs.int));
        let num_ty = self.types.intern(Type::Number(self.defs.number));
        let string_ty = self.types.intern(Type::String(self.defs.string));

        self.def_types.map.insert(self.defs.unit, unit_ty);
        self.def_types
            .map
            .insert(self.defs.empty_sequence, empty_sequence_ty);

        let int = self.def_core_type("int", self.defs.int, int_ty);
        let _number = self.def_core_type("number", self.defs.number, num_ty);
        let string = self.def_core_type("string", self.defs.string, string_ty);

        let int_int = self.types.intern([int, int]);
        let string_string = self.types.intern([string, string]);

        let int_int_to_int = self.types.intern(Type::Function {
            params: int_int,
            output: int,
        });
        let string_string_to_string = self.types.intern(Type::Function {
            params: string_string,
            output: string,
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

        self
    }

    fn def_core_type(&mut self, ident: &str, def_id: DefId, ty: TypeRef<'m>) -> TypeRef<'m> {
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
