use std::{borrow::Cow, collections::HashMap, ops::Range};

use derive_debug_extras::DebugExtras;
use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    ontology::Cardinality, string_types::StringLikeType, vm::proc::BuiltinProc, DefId, DefParamId,
    PackageId, RelationshipId, Role,
};
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{
    expr::ExprId,
    mem::{Intern, Mem},
    namespace::Space,
    package::ONTOL_PKG,
    primitive::PrimitiveKind,
    regex_util::parse_literal_regex_to_hir,
    source::SourceSpan,
    strings::Strings,
    types::Type,
    Compiler, SpannedBorrow, NO_SPAN,
};
use ontol_parser::Span;

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
    Package(PackageId),
    Primitive(PrimitiveKind),
    StringLiteral(&'m str),
    NumberLiteral(&'m str),
    EmptySequence,
    Regex(&'m str),
    /// A type definition in some domain:
    Type(TypeDef<'m>),
    Relation(Relation<'m>),
    Relationship(Relationship<'m>),
    // FIXME: This should not be builtin proc directly.
    // we may find the _actual_ builtin proc to call during type check,
    // if there are different variants per type.
    Fn(BuiltinProc),
    Constant(ExprId),
    Mapping(MapDirection, Variables, ExprId, ExprId),
}

impl<'m> DefKind<'m> {
    pub fn opt_identifier(&self) -> Option<Cow<str>> {
        match self {
            Self::Package(_) => None,
            Self::Primitive(PrimitiveKind::Unit) => Some("unit".into()),
            Self::Primitive(PrimitiveKind::False) => Some("false".into()),
            Self::Primitive(PrimitiveKind::True) => Some("true".into()),
            Self::Primitive(PrimitiveKind::Bool) => Some("bool".into()),
            Self::Primitive(PrimitiveKind::Int) => Some("int".into()),
            Self::Primitive(PrimitiveKind::Number) => Some("number".into()),
            Self::Primitive(PrimitiveKind::String) => Some("string".into()),
            Self::StringLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::NumberLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::Regex(_) => None,
            Self::EmptySequence => None,
            Self::Fn(_) => None,
            Self::Type(domain_type) => domain_type.ident.map(|ident| ident.into()),
            Self::Relation(_) => None,
            Self::Relationship(_) => None,
            Self::Constant(_) => None,
            Self::Mapping(..) => None,
        }
    }
}

#[derive(Debug)]
pub struct TypeDef<'m> {
    pub public: bool,
    pub ident: Option<&'m str>,
    pub params: Option<IndexMap<&'m str, TypeDefParam>>,
    pub rel_type_for: Option<RelationshipId>,
}

#[derive(Debug)]
pub struct TypeDefParam {
    pub id: DefParamId,
}

#[derive(Debug)]
pub struct Variables(pub SmallVec<[ExprId; 2]>);

#[derive(Debug, Clone)]
pub struct DefReference {
    pub def_id: DefId,
    pub pattern_bindings: Option<FnvHashMap<DefParamId, DefParamBinding>>,
}

impl From<DefId> for DefReference {
    fn from(value: DefId) -> Self {
        Self {
            def_id: value,
            pattern_bindings: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DefParamBinding {
    Bound(u32),
    Provided(DefReference, SourceSpan),
}

/// This definition expresses that a relation _exists_
#[derive(Debug)]
pub struct Relation<'m> {
    pub kind: RelationKind,
    pub subject_prop: Option<&'m str>,
}

impl<'m> Relation<'m> {
    pub fn named_ident(&self, defs: &'m Defs) -> Option<&'m str> {
        match &self.kind {
            RelationKind::Named(def) => match defs.get_def_kind(def.def_id) {
                Some(DefKind::StringLiteral(lit)) => Some(lit),
                _ => panic!(),
            },
            _ => None,
        }
    }

    pub fn subject_prop(&self, defs: &'m Defs) -> Option<&'m str> {
        self.subject_prop.or_else(|| self.named_ident(defs))
    }
}

#[derive(Clone, Debug)]
pub enum RelationKind {
    Named(DefReference),
    FmtTransition(DefReference, FmtFinalState),
    Builtin(BuiltinRelationKind),
}

#[derive(Clone, Copy, Debug)]
pub struct FmtFinalState(pub bool);

#[derive(Clone, Debug)]
pub enum BuiltinRelationKind {
    Is,
    Identifies,
    Id,
    Indexed,
    Min,
    Max,
    Default,
    Gen,
    Route,
    Doc,
    Example,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct RelationId(pub DefId);

/// This definition expresses that a relation is a relationship between a subject and an object
#[derive(Debug)]
pub struct Relationship<'m> {
    pub relation_id: RelationId,

    pub subject: (DefReference, SourceSpan),
    /// The cardinality of the relationship, i.e. how many objects are related to the subject
    pub subject_cardinality: Cardinality,

    pub object: (DefReference, SourceSpan),
    /// How many subjects are related to the object
    pub object_cardinality: Cardinality,

    pub object_prop: Option<&'m str>,

    pub rel_params: RelParams,
}

impl<'m> Relationship<'m> {
    pub fn left_side(&self, role: Role) -> (&DefReference, SourceSpan, Cardinality) {
        match role {
            Role::Subject => (&self.subject.0, self.subject.1, self.subject_cardinality),
            Role::Object => (&self.object.0, self.object.1, self.object_cardinality),
        }
    }

    pub fn right_side(&self, role: Role) -> (&DefReference, SourceSpan, Cardinality) {
        match role {
            Role::Subject => (&self.object.0, self.object.1, self.object_cardinality),
            Role::Object => (&self.subject.0, self.subject.1, self.subject_cardinality),
        }
    }
}

#[derive(Debug)]
pub enum RelParams {
    Unit,
    Type(DefReference),
    IndexRange(Range<Option<u16>>),
}

#[derive(Clone)]
pub struct RelationshipMeta<'m> {
    pub relationship_id: RelationshipId,
    pub relationship: SpannedBorrow<'m, Relationship<'m>>,
    pub relation: SpannedBorrow<'m, Relation<'m>>,
}

#[derive(Clone, Copy, Debug)]
pub enum MapDirection {
    Omni,
    Forwards,
}

#[derive(Debug)]
pub struct Defs<'m> {
    pub(crate) mem: &'m Mem,
    def_id_allocators: FnvHashMap<PackageId, u16>,
    next_def_param: DefParamId,
    pub(crate) table: FnvHashMap<DefId, &'m Def<'m>>,
    pub(crate) string_literals: HashMap<&'m str, DefId>,
    pub(crate) regex_strings: HashMap<&'m str, DefId>,
    pub(crate) literal_regex_hirs: FnvHashMap<DefId, regex_syntax::hir::Hir>,
    pub(crate) string_like_types: FnvHashMap<DefId, StringLikeType>,
}

impl<'m> Defs<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
            def_id_allocators: Default::default(),
            next_def_param: DefParamId(0),
            table: Default::default(),
            string_literals: Default::default(),
            regex_strings: Default::default(),
            literal_regex_hirs: Default::default(),
            string_like_types: Default::default(),
        }
    }

    pub fn get_def_kind(&self, def_id: DefId) -> Option<&'m DefKind<'m>> {
        self.table.get(&def_id).map(|def| &def.kind)
    }

    pub fn get_spanned_def_kind(&self, def_id: DefId) -> Option<SpannedBorrow<'m, DefKind<'m>>> {
        self.table.get(&def_id).map(|def| SpannedBorrow {
            value: &def.kind,
            span: &def.span,
        })
    }

    pub fn alloc_def_id(&mut self, package_id: PackageId) -> DefId {
        let idx = self.def_id_allocators.entry(package_id).or_default();
        let def_id = DefId(package_id, *idx);
        *idx += 1;

        def_id
    }

    pub fn alloc_def_param_id(&mut self) -> DefParamId {
        let id = self.next_def_param;
        self.next_def_param.0 += 1;
        id
    }

    pub fn add_def(&mut self, kind: DefKind<'m>, package: PackageId, span: SourceSpan) -> DefId {
        let def_id = self.alloc_def_id(package);
        self.table.insert(
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

    pub fn add_builtin_relation(&mut self, kind: BuiltinRelationKind) -> DefId {
        self.add_def(
            DefKind::Relation(Relation {
                kind: RelationKind::Builtin(kind),
                subject_prop: None,
            }),
            ONTOL_PKG,
            NO_SPAN,
        )
    }

    pub fn def_string_literal(&mut self, lit: &str, strings: &mut Strings<'m>) -> DefId {
        match self.string_literals.get(&lit) {
            Some(def_id) => *def_id,
            None => {
                let lit = strings.intern(lit);
                let def_id = self.add_def(DefKind::StringLiteral(lit), ONTOL_PKG, NO_SPAN);
                self.string_literals.insert(lit, def_id);
                def_id
            }
        }
    }

    pub fn def_regex(
        &mut self,
        lit: &str,
        span: &Span,
        strings: &mut Strings<'m>,
    ) -> Result<DefId, (String, Span)> {
        match self.regex_strings.get(&lit) {
            Some(def_id) => Ok(*def_id),
            None => {
                let hir = parse_literal_regex_to_hir(lit, span)?;
                let lit = strings.intern(lit);
                let def_id = self.add_def(DefKind::Regex(lit), ONTOL_PKG, NO_SPAN);
                self.regex_strings.insert(lit, def_id);
                self.literal_regex_hirs.insert(def_id, hir);

                Ok(def_id)
            }
        }
    }

    pub fn get_string_representation(&self, def_id: DefId) -> &str {
        match self.get_def_kind(def_id) {
            Some(DefKind::StringLiteral(lit)) => lit,
            Some(DefKind::Regex(lit)) => lit,
            kind => panic!("BUG: not a string literal: {kind:?}"),
        }
    }

    pub fn add_primitive(&mut self, kind: PrimitiveKind) -> DefId {
        self.add_def(DefKind::Primitive(kind), ONTOL_PKG, NO_SPAN)
    }
}

#[cfg_attr(test, unimock::unimock(api = LookupRelationshipMetaMock))]
pub trait LookupRelationshipMeta<'m> {
    fn lookup_relationship_meta(
        &self,
        relationship_id: RelationshipId,
    ) -> Result<RelationshipMeta<'m>, ()>;
}

impl<'m> LookupRelationshipMeta<'m> for Defs<'m> {
    fn lookup_relationship_meta(
        &self,
        relationship_id: RelationshipId,
    ) -> Result<RelationshipMeta<'m>, ()> {
        let relationship = self
            .get_spanned_def_kind(relationship_id.0)
            .ok_or(())?
            .filter(|kind| match kind {
                DefKind::Relationship(relationship) => Some(relationship),
                _ => None,
            })
            .ok_or(())?;

        let relation = self
            .get_spanned_def_kind(relationship.relation_id.0)
            .ok_or(())?
            .filter(|kind| match kind {
                DefKind::Relation(relation) => Some(relation),
                _ => None,
            })
            .ok_or(())?;

        Ok(RelationshipMeta {
            relationship_id,
            relationship,
            relation,
        })
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
        let def_id = self.defs.alloc_def_id(package);
        self.namespaces
            .get_namespace_mut(package, space)
            .insert(name.into(), def_id);
        self.defs.table.insert(
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

    pub fn define_package(&mut self, package_id: PackageId) -> DefId {
        let def_id = self.defs.alloc_def_id(package_id);
        self.defs.table.insert(
            def_id,
            self.defs.mem.bump.alloc(Def {
                id: def_id,
                package: package_id,
                span: NO_SPAN,
                kind: DefKind::Package(package_id),
            }),
        );
        let ty = self.types.intern(Type::Package);
        self.def_types.table.insert(def_id, ty);

        // make sure the namespace exists
        self.namespaces.get_namespace_mut(package_id, Space::Type);

        def_id
    }
}
