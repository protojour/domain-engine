use std::{borrow::Cow, collections::HashMap, ops::Range};

use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::Cardinality, text_like_types::TextLikeType, vm::proc::BuiltinProc, DefId, PackageId,
    RelationshipId, Role,
};
use smartstring::alias::String;

use crate::{
    mem::Intern, namespace::Space, package::ONTOL_PKG, pattern::PatId, primitive::PrimitiveKind,
    regex_util::parse_literal_regex, source::SourceSpan, strings::Strings, types::Type, Compiler,
    SpannedBorrow, NO_SPAN,
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
    Primitive(PrimitiveKind, Option<&'static str>),
    TextLiteral(&'m str),
    NumberLiteral(&'m str),
    EmptySequence,
    Regex(&'m str),
    /// A type definition in some domain:
    Type(TypeDef<'m>),
    BuiltinRelType(BuiltinRelationKind, Option<&'static str>),
    FmtTransition(DefId, FmtFinalState),
    Relationship(Relationship<'m>),
    // FIXME: This should not be builtin proc directly.
    // we may find the _actual_ builtin proc to call during type check,
    // if there are different variants per type.
    Fn(BuiltinProc),
    Constant(PatId),
    Mapping {
        ident: Option<&'m str>,
        arms: [PatId; 2],
        var_alloc: ontol_hir::VarAllocator,
    },
    AutoMapping,
}

impl<'m> DefKind<'m> {
    pub fn opt_identifier(&self) -> Option<Cow<str>> {
        match self {
            Self::Package(_) => None,
            Self::Primitive(_, ident) => ident.map(|ident| ident.into()),
            Self::TextLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::NumberLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::Regex(_) => None,
            Self::EmptySequence => None,
            Self::Fn(_) => None,
            Self::Type(domain_type) => domain_type.ident.map(|ident| ident.into()),
            Self::BuiltinRelType(_, ident) => ident.map(|ident| ident.into()),
            Self::FmtTransition(..) => None,
            Self::Relationship(_) => None,
            Self::Constant(_) => None,
            Self::Mapping { ident, .. } => ident.map(|ident| ident.into()),
            Self::AutoMapping => None,
        }
    }
}

#[derive(Debug)]
pub struct TypeDef<'m> {
    /// Whether the definition's export is ignored
    pub visibility: DefVisibility,
    /// Whether the definition can have an "open relationship" to arbitrary data
    pub open: bool,
    pub ident: Option<&'m str>,
    pub rel_type_for: Option<RelationshipId>,
    /// for now: Every user-domain defined type is concrete.
    pub concrete: bool,
}

#[derive(Copy, Clone, Debug)]
pub enum DefVisibility {
    Public,
    Private,
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

/// This definition expresses that a relation is a relationship between a subject and an object
#[derive(Debug)]
pub struct Relationship<'m> {
    pub relation_def_id: DefId,

    pub subject: (DefId, SourceSpan),
    /// The cardinality of the relationship, i.e. how many objects are related to the subject
    pub subject_cardinality: Cardinality,

    pub object: (DefId, SourceSpan),
    /// How many subjects are related to the object
    pub object_cardinality: Cardinality,

    pub object_prop: Option<&'m str>,

    pub rel_params: RelParams,
}

impl<'m> Relationship<'m> {
    /// Get relationship data by a specific role: subject or object
    pub fn by(&self, role: Role) -> (DefId, Cardinality, SourceSpan) {
        match role {
            Role::Subject => (self.subject.0, self.subject_cardinality, self.subject.1),
            Role::Object => (self.object.0, self.object_cardinality, self.object.1),
        }
    }
}

#[derive(Debug)]
pub enum RelParams {
    Unit,
    Type(DefId),
    IndexRange(Range<Option<u16>>),
}

#[derive(Clone)]
pub struct RelationshipMeta<'d, 'm> {
    pub relationship_id: RelationshipId,
    pub relationship: SpannedBorrow<'d, Relationship<'m>>,
    pub relation_def_kind: SpannedBorrow<'d, DefKind<'m>>,
}

#[derive(Debug)]
pub struct Defs<'m> {
    def_id_allocators: FnvHashMap<PackageId, u16>,
    pub(crate) table: FnvHashMap<DefId, Def<'m>>,
    pub(crate) string_literals: HashMap<&'m str, DefId>,
    pub(crate) regex_strings: HashMap<&'m str, DefId>,
    pub(crate) literal_regex_meta_table: FnvHashMap<DefId, RegexMeta<'m>>,
    pub(crate) string_like_types: FnvHashMap<DefId, TextLikeType>,
}

impl<'m> Default for Defs<'m> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct RegexMeta<'m> {
    pub pattern: &'m str,
    pub ast: regex_syntax::ast::Ast,
    pub hir: regex_syntax::hir::Hir,
}

impl<'m> Defs<'m> {
    fn new() -> Self {
        Self {
            def_id_allocators: Default::default(),
            table: Default::default(),
            string_literals: Default::default(),
            regex_strings: Default::default(),
            literal_regex_meta_table: Default::default(),
            string_like_types: Default::default(),
        }
    }

    pub fn def_kind(&self, def_id: DefId) -> &DefKind<'m> {
        self.table.get(&def_id).map(|def| &def.kind).unwrap()
    }

    pub fn def_span(&self, def_id: DefId) -> SourceSpan {
        self.table.get(&def_id).map(|def| def.span).unwrap()
    }

    pub fn get_spanned_def_kind(&self, def_id: DefId) -> Option<SpannedBorrow<'_, DefKind<'m>>> {
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

    pub fn iter_package_def_ids(&self, package_id: PackageId) -> impl Iterator<Item = DefId> {
        let max_idx = self
            .def_id_allocators
            .get(&package_id)
            .cloned()
            .unwrap_or(0);

        (0..max_idx).map(move |idx| DefId(package_id, idx))
    }

    pub fn add_def(&mut self, kind: DefKind<'m>, package: PackageId, span: SourceSpan) -> DefId {
        let def_id = self.alloc_def_id(package);
        self.table.insert(
            def_id,
            Def {
                id: def_id,
                package,
                span,
                kind,
            },
        );

        def_id
    }

    pub fn add_builtin_relation(
        &mut self,
        kind: BuiltinRelationKind,
        ident: Option<&'static str>,
    ) -> DefId {
        self.add_def(DefKind::BuiltinRelType(kind, ident), ONTOL_PKG, NO_SPAN)
    }

    pub fn def_string_literal(&mut self, lit: &str, strings: &mut Strings<'m>) -> DefId {
        match self.string_literals.get(&lit) {
            Some(def_id) => *def_id,
            None => {
                let lit = strings.intern(lit);
                let def_id = self.add_def(DefKind::TextLiteral(lit), ONTOL_PKG, NO_SPAN);
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
                let lit = strings.intern(lit);
                let hir = parse_literal_regex(lit, span)?;
                let def_id = self.add_def(DefKind::Regex(lit), ONTOL_PKG, NO_SPAN);
                self.regex_strings.insert(lit, def_id);
                self.literal_regex_meta_table.insert(def_id, hir);

                Ok(def_id)
            }
        }
    }

    pub fn get_string_representation(&self, def_id: DefId) -> &str {
        match self.def_kind(def_id) {
            DefKind::TextLiteral(lit) => lit,
            DefKind::Regex(lit) => lit,
            kind => panic!("BUG: not a string literal: {kind:?}"),
        }
    }

    pub fn add_primitive(&mut self, kind: PrimitiveKind, ident: Option<&'static str>) -> DefId {
        self.add_def(DefKind::Primitive(kind, ident), ONTOL_PKG, NO_SPAN)
    }
}

#[cfg_attr(test, unimock::unimock(api = LookupRelationshipMetaMock))]
pub trait LookupRelationshipMeta<'m> {
    fn relationship_meta(&self, relationship_id: RelationshipId) -> RelationshipMeta<'_, 'm>;
}

impl<'m> LookupRelationshipMeta<'m> for Defs<'m> {
    #[track_caller]
    fn relationship_meta(&self, relationship_id: RelationshipId) -> RelationshipMeta<'_, 'm> {
        let relationship = self
            .get_spanned_def_kind(relationship_id.0)
            .unwrap()
            .filter(|kind| match kind {
                DefKind::Relationship(relationship) => Some(relationship),
                _ => None,
            })
            .unwrap();

        let relation_def_kind = self
            .get_spanned_def_kind(relationship.relation_def_id)
            .or_else(|| panic!("No def for relation id"))
            .unwrap();

        RelationshipMeta {
            relationship_id,
            relationship,
            relation_def_kind,
        }
    }
}

impl<'m> Compiler<'m> {
    pub fn add_named_def(
        &mut self,
        name: &'m str,
        space: Space,
        kind: DefKind<'m>,
        package: PackageId,
        span: SourceSpan,
    ) -> DefId {
        let def_id = self.defs.alloc_def_id(package);
        self.namespaces
            .get_namespace_mut(package, space)
            .insert(name, def_id);
        self.defs.table.insert(
            def_id,
            Def {
                id: def_id,
                package,
                span,
                kind,
            },
        );

        def_id
    }

    pub fn define_package(&mut self, package_def_id: DefId) -> DefId {
        let package_id = package_def_id.package_id();
        self.defs.table.insert(
            package_def_id,
            Def {
                id: package_def_id,
                package: package_id,
                span: NO_SPAN,
                kind: DefKind::Package(package_def_id.package_id()),
            },
        );
        let ty = self.types.intern(Type::Package);
        self.def_types.table.insert(package_def_id, ty);

        // make sure the namespace exists
        let namespace = self.namespaces.get_namespace_mut(package_id, Space::Type);

        // The name `ontol` is always defined, and refers to the ontol domain
        namespace.insert("ontol", self.primitives.ontol_domain);

        package_def_id
    }
}
