use std::{borrow::Cow, collections::HashMap, ops::Range};

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    proc::BuiltinProc, string_types::StringLikeType, DefId, DefParamId, PackageId, RelationId,
};
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{
    compiler_queries::RelationshipMeta,
    expr::ExprId,
    mem::{Intern, Mem},
    namespace::Space,
    package::CORE_PKG,
    regex_util::parse_literal_regex_to_hir,
    relation::RelationshipId,
    source::SourceSpan,
    strings::Strings,
    types::Type,
    Compiler, SpannedBorrow,
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
    Primitive(Primitive),
    StringLiteral(&'m str),
    EmptySequence,
    Regex(&'m str),
    /// A type definition in some domain:
    Type(TypeDef<'m>),
    Relation(Relation<'m>),
    Relationship(Relationship),
    // FIXME: This should not be builtin proc directly.
    // we may find the _actual_ builtin proc to call during type check,
    // if there are different variants per type.
    CoreFn(BuiltinProc),
    Mapping(Variables, ExprId, ExprId),
}

impl<'m> DefKind<'m> {
    pub fn opt_identifier(&self) -> Option<Cow<str>> {
        match self {
            Self::Package(_) => None,
            Self::Primitive(Primitive::Unit) => Some("unit".into()),
            Self::Primitive(Primitive::Int) => Some("int".into()),
            Self::Primitive(Primitive::Number) => Some("number".into()),
            Self::Primitive(Primitive::String) => Some("string".into()),
            Self::StringLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::Regex(_) => None,
            Self::EmptySequence => None,
            Self::CoreFn(_) => None,
            Self::Type(domain_type) => domain_type.ident.map(|ident| ident.into()),
            Self::Relation(_) => None,
            Self::Relationship(_) => None,
            Self::Mapping(_, _, _) => None,
        }
    }
}

#[derive(Debug)]
pub struct TypeDef<'m> {
    pub public: bool,
    pub ident: Option<&'m str>,
    pub params: Option<IndexMap<&'m str, TypeDefParam>>,
}

#[derive(Debug)]
pub struct TypeDefParam {
    pub id: DefParamId,
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

#[derive(Debug, Clone)]
pub struct DefReference {
    pub def_id: DefId,
    pub pattern_bindings: FnvHashMap<DefParamId, DefParamBinding>,
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
    pub object_prop: Option<&'m str>,
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

    pub fn object_prop(&self, defs: &'m Defs) -> Option<&'m str> {
        self.object_prop.or_else(|| self.named_ident(defs))
    }
}

#[derive(Clone, Debug)]
pub enum RelationKind {
    Named(DefReference),
    Transition(DefReference),
    Is,
    Identifies,
    Indexed,
}

/// This definition expresses that a relation is a relationship between a subject and an object
#[derive(Debug)]
pub struct Relationship {
    pub relation_id: RelationId,

    pub subject: (DefReference, SourceSpan),
    /// The cardinality of the relationship, i.e. how many objects are related to the subject
    pub subject_cardinality: Cardinality,

    pub object: (DefReference, SourceSpan),
    /// How many subjects are related to the object
    pub object_cardinality: Cardinality,

    pub rel_params: RelParams,
}

#[derive(Debug)]
pub enum RelParams {
    Unit,
    Type(DefReference),
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
    def_id_allocators: FnvHashMap<PackageId, u16>,
    next_def_param: DefParamId,
    next_expr_id: ExprId,
    unit: DefId,
    is_relation: DefId,
    identifies_relation: DefId,
    indexed_relation: DefId,
    empty_sequence: DefId,
    empty_string: DefId,
    int: DefId,
    number: DefId,
    string: DefId,
    pub(crate) map: FnvHashMap<DefId, &'m Def<'m>>,
    pub(crate) string_literals: HashMap<&'m str, DefId>,
    pub(crate) regex_strings: HashMap<&'m str, DefId>,
    pub(crate) literal_regex_hirs: FnvHashMap<DefId, regex_syntax::hir::Hir>,
    pub(crate) string_like_types: FnvHashMap<DefId, StringLikeType>,
}

impl<'m> Defs<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        let mut defs = Self {
            mem,
            def_id_allocators: Default::default(),
            next_expr_id: ExprId(0),
            next_def_param: DefParamId(0),
            unit: DefId::unit(),
            is_relation: DefId::unit(),
            identifies_relation: DefId::unit(),
            indexed_relation: DefId::unit(),
            empty_sequence: DefId::unit(),
            empty_string: DefId::unit(),
            int: DefId::unit(),
            number: DefId::unit(),
            string: DefId::unit(),
            map: Default::default(),
            string_literals: Default::default(),
            regex_strings: Default::default(),
            literal_regex_hirs: Default::default(),
            string_like_types: Default::default(),
        };

        defs.unit = defs.add_primitive(Primitive::Unit);
        assert_eq!(DefId::unit(), defs.unit);

        // Add some extremely fundamental definitions here already.
        // These are even independent from CORE being defined.
        defs.is_relation = defs.add_def(
            DefKind::Relation(Relation {
                kind: RelationKind::Is,
                subject_prop: None,
                object_prop: None,
            }),
            CORE_PKG,
            SourceSpan::none(),
        );
        defs.identifies_relation = defs.add_def(
            DefKind::Relation(Relation {
                kind: RelationKind::Identifies,
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

    pub fn is_relation(&self) -> DefId {
        self.is_relation
    }

    pub fn identifies_relation(&self) -> DefId {
        self.identifies_relation
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

    pub fn get_spanned_def_kind(&self, def_id: DefId) -> Option<SpannedBorrow<'m, DefKind<'m>>> {
        self.map.get(&def_id).map(|def| SpannedBorrow {
            value: &def.kind,
            span: &def.span,
        })
    }

    pub fn lookup_relationship_meta(
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

        Ok((relationship, relation))
    }

    pub fn alloc_def_id(&mut self, package_id: PackageId) -> DefId {
        let idx = self.def_id_allocators.entry(package_id).or_default();
        let def_id = DefId(package_id, *idx);
        *idx += 1;

        def_id
    }

    pub fn alloc_expr_id(&mut self) -> ExprId {
        let id = self.next_expr_id;
        self.next_expr_id.0 += 1;
        id
    }

    pub fn alloc_def_param_id(&mut self) -> DefParamId {
        let id = self.next_def_param;
        self.next_def_param.0 += 1;
        id
    }

    pub fn add_def(&mut self, kind: DefKind<'m>, package: PackageId, span: SourceSpan) -> DefId {
        let def_id = self.alloc_def_id(package);
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

    pub fn def_string_literal(&mut self, lit: &str, strings: &mut Strings<'m>) -> DefId {
        match self.string_literals.get(&lit) {
            Some(def_id) => *def_id,
            None => {
                let lit = strings.intern(lit);
                let def_id =
                    self.add_def(DefKind::StringLiteral(lit), CORE_PKG, SourceSpan::none());
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
                let def_id = self.add_def(DefKind::Regex(lit), CORE_PKG, SourceSpan::none());
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

    pub fn add_primitive(&mut self, primitive: Primitive) -> DefId {
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
        let def_id = self.defs.alloc_def_id(package);
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

    pub fn define_package(&mut self, package_id: PackageId) -> DefId {
        let def_id = self.defs.alloc_def_id(package_id);
        self.defs.map.insert(
            def_id,
            self.defs.mem.bump.alloc(Def {
                id: def_id,
                package: package_id,
                span: SourceSpan::none(),
                kind: DefKind::Package(package_id),
            }),
        );
        let ty = self.types.intern(Type::Package);
        self.def_types.map.insert(def_id, ty);

        // make sure the namespace exists
        self.namespaces.get_mut(package_id, Space::Type);

        def_id
    }
}
