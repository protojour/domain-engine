use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
};

use fnv::FnvHashMap;
use ontol_macros::RustDoc;
use ontol_parser::source::{NO_SPAN, SourceSpan};
use ontol_runtime::{
    DefId, OntolDefTag, OntolDefTagExt,
    ontology::{
        domain::{self, BasicDef},
        ontol::TextLikeType,
    },
    var::VarAllocator,
};

use crate::{
    Compiler, SpannedBorrow,
    mem::Intern,
    namespace::Space,
    pattern::PatId,
    primitive::PrimitiveKind,
    regex_util::parse_literal_regex,
    relation::RelId,
    repr::repr_model::{Repr, ReprKind, ReprScalarKind},
    strings::StringCtx,
    types::Type,
};
use ontol_core::{span::U32Span, tag::DomainIndex};

/// A definition in some package
#[derive(Debug)]
pub struct Def<'m> {
    pub id: DefId,
    #[expect(unused)]
    pub domain_index: DomainIndex,
    pub kind: DefKind<'m>,
    pub span: SourceSpan,
}

#[derive(Debug)]
pub enum DefKind<'m> {
    Domain(DomainIndex),
    /// A namespace in the ONTOL domain
    /// TODO: make it possible also for user domains to have modules?
    BuiltinModule(&'static [&'static str]),
    Primitive(PrimitiveKind, &'static [&'static str]),
    TextLiteral(&'m str),
    NumberLiteral(&'m str),
    EmptySequence,
    Regex(&'m str),
    /// A type definition in some domain:
    Type(TypeDef<'m>),
    Macro(&'m str),
    InlineUnion(BTreeSet<DefId>),
    BuiltinRelType(BuiltinRelationKind, &'static [&'static str]),
    Arc(&'m str),
    // FIXME: This should not be builtin proc directly.
    // we may find the _actual_ builtin proc to call during type check,
    // if there are different variants per type.
    Fn(ontol_hir::OverloadFunc),
    Constant(PatId),
    Mapping {
        ident: Option<&'m str>,
        arms: [PatId; 2],
        var_alloc: VarAllocator,
        extern_def_id: Option<DefId>,
        is_abstract: bool,
    },
    AutoMapping,
    Extern(&'m str),
}

impl DefKind<'_> {
    pub fn opt_identifier(&self) -> Option<Cow<str>> {
        match self {
            Self::Domain(_) => None,
            Self::BuiltinModule(_path) => None,
            Self::Primitive(_, path) => path.last().map(|ident| (*ident).into()),
            Self::TextLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::NumberLiteral(lit) => Some(format!("\"{lit}\"").into()),
            Self::Regex(_) => None,
            Self::EmptySequence => None,
            Self::Fn(_) => None,
            Self::Type(domain_type) => domain_type.ident.map(|ident| ident.into()),
            Self::Macro(ident) => Some((*ident).into()),
            Self::InlineUnion(_) => None,
            Self::BuiltinRelType(_, path) => path.last().map(|ident| (*ident).into()),
            Self::Arc(ident) => Some(Cow::Borrowed(ident)),
            Self::Constant(_) => None,
            Self::Mapping { ident, .. } => ident.map(|ident| ident.into()),
            Self::AutoMapping => None,
            Self::Extern(ident) => Some((*ident).into()),
        }
    }

    pub fn as_ontology_type_kind(&self, info: BasicDef) -> domain::DefKind {
        match self {
            DefKind::BuiltinRelType(..) => domain::DefKind::Relation(info),
            DefKind::Fn(_) => domain::DefKind::Function(info),
            DefKind::EmptySequence => domain::DefKind::Generator(info),
            DefKind::Domain(_) => domain::DefKind::Domain(info),
            _ => domain::DefKind::Data(info),
        }
    }
}

#[derive(Debug)]
pub struct TypeDef<'m> {
    pub ident: Option<&'m str>,
    pub rel_type_for: Option<RelId>,
    pub flags: TypeDefFlags,
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug)]
    pub struct TypeDefFlags: u8 {
        /// Whether the definition is exported
        const PUBLIC         = 0b00000001;
        /// Whether the definition can have an "open relationship" to arbitrary data
        const OPEN           = 0b00000010;
        /// for now: Every user-domain defined type is concrete.
        const CONCRETE       = 0b00000100;
        /// Whether the definition is a ontol-builtin symbol
        const BUILTIN_SYMBOL = 0b00001000;
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, RustDoc)]
pub enum BuiltinRelationKind {
    /// Relates one definition to another.
    /// The subject type takes on all properties of the object type,
    /// or binds the subject type to a [union](rel.md#unions) if the `is` relation is conditional.
    /// ```ontol
    /// rel* is: text
    /// ```
    Is,
    /// Binds an identifier to a type.
    Identifies,
    /// Binds a type to an identifier, making instances of a type unique entities.
    /// Usually used with the `|` operator to simultaneously bind to a named property.
    /// ```ontol
    /// rel. 'id': some_id
    /// ```
    Id,
    Indexed,
    /// Associates a data store collection name with a type.
    /// Every named type will have an implicit `store_key` name equal to their `def` name,
    /// but this allows overriding that name for storage purposes.
    /// ```ontol
    /// rel* store_key: 'specific_table'
    /// ```
    StoreKey,
    /// Minimum value for the subject type, which may be any `number`.
    /// ```ontol
    /// rel* is: number
    /// rel* min: 1
    /// ```
    Min,
    /// Maximum value for the subject type, which may be any `number`.
    /// ```ontol
    /// rel* is: number
    /// rel* max: 100
    /// ```
    Max,
    /// Assigns a default value to a type or property if none is given.
    /// Uses the operator `:=` to indicate assignment.
    /// Often used inline in a property relationship.
    /// ```ontol
    /// rel* 'active'[rel* default := true]: boolean
    /// ```
    Default,
    /// Generates a value using a [generator type](generator_types.md) if no value is given.
    /// Often used inline in a property relationship.
    /// ```ontol
    /// rel* 'id'[rel* gen: auto]|id: (rel* is: uuid)
    /// ```
    Gen,
    /// Used as a `rel` annotation to instruct ONTOL to use a specific data representation
    /// (e.g. `crdt`) for the object of that relation.
    /// ```ontol
    /// rel* 'data'[rel* repr: crdt]: Data
    /// ```
    Repr,
    /// A relation between an entity and an [ordering](interfaces.md#ordering).
    /// ```ontol
    /// order: 'some_field'
    /// ```
    Order,
    /// A relation expressing that something has a direction. Used in [ordering](interfaces.md#ordering).
    /// ```ontol
    /// direction: ascending
    /// ```
    Direction,
    /// Gives an example value for a type, for documentation purposes.
    /// ```ontol
    /// rel* is: text
    /// rel* example: 'Alice'
    /// rel* example: 'Bob'
    /// rel* example: 'Carlos'
    /// ```
    Example,
    /// A de/serialization format for an abstract type.
    /// Implemented for `octets`, which can be _formatted_ in
    /// `hex` (hexadecimal representation) or
    /// `base64` (base64 representation).
    /// ```ontol
    /// rel* is: octets
    /// rel* is: format.hex
    /// rel* is: format.base64
    /// ```
    Format,
    FmtTransition,
}

impl BuiltinRelationKind {
    pub fn context(&self) -> RelationContext {
        match self {
            Self::Is => RelationContext::Def,
            Self::Identifies => RelationContext::Def,
            Self::Id => RelationContext::Def,
            Self::Indexed => RelationContext::Def,
            Self::StoreKey => RelationContext::Def,
            Self::Min => RelationContext::Def,
            Self::Max => RelationContext::Def,
            Self::Default => RelationContext::Rel,
            Self::Gen => RelationContext::Rel,
            Self::Repr => RelationContext::Rel,
            Self::Order => RelationContext::Def,
            Self::Direction => RelationContext::Def,
            Self::Example => RelationContext::Def,
            Self::Format => RelationContext::Def,
            Self::FmtTransition => RelationContext::Def,
        }
    }
}

pub enum RelationContext {
    Def,
    Rel,
}

pub struct Defs<'m> {
    def_id_allocators: FnvHashMap<DomainIndex, DefIdAlloc>,
    pub(crate) table: FnvHashMap<DefId, Def<'m>>,
    pub(crate) text_literals: HashMap<&'m str, DefId>,
    pub(crate) regex_strings: HashMap<&'m str, DefId>,
    pub(crate) literal_regex_meta_table: FnvHashMap<DefId, RegexMeta<'m>>,
    pub(crate) text_like_types: FnvHashMap<DefId, TextLikeType>,
}

#[derive(Clone, Default)]
struct DefIdAlloc {
    persistent: u16,
    transient: u16,
}

impl Default for Defs<'_> {
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
        let mut def_id_allocators: FnvHashMap<DomainIndex, DefIdAlloc> = Default::default();
        def_id_allocators.insert(
            DomainIndex::ontol(),
            DefIdAlloc {
                persistent: OntolDefTag::_LastEntry as u16,
                transient: 0,
            },
        );

        Self {
            def_id_allocators,
            table: Default::default(),
            text_literals: Default::default(),
            regex_strings: Default::default(),
            literal_regex_meta_table: Default::default(),
            text_like_types: Default::default(),
        }
    }

    pub fn def_kind(&self, def_id: DefId) -> &DefKind<'m> {
        self.table
            .get(&def_id)
            .map(|def| &def.kind)
            .unwrap_or_else(|| panic!("no DefKind for {def_id:?}"))
    }

    pub fn is_macro(&self, def_id: DefId) -> bool {
        matches!(self.def_kind(def_id), DefKind::Macro(_))
    }

    pub fn def_kind_option(&self, def_id: DefId) -> Option<&DefKind<'m>> {
        self.table.get(&def_id).map(|def| &def.kind)
    }

    pub fn def_span(&self, def_id: DefId) -> SourceSpan {
        self.table.get(&def_id).map(|def| def.span).unwrap()
    }

    pub fn text_literal(&self, def_id: DefId) -> Option<&'m str> {
        match self.def_kind(def_id) {
            DefKind::TextLiteral(str) => Some(str),
            _ => None,
        }
    }

    pub fn get_spanned_def_kind(&self, def_id: DefId) -> Option<SpannedBorrow<'_, DefKind<'m>>> {
        self.table.get(&def_id).map(|def| SpannedBorrow {
            value: &def.kind,
            span: &def.span,
        })
    }

    pub fn alloc_persistent_def_id(&mut self, domain_index: DomainIndex) -> DefId {
        if domain_index == DomainIndex::ontol() {
            panic!("ontol domain should not dynamically allocate persistent DefIds");
        }

        let alloc = self.def_id_allocators.entry(domain_index).or_default();
        let def_id = DefId::new_persistent(domain_index, alloc.persistent);

        alloc.persistent += 1;

        def_id
    }

    pub fn register_persistent_def_id(&mut self, domain_index: DomainIndex, tag: u16) -> DefId {
        let alloc = self.def_id_allocators.entry(domain_index).or_default();
        let def_id = DefId::new_persistent(domain_index, tag);
        if tag >= alloc.persistent {
            alloc.persistent = tag + 1;
        }

        def_id
    }

    pub fn alloc_transient_def_id(&mut self, domain_index: DomainIndex) -> DefId {
        let alloc = self.def_id_allocators.entry(domain_index).or_default();
        let def_id = DefId::new_transient(domain_index, alloc.transient);
        alloc.transient += 1;

        def_id
    }

    pub fn iter_domain_def_ids(
        &self,
        domain_index: DomainIndex,
    ) -> impl Iterator<Item = DefId> + use<> {
        let alloc = self
            .def_id_allocators
            .get(&domain_index)
            .cloned()
            .unwrap_or_default();

        (0..alloc.persistent)
            .map(move |idx| DefId::new_persistent(domain_index, idx))
            .chain((0..alloc.transient).map(move |idx| DefId::new_transient(domain_index, idx)))
    }

    pub fn add_persistent_def(
        &mut self,
        kind: DefKind<'m>,
        domain_index: DomainIndex,
        span: SourceSpan,
    ) -> DefId {
        let def_id = self.alloc_persistent_def_id(domain_index);
        self.table.insert(
            def_id,
            Def {
                id: def_id,
                domain_index,
                span,
                kind,
            },
        );

        def_id
    }

    pub fn add_transient_def(
        &mut self,
        kind: DefKind<'m>,
        domain_index: DomainIndex,
        span: SourceSpan,
    ) -> DefId {
        let def_id = self.alloc_transient_def_id(domain_index);
        self.table.insert(
            def_id,
            Def {
                id: def_id,
                domain_index,
                span,
                kind,
            },
        );

        def_id
    }

    pub fn def_text_literal(&mut self, lit: &str, strings: &mut StringCtx<'m>) -> DefId {
        match self.text_literals.get(&lit) {
            Some(def_id) => *def_id,
            None => {
                let lit = strings.intern(lit);
                let def_id = self.add_transient_def(
                    DefKind::TextLiteral(lit),
                    DomainIndex::ontol(),
                    NO_SPAN,
                );
                self.text_literals.insert(lit, def_id);
                def_id
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

    pub fn add_ontol(&mut self, tag: OntolDefTag, kind: DefKind<'m>) -> DefId {
        let def_id = DefId::new_persistent(DomainIndex::ontol(), tag as u16);
        self.table.insert(
            def_id,
            Def {
                id: def_id,
                domain_index: DomainIndex::ontol(),
                kind,
                span: NO_SPAN,
            },
        );
        def_id
    }

    pub fn add_primitive(
        &mut self,
        tag: OntolDefTag,
        kind: PrimitiveKind,
        path: &'static [&'static str],
    ) -> DefId {
        self.add_ontol(tag, DefKind::Primitive(kind, path))
    }

    pub fn add_builtin_relation(
        &mut self,
        tag: OntolDefTag,
        kind: BuiltinRelationKind,
        path: &'static [&'static str],
    ) -> DefId {
        self.add_ontol(tag, DefKind::BuiltinRelType(kind, path))
    }

    pub fn add_builtin_symbol(&mut self, tag: OntolDefTag, ident: &'static str) -> DefId {
        self.add_ontol(
            tag,
            DefKind::Type(TypeDef {
                ident: Some(ident),
                rel_type_for: None,
                flags: TypeDefFlags::PUBLIC | TypeDefFlags::CONCRETE | TypeDefFlags::BUILTIN_SYMBOL,
            }),
        )
    }
}

impl<'m> Compiler<'m> {
    pub fn add_named_transient_def(
        &mut self,
        name: &'m str,
        space: Space,
        kind: DefKind<'m>,
        parent: DefId,
        span: SourceSpan,
    ) -> DefId {
        let def_id = self.defs.alloc_transient_def_id(parent.domain_index());
        self.namespaces
            .get_namespace_mut(parent, space)
            .insert(name, def_id);
        self.defs.table.insert(
            def_id,
            Def {
                id: def_id,
                domain_index: parent.domain_index(),
                span,
                kind,
            },
        );

        def_id
    }

    pub fn define_domain(&mut self, domain_def_id: DefId) -> DefId {
        let domain_index = domain_def_id.domain_index();
        self.domain_def_ids.insert(domain_index, domain_def_id);
        self.defs.table.insert(
            domain_def_id,
            Def {
                id: domain_def_id,
                domain_index,
                span: NO_SPAN,
                kind: DefKind::Domain(domain_def_id.domain_index()),
            },
        );
        let ty = self.ty_ctx.intern(Type::Domain);
        self.def_ty_ctx.def_table.insert(domain_def_id, ty);

        // make sure the namespace exists
        let namespace = self.namespaces.get_namespace_mut(domain_def_id, Space::Def);

        // The name `ontol` is always defined, and refers to the ontol domain
        namespace.insert("ontol", OntolDefTag::Ontol.def_id());

        domain_def_id
    }

    pub fn def_regex(&mut self, lit: &str, span: U32Span) -> Result<DefId, (String, U32Span)> {
        match self.defs.regex_strings.get(&lit) {
            Some(def_id) => Ok(*def_id),
            None => {
                let lit = self.str_ctx.intern(lit);
                let hir = parse_literal_regex(lit, span)?;
                let def_id =
                    self.defs
                        .add_transient_def(DefKind::Regex(lit), DomainIndex::ontol(), NO_SPAN);
                self.defs.regex_strings.insert(lit, def_id);
                self.defs.literal_regex_meta_table.insert(def_id, hir);

                self.namespaces
                    .add_anonymous(OntolDefTag::Ontol.def_id(), def_id);

                let ty = self.ty_ctx.intern(Type::Regex(def_id));
                self.def_ty_ctx.def_table.insert(def_id, ty);
                self.repr_ctx.repr_table.insert(
                    def_id,
                    Repr {
                        kind: ReprKind::Scalar(def_id, ReprScalarKind::Text, NO_SPAN),
                        type_params: Default::default(),
                    },
                );

                Ok(def_id)
            }
        }
    }
}
