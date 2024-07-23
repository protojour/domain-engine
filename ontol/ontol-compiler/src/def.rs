use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
};

use fnv::FnvHashMap;
use ontol_macros::RustDoc;
use ontol_runtime::{
    ontology::{
        domain::{self, BasicDef},
        ontol::TextLikeType,
    },
    var::VarAllocator,
    DefId, PackageId, RelId,
};

use crate::{
    mem::Intern, namespace::Space, package::ONTOL_PKG, pattern::PatId, primitive::PrimitiveKind,
    regex_util::parse_literal_regex, source::SourceSpan, strings::StringCtx, types::Type, Compiler,
    SpannedBorrow, NO_SPAN,
};
use ontol_parser::U32Span;

/// A definition in some package
#[derive(Debug)]
#[allow(unused)]
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
    InlineUnion(BTreeSet<DefId>),
    BuiltinRelType(BuiltinRelationKind, Option<&'static str>),
    Edge,
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
            Self::InlineUnion(_) => None,
            Self::BuiltinRelType(_, ident) => ident.map(|ident| ident.into()),
            Self::Edge => None,
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
            DefKind::Package(_) => domain::DefKind::Domain(info),
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
        /// Whether the defintion is a ontol-builtin symbol
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
    FmtTransition,
}

pub struct Defs<'m> {
    def_id_allocators: FnvHashMap<PackageId, u16>,
    pub(crate) table: FnvHashMap<DefId, Def<'m>>,
    pub(crate) text_literals: HashMap<&'m str, DefId>,
    pub(crate) regex_strings: HashMap<&'m str, DefId>,
    pub(crate) literal_regex_meta_table: FnvHashMap<DefId, RegexMeta<'m>>,
    pub(crate) text_like_types: FnvHashMap<DefId, TextLikeType>,
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

    pub fn alloc_def_id(&mut self, package_id: PackageId) -> DefId {
        let idx = self.def_id_allocators.entry(package_id).or_default();
        let def_id = DefId(package_id, *idx);
        *idx += 1;

        def_id
    }

    pub fn iter_packages(&self) -> impl Iterator<Item = PackageId> + '_ {
        self.def_id_allocators.keys().copied()
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

    pub fn add_builtin_symbol(&mut self, ident: &'static str) -> DefId {
        self.add_def(
            DefKind::Type(TypeDef {
                ident: Some(ident),
                rel_type_for: None,
                flags: TypeDefFlags::PUBLIC | TypeDefFlags::CONCRETE | TypeDefFlags::BUILTIN_SYMBOL,
            }),
            ONTOL_PKG,
            NO_SPAN,
        )
    }

    pub fn def_text_literal(&mut self, lit: &str, strings: &mut StringCtx<'m>) -> DefId {
        match self.text_literals.get(&lit) {
            Some(def_id) => *def_id,
            None => {
                let lit = strings.intern(lit);
                let def_id = self.add_def(DefKind::TextLiteral(lit), ONTOL_PKG, NO_SPAN);
                self.text_literals.insert(lit, def_id);
                def_id
            }
        }
    }

    pub fn def_regex(
        &mut self,
        lit: &str,
        span: U32Span,
        strings: &mut StringCtx<'m>,
    ) -> Result<DefId, (String, U32Span)> {
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
        self.package_def_ids.insert(package_id, package_def_id);
        self.defs.table.insert(
            package_def_id,
            Def {
                id: package_def_id,
                package: package_id,
                span: NO_SPAN,
                kind: DefKind::Package(package_def_id.package_id()),
            },
        );
        let ty = self.ty_ctx.intern(Type::Package);
        self.def_ty_ctx.def_table.insert(package_def_id, ty);

        // make sure the namespace exists
        let namespace = self.namespaces.get_namespace_mut(package_id, Space::Type);

        // The name `ontol` is always defined, and refers to the ontol domain
        namespace.insert("ontol", self.primitives.ontol_domain);

        package_def_id
    }
}
