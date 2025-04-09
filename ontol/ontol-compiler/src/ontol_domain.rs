//! The ontol (builtin-in) domain

use std::{ops::Range, str::FromStr};

use indexmap::IndexMap;
use ontol_core::{DomainId, OntologyDomainId, tag::DomainIndex};
use ontol_hir::OverloadFunc;
use ontol_macros::RustDoc;
use ontol_parser::source::NO_SPAN;
use ontol_runtime::{DefId, OntolDefTag, OntolDefTagExt, ontology::ontol::TextLikeType};
use ulid::Ulid;

use crate::{
    Compiler,
    def::{BuiltinRelationKind, DefKind, TypeDef, TypeDefFlags},
    mem::Intern,
    misc::{RelObjectConstraint, RelTypeConstraints, TypeParam},
    namespace::Space,
    properties::Constructor,
    regex_util,
    text_patterns::{TextPatternSegment, store_text_pattern_segment},
    thesaurus::TypeRelation,
    types::{FunctionType, Type, TypeRef},
};

/// `ontol` is the original domain.
///
/// No other domain should have a timestamp earlier than what's encoded inside this ULID (2023-01-04)
const ONTOL_DOMAIN_ID: &str = "01GNYFZP30ED0EZ1579TH0D55P";

/// documentation for the ontol domain
#[derive(RustDoc)]
pub enum Ontol {
    /// The original domain.
    ///
    /// ontol defines fundamental primitives that are used to compose definitions in higher level domains.
    Ontol,
}

struct DefPath<'a, 'm>(&'a [&'m str], DefId);

#[derive(Default)]
struct PathNode<'m> {
    def_id: Option<DefId>,
    children: IndexMap<&'m str, PathNode<'m>>,
}

impl<'m> PathNode<'m> {
    fn insert<'a>(&mut self, def_path: DefPath<'a, 'm>) {
        if def_path.0.is_empty() {
            self.def_id = Some(def_path.1);
        } else {
            let route = def_path.0[0];
            let rest = &def_path.0[1..];

            self.children
                .entry(route)
                .or_default()
                .insert(DefPath(rest, def_path.1));
        }
    }
}

impl<'m> Compiler<'m> {
    pub fn register_ontol_domain(&mut self) {
        self.str_ctx.intern_constant("ontol");
        let ontol_def_id = self.define_domain(OntolDefTag::Ontol.def_id());

        assert_eq!(ontol_def_id.domain_index(), DomainIndex::ontol());
        self.domain_ids.insert(
            ontol_def_id.domain_index(),
            OntologyDomainId {
                id: DomainId {
                    ulid: Ulid::from_str(ONTOL_DOMAIN_ID).unwrap(),
                    subdomain: 0,
                },
                stable: true,
            },
        );

        // fundamental types
        self.register_type(OntolDefTag::EmptySequence.def_id(), Type::EmptySequence);

        let mut root_path_node = PathNode::default();

        // pre-process primitive definitions
        let mut named_builtin_relations: Vec<DefPath> = vec![];
        for def_id in self.defs.iter_domain_def_ids(DomainIndex::ontol()) {
            let Some(def_kind) = self.defs.def_kind_option(def_id) else {
                continue;
            };

            match def_kind {
                DefKind::BuiltinModule(path) => {
                    root_path_node.insert(DefPath(path, def_id));
                }
                DefKind::Primitive(kind, path) => {
                    let ty = self.ty_ctx.intern(Type::Primitive(*kind, def_id));
                    root_path_node.insert(DefPath(path, def_id));
                    self.def_ty_ctx.def_table.insert(def_id, ty);
                }
                DefKind::BuiltinRelType(kind, path) => {
                    if !path.is_empty() {
                        named_builtin_relations.push(DefPath(path, def_id));
                    }

                    let constraints = match kind {
                        BuiltinRelationKind::Min | BuiltinRelationKind::Max => RelTypeConstraints {
                            subject_set: [OntolDefTag::Number.def_id()].into(),
                            object: vec![RelObjectConstraint::ConstantOfSubjectType],
                        },
                        _ => RelTypeConstraints::default(),
                    };

                    self.misc_ctx
                        .rel_type_constraints
                        .insert(def_id, constraints);
                }
                DefKind::Type(type_def) => {
                    if type_def.flags.contains(TypeDefFlags::BUILTIN_SYMBOL) {
                        let ident = type_def.ident.unwrap();

                        let symbol_literal_def_id =
                            self.defs.def_text_literal(ident, &mut self.str_ctx);

                        self.register_type(def_id, Type::DomainDef);

                        self.is(
                            def_id,
                            (TypeRelation::Subset, TypeRelation::Super),
                            symbol_literal_def_id,
                        );

                        root_path_node.insert(DefPath(&[ident], def_id));

                        // self.namespaces
                        //     .get_namespace_mut(OntolDefTag::Ontol.def_id(), Space::Def)
                        //     .insert(ident, def_id);
                    } else {
                        self.namespaces.add_anonymous(ontol_def_id, def_id);
                    }
                }
                _ => {}
            }
        }

        for DefPath(path, def_id) in named_builtin_relations {
            self.register_def_type(def_id, |_| Type::BuiltinRelation);
            root_path_node.insert(DefPath(path, def_id));
        }

        // bools
        self.is(
            OntolDefTag::True.def_id(),
            (TypeRelation::Subset, TypeRelation::ImplicitSuper),
            OntolDefTag::Boolean.def_id(),
        );
        self.is(
            OntolDefTag::False.def_id(),
            (TypeRelation::Subset, TypeRelation::ImplicitSuper),
            OntolDefTag::Boolean.def_id(),
        );

        self.setup_number_system();

        let binary_arithmetic = self
            .ty_ctx
            .intern(Type::Function(FunctionType::BinaryArithmetic));
        let binary_text = self.ty_ctx.intern(Type::Function(FunctionType::BinaryText));

        // built-in functions
        // arithmetic
        self.def_proc("+", DefKind::Fn(OverloadFunc::Add), binary_arithmetic);
        self.def_proc("-", DefKind::Fn(OverloadFunc::Sub), binary_arithmetic);
        self.def_proc("*", DefKind::Fn(OverloadFunc::Mul), binary_arithmetic);
        self.def_proc("/", DefKind::Fn(OverloadFunc::Div), binary_arithmetic);

        // string manipulation
        self.def_proc("append", DefKind::Fn(OverloadFunc::Append), binary_text);

        root_path_node.insert(self.def_uuid());
        root_path_node.insert(self.def_ulid());
        root_path_node.insert(self.def_datetime());

        root_path_node.insert(DefPath(
            OntolDefTag::Auto.ontol_path(),
            self.register_def_type(OntolDefTag::Auto.def_id(), Type::ValueGenerator)
                .0,
        ));
        root_path_node.insert(DefPath(
            OntolDefTag::CreateTime.ontol_path(),
            self.register_def_type(OntolDefTag::CreateTime.def_id(), Type::ValueGenerator)
                .0,
        ));

        root_path_node.insert(DefPath(
            OntolDefTag::UpdateTime.ontol_path(),
            self.register_def_type(OntolDefTag::UpdateTime.def_id(), Type::ValueGenerator)
                .0,
        ));

        root_path_node.insert(DefPath(
            OntolDefTag::Crdt.ontol_path(),
            self.register_def_type(OntolDefTag::Crdt.def_id(), Type::ObjectRepr)
                .0,
        ));

        // union setup
        {
            for sym_tag in [OntolDefTag::SymAscending, OntolDefTag::SymDescending] {
                self.thesaurus.insert_domain_is(
                    OntolDefTag::UnionDirection.def_id(),
                    TypeRelation::SubVariant,
                    sym_tag.def_id(),
                    NO_SPAN,
                );
            }
        }

        for def_id in self.defs.iter_domain_def_ids(DomainIndex::ontol()) {
            self.type_check().check_def(def_id);
        }

        self.write_namespace(OntolDefTag::Ontol.def_id(), root_path_node);

        self.seal_domain(DomainIndex::ontol());
        self.repr_smoke_test();
    }

    fn setup_number_system(&mut self) {
        // integers
        self.is(
            OntolDefTag::Integer.def_id(),
            (TypeRelation::Subset, TypeRelation::Super),
            OntolDefTag::Number.def_id(),
        );
        self.is(
            OntolDefTag::I64.def_id(),
            (TypeRelation::Subset, TypeRelation::Super),
            OntolDefTag::Integer.def_id(),
        );
        self.define_number_range(
            OntolDefTag::I64.def_id(),
            "-9223372036854775808".."9223372036854775807",
        );

        // floats
        self.is(
            OntolDefTag::Float.def_id(),
            (TypeRelation::Subset, TypeRelation::Super),
            OntolDefTag::Number.def_id(),
        );
        self.is(
            OntolDefTag::F64.def_id(),
            (TypeRelation::Subset, TypeRelation::Super),
            OntolDefTag::Float.def_id(),
        );

        self.define_number_range(
            OntolDefTag::F64.def_id(),
            "-1.7976931348623157E+308".."1.7976931348623157E+308",
        );
    }

    fn define_number_range(&mut self, def_id: DefId, range: Range<&'static str>) {
        self.define_number_literal_type_param(
            def_id,
            OntolDefTag::RelationMin.def_id(),
            range.start,
        );
        self.define_number_literal_type_param(def_id, OntolDefTag::RelationMax.def_id(), range.end);
    }

    fn define_number_literal_type_param(
        &mut self,
        subject: DefId,
        param: DefId,
        literal: &'static str,
    ) {
        let literal = self.defs.add_transient_def(
            DefKind::NumberLiteral(literal),
            DomainIndex::ontol(),
            NO_SPAN,
        );
        self.misc_ctx
            .type_params
            .entry(subject)
            .or_default()
            .insert(
                param,
                TypeParam {
                    definition_site: DomainIndex::ontol(),
                    object: literal,
                    span: NO_SPAN,
                },
            );
    }

    fn def_uuid(&mut self) -> DefPath<'static, 'static> {
        let tag = OntolDefTag::Uuid;
        let (uuid, _) = self
            .define_concrete_ontol_type(tag, |def_id| Type::TextLike(def_id, TextLikeType::Uuid));
        let segment = TextPatternSegment::Regex(regex_util::well_known::uuid());
        store_text_pattern_segment(uuid, &segment, &mut self.text_patterns, &mut self.str_ctx);
        self.prop_ctx.properties_by_def_id_mut(uuid).constructor = Constructor::TextFmt(segment);
        self.defs.text_like_types.insert(uuid, TextLikeType::Uuid);
        DefPath(tag.ontol_path(), uuid)
    }

    fn def_ulid(&mut self) -> DefPath<'static, 'static> {
        let tag = OntolDefTag::Ulid;
        let (ulid, _) = self.define_concrete_ontol_type(OntolDefTag::Ulid, |def_id| {
            Type::TextLike(def_id, TextLikeType::Ulid)
        });
        let segment = TextPatternSegment::Regex(regex_util::well_known::ulid());
        store_text_pattern_segment(ulid, &segment, &mut self.text_patterns, &mut self.str_ctx);
        self.prop_ctx.properties_by_def_id_mut(ulid).constructor = Constructor::TextFmt(segment);
        self.defs.text_like_types.insert(ulid, TextLikeType::Ulid);
        DefPath(tag.ontol_path(), ulid)
    }

    fn def_datetime(&mut self) -> DefPath<'static, 'static> {
        let tag = OntolDefTag::DateTime;
        let (datetime, _) = self.define_concrete_ontol_type(tag, |def_id| {
            Type::TextLike(def_id, TextLikeType::DateTime)
        });
        let segment = TextPatternSegment::Regex(regex_util::well_known::datetime_rfc3339());
        store_text_pattern_segment(
            datetime,
            &segment,
            &mut self.text_patterns,
            &mut self.str_ctx,
        );
        self.prop_ctx.properties_by_def_id_mut(datetime).constructor =
            Constructor::TextFmt(segment.clone());
        self.defs
            .text_like_types
            .insert(datetime, TextLikeType::DateTime);
        DefPath(tag.ontol_path(), datetime)
    }

    /// Define an ontol _domain_ type, i.e. not a primitive
    fn define_concrete_ontol_type(
        &mut self,
        tag: OntolDefTag,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> (DefId, TypeRef<'m>) {
        let def_id = self.defs.add_ontol(
            tag,
            DefKind::Type(TypeDef {
                ident: tag.ontol_path().last().copied(),
                rel_type_for: None,
                flags: TypeDefFlags::PUBLIC | TypeDefFlags::CONCRETE,
            }),
        );
        self.register_def_type(def_id, ty_fn)
    }

    fn register_type(&mut self, def_id: DefId, ty_fn: impl Fn(DefId) -> Type<'m>) -> TypeRef<'m> {
        let ty = self.ty_ctx.intern(ty_fn(def_id));
        self.def_ty_ctx.def_table.insert(def_id, ty);
        ty
    }

    fn register_def_type(
        &mut self,
        def_id: DefId,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> (DefId, TypeRef<'m>) {
        let ty = self.ty_ctx.intern(ty_fn(def_id));
        self.def_ty_ctx.def_table.insert(def_id, ty);
        (def_id, ty)
    }

    fn def_proc(&mut self, ident: &'static str, def_kind: DefKind<'m>, ty: TypeRef<'m>) -> DefId {
        let def_id = self.add_named_transient_def(
            ident,
            Space::Def,
            def_kind,
            OntolDefTag::Ontol.def_id(),
            NO_SPAN,
        );
        self.def_ty_ctx.def_table.insert(def_id, ty);

        def_id
    }

    fn is(
        &mut self,
        sub_def_id: DefId,
        (super_rel, sub_rel): (TypeRelation, TypeRelation),
        super_def_id: DefId,
    ) {
        self.thesaurus
            .insert_builtin_is(sub_def_id, (super_rel, sub_rel), super_def_id);
    }

    fn write_namespace(&mut self, parent_def_id: DefId, path_node: PathNode<'m>) {
        for (ident, node) in path_node.children {
            if let Some(def_id) = node.def_id {
                self.namespaces
                    .get_namespace_mut(parent_def_id, Space::Def)
                    .insert(ident, def_id);

                // recurse into children
                self.write_namespace(def_id, node);
            }
        }
    }

    fn repr_smoke_test(&self) {
        let repr_table = &self.repr_ctx.repr_table;
        assert!(!repr_table.contains_key(&OntolDefTag::Number.def_id()));
        assert!(!repr_table.contains_key(&OntolDefTag::Integer.def_id()));
        assert!(!repr_table.contains_key(&OntolDefTag::Float.def_id()));
        assert!(repr_table.contains_key(&OntolDefTag::I64.def_id()));
        assert!(repr_table.contains_key(&OntolDefTag::F64.def_id()));
    }
}
