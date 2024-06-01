//! The ontol (builtin-in) domain

use std::ops::Range;

use fnv::FnvHashMap;
use ontol_hir::OverloadFunc;
use ontol_runtime::{
    ontology::{domain::CardinalIdx, ontol::TextLikeType},
    DefId,
};

use crate::{
    def::{BuiltinRelationKind, DefKind, TypeDef, TypeDefFlags},
    edge::{Edge, Slot},
    mem::Intern,
    namespace::Space,
    package::ONTOL_PKG,
    regex_util,
    relation::{Constructor, RelObjectConstraint, RelTypeConstraints, TypeParam},
    text_patterns::{store_text_pattern_segment, TextPatternSegment},
    thesaurus::TypeRelation,
    types::{FunctionType, Type, TypeRef},
    Compiler, NO_SPAN,
};

impl<'m> Compiler<'m> {
    pub fn register_ontol_domain(&mut self) {
        self.str_ctx.intern_constant("ontol");
        self.define_package(self.primitives.ontol_domain);

        // fundamental types
        self.register_type(self.primitives.empty_sequence, Type::EmptySequence);

        // pre-process primitive definitions
        let mut named_builtin_relations: Vec<(DefId, &'static str)> = vec![];
        for def_id in self.defs.iter_package_def_ids(ONTOL_PKG) {
            let def_kind = self.defs.def_kind(def_id);

            match def_kind {
                DefKind::Primitive(kind, ident) => {
                    let ty = self.ty_ctx.intern(Type::Primitive(*kind, def_id));
                    if let Some(ident) = *ident {
                        self.namespaces
                            .get_namespace_mut(ONTOL_PKG, Space::Type)
                            .insert(ident, def_id);
                    }
                    self.def_ty_ctx.table.insert(def_id, ty);
                }
                DefKind::BuiltinRelType(kind, ident) => {
                    if let Some(ident) = ident {
                        named_builtin_relations.push((def_id, ident));
                    }

                    let constraints = match kind {
                        BuiltinRelationKind::Min | BuiltinRelationKind::Max => RelTypeConstraints {
                            subject_set: [self.primitives.number].into(),
                            object: vec![RelObjectConstraint::ConstantOfSubjectType],
                        },
                        _ => RelTypeConstraints::default(),
                    };

                    self.rel_ctx
                        .rel_type_constraints
                        .insert(def_id, constraints);
                }
                DefKind::Type(type_def) => {
                    if type_def.flags.contains(TypeDefFlags::BUILTIN_SYMBOL) {
                        let ident = type_def.ident.unwrap();

                        let symbol_literal_def_id =
                            self.defs.def_text_literal(ident, &mut self.str_ctx);

                        self.register_type(def_id, Type::Domain);

                        self.is(
                            def_id,
                            (TypeRelation::Subset, TypeRelation::Super),
                            symbol_literal_def_id,
                        );

                        self.namespaces
                            .get_namespace_mut(ONTOL_PKG, Space::Type)
                            .insert(ident, def_id);
                    }
                }
                _ => {}
            }
        }

        for (def_id, ident) in named_builtin_relations {
            self.register_named_type(def_id, ident, |_| Type::BuiltinRelation);
        }

        // bools
        self.is(
            self.primitives.true_value,
            (TypeRelation::Subset, TypeRelation::ImplicitSuper),
            self.primitives.bool,
        );
        self.is(
            self.primitives.false_value,
            (TypeRelation::Subset, TypeRelation::ImplicitSuper),
            self.primitives.bool,
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

        // union setup
        {
            let symbols = &self.primitives.symbols;

            for direction in [symbols.ascending, symbols.descending] {
                self.thesaurus.insert_domain_is(
                    self.primitives.direction_union,
                    TypeRelation::SubVariant,
                    direction,
                    NO_SPAN,
                );
            }
        }

        for def_id in self.defs.iter_package_def_ids(ONTOL_PKG) {
            self.type_check().check_def(def_id);
        }

        // "identifies" edge
        {
            let edge_id = self.primitives.edges.identifies;
            self.edge_ctx
                .symbols
                .insert(self.primitives.relations.id, edge_id);
            self.edge_ctx
                .symbols
                .insert(self.primitives.relations.identifies, edge_id);

            self.edge_ctx.edges.insert(
                edge_id,
                Edge {
                    slots: FnvHashMap::from_iter([
                        (
                            self.primitives.relations.identifies,
                            Slot {
                                left: Some(CardinalIdx(0)),
                                depth: 0,
                                right: CardinalIdx(1),
                            },
                        ),
                        (
                            self.primitives.relations.id,
                            Slot {
                                left: Some(CardinalIdx(1)),
                                depth: 0,
                                right: CardinalIdx(0),
                            },
                        ),
                    ]),
                    cardinality: 2,
                    materialized: false,
                },
            );
        }

        self.seal_domain(ONTOL_PKG);
        self.repr_smoke_test();
    }

    fn setup_number_system(&mut self) {
        // integers
        self.is(
            self.primitives.integer,
            (TypeRelation::Subset, TypeRelation::Super),
            self.primitives.number,
        );
        self.is(
            self.primitives.i64,
            (TypeRelation::Subset, TypeRelation::Super),
            self.primitives.integer,
        );
        self.define_number_range(
            self.primitives.i64,
            "-9223372036854775808".."9223372036854775807",
        );

        // floats
        self.is(
            self.primitives.float,
            (TypeRelation::Subset, TypeRelation::Super),
            self.primitives.number,
        );
        self.is(
            self.primitives.f64,
            (TypeRelation::Subset, TypeRelation::Super),
            self.primitives.float,
        );

        self.define_number_range(
            self.primitives.f64,
            "-1.7976931348623157E+308".."1.7976931348623157E+308",
        );
    }

    fn define_number_range(&mut self, def_id: DefId, range: Range<&'static str>) {
        self.define_number_literal_type_param(def_id, self.primitives.relations.min, range.start);
        self.define_number_literal_type_param(def_id, self.primitives.relations.max, range.end);
    }

    fn define_number_literal_type_param(
        &mut self,
        subject: DefId,
        param: DefId,
        literal: &'static str,
    ) {
        let literal = self
            .defs
            .add_def(DefKind::NumberLiteral(literal), ONTOL_PKG, NO_SPAN);
        self.rel_ctx.type_params.entry(subject).or_default().insert(
            param,
            TypeParam {
                definition_site: ONTOL_PKG,
                object: literal,
                span: NO_SPAN,
            },
        );
    }

    fn def_uuid(&mut self) {
        let (uuid, _) = self.define_concrete_domain_type("uuid", |def_id| {
            Type::TextLike(def_id, TextLikeType::Uuid)
        });
        let segment = TextPatternSegment::Regex(regex_util::uuid());
        store_text_pattern_segment(uuid, &segment, &mut self.text_patterns, &mut self.str_ctx);
        self.rel_ctx.properties_by_def_id_mut(uuid).constructor = Constructor::TextFmt(segment);
        self.defs.string_like_types.insert(uuid, TextLikeType::Uuid);
    }

    fn def_datetime(&mut self) {
        let (datetime, _) = self.define_concrete_domain_type("datetime", |def_id| {
            Type::TextLike(def_id, TextLikeType::DateTime)
        });
        let segment = TextPatternSegment::Regex(regex_util::datetime_rfc3339());
        store_text_pattern_segment(
            datetime,
            &segment,
            &mut self.text_patterns,
            &mut self.str_ctx,
        );
        self.rel_ctx.properties_by_def_id_mut(datetime).constructor =
            Constructor::TextFmt(segment.clone());
        self.defs
            .string_like_types
            .insert(datetime, TextLikeType::DateTime);
    }

    /// Define an ontol _domain_ type, i.e. not a primitive
    fn define_concrete_domain_type(
        &mut self,
        ident: &'static str,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> (DefId, TypeRef<'m>) {
        let def_id = self.defs.add_def(
            DefKind::Type(TypeDef {
                ident: Some(ident),
                rel_type_for: None,
                flags: TypeDefFlags::PUBLIC | TypeDefFlags::CONCRETE,
            }),
            ONTOL_PKG,
            NO_SPAN,
        );
        let type_ref = self.register_named_type(def_id, ident, ty_fn);
        (def_id, type_ref)
    }

    fn register_type(&mut self, def_id: DefId, ty_fn: impl Fn(DefId) -> Type<'m>) -> TypeRef<'m> {
        let ty = self.ty_ctx.intern(ty_fn(def_id));
        self.def_ty_ctx.table.insert(def_id, ty);
        ty
    }

    fn register_named_type(
        &mut self,
        def_id: DefId,
        ident: &'m str,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> TypeRef<'m> {
        let ty = self.ty_ctx.intern(ty_fn(def_id));
        self.namespaces
            .get_namespace_mut(ONTOL_PKG, Space::Type)
            .insert(ident, def_id);
        self.def_ty_ctx.table.insert(def_id, ty);
        ty
    }

    fn def_proc(&mut self, ident: &'static str, def_kind: DefKind<'m>, ty: TypeRef<'m>) -> DefId {
        let def_id = self.add_named_def(ident, Space::Type, def_kind, ONTOL_PKG, NO_SPAN);
        self.def_ty_ctx.table.insert(def_id, ty);

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

    fn repr_smoke_test(&self) {
        let repr_table = &self.repr_ctx.repr_table;
        assert!(!repr_table.contains_key(&self.primitives.number));
        assert!(!repr_table.contains_key(&self.primitives.integer));
        assert!(!repr_table.contains_key(&self.primitives.float));
        assert!(repr_table.contains_key(&self.primitives.i64));
        assert!(repr_table.contains_key(&self.primitives.f64));
    }
}
