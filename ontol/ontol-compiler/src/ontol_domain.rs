//! The ontol (builtin-in) domain

use std::{ops::Range, str::FromStr};

use ontol_hir::OverloadFunc;
use ontol_runtime::{
    ontology::{domain::DomainId, ontol::TextLikeType},
    DefId, OntolDefTag, PackageId,
};
use ulid::Ulid;

use crate::{
    def::{BuiltinRelationKind, DefKind, TypeDef, TypeDefFlags},
    mem::Intern,
    misc::{RelObjectConstraint, RelTypeConstraints, TypeParam},
    namespace::Space,
    package::ONTOL_PKG,
    properties::Constructor,
    regex_util,
    text_patterns::{store_text_pattern_segment, TextPatternSegment},
    thesaurus::TypeRelation,
    types::{FunctionType, Type, TypeRef},
    Compiler, NO_SPAN,
};

/// `ontol` is the original domain.
///
/// No other domain should have a timestamp earlier than what's encoded inside this ULID (2023-01-04)
const ONTOL_DOMAIN_ID: &str = "01GNYFZP30ED0EZ1579TH0D55P";

impl<'m> Compiler<'m> {
    pub fn register_ontol_domain(&mut self) {
        self.str_ctx.intern_constant("ontol");
        let def_id = self.define_package(OntolDefTag::Ontol.def_id());

        assert_eq!(def_id.package_id(), PackageId::ontol());
        self.domain_ids.insert(
            def_id.package_id(),
            DomainId {
                ulid: Ulid::from_str(ONTOL_DOMAIN_ID).unwrap(),
                stable: true,
            },
        );

        // fundamental types
        self.register_type(OntolDefTag::EmptySequence.def_id(), Type::EmptySequence);

        // pre-process primitive definitions
        let mut named_builtin_relations: Vec<(DefId, &'static str)> = vec![];
        for def_id in self.defs.iter_package_def_ids(ONTOL_PKG) {
            let Some(def_kind) = self.defs.def_kind_option(def_id) else {
                continue;
            };

            match def_kind {
                DefKind::Primitive(kind, ident) => {
                    let ty = self.ty_ctx.intern(Type::Primitive(*kind, def_id));
                    if let Some(ident) = *ident {
                        self.namespaces
                            .get_namespace_mut(OntolDefTag::Ontol.def_id(), Space::Def)
                            .insert(ident, def_id);
                    }
                    self.def_ty_ctx.def_table.insert(def_id, ty);
                }
                DefKind::BuiltinRelType(kind, ident) => {
                    if let Some(ident) = ident {
                        named_builtin_relations.push((def_id, ident));
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

                        self.namespaces
                            .get_namespace_mut(OntolDefTag::Ontol.def_id(), Space::Def)
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

        self.def_uuid();
        self.def_ulid();
        self.def_datetime();

        self.register_named_type(
            OntolDefTag::GeneratorAuto.def_id(),
            "auto",
            Type::ValueGenerator,
        );
        self.register_named_type(
            OntolDefTag::GeneratorCreateTime.def_id(),
            "create_time",
            Type::ValueGenerator,
        );
        self.register_named_type(
            OntolDefTag::GeneratorUpdateTime.def_id(),
            "update_time",
            Type::ValueGenerator,
        );

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

        for def_id in self.defs.iter_package_def_ids(ONTOL_PKG) {
            self.type_check().check_def(def_id);
        }

        self.seal_domain(ONTOL_PKG);
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
        let literal = self
            .defs
            .add_def(DefKind::NumberLiteral(literal), ONTOL_PKG, NO_SPAN);
        self.misc_ctx
            .type_params
            .entry(subject)
            .or_default()
            .insert(
                param,
                TypeParam {
                    definition_site: ONTOL_PKG,
                    object: literal,
                    span: NO_SPAN,
                },
            );
    }

    fn def_uuid(&mut self) {
        let (uuid, _) = self.define_concrete_ontol_type(OntolDefTag::Uuid, "uuid", |def_id| {
            Type::TextLike(def_id, TextLikeType::Uuid)
        });
        let segment = TextPatternSegment::Regex(regex_util::well_known::uuid());
        store_text_pattern_segment(uuid, &segment, &mut self.text_patterns, &mut self.str_ctx);
        self.prop_ctx.properties_by_def_id_mut(uuid).constructor = Constructor::TextFmt(segment);
        self.defs.text_like_types.insert(uuid, TextLikeType::Uuid);
    }

    fn def_ulid(&mut self) {
        let (ulid, _) = self.define_concrete_ontol_type(OntolDefTag::Ulid, "ulid", |def_id| {
            Type::TextLike(def_id, TextLikeType::Ulid)
        });
        let segment = TextPatternSegment::Regex(regex_util::well_known::ulid());
        store_text_pattern_segment(ulid, &segment, &mut self.text_patterns, &mut self.str_ctx);
        self.prop_ctx.properties_by_def_id_mut(ulid).constructor = Constructor::TextFmt(segment);
        self.defs.text_like_types.insert(ulid, TextLikeType::Ulid);
    }

    fn def_datetime(&mut self) {
        let (datetime, _) =
            self.define_concrete_ontol_type(OntolDefTag::DateTime, "datetime", |def_id| {
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
    }

    /// Define an ontol _domain_ type, i.e. not a primitive
    fn define_concrete_ontol_type(
        &mut self,
        tag: OntolDefTag,
        ident: &'static str,
        ty_fn: impl Fn(DefId) -> Type<'m>,
    ) -> (DefId, TypeRef<'m>) {
        let def_id = self.defs.add_ontol(
            tag,
            DefKind::Type(TypeDef {
                ident: Some(ident),
                rel_type_for: None,
                flags: TypeDefFlags::PUBLIC | TypeDefFlags::CONCRETE,
            }),
        );
        let type_ref = self.register_named_type(def_id, ident, ty_fn);
        (def_id, type_ref)
    }

    fn register_type(&mut self, def_id: DefId, ty_fn: impl Fn(DefId) -> Type<'m>) -> TypeRef<'m> {
        let ty = self.ty_ctx.intern(ty_fn(def_id));
        self.def_ty_ctx.def_table.insert(def_id, ty);
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
            .get_namespace_mut(OntolDefTag::Ontol.def_id(), Space::Def)
            .insert(ident, def_id);
        self.def_ty_ctx.def_table.insert(def_id, ty);
        ty
    }

    fn def_proc(&mut self, ident: &'static str, def_kind: DefKind<'m>, ty: TypeRef<'m>) -> DefId {
        let def_id = self.add_named_def(
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

    fn repr_smoke_test(&self) {
        let repr_table = &self.repr_ctx.repr_table;
        assert!(!repr_table.contains_key(&OntolDefTag::Number.def_id()));
        assert!(!repr_table.contains_key(&OntolDefTag::Integer.def_id()));
        assert!(!repr_table.contains_key(&OntolDefTag::Float.def_id()));
        assert!(repr_table.contains_key(&OntolDefTag::I64.def_id()));
        assert!(repr_table.contains_key(&OntolDefTag::F64.def_id()));
    }
}
