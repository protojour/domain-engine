use std::collections::HashSet;

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::{IndexMap, IndexSet};
use ontol_runtime::{
    interface::discriminator::{Discriminant, LeafDiscriminant},
    ontology::ontol::TextConstant,
    DefId, PropId,
};
use patricia_tree::PatriciaMap;
use tracing::{debug, debug_span};

use crate::{
    def::{Def, DefKind},
    error::CompileError,
    misc::{UnionDiscriminator, UnionDiscriminatorRole, UnionDiscriminatorVariant},
    primitive::PrimitiveKind,
    properties::{Constructor, Property},
    relation::rel_def_meta,
    repr::repr_model::{ReprKind, ReprScalarKind},
    sequence::Sequence,
    strings::StringCtx,
    text_patterns::TextPatternSegment,
    types::{FormatType, Type},
    SourceSpan, SpannedCompileError,
};

use super::TypeCheck;

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_union(&mut self, value_union_def_id: DefId) -> Vec<SpannedCompileError> {
        let _entered = debug_span!("union", id = ?value_union_def_id).entered();

        let mut strings = self.str_ctx.detach();

        // An error set to avoid reporting the same error more than once
        let mut error_set = ErrorSet::default();

        let union_def = self.defs.table.get(&value_union_def_id).unwrap();

        let repr_kind = &self.repr_ctx.get_repr_kind(&value_union_def_id).unwrap();

        let union_variants = match repr_kind {
            ReprKind::Union(variants, _) => variants,
            _ => panic!("not a union"),
        };

        // debug!("variants: {union_variants:?}");

        let mut inherent_builder = DiscriminatorBuilder::default();
        // Also verify that entity ids are disjoint:
        let mut entity_id_builder = DiscriminatorBuilder::default();

        let mut used_variants: FnvHashSet<DefId> = Default::default();

        let mut entity_detector = EntityDetector::default();

        for (variant_def_id, span) in union_variants {
            let variant_def_id = *variant_def_id;

            self.add_variant_to_builder(
                &mut inherent_builder,
                VariantKey::Instance,
                variant_def_id,
                &mut error_set,
                span,
                &mut strings,
            );

            if let Some(properties) = self.prop_ctx.properties_by_def_id(variant_def_id) {
                if let Some(id_relationship_id) = &properties.identified_by {
                    let identifies_meta =
                        rel_def_meta(*id_relationship_id, self.rel_ctx, self.defs);

                    let name = match identifies_meta.relation_def_kind.value {
                        DefKind::TextLiteral(lit) => String::from(*lit),
                        // FIXME: Choose the real name
                        _ => String::from("id"),
                    };

                    let name = strings.intern_constant(&name);

                    let table = properties.table.as_ref().unwrap();
                    let (id_prop_id, _) = table.iter().find(|(_, prop)| prop.is_entity_id).unwrap();

                    self.add_variant_to_builder(
                        &mut entity_id_builder,
                        VariantKey::IdProperty {
                            entity_id: variant_def_id,
                            name,
                            prop_id: *id_prop_id,
                        },
                        identifies_meta.relationship.subject.0,
                        &mut error_set,
                        span,
                        &mut strings,
                    );
                    entity_detector.entity_count += 1;
                } else {
                    entity_detector.non_entity_count += 1;
                }
            } else {
                entity_detector.non_entity_count += 1;
            }

            used_variants.insert(variant_def_id);
        }

        let mut errors = vec![];

        let entities_only = match entity_detector.detect(union_variants.len()) {
            Ok(entities_only) => Some(entities_only),
            Err(error) => {
                errors.push(error.span(union_def.span));

                None
            }
        };

        self.limit_property_discriminators(
            value_union_def_id,
            union_def,
            &mut inherent_builder,
            &mut error_set,
        );
        self.verify_disjoint_text_patterns(
            value_union_def_id,
            union_def,
            &mut inherent_builder,
            UnionCheckError::SharedPrefixInPatternUnion,
            &mut error_set,
        );
        self.verify_disjoint_text_patterns(
            value_union_def_id,
            union_def,
            &mut entity_id_builder,
            UnionCheckError::NonDisjointIdsInEntityUnion,
            &mut error_set,
        );

        let mut union_discriminator = self.make_union_discriminator(
            inherent_builder,
            DiscriminatorType::Data,
            &error_set,
            &mut strings,
        );

        if let Some(EntitiesOnly(true)) = entities_only {
            for (_, errors) in error_set.errors.iter_mut() {
                // If there are only entities, filter out CannotDiscriminateTypeByProperty errors,
                // since this is allowed for entities.
                errors.retain(|error, _| {
                    !matches!(error, UnionCheckError::CannotDiscriminateTypeByProperty)
                });
            }

            if union_discriminator.variants.is_empty() && !union_variants.is_empty() {
                union_discriminator = self.make_union_discriminator(
                    entity_id_builder,
                    DiscriminatorType::Identification,
                    &error_set,
                    &mut strings,
                );
            }
        }

        self.misc_ctx
            .union_discriminators
            .insert(value_union_def_id, union_discriminator);

        errors.extend(
            error_set
                .errors
                .into_iter()
                .flat_map(|(_, errors)| errors.into_iter())
                .map(|(union_error, span)| self.make_compile_error(union_error).span(span)),
        );

        self.str_ctx.attach(strings);

        errors
    }

    fn add_variant_to_builder<'t, 'b>(
        &'t self,
        builder: &'b mut DiscriminatorBuilder<'t>,
        key: VariantKey,
        variant_def: DefId,
        error_set: &mut ErrorSet,
        span: &SourceSpan,
        strings: &mut StringCtx<'m>,
    ) where
        't: 'b,
    {
        let _entered = debug_span!("variant", def = ?variant_def).entered();

        let variant_ty = self
            .def_ty_ctx
            .def_table
            .get(&variant_def)
            .unwrap_or_else(|| {
                let def = self.defs.def_kind(variant_def);
                panic!("No type found for {def:?}");
            });

        debug!("Add variant to builder variant_ty: {variant_ty:?}");

        match variant_ty {
            Type::Primitive(PrimitiveKind::Unit, def_id) => builder.unit = Some(*def_id),
            Type::Primitive(PrimitiveKind::I64, _) => {
                builder.number = IntDiscriminator::Any(variant_def);
            }
            Type::Primitive(PrimitiveKind::Text, _) => {
                builder.string = TextDiscriminator::Any(variant_def);
                builder.any_string.push(VariantKeyed {
                    key,
                    value: variant_def,
                });
            }
            Type::TextConstant(def_id) => {
                let string_literal = self.defs.get_string_representation(*def_id);
                builder.add_text_literal(key, string_literal, *def_id);
            }
            Type::IntConstant(int) => match &mut builder.number {
                IntDiscriminator::None => {
                    builder.number = IntDiscriminator::Literals(
                        [VariantKeyed {
                            key: VariantKey::Instance,
                            value: (*int, variant_def),
                        }]
                        .into(),
                    )
                }
                IntDiscriminator::Any(_) => {}
                IntDiscriminator::Literals(ints) => {
                    ints.insert(VariantKeyed {
                        key: VariantKey::Instance,
                        value: (*int, variant_def),
                    });
                }
            },
            Type::DomainDef(domain_def_id) | Type::Anonymous(domain_def_id) => {
                match self.find_domain_type_match_data(*domain_def_id) {
                    Ok(DomainTypeMatchData::Struct(property_set)) => {
                        self.add_property_set_to_discriminator(
                            builder,
                            variant_def,
                            property_set,
                            span,
                            error_set,
                            strings,
                        );
                    }
                    Ok(DomainTypeMatchData::Sequence(_)) => {
                        if builder.sequence.is_some() {
                            error_set.report(
                                variant_def,
                                UnionCheckError::CannotDiscriminateType,
                                span,
                            );
                        } else {
                            builder.sequence = Some(VariantKeyed {
                                key,
                                value: variant_def,
                            });
                        }
                    }
                    Ok(DomainTypeMatchData::ConstructorStringPattern(segment)) => {
                        builder.add_text_pattern(key, segment, variant_def);
                    }
                    Ok(DomainTypeMatchData::TextLiteral(lit)) => {
                        builder.add_text_literal(key, lit, variant_def);
                    }
                    Err(error) => {
                        error_set.report(variant_def, error, span);
                    }
                }
            }
            _other => {
                error_set.report(variant_def, UnionCheckError::CannotDiscriminateType, span);
            }
        }
    }

    fn find_domain_type_match_data(
        &self,
        def_id: DefId,
    ) -> Result<DomainTypeMatchData<'_>, UnionCheckError> {
        let repr_kind = self.repr_ctx.get_repr_kind(&def_id).unwrap();

        debug!("find domain type match data {def_id:?}: {repr_kind:?}");

        if let Some(properties) = self.prop_ctx.properties_by_def_id(def_id) {
            match &properties.constructor {
                Constructor::Transparent => {
                    debug!("was Transparent: {properties:?}");
                    if let Some(property_set) = &properties.table {
                        return Ok(DomainTypeMatchData::Struct(property_set));
                    }
                }
                Constructor::Sequence(sequence) => {
                    return Ok(DomainTypeMatchData::Sequence(sequence))
                }
                Constructor::TextFmt(segment) => {
                    return Ok(DomainTypeMatchData::ConstructorStringPattern(segment))
                }
            }
        }

        match repr_kind {
            ReprKind::Scalar(
                scalar_def_id,
                ReprScalarKind::Text | ReprScalarKind::TextConstant(_),
                _,
            ) => match self.defs.def_kind(*scalar_def_id) {
                DefKind::TextLiteral(lit) => Ok(DomainTypeMatchData::TextLiteral(lit)),
                _other => {
                    debug!("other: {_other:?}");
                    Err(UnionCheckError::UnitTypePartOfUnion(*scalar_def_id))
                }
            },
            repr_kind => {
                // FIXME: The error message here is not helpful.
                // ONTOL needs to tell about the reasons why it can't make
                // a meaningful union that's "sound" JSON-wise, with clear
                // disambiguating properties.
                debug!("THIS ERROR: {def_id:?} {repr_kind:?}");
                Err(UnionCheckError::UnitTypePartOfUnion(def_id))
            }
        }
    }

    fn add_property_set_to_discriminator(
        &self,
        discriminator_builder: &mut DiscriminatorBuilder,
        variant_def: DefId,
        property_set: &IndexMap<PropId, Property>,
        span: &SourceSpan,
        error_set: &mut ErrorSet,
        strings: &mut StringCtx<'m>,
    ) {
        let mut map_discriminator_candidate = MapDiscriminatorCandidate {
            result_type: variant_def,
            property_candidates: vec![],
        };

        for (prop_id, property) in property_set {
            let meta = rel_def_meta(property.rel_id, self.rel_ctx, self.defs);

            let (object_def_id, _) = meta.relationship.object;
            let object_ty = self.def_ty_ctx.def_table.get(&object_def_id).unwrap();
            let Some(property_name) = (match meta.relation_def_kind.value {
                DefKind::TextLiteral(lit) => Some(lit),
                _ => None,
            }) else {
                continue;
            };

            debug!(
                "trying rel {:?} {:?} ty: {object_ty:?}",
                meta.relationship, meta.relation_def_kind
            );

            match object_ty {
                Type::IntConstant(_) => {
                    todo!("Cannot match against numeric constants yet");
                }
                Type::TextConstant(def_id) => {
                    let string_literal = self.defs.get_string_representation(*def_id);
                    map_discriminator_candidate.property_candidates.push(
                        PropertyDiscriminatorCandidate {
                            relation_def_id: meta.relationship.relation_def_id,
                            discriminant: Discriminant::HasAttribute(
                                *prop_id,
                                strings.intern_constant(property_name),
                                LeafDiscriminant::IsTextLiteral(
                                    strings.intern_constant(string_literal),
                                ),
                            ),
                        },
                    );
                }
                _ => {}
            }
        }

        if map_discriminator_candidate.property_candidates.is_empty() {
            error_set.report(
                variant_def,
                UnionCheckError::CannotDiscriminateTypeByProperty,
                span,
            );
        } else {
            discriminator_builder
                .map_discriminator_candidates
                .push(map_discriminator_candidate);
        }
    }

    fn limit_property_discriminators(
        &self,
        union_def_id: DefId,
        union_def: &Def,
        builder: &mut DiscriminatorBuilder,
        error_set: &mut ErrorSet,
    ) {
        let total_candidates = builder.map_discriminator_candidates.len();
        if total_candidates == 0 {
            debug!("no candidates");
            return;
        }

        let mut relation_counters: FnvHashMap<DefId, usize> = Default::default();

        for discriminator in &builder.map_discriminator_candidates {
            for property_candidate in &discriminator.property_candidates {
                *relation_counters
                    .entry(property_candidate.relation_def_id)
                    .or_default() += 1;
            }
        }

        if let Some((selected_relation, _)) = relation_counters
            .into_iter()
            .find(|(_, count)| *count == total_candidates)
        {
            debug!("selected relation {selected_relation:?}");

            for discriminator in &mut builder.map_discriminator_candidates {
                discriminator
                    .property_candidates
                    .retain(|property_candidate| {
                        property_candidate.relation_def_id == selected_relation
                    })
            }
        } else {
            error_set.report(
                union_def_id,
                UnionCheckError::NoUniformDiscriminatorFound,
                &union_def.span,
            );
        }
    }

    /// For now, patterns must have a unique constant prefix.
    fn verify_disjoint_text_patterns(
        &self,
        union_def_id: DefId,
        union_def: &Def,
        builder: &mut DiscriminatorBuilder,
        error_variant: UnionCheckError,
        error_set: &mut ErrorSet,
    ) {
        if builder.any_string.len() > 1 {
            error_set.report(union_def_id, error_variant, &union_def.span);
            return;
        }

        if builder.pattern_candidates.is_empty() {
            return;
        }

        let mut prefix_index: PatriciaMap<FnvHashSet<DefId>> = Default::default();
        let mut guaranteed_ambiguous_count = 0;

        for (variant_def_id, keyed_segment) in &builder.pattern_candidates {
            let prefix = keyed_segment.value.constant_prefix();

            if let Some(prefix) = prefix {
                if let Some(set) = prefix_index.get_mut(&prefix) {
                    set.insert(*variant_def_id);
                } else {
                    prefix_index.insert(prefix, HashSet::from_iter([*variant_def_id]));
                }
            } else {
                guaranteed_ambiguous_count += 1;
            }
        }

        if guaranteed_ambiguous_count > 1 {
            error_set.report(union_def_id, error_variant, &union_def.span);
            return;
        }

        // Also check for ambiguity with string literals
        if let TextDiscriminator::Literals(literals) = &builder.string {
            for keyed in literals {
                let (literal, variant_def_id) = &keyed.value;
                if let Some(set) = prefix_index.get_mut(literal) {
                    set.insert(*variant_def_id);
                } else {
                    prefix_index.insert(literal, HashSet::from_iter([*variant_def_id]));
                }
            }
        }

        if !prefix_index.is_empty() && !builder.any_string.is_empty() {
            error_set.report(union_def_id, error_variant, &union_def.span);
            return;
        }

        let pattern_variants: HashSet<_> = builder.pattern_candidates.keys().copied().collect();
        let mut is_error = false;

        for (prefix, variant_set) in prefix_index.iter() {
            for variant_def_id in variant_set {
                if pattern_variants.contains(variant_def_id) {
                    if variant_set.len() > 1 {
                        // two or more variants have the same prefix
                        is_error = true;
                    }

                    let common_prefixes_len = prefix_index.common_prefixes(&prefix).count();
                    if common_prefixes_len > 1 {
                        // another variant shares a common prefix with this one
                        is_error = true;
                    }
                }
            }
        }

        if is_error {
            error_set.report(union_def_id, error_variant, &union_def.span);
        }
    }

    fn make_union_discriminator(
        &self,
        builder: DiscriminatorBuilder,
        discriminator_type: DiscriminatorType,
        error_set: &ErrorSet,
        strings: &mut StringCtx<'m>,
    ) -> UnionDiscriminator {
        let mut union_discriminator = UnionDiscriminator { variants: vec![] };

        if let Some(def_id) = builder.unit {
            union_discriminator
                .variants
                .push(UnionDiscriminatorVariant {
                    discriminant: Discriminant::MatchesLeaf(LeafDiscriminant::IsUnit),
                    role: discriminator_type.into_role(&VariantKey::Instance),
                    def_id,
                });
        }

        match builder.number {
            IntDiscriminator::None => {}
            IntDiscriminator::Any(def_id) => {
                union_discriminator
                    .variants
                    .push(UnionDiscriminatorVariant {
                        discriminant: Discriminant::MatchesLeaf(LeafDiscriminant::IsInt),
                        role: discriminator_type.into_role(&VariantKey::Instance),
                        def_id,
                    });
            }
            IntDiscriminator::Literals(literals) => {
                for keyed in literals {
                    let (literal, def_id) = keyed.value;
                    union_discriminator
                        .variants
                        .push(UnionDiscriminatorVariant {
                            discriminant: Discriminant::MatchesLeaf(
                                LeafDiscriminant::IsIntLiteral(literal),
                            ),
                            role: discriminator_type.into_role(&keyed.key),
                            def_id,
                        });
                }
            }
        }

        // match patterns before strings
        for (variant_def_id, keyed) in builder.pattern_candidates {
            let leaf_discriminant = LeafDiscriminant::MatchesCapturingTextPattern(variant_def_id);

            union_discriminator
                .variants
                .push(UnionDiscriminatorVariant {
                    role: discriminator_type.into_role(&keyed.key),
                    discriminant: match (discriminator_type, keyed.key) {
                        (DiscriminatorType::Data, _) => {
                            Discriminant::MatchesLeaf(leaf_discriminant)
                        }
                        (
                            DiscriminatorType::Identification,
                            VariantKey::IdProperty { name, prop_id, .. },
                        ) => Discriminant::HasAttribute(prop_id, name, leaf_discriminant),
                        (DiscriminatorType::Identification, _) => {
                            Discriminant::MatchesLeaf(leaf_discriminant)
                        }
                    },
                    def_id: variant_def_id,
                });
        }

        match builder.string {
            TextDiscriminator::None => {}
            TextDiscriminator::Any(def_id) => {
                union_discriminator
                    .variants
                    .push(UnionDiscriminatorVariant {
                        discriminant: Discriminant::MatchesLeaf(LeafDiscriminant::IsText),
                        role: discriminator_type.into_role(&VariantKey::Instance),
                        def_id,
                    });
            }
            TextDiscriminator::Literals(literals) => {
                for keyed in literals {
                    let (literal, def_id) = keyed.value;
                    union_discriminator
                        .variants
                        .push(UnionDiscriminatorVariant {
                            discriminant: Discriminant::MatchesLeaf(
                                LeafDiscriminant::IsTextLiteral(strings.intern_constant(&literal)),
                            ),
                            role: discriminator_type.into_role(&keyed.key),
                            def_id,
                        });
                }
            }
        }

        if let Some(variant_keyed) = builder.sequence {
            union_discriminator
                .variants
                .push(UnionDiscriminatorVariant {
                    discriminant: Discriminant::MatchesLeaf(LeafDiscriminant::IsSequence),
                    role: discriminator_type.into_role(&variant_keyed.key),
                    def_id: variant_keyed.value,
                });
        }

        for map_discriminator in builder.map_discriminator_candidates {
            for candidate in map_discriminator.property_candidates {
                union_discriminator
                    .variants
                    .push(UnionDiscriminatorVariant {
                        discriminant: candidate.discriminant,
                        role: discriminator_type.into_role(&VariantKey::Instance),
                        def_id: map_discriminator.result_type,
                    })
            }
        }

        if union_discriminator.variants.is_empty() {
            assert!(!error_set.errors.is_empty());
        }

        union_discriminator
    }

    fn make_compile_error(&self, union_error: UnionCheckError) -> CompileError {
        match union_error {
            UnionCheckError::UnitTypePartOfUnion(def_id) => {
                let ty = self.def_ty_ctx.def_table.get(&def_id).unwrap();
                CompileError::UnitTypePartOfUnion(format!("{}", FormatType::new(ty, self.defs)))
            }
            UnionCheckError::CannotDiscriminateType
            | UnionCheckError::CannotDiscriminateTypeByProperty => {
                CompileError::CannotDiscriminateType
            }
            UnionCheckError::NoUniformDiscriminatorFound => {
                CompileError::NoUniformDiscriminatorFound
            }
            UnionCheckError::SharedPrefixInPatternUnion => CompileError::SharedPrefixInPatternUnion,
            UnionCheckError::NonDisjointIdsInEntityUnion => {
                CompileError::NonDisjointIdsInEntityUnion
            }
        }
    }
}

enum DomainTypeMatchData<'a> {
    Struct(&'a IndexMap<PropId, Property>),
    #[allow(dead_code)]
    Sequence(&'a Sequence),
    ConstructorStringPattern(&'a TextPatternSegment),
    TextLiteral(&'a str),
}

#[derive(Default, Debug)]
struct DiscriminatorBuilder<'a> {
    unit: Option<DefId>,
    number: IntDiscriminator,
    any_string: Vec<VariantKeyed<DefId>>,
    string: TextDiscriminator,
    sequence: Option<VariantKeyed<DefId>>,
    pattern_candidates: IndexMap<DefId, VariantKeyed<&'a TextPatternSegment>>,
    map_discriminator_candidates: Vec<MapDiscriminatorCandidate>,
}

impl<'a> DiscriminatorBuilder<'a> {
    fn add_text_literal(&mut self, key: VariantKey, lit: &str, def_id: DefId) {
        match &mut self.string {
            TextDiscriminator::None => {
                self.string = TextDiscriminator::Literals(
                    [VariantKeyed {
                        key,
                        value: (lit.into(), def_id),
                    }]
                    .into(),
                );
            }
            TextDiscriminator::Any(_) => {}
            TextDiscriminator::Literals(set) => {
                set.insert(VariantKeyed {
                    key,
                    value: (lit.into(), def_id),
                });
            }
        }
    }

    fn add_text_pattern(
        &mut self,
        key: VariantKey,
        segment: &'a TextPatternSegment,
        variant_def_id: DefId,
    ) {
        self.pattern_candidates.insert(
            variant_def_id,
            VariantKeyed {
                key,
                value: segment,
            },
        );
    }
}

#[derive(Clone, Copy)]
enum DiscriminatorType {
    Data,
    Identification,
}

impl DiscriminatorType {
    fn into_role(self, key: &VariantKey) -> UnionDiscriminatorRole {
        match (self, key) {
            (Self::Data, VariantKey::Instance) => UnionDiscriminatorRole::Data,
            (Self::Identification, VariantKey::IdProperty { entity_id, .. }) => {
                UnionDiscriminatorRole::IdentifierOf(*entity_id)
            }
            _ => unreachable!(),
        }
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
enum VariantKey {
    Instance,
    IdProperty {
        entity_id: DefId,
        name: TextConstant,
        prop_id: PropId,
    },
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct VariantKeyed<T> {
    key: VariantKey,
    value: T,
}

#[derive(Default, Debug)]
enum IntDiscriminator {
    #[default]
    None,
    Any(DefId),
    Literals(IndexSet<VariantKeyed<(i64, DefId)>>),
}

#[derive(Default, Debug)]
enum TextDiscriminator {
    #[default]
    None,
    Any(DefId),
    Literals(IndexSet<VariantKeyed<(String, DefId)>>),
}

#[derive(Debug)]
struct MapDiscriminatorCandidate {
    result_type: DefId,
    property_candidates: Vec<PropertyDiscriminatorCandidate>,
}

#[derive(Debug)]
struct PropertyDiscriminatorCandidate {
    relation_def_id: DefId,
    discriminant: Discriminant,
}

#[derive(Default)]
struct ErrorSet {
    errors: FnvHashMap<DefId, FnvHashMap<UnionCheckError, SourceSpan>>,
}

impl ErrorSet {
    fn report(&mut self, def_id: DefId, error: UnionCheckError, span: &SourceSpan) {
        self.errors
            .entry(def_id)
            .or_default()
            .entry(error)
            .or_insert_with(|| *span);
    }
}

#[derive(Hash, Eq, PartialEq)]
enum UnionCheckError {
    UnitTypePartOfUnion(DefId),
    CannotDiscriminateType,
    CannotDiscriminateTypeByProperty,
    NoUniformDiscriminatorFound,
    SharedPrefixInPatternUnion,
    NonDisjointIdsInEntityUnion,
}

#[derive(Clone, Copy)]
struct EntitiesOnly(bool);

#[derive(Default)]
struct EntityDetector {
    entity_count: usize,
    non_entity_count: usize,
}

impl EntityDetector {
    fn detect(self, variants_len: usize) -> Result<EntitiesOnly, CompileError> {
        if !(self.entity_count == variants_len || self.non_entity_count == variants_len) {
            return Err(CompileError::CannotMixNonEntitiesInUnion);
        }

        Ok(EntitiesOnly(self.entity_count == variants_len))
    }
}
