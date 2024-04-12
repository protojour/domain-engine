use std::{borrow::Cow, collections::HashSet};

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use ontol_runtime::{
    debug::NoFmt,
    interface::{
        discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
        serde::{
            operator::{
                SerdeOperator, SerdeOperatorAddr, SerdeProperty, SerdePropertyFlags,
                SerdePropertyKind, SerdeStructFlags, SerdeUnionVariant, StructOperator,
                UnionOperator,
            },
            SerdeDef, SerdeModifier,
        },
    },
    ontology::ontol::{TextConstant, ValueGenerator},
    phf::PhfKey,
    property::{PropertyId, Role},
    DefId, RelationshipId,
};
use smartstring::alias::String;
use tracing::{debug, debug_span, warn};

use crate::{
    def::{DefKind, LookupRelationshipMeta, RelParams, RelationshipMeta},
    phf_build::build_phf_index_map,
    relation::{Properties, Property},
    repr::repr_model::ReprKind,
};

use super::{
    serde_generator::{insert_property, SerdeGenerator},
    union_builder::UnionBuilder,
    SerdeIntersection, SerdeKey, EDGE_PROPERTY,
};

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub(super) fn populate_struct_operator(
        &mut self,
        addr: SerdeOperatorAddr,
        def: SerdeDef,
        properties: &'c Properties,
    ) {
        let mut serde_properties: IndexMap<String, (PhfKey, SerdeProperty)> = Default::default();

        let mut struct_flags = SerdeStructFlags::default();
        let mut must_flatten_unions = false;

        if let Some(table) = &properties.table {
            for (property_id, property) in table {
                self.add_struct_op_property(
                    *property_id,
                    property,
                    def.modifier,
                    &mut struct_flags,
                    &mut serde_properties,
                    &mut must_flatten_unions,
                );
            }
        }

        if let Some(union_memberships) = self.union_member_cache.cache.get(&def.def_id) {
            for union_def_id in union_memberships {
                let Some(properties) = self.relations.properties_by_def_id(*union_def_id) else {
                    continue;
                };
                let Some(table) = &properties.table else {
                    continue;
                };

                for (property_id, property) in table {
                    let meta = self.defs.relationship_meta(property_id.relationship_id);

                    if meta.relationship.object.0 == *union_def_id {
                        self.add_struct_op_property(
                            *property_id,
                            property,
                            def.modifier,
                            &mut struct_flags,
                            &mut serde_properties,
                            &mut must_flatten_unions,
                        );
                    }
                }
            }
        }

        if must_flatten_unions {
            self.lazy_union_flattener_tasks
                .push_back((addr, def, properties));
            return;
        }

        serde_properties.insert(
            EDGE_PROPERTY.into(),
            (
                self.strings.make_phf_key(EDGE_PROPERTY),
                SerdeProperty {
                    property_id: PropertyId::subject(RelationshipId(DefId::unit())),
                    flags: SerdePropertyFlags::REL_PARAMS | SerdePropertyFlags::OPTIONAL,
                    value_addr: SerdeOperatorAddr(0),
                    value_generator: None,
                    kind: SerdePropertyKind::Plain {
                        rel_params_addr: None,
                    },
                },
            ),
        );

        let SerdeOperator::Struct(struct_op) = &mut self.operators_by_addr[addr.0 as usize] else {
            panic!();
        };
        struct_op.properties = build_phf_index_map(serde_properties.into_values());
        struct_op.flags.extend(struct_flags);
    }

    fn add_struct_op_property(
        &mut self,
        property_id: PropertyId,
        property: &Property,
        modifier: SerdeModifier,
        struct_flags: &mut SerdeStructFlags,
        output: &mut IndexMap<String, (PhfKey, SerdeProperty)>,
        must_flatten_unions: &mut bool,
    ) {
        let meta = self.defs.relationship_meta(property_id.relationship_id);
        let (value_type_def_id, ..) = meta.relationship.by(property_id.role.opposite());

        let (property_cardinality, value_addr) = self.get_property_operator(
            value_type_def_id,
            property.cardinality,
            modifier.cross_def_flags(),
        );

        let rel_params_addr = match &meta.relationship.rel_params {
            RelParams::Type(def_id) => {
                self.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(*def_id, modifier.reset())))
            }
            RelParams::Unit => None,
            _ => todo!(),
        };

        let prop_key: Cow<str> = match property_id.role {
            Role::Subject => match meta.relation_def_kind.value {
                DefKind::TextLiteral(prop_key) => Cow::Borrowed(prop_key),
                DefKind::Type(_) => {
                    return self.add_flattened_union_properties(
                        property_id,
                        modifier,
                        meta,
                        output,
                        must_flatten_unions,
                    );
                }
                _ => {
                    panic!("Unsupported property");
                }
            },
            Role::Object => Cow::Borrowed(
                meta.relationship
                    .object_prop
                    .expect("Object property has no name"),
            ),
        };

        let mut value_generator: Option<ValueGenerator> = None;
        let mut flags = SerdePropertyFlags::default();

        if let Some(default_const_def) = self
            .relations
            .default_const_objects
            .get(&meta.relationship_id)
        {
            let proc = self
                .codegen_tasks
                .result_const_procs
                .get(default_const_def)
                .unwrap_or_else(|| panic!());

            value_generator = Some(ValueGenerator::DefaultProc(proc.address));
        }

        if let Some(explicit_value_generator) = self
            .relations
            .value_generators
            .get(&property_id.relationship_id)
        {
            flags |= SerdePropertyFlags::READ_ONLY;
            if value_generator.is_some() {
                panic!("BUG: Cannot have both a default value and a generator. Solve this in type check.");
            }
            value_generator = Some(*explicit_value_generator);
        }

        if property_cardinality.is_optional() {
            flags |= SerdePropertyFlags::OPTIONAL;
        }

        if property.is_entity_id {
            flags |= SerdePropertyFlags::ENTITY_ID;
        }

        if flags.contains(SerdePropertyFlags::OPTIONAL | SerdePropertyFlags::ENTITY_ID) {
            struct_flags.insert(SerdeStructFlags::ENTITY_ID_OPTIONAL);
        }

        if let Some(target_properties) = self.relations.properties_by_def_id(value_type_def_id) {
            if target_properties.identified_by.is_some() && !property.is_entity_id {
                flags |= SerdePropertyFlags::IN_ENTITY_GRAPH;
            }
        }

        insert_property(
            output,
            prop_key.as_ref(),
            SerdeProperty {
                property_id,
                value_addr,
                flags,
                value_generator,
                kind: SerdePropertyKind::Plain { rel_params_addr },
            },
            modifier,
            self.strings,
        );
    }

    fn add_flattened_union_properties(
        &mut self,
        property_id: PropertyId,
        modifier: SerdeModifier,
        meta: RelationshipMeta,
        output: &mut IndexMap<String, (PhfKey, SerdeProperty)>,
        must_flatten_unions: &mut bool,
    ) {
        let object = meta.relationship.object;
        let ReprKind::StructUnion(_members) = self.repr_ctx.get_repr_kind(&object.0).unwrap()
        else {
            panic!("flattened object is not a struct union");
        };

        let default_modifier = SerdeModifier::json_default() | modifier.cross_def_flags();

        let union_addr = self
            .gen_addr_lazy(SerdeKey::Def(SerdeDef {
                def_id: object.0,
                modifier: default_modifier,
            }))
            .unwrap();
        let SerdeOperator::Union(union_operator) = self.get_operator(union_addr) else {
            // Need to generate union operators for all properties,
            // then reschedule this lazy generator
            *must_flatten_unions = true;
            return;
        };

        let mut text_constant_set = FnvHashSet::<TextConstant>::default();

        for variant in union_operator.unfiltered_variants() {
            match &variant.discriminator.discriminant {
                Discriminant::HasAttribute(_, text_constant, _) => {
                    text_constant_set.insert(*text_constant);
                }
                _ => {
                    panic!("must use a named attribute");
                }
            }
        }

        let discriminator_property_text_constant = {
            if text_constant_set.len() > 1 {
                panic!("ambiguous flattened entry-point/discriminator property");
            }

            text_constant_set.into_iter().next().unwrap()
        };

        let mut covered_properties: HashSet<std::string::String> = Default::default();

        // detect set of all properties belonging to the union
        for variant in union_operator.unfiltered_variants() {
            let SerdeOperator::Struct(struct_op) = self.get_operator(variant.addr) else {
                todo!("handle non-struct-op");
            };

            for (key, _) in struct_op.properties.iter() {
                covered_properties.insert(key.arc_str().as_str().into());
            }
        }

        let entrypoint_prop_name = self.strings[discriminator_property_text_constant].to_string();

        insert_property(
            output,
            &entrypoint_prop_name.clone(),
            SerdeProperty {
                property_id,
                value_addr: union_addr,
                flags: SerdePropertyFlags::empty(),
                value_generator: None,
                kind: SerdePropertyKind::FlatUnionDiscriminator { union_addr },
            },
            modifier,
            self.strings,
        );

        for key in covered_properties {
            if key != entrypoint_prop_name {
                let old = output.insert(
                    key.as_str().into(),
                    (
                        self.strings.make_phf_key(&key),
                        SerdeProperty {
                            property_id: PropertyId::subject(RelationshipId(DefId::unit())),
                            value_addr: SerdeOperatorAddr(0),
                            flags: SerdePropertyFlags::OPTIONAL,
                            value_generator: None,
                            kind: SerdePropertyKind::FlatUnionData,
                        },
                    ),
                );
                if let Some((old_phf_key, _)) = old {
                    warn!("property was overwritten: {:?}", old_phf_key.arc_str());
                }
            }
        }
    }

    pub(super) fn populate_struct_intersection_operator(
        &mut self,
        addr: SerdeOperatorAddr,
        intersection: SerdeIntersection,
    ) {
        let mut iterator = intersection.defs.iter();

        let mut new_operator = match intersection.main {
            Some(main_def) => self
                .lookup_addr_by_key(&SerdeKey::Def(main_def))
                .and_then(|addr| {
                    find_unambiguous_struct_operator(*addr, &self.operators_by_addr).ok()
                })
                .cloned()
                .unwrap(),
            None => loop {
                if let Some(next_def) = iterator.next() {
                    let origin_addr = self.lookup_addr_by_key(&SerdeKey::Def(*next_def)).unwrap();
                    if let Ok(operator) =
                        find_unambiguous_struct_operator(*origin_addr, &self.operators_by_addr)
                    {
                        break operator.clone();
                    }
                } else {
                    warn!("Intersection noop");
                    return;
                }
            },
        };

        // avoid duplicated properties, since some properties may already be "imported" from unions in which
        // the types are members,
        let mut dedup: FnvHashSet<PropertyId> = Default::default();

        for (_, serde_property) in new_operator.properties.iter() {
            dedup.insert(serde_property.property_id);
        }

        let mut properties: IndexMap<String, (PhfKey, SerdeProperty)> = Default::default();
        for (phf_key, val) in new_operator.properties.iter() {
            let string = &self.strings[phf_key.constant()];

            properties.insert(string.into(), (phf_key.clone(), val.clone()));
        }

        for next_def in iterator {
            if let Some(main) = &intersection.main {
                if next_def == main {
                    continue;
                }
            }

            let next_addr = self
                .lookup_addr_by_key(&SerdeKey::Def(*next_def))
                .expect("should be preregisted");

            if let Ok(next_map_type) =
                find_unambiguous_struct_operator(*next_addr, &self.operators_by_addr)
            {
                for (key, serde_property) in next_map_type.properties.iter() {
                    if dedup.insert(serde_property.property_id) {
                        insert_property(
                            &mut properties,
                            key.arc_str().as_str(),
                            serde_property.clone(),
                            next_def.modifier,
                            self.strings,
                        );
                    }
                }
            }
        }

        new_operator.properties = build_phf_index_map(properties.into_values());

        self.operators_by_addr[addr.0 as usize] = SerdeOperator::Struct(Box::new(new_operator));
    }

    pub(super) fn populate_union_repr_operator(
        &mut self,
        addr: SerdeOperatorAddr,
        def: SerdeDef,
        typename: TextConstant,
        properties: &'c Properties,
    ) {
        let _entered = debug_span!("lazy_union", def=?def.def_id).entered();

        let union_discriminator = self
            .relations
            .union_discriminators
            .get(&def.def_id)
            .expect("no union discriminator available. Should fail earlier");

        if union_discriminator.variants.is_empty() {
            panic!("No input variants");
        }

        let mut union_builder = UnionBuilder::new(def);
        let mut root_types: HashSet<DefId> = Default::default();

        for root_discriminator in &union_discriminator.variants {
            union_builder
                .add_root_discriminator(root_discriminator, self)
                .expect("Could not add root discriminator to union builder");

            root_types.insert(root_discriminator.serde_def.def_id);
        }

        let mut variants: Vec<_> = if properties.table.is_some() {
            // Need to do an intersection of the union type's _inherent_
            // properties and each variant's properties
            let inherent_properties_def = SerdeDef::new(
                def.def_id,
                SerdeModifier::INHERENT_PROPS | def.modifier.cross_def_flags(),
            );

            union_builder
                .build(self, |this, addr, result_def| {
                    if result_def.modifier.contains(SerdeModifier::INHERENT_PROPS)
                        && root_types.contains(&result_def.def_id)
                    {
                        debug!("Intersection with {result_def:?}");

                        // Make the intersection:
                        this.gen_addr_lazy(SerdeKey::Intersection(Box::new(SerdeIntersection {
                            main: None,
                            defs: [result_def, inherent_properties_def].into(),
                        })))
                        .expect("No inner operator")
                    } else {
                        addr
                    }
                })
                .unwrap()
        } else {
            union_builder
                .build(self, |_this, addr, _result_type| addr)
                .unwrap()
        };

        if variants.is_empty() {
            panic!(
                "empty variant set for {def:?}. Input variants were {:?}",
                union_discriminator.variants
            );
        }

        // Data coverage check.
        // If there's no designated ::Data variant purpose, we want to use the ::Identification for both purposes.
        {
            let mut cov_table: FnvHashMap<DefId, UnionDefVariantCoverage> = Default::default();

            for variant in &variants {
                let discriminator = &variant.discriminator;
                match discriminator.purpose {
                    VariantPurpose::Identification { entity_id } => {
                        cov_table.entry(entity_id).or_default().has_id = true;
                    }
                    VariantPurpose::Data => {
                        cov_table
                            .entry(discriminator.serde_def.def_id)
                            .or_default()
                            .has_data = true;
                    }
                    VariantPurpose::RawDynamicEntity => {}
                }
            }

            let mut extensions = vec![];

            for variant in &mut variants {
                let discriminator = &mut variant.discriminator;

                if let VariantPurpose::Identification { entity_id } = discriminator.purpose {
                    let coverage = cov_table.get(&entity_id).unwrap();

                    if !coverage.has_data {
                        let mut struct_modifier =
                            def.modifier.cross_def_flags() | SerdeModifier::json_default();
                        struct_modifier.remove(SerdeModifier::UNION | SerdeModifier::PRIMARY_ID);

                        let struct_def = SerdeDef {
                            def_id: entity_id,
                            modifier: struct_modifier,
                        };

                        let struct_properties_addr = self
                            .gen_addr_lazy(SerdeKey::Def(struct_def))
                            .expect("No property struct operator");

                        extensions.push(SerdeUnionVariant {
                            discriminator: VariantDiscriminator {
                                discriminant: discriminator.discriminant.clone(),
                                purpose: VariantPurpose::RawDynamicEntity,
                                serde_def: struct_def,
                            },
                            addr: struct_properties_addr,
                        });
                    }
                }
            }

            variants.extend(extensions);
        }

        self.operators_by_addr[addr.0 as usize] =
            SerdeOperator::Union(Box::new(UnionOperator::new(typename, def, variants)));
    }
}

fn find_unambiguous_struct_operator(
    addr: SerdeOperatorAddr,
    operators_by_addr: &[SerdeOperator],
) -> Result<&StructOperator, &SerdeOperator> {
    let operator = &operators_by_addr[addr.0 as usize];
    match operator {
        SerdeOperator::Struct(struct_op) => Ok(struct_op),
        SerdeOperator::Union(union_op) => {
            let mut map_count = 0;
            let mut result = Err(operator);

            for discriminator in union_op.unfiltered_variants() {
                if let Ok(map_type) =
                    find_unambiguous_struct_operator(discriminator.addr, operators_by_addr)
                {
                    result = Ok(map_type);
                    map_count += 1;
                } else {
                    let operator = &operators_by_addr[discriminator.addr.0 as usize];
                    debug!(
                        "SKIPPED SOMETHING: {operator:?}\n\n",
                        operator = NoFmt(operator)
                    );
                }
            }

            if map_count > 1 {
                Err(operator)
            } else {
                result
            }
        }
        SerdeOperator::Alias(value_op) => {
            find_unambiguous_struct_operator(value_op.inner_addr, operators_by_addr)
        }
        _ => Err(operator),
    }
}

#[derive(Default)]
struct UnionDefVariantCoverage {
    has_id: bool,
    has_data: bool,
}
