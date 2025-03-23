use std::{borrow::Cow, ops::Deref};

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{
    DefId, DefPropTag, OntolDefTag, OntolDefTagExt, PropId,
    debug::OntolDebug,
    interface::{
        discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
        serde::{
            SerdeDef, SerdeModifier,
            operator::{
                SerdeDefAddr, SerdeOperator, SerdeOperatorAddr, SerdeProperty, SerdePropertyFlags,
                SerdePropertyKind, SerdeStructFlags, SerdeUnionVariant, StructOperator,
                UnionOperator,
            },
        },
    },
    ontology::ontol::{TextConstant, ValueGenerator},
    phf::PhfKey,
};
use tracing::{debug, debug_span, warn};

use crate::{
    edge::EdgeId,
    misc::UnionDiscriminatorRole,
    phf_build::build_phf_index_map,
    properties::{Properties, Property, identifies_any},
    relation::{RelReprMeta, rel_def_meta, rel_repr_meta},
    repr::repr_model::{ReprKind, ReprScalarKind, UnionBound},
};

use super::{
    EDGE_PROPERTY, SerdeIntersection, SerdeKey,
    serde_generator::{SerdeGenerator, insert_property, operator_to_leaf_discriminant},
};

impl<'c> SerdeGenerator<'c, '_> {
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
                let Some(properties) = self.prop_ctx.properties_by_def_id(*union_def_id) else {
                    continue;
                };
                let Some(table) = &properties.table else {
                    continue;
                };

                for (rel_id, property) in table {
                    let meta = rel_def_meta(property.rel_id, self.rel_ctx, self.defs);

                    if meta.relationship.subject.0 == *union_def_id {
                        self.add_struct_op_property(
                            *rel_id,
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
                self.str_ctx.make_phf_key(EDGE_PROPERTY),
                SerdeProperty {
                    id: PropId(OntolDefTag::RelationEdge.def_id(), DefPropTag(0)),
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

        // post-process PROPER_ENTITY
        if struct_op.flags.contains(SerdeStructFlags::PROPER_ENTITY) {
            let mut inherent_property_count = 0;

            // BUG: This might not be accurate
            // the point is to unset IDENTIFIABLE for self-identifying structs
            // (structs whose only property is an identifier)
            for (_, property) in serde_properties.values() {
                if !property.flags.contains(SerdePropertyFlags::IN_ENTITY_GRAPH)
                    && !property.flags.contains(SerdePropertyFlags::REL_PARAMS)
                {
                    inherent_property_count += 1;
                }
            }

            if inherent_property_count <= 1 {
                struct_op.flags.remove(SerdeStructFlags::PROPER_ENTITY);
            }
        }

        struct_op.properties = build_phf_index_map(serde_properties.into_values());
        struct_op.flags.extend(struct_flags);
    }

    fn add_struct_op_property(
        &mut self,
        id: PropId,
        property: &Property,
        modifier: SerdeModifier,
        struct_flags: &mut SerdeStructFlags,
        output: &mut IndexMap<String, (PhfKey, SerdeProperty)>,
        must_flatten_unions: &mut bool,
    ) {
        let meta = rel_repr_meta(property.rel_id, self.rel_ctx, self.defs, self.repr_ctx);
        let (value_type_def_id, ..) = meta.relationship.object();

        let (property_cardinality, value_addr) = self.get_property_operator(
            value_type_def_id,
            property.cardinality,
            modifier.cross_def_flags(),
        );

        let rel_params_addr = if let Some(edge_projection) = &meta.relationship.edge_projection {
            let edge_id = EdgeId(edge_projection.edge_id);
            let edge = self.edge_ctx.symbolic_edges.get(&edge_id).unwrap();
            if let Some((_, param_def_id)) = edge.find_parameter_cardinal() {
                self.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(param_def_id, modifier.reset())))
            } else {
                None
            }
        } else {
            None
        };

        // let rel_params_addr = match &meta.relationship.rel_params {
        //     RelParams::Type(def_id) => {
        //         self.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(*def_id, modifier.reset())))
        //     }
        //     RelParams::Unit => None,
        //     _ => todo!(),
        // };

        let prop_key: Cow<str> = match meta.relation_repr_kind.deref() {
            ReprKind::Scalar(_, ReprScalarKind::TextConstant(constant_def_id), _) => {
                let lit = self.defs.text_literal(*constant_def_id).unwrap();
                Cow::Borrowed(lit)
            }
            ReprKind::Unit => {
                return self.add_flattened_union_properties(
                    id,
                    modifier,
                    meta,
                    output,
                    must_flatten_unions,
                );
            }
            kind => {
                panic!("Unsupported property: {kind:?}");
            }
        };

        let mut value_generator: Option<ValueGenerator> = None;
        let mut flags = SerdePropertyFlags::default();

        if let Some(default_const_def) = self.misc_ctx.default_const_objects.get(&meta.rel_id) {
            let proc = self
                .code_ctx
                .result_const_procs
                .get(default_const_def)
                .unwrap_or_else(|| panic!());

            value_generator = Some(ValueGenerator::DefaultProc(proc.address));
        }

        if let Some(explicit_value_generator) = self.misc_ctx.value_generators.get(&property.rel_id)
        {
            flags |= SerdePropertyFlags::READ_ONLY | SerdePropertyFlags::GENERATOR;
            if value_generator.is_some() {
                panic!(
                    "BUG: Cannot have both a default value and a generator. Solve this in type check."
                );
            }
            value_generator = Some(*explicit_value_generator);
        }

        if let Some((repr_def_id, _)) = self.misc_ctx.relationship_repr.get(&property.rel_id) {
            if repr_def_id == &OntolDefTag::Crdt.def_id() {
                flags |= SerdePropertyFlags::WRITE_ONCE;
            }
        }

        if property_cardinality.is_optional() {
            flags |= SerdePropertyFlags::OPTIONAL;
        }

        if property.is_entity_id {
            flags |= SerdePropertyFlags::ENTITY_ID;
        }

        if property.is_edge_partial {
            flags |= SerdePropertyFlags::READ_ONLY;
        }

        if identifies_any(value_type_def_id, self.prop_ctx, self.repr_ctx) {
            flags |= SerdePropertyFlags::ANY_ID;
        }

        if flags.contains(SerdePropertyFlags::OPTIONAL | SerdePropertyFlags::ENTITY_ID) {
            struct_flags.insert(SerdeStructFlags::ENTITY_ID_OPTIONAL);
        }

        if let Some(target_properties) = self.prop_ctx.properties_by_def_id(value_type_def_id) {
            if target_properties.identified_by.is_some() && !property.is_entity_id {
                flags |= SerdePropertyFlags::IN_ENTITY_GRAPH;
            }
        }

        insert_property(
            output,
            prop_key.as_ref(),
            SerdeProperty {
                id,
                value_addr,
                flags,
                value_generator,
                kind: SerdePropertyKind::Plain { rel_params_addr },
            },
            modifier,
            self.str_ctx,
        );
    }

    fn add_flattened_union_properties(
        &mut self,
        id: PropId,
        modifier: SerdeModifier,
        meta: RelReprMeta,
        output: &mut IndexMap<String, (PhfKey, SerdeProperty)>,
        must_flatten_unions: &mut bool,
    ) {
        let object = meta.relationship.object;
        let ReprKind::Union(_members, UnionBound::Struct) =
            self.repr_ctx.get_repr_kind(&object.0).unwrap()
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
                Discriminant::HasAttribute(_, text_constant, ..) => {
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

        let mut covered_properties: IndexMap<std::string::String, FnvHashSet<SerdeOperatorAddr>> =
            Default::default();

        let mut variant_addrs: Vec<SerdeOperatorAddr> =
            Vec::with_capacity(union_operator.unfiltered_variants().len());

        // detect set of all properties belonging to the union
        for variant in union_operator.unfiltered_variants() {
            variant_addrs.push(variant.deserialize.addr);

            let SerdeOperator::Struct(struct_op) = self.get_operator(variant.deserialize.addr)
            else {
                todo!("handle non-struct-op");
            };

            for (key, property) in struct_op.properties.iter() {
                covered_properties
                    .entry(key.arc_str().as_str().into())
                    .or_default()
                    .insert(property.value_addr);
            }
        }

        let entrypoint_prop_name = self.str_ctx[discriminator_property_text_constant].to_string();

        insert_property(
            output,
            &entrypoint_prop_name.clone(),
            SerdeProperty {
                id,
                value_addr: union_addr,
                flags: SerdePropertyFlags::empty(),
                value_generator: None,
                kind: SerdePropertyKind::FlatUnionDiscriminator { union_addr },
            },
            modifier,
            self.str_ctx,
        );

        for (key, addrs) in covered_properties {
            if key != entrypoint_prop_name {
                // make a single address out of the addresses.
                // This is mostly to make documentation easier.

                let value_addr = if addrs.len() == 1 {
                    // unambiguous
                    addrs.iter().copied().next().unwrap()
                } else {
                    self.any_placeholder_addr()
                };

                let old = output.insert(
                    key.as_str().into(),
                    (
                        self.str_ctx.make_phf_key(&key),
                        SerdeProperty {
                            id: PropId(OntolDefTag::RelationFlatUnion.def_id(), DefPropTag(0)),
                            value_addr,
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
        let mut dedup: FnvHashSet<PropId> = Default::default();

        for (_, serde_property) in new_operator.properties.iter() {
            dedup.insert(serde_property.id);
        }

        let mut properties: IndexMap<String, (PhfKey, SerdeProperty)> = Default::default();
        for (phf_key, val) in new_operator.properties.iter() {
            let string = &self.str_ctx[phf_key.constant()];

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
                    if dedup.insert(serde_property.id) {
                        insert_property(
                            &mut properties,
                            key.arc_str().as_str(),
                            serde_property.clone(),
                            next_def.modifier,
                            self.str_ctx,
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
        _properties: &'c Properties,
    ) {
        let _entered = debug_span!("lazy_union", addr=?addr.0, def=?def.def_id).entered();

        let union_discriminator = self
            .misc_ctx
            .union_discriminators
            .get(&def.def_id)
            .expect("no union discriminator available. Should fail earlier");

        if union_discriminator.variants.is_empty() {
            panic!("No input variants");
        }

        let mut serde_variants: Vec<SerdeUnionVariant> = vec![];

        for source_variant in &union_discriminator.variants {
            let type_def_id = match &source_variant.role {
                UnionDiscriminatorRole::Data => source_variant.def_id,
                UnionDiscriminatorRole::IdentifierOf(entity_id) => *entity_id,
            };

            let mut variant_serde_def = def.with_def(type_def_id);
            variant_serde_def.modifier |= SerdeModifier::INHERENT_PROPS;

            let addr = self.gen_addr_lazy(SerdeKey::Def(variant_serde_def));

            let discriminator = VariantDiscriminator {
                discriminant: source_variant.discriminant.clone(),
                purpose: match &source_variant.role {
                    UnionDiscriminatorRole::Data => VariantPurpose::Data,
                    UnionDiscriminatorRole::IdentifierOf(_) => VariantPurpose::Identification,
                },
            };

            if let Some(addr) = addr {
                debug!(
                    op = ?self.get_operator(addr).debug(self.str_ctx),
                    "  made sub-operator",
                );

                let variant = SerdeUnionVariant {
                    discriminator,
                    deserialize: SerdeDefAddr::new(type_def_id, addr),
                    serialize: SerdeDefAddr::new(type_def_id, addr),
                };

                debug!(
                    variant = ?variant.debug(self.str_ctx),
                    "add union serde variant",
                );
                serde_variants.push(variant);

                if let Err(err) = self.try_add_id_variant(
                    type_def_id,
                    def.modifier.cross_def_flags(),
                    addr,
                    &mut serde_variants,
                ) {
                    debug!("id not added: {err}");
                }
            }
        }

        self.operators_by_addr[addr.0 as usize] =
            SerdeOperator::Union(Box::new(UnionOperator::new(typename, def, serde_variants)));
    }

    fn try_add_id_variant(
        &mut self,
        type_def_id: DefId,
        cross_def_flags: SerdeModifier,
        struct_addr: SerdeOperatorAddr,
        serde_variants: &mut Vec<SerdeUnionVariant>,
    ) -> Result<(), &str> {
        let Some(properties) = self.prop_ctx.properties_by_def_id(type_def_id) else {
            return Err("no properties");
        };

        let Some(identifies_relationship_id) = properties.identified_by else {
            return Err("not identified");
        };
        let Some(id_addr) = self.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
            type_def_id,
            SerdeModifier::PRIMARY_ID | cross_def_flags,
        ))) else {
            // This type has no inherent id
            return Err("no address for ID");
        };

        let Some((prop_id, _)) = properties
            .table
            .as_ref()
            .and_then(|table| table.iter().find(|(_, prop)| prop.is_entity_id))
        else {
            return Err("entity id property not found");
        };

        let identifies_meta = rel_def_meta(identifies_relationship_id, self.rel_ctx, self.defs);

        let (id_property_name, id_leaf_discriminant) =
            match self.operators_by_addr.get(id_addr.0 as usize).unwrap() {
                SerdeOperator::IdSingletonStruct(_entity_id, id_property_name, inner_addr) => (
                    *id_property_name,
                    if true {
                        operator_to_leaf_discriminant(self.get_operator(*inner_addr))
                    } else {
                        panic!();
                        // The point of using IsAny is that as soon as the `ìd_property_name`
                        // matches, the variant has been found. The _value_ matcher
                        // is then decided, without further fallback.
                        // I.e. handled properly by its deserializer.
                        // LeafDiscriminant::IsAny
                    },
                ),
                other => panic!("id operator was not an Id: {:?}", other.debug(self.str_ctx)),
            };

        let id_variant = SerdeUnionVariant {
            discriminator: VariantDiscriminator {
                discriminant: Discriminant::HasAttribute(
                    *prop_id,
                    id_property_name,
                    id_leaf_discriminant,
                ),
                purpose: VariantPurpose::Identification,
            },
            deserialize: SerdeDefAddr::new(type_def_id, struct_addr),
            serialize: SerdeDefAddr::new(identifies_meta.relationship.subject.0, id_addr),
        };

        debug!("add ID variant {:?}", id_variant.debug(self.str_ctx));

        serde_variants.push(id_variant);
        Ok(())
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
                if let Ok(map_type) = find_unambiguous_struct_operator(
                    discriminator.deserialize.addr,
                    operators_by_addr,
                ) {
                    result = Ok(map_type);
                    map_count += 1;
                } else {
                    let operator = &operators_by_addr[discriminator.deserialize.addr.0 as usize];
                    debug!(
                        "SKIPPED SOMETHING: {operator:?}\n\n",
                        operator = operator.debug(&())
                    );
                }
            }

            if map_count > 1 { Err(operator) } else { result }
        }
        SerdeOperator::Alias(value_op) => {
            find_unambiguous_struct_operator(value_op.inner_addr, operators_by_addr)
        }
        _ => Err(operator),
    }
}
