use std::collections::HashSet;

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{
    interface::serde::{
        operator::{
            SerdeOperator, SerdeOperatorAddr, SerdeProperty, SerdePropertyFlags, StructOperator,
            UnionOperator,
        },
        SerdeDef, SerdeModifier,
    },
    value::PropertyId,
    value_generator::ValueGenerator,
    DefId, Role,
};
use smartstring::alias::String;
use tracing::{debug, debug_span};

use crate::{
    def::{DefKind, LookupRelationshipMeta, RelParams},
    relation::{Properties, Property},
};

use super::{
    serde_generator::{insert_property, SerdeGenerator},
    union_builder::UnionBuilder,
    SerdeIntersection, SerdeKey,
};

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub(super) fn populate_struct_operator(
        &mut self,
        addr: SerdeOperatorAddr,
        def: SerdeDef,
        properties: &'c Properties,
    ) {
        let mut serde_properties = Default::default();

        if let Some(table) = &properties.table {
            for (property_id, property) in table {
                self.add_struct_op_property(
                    *property_id,
                    property,
                    def.modifier,
                    &mut serde_properties,
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
                            &mut serde_properties,
                        );
                    }
                }
            }
        }

        let SerdeOperator::Struct(struct_op) = &mut self.operators_by_addr[addr.0 as usize] else {
            panic!();
        };
        struct_op.properties = serde_properties;
    }

    pub(super) fn add_struct_op_property(
        &mut self,
        property_id: PropertyId,
        property: &Property,
        modifier: SerdeModifier,
        output: &mut IndexMap<String, SerdeProperty>,
    ) {
        let meta = self.defs.relationship_meta(property_id.relationship_id);
        let (value_type_def_id, ..) = meta.relationship.by(property_id.role.opposite());
        let prop_key = match property_id.role {
            Role::Subject => {
                let DefKind::TextLiteral(prop_key) = meta.relation_def_kind.value else {
                    panic!("Subject property is not a string literal");
                };

                *prop_key
            }
            Role::Object => meta
                .relationship
                .object_prop
                .expect("Object property has no name"),
        };

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

        if let Some(target_properties) = self.relations.properties_by_def_id(value_type_def_id) {
            if target_properties.identified_by.is_some() && !property.is_entity_id {
                flags |= SerdePropertyFlags::IN_ENTITY_GRAPH;
            }
        }

        insert_property(
            output,
            prop_key,
            SerdeProperty {
                property_id,
                value_addr,
                flags,
                value_generator,
                rel_params_addr,
            },
            modifier,
        );
    }

    pub(super) fn populate_struct_intersection_operator(
        &mut self,
        addr: SerdeOperatorAddr,
        intersection: SerdeIntersection,
    ) {
        let mut iterator = intersection.set.iter();

        let mut new_operator = match intersection.main {
            Some(main_def) => self
                .lookup_addr_by_key(&SerdeKey::Def(main_def))
                .and_then(|addr| self.find_unambiguous_struct_operator(*addr).ok())
                .cloned()
                .unwrap(),
            None => {
                // Does not matter what is chosen first
                let random_def = *iterator.next().unwrap();
                let origin_addr = self.lookup_addr_by_key(&SerdeKey::Def(random_def)).unwrap();
                self.find_unambiguous_struct_operator(*origin_addr)
                    .unwrap_or_else(|operator| {
                        panic!("Initial map not found for intersection: {operator:?}")
                    })
                    .clone()
            }
        };

        // avoid duplicated properties, since some properties may already be "imported" from unions in which
        // the types are members,
        let mut dedup: FnvHashSet<PropertyId> = Default::default();

        for serde_property in new_operator.properties.values() {
            dedup.insert(serde_property.property_id);
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

            if let Ok(next_map_type) = self.find_unambiguous_struct_operator(*next_addr) {
                for (key, serde_property) in &next_map_type.properties {
                    if dedup.insert(serde_property.property_id) {
                        insert_property(
                            &mut new_operator.properties,
                            key,
                            *serde_property,
                            next_def.modifier,
                        );
                    }
                }
            }
        }

        self.operators_by_addr[addr.0 as usize] = SerdeOperator::Struct(new_operator);
    }

    fn find_unambiguous_struct_operator(
        &self,
        addr: SerdeOperatorAddr,
    ) -> Result<&StructOperator, &SerdeOperator> {
        let operator = &self.operators_by_addr[addr.0 as usize];
        match operator {
            SerdeOperator::Struct(struct_op) => Ok(struct_op),
            SerdeOperator::Union(union_op) => {
                let mut map_count = 0;
                let mut result = Err(operator);

                for discriminator in union_op.unfiltered_variants() {
                    if let Ok(map_type) = self.find_unambiguous_struct_operator(discriminator.addr)
                    {
                        result = Ok(map_type);
                        map_count += 1;
                    } else {
                        let operator = &self.operators_by_addr[discriminator.addr.0 as usize];
                        debug!("SKIPPED SOMETHING: {operator:?}\n\n");
                    }
                }

                if map_count > 1 {
                    Err(operator)
                } else {
                    result
                }
            }
            SerdeOperator::Alias(value_op) => {
                self.find_unambiguous_struct_operator(value_op.inner_addr)
            }
            _ => Err(operator),
        }
    }

    pub(super) fn populate_union_repr_operator(
        &mut self,
        addr: SerdeOperatorAddr,
        def: SerdeDef,
        typename: &'c str,
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

        let variants: Vec<_> = if properties.table.is_some() {
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
                            set: [inherent_properties_def, result_def].into(),
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

        self.operators_by_addr[addr.0 as usize] =
            SerdeOperator::Union(UnionOperator::new(typename.into(), def, variants));
    }
}
