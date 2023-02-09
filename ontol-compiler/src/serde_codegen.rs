use std::{collections::HashMap, ops::Range};

use indexmap::IndexMap;
use ontol_runtime::{
    serde::{
        MapType, SerdeOperator, SerdeOperatorId, SerdeProperty, ValueType, ValueUnionDiscriminator,
        ValueUnionType,
    },
    value::PropertyId,
    DefId, RelationId,
};
use smallvec::SmallVec;

use crate::{
    compiler::Compiler,
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{Cardinality, DefKind, Defs},
    relation::{ObjectProperties, Properties, Relations, SubjectProperties},
    types::{DefTypes, Type},
};

pub struct SerdeGenerator<'c, 'm> {
    defs: &'c Defs<'m>,
    def_types: &'c DefTypes<'m>,
    relations: &'c Relations,
    serde_operators: Vec<SerdeOperator>,
    serde_operators_per_def: HashMap<DefId, SerdeOperatorId>,
    array_operators: HashMap<(SerdeOperatorId, Option<u16>, Option<u16>), SerdeOperatorId>,
}

impl<'m> Compiler<'m> {
    pub fn serde_generator(&self) -> SerdeGenerator<'_, 'm> {
        SerdeGenerator {
            defs: &self.defs,
            def_types: &self.def_types,
            relations: &self.relations,
            serde_operators: Default::default(),
            serde_operators_per_def: Default::default(),
            array_operators: Default::default(),
        }
    }
}

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub fn finish(self) -> (Vec<SerdeOperator>, HashMap<DefId, SerdeOperatorId>) {
        (self.serde_operators, self.serde_operators_per_def)
    }

    pub fn get_serde_operator_id(&mut self, type_def_id: DefId) -> Option<SerdeOperatorId> {
        if let Some(id) = self.serde_operators_per_def.get(&type_def_id) {
            return Some(*id);
        }

        if let Some((operator_id, kind)) = self.create_serde_operator(type_def_id) {
            self.serde_operators[operator_id.0 as usize] = kind;
            Some(operator_id)
        } else {
            None
        }
    }

    fn create_serde_operator(
        &mut self,
        type_def_id: DefId,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match self.get_def_type(type_def_id) {
            Some(Type::Unit(_)) => Some((self.alloc_operator_id(type_def_id), SerdeOperator::Unit)),
            Some(Type::IntConstant(_)) => todo!(),
            Some(Type::Int(_)) => Some((
                self.alloc_operator_id(type_def_id),
                SerdeOperator::Int(type_def_id),
            )),
            Some(Type::Number(_)) => Some((
                self.alloc_operator_id(type_def_id),
                SerdeOperator::Number(type_def_id),
            )),
            Some(Type::String(_)) => Some((
                self.alloc_operator_id(type_def_id),
                SerdeOperator::String(type_def_id),
            )),
            Some(Type::StringConstant(def_id)) => {
                assert_eq!(type_def_id, *def_id);

                let literal = self.defs.get_string_literal(*def_id);
                Some((
                    self.alloc_operator_id(*def_id),
                    SerdeOperator::StringConstant(literal.into(), type_def_id),
                ))
            }
            Some(Type::Tuple(types)) => {
                let operator_ids = types
                    .iter()
                    .map(|ty| {
                        ty.get_single_def_id()
                            .and_then(|def_id| self.get_serde_operator_id(def_id))
                    })
                    .collect::<Option<SmallVec<_>>>()?;

                Some((
                    self.alloc_operator_id(type_def_id),
                    SerdeOperator::Tuple(operator_ids, type_def_id),
                ))
            }
            Some(Type::Array(_)) => {
                panic!("not handled here")
            }
            Some(Type::Domain(def_id) | Type::DomainEntity(def_id)) => {
                let properties = self.relations.properties_by_type.get(def_id);
                let typename = match self.defs.get_def_kind(*def_id) {
                    Some(DefKind::DomainType(ident) | DefKind::DomainEntity(ident)) => ident,
                    _ => "Unknown type",
                };
                let operator_id = self.alloc_operator_id(type_def_id);
                Some((
                    operator_id,
                    self.create_domain_type_serde_operator(typename, *def_id, properties),
                ))
            }
            Some(Type::Function { .. }) => None,
            Some(Type::Tautology | Type::Infer(_) | Type::Error) => {
                panic!("crap: {:?}", self.get_def_type(type_def_id));
            }
            None => panic!("No type available"),
        }
    }

    fn alloc_operator_id(&mut self, def_id: DefId) -> SerdeOperatorId {
        let operator_id = SerdeOperatorId(self.serde_operators.len() as u32);
        // We just need a temporary placeholder for this operator kind,
        // this will be properly overwritten after it's created:
        self.serde_operators.push(SerdeOperator::Unit);
        self.serde_operators_per_def.insert(def_id, operator_id);
        operator_id
    }

    fn create_domain_type_serde_operator(
        &mut self,
        typename: &str,
        type_def_id: DefId,
        properties: Option<&Properties>,
    ) -> SerdeOperator {
        let properties = match properties {
            None => return MapTypeBuilder::new(typename, type_def_id).build(),
            Some(properties) => properties,
        };

        match &properties.subject {
            SubjectProperties::Empty => MapTypeBuilder::new(typename, type_def_id)
                .add_object_properties(self, &properties.object)
                .build(),
            SubjectProperties::Value(relationship_id, _, cardinality) => {
                let Ok((relationship, _)) = self.get_relationship_meta(*relationship_id) else {
                    panic!("Problem getting property meta");
                };

                let inner_operator_id = self
                    .get_serde_operator_id(relationship.object)
                    .expect("No inner operator");

                let inner_operator_id =
                    self.cardinality_operator(inner_operator_id, relationship.object, *cardinality);

                SerdeOperator::ValueType(ValueType {
                    typename: typename.into(),
                    type_def_id,
                    inner_operator_id,
                })
            }
            SubjectProperties::ValueUnion(_) => {
                let union_disciminator = self
                    .relations
                    .union_discriminators
                    .get(&type_def_id)
                    .expect("no union discriminator available. Should fail earlier");

                SerdeOperator::ValueUnionType(ValueUnionType {
                    typename: typename.into(),
                    discriminators: union_disciminator
                        .variants
                        .iter()
                        .map(|discriminator| {
                            let operator_id = self
                                .get_serde_operator_id(discriminator.result_type)
                                .expect("No inner operator");

                            ValueUnionDiscriminator {
                                discriminator: discriminator.clone(),
                                operator_id,
                            }
                        })
                        .collect(),
                })
            }
            SubjectProperties::Map(property_set) => MapTypeBuilder::new(typename, type_def_id)
                .add_subject_property_set(self, property_set)
                .add_object_properties(self, &properties.object)
                .build(),
        }
    }

    fn cardinality_operator(
        &mut self,
        operator_id: SerdeOperatorId,
        element_def_id: DefId,
        cardinality: Cardinality,
    ) -> SerdeOperatorId {
        match cardinality {
            Cardinality::One => operator_id,
            Cardinality::Many => self.array_operator(operator_id, element_def_id, (None, None)),
            Cardinality::ManyWithRange(start, end) => {
                self.array_operator(operator_id, element_def_id, (start, end))
            }
        }
    }

    fn array_operator(
        &mut self,
        operator_id: SerdeOperatorId,
        element_def_id: DefId,
        range: (Option<u16>, Option<u16>),
    ) -> SerdeOperatorId {
        if let Some(array_operator_id) = self.array_operators.get(&(operator_id, range.0, range.1))
        {
            return *array_operator_id;
        }

        let array_operator_id = SerdeOperatorId(self.serde_operators.len() as u32);
        self.serde_operators.push(match range {
            (None, None) => SerdeOperator::Array(element_def_id, operator_id),
            (start, end) => {
                SerdeOperator::RangeArray(element_def_id, Range { start, end }, operator_id)
            }
        });
        self.array_operators
            .insert((operator_id, range.0, range.1), array_operator_id);
        array_operator_id
    }
}

struct MapTypeBuilder {
    map_type: MapType,
}

impl MapTypeBuilder {
    fn new(typename: &str, type_def_id: DefId) -> Self {
        Self {
            map_type: MapType {
                typename: typename.into(),
                type_def_id,
                properties: Default::default(),
            },
        }
    }

    fn build(self) -> SerdeOperator {
        SerdeOperator::MapType(self.map_type)
    }

    fn add_subject_property_set(
        mut self,
        generator: &mut SerdeGenerator,
        property_set: &IndexMap<RelationId, Cardinality>,
    ) -> Self {
        self.map_type
            .properties
            .extend(property_set.iter().map(|(relation_id, cardinality)| {
                let Ok((relationship, relation)) = generator
                    .get_subject_property_meta(self.map_type.type_def_id, *relation_id)
                else {
                    panic!("Problem getting subject property meta");
                };

                let subject_key = relation.subject_prop().expect("Property has no name");
                let operator_id = generator
                    .get_serde_operator_id(relationship.object)
                    .expect("No inner operator");
                let value_operator_id =
                    generator.cardinality_operator(operator_id, relationship.object, *cardinality);
                let edge_operator_id = if relationship.edge_params == DefId::unit() {
                    None
                } else {
                    generator.get_serde_operator_id(relationship.edge_params)
                };

                (
                    subject_key.into(),
                    SerdeProperty {
                        property_id: PropertyId::subject(*relation_id),
                        value_operator_id,
                        edge_operator_id,
                    },
                )
            }));
        self
    }

    fn add_object_properties(
        self,
        generator: &mut SerdeGenerator,
        object_properties: &ObjectProperties,
    ) -> Self {
        match object_properties {
            ObjectProperties::Map(property_set) => {
                self.add_object_property_set(generator, property_set)
            }
            ObjectProperties::Empty => self,
        }
    }

    fn add_object_property_set(
        mut self,
        generator: &mut SerdeGenerator,
        property_set: &IndexMap<RelationId, Cardinality>,
    ) -> Self {
        self.map_type
            .properties
            .extend(property_set.iter().map(|(relation_id, cardinality)| {
                let Ok((relationship, relation)) = generator
                    .get_object_property_meta(self.map_type.type_def_id, *relation_id)
                else {
                    panic!("Problem getting object property meta");
                };

                let object_key = relation.object_prop().expect("Property has no name");
                let operator_id = generator
                    .get_serde_operator_id(relationship.subject)
                    .expect("No inner operator");
                let value_operator_id =
                    generator.cardinality_operator(operator_id, relationship.subject, *cardinality);
                let edge_operator_id = if relationship.edge_params == DefId::unit() {
                    None
                } else {
                    generator.get_serde_operator_id(relationship.edge_params)
                };

                (
                    object_key.into(),
                    SerdeProperty {
                        property_id: PropertyId::object(*relation_id),
                        value_operator_id,
                        edge_operator_id,
                    },
                )
            }));
        self
    }
}

impl<'c, 'm> AsRef<Defs<'m>> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &Defs<'m> {
        self.defs
    }
}

impl<'c, 'm> AsRef<DefTypes<'m>> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &DefTypes<'m> {
        self.def_types
    }
}

impl<'c, 'm> AsRef<Relations> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &Relations {
        self.relations
    }
}
