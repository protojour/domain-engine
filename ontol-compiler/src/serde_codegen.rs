use std::collections::HashMap;

use indexmap::IndexMap;
use ontol_runtime::{
    serde::{
        MapType, SerdeOperator, SerdeOperatorId, SerdeProperty, ValueType, ValueUnionDiscriminator,
        ValueUnionType,
    },
    DefId,
};
use smallvec::SmallVec;

use crate::{
    compiler::Compiler,
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{DefKind, Defs},
    relation::{Properties, Relations, SubjectProperties},
    types::{DefTypes, Type},
};

impl<'m> Compiler<'m> {
    pub fn serde_generator(&self) -> SerdeGenerator<'_, 'm> {
        SerdeGenerator {
            defs: &self.defs,
            def_types: &self.def_types,
            relations: &self.relations,
            serde_operators: Default::default(),
            serde_operators_per_def: Default::default(),
        }
    }
}

pub struct SerdeGenerator<'c, 'm> {
    defs: &'c Defs<'m>,
    def_types: &'c DefTypes<'m>,
    relations: &'c Relations,
    serde_operators: Vec<SerdeOperator>,
    serde_operators_per_def: HashMap<DefId, SerdeOperatorId>,
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
            Some(Type::Domain(def_id)) => {
                let properties = self.relations.properties_by_type.get(def_id);
                let typename = match self.defs.get_def_kind(*def_id) {
                    Some(DefKind::DomainType(ident)) => ident.clone(),
                    _ => "Unknown type".into(),
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
        match properties.map(|prop| &prop.subject) {
            Some(SubjectProperties::Empty) | None => SerdeOperator::MapType(MapType {
                typename: typename.into(),
                type_def_id,
                properties: Default::default(),
            }),
            Some(SubjectProperties::Value(relationship_id, _)) => {
                let Ok((relationship, _)) = self.get_relationship_meta(*relationship_id) else {
                    panic!("Problem getting property meta");
                };

                let operator_id = self
                    .get_serde_operator_id(relationship.object)
                    .expect("No inner operator");

                SerdeOperator::ValueType(ValueType {
                    typename: typename.into(),
                    type_def_id,
                    inner_operator_id: operator_id,
                })
            }
            Some(SubjectProperties::ValueUnion(_)) => {
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
            Some(SubjectProperties::Map(property_set)) => {
                let serde_properties = property_set.iter().map(|relation_id| {
                    let Ok((relationship, relation)) = self.get_property_meta(type_def_id, *relation_id) else {
                        panic!("Problem getting property meta");
                    };

                    let object_key = relation.object_prop().expect("Property has no name").clone();
                    let operator_id =
                        self.get_serde_operator_id(relationship.object)
                            .expect("No inner operator");

                    (
                        object_key.into(),
                        SerdeProperty {
                            relation_id: *relation_id,
                            operator_id,
                        }
                    )
                }).collect::<IndexMap<_, _>>();

                SerdeOperator::MapType(MapType {
                    typename: typename.into(),
                    type_def_id,
                    properties: serde_properties,
                })
            }
        }
    }
}

impl<'c, 'm> AsRef<Defs<'m>> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &Defs<'m> {
        &self.defs
    }
}

impl<'c, 'm> AsRef<DefTypes<'m>> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &DefTypes<'m> {
        &self.def_types
    }
}

impl<'c, 'm> AsRef<Relations> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &Relations {
        &self.relations
    }
}
