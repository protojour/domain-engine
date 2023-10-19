use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::{
    de::{DeserializeSeed, Error, MapAccess, Visitor},
    Deserializer,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    format_utils::{DoubleQuote, LogicOp, Missing},
    interface::serde::EDGE_PROPERTY,
    value::{Attribute, Data, PropertyId, Value},
    value_generator::ValueGenerator,
    vm::proc::{NParams, Procedure},
    DefId,
};

use super::{
    operator::{SerdeOperatorAddr, SerdeProperty, StructOperator},
    processor::{
        ProcessorMode, ProcessorProfile, RecursionLimitError, SerdeProcessor, SubProcessorContext,
    },
};

pub struct StructVisitor<'on, 'p> {
    pub processor: SerdeProcessor<'on, 'p>,
    pub buffered_attrs: Vec<(String, serde_value::Value)>,
    pub struct_op: &'on StructOperator,
    pub ctx: SubProcessorContext,
}

pub enum PropertyKey {
    Property(SerdeProperty),
    RelParams(SerdeOperatorAddr),
    Id(SerdeOperatorAddr),
    Ignored,
}

#[derive(Clone, Copy)]
pub struct SpecialAddrs<'s> {
    pub rel_params: Option<SerdeOperatorAddr>,
    pub id: Option<(&'s str, SerdeOperatorAddr)>,
}

pub struct DeserializedStruct {
    pub attributes: BTreeMap<PropertyId, Attribute>,
    pub id: Option<Value>,
    pub rel_params: Value,
}

#[derive(Clone, Copy)]
struct PropertySet<'a> {
    properties: &'a IndexMap<String, SerdeProperty>,
    special_addrs: SpecialAddrs<'a>,
    processor_mode: ProcessorMode,
    processor_profile: &'a ProcessorProfile,
    parent_property_id: Option<PropertyId>,
}

impl<'on, 'p, 'de> Visitor<'de> for StructVisitor<'on, 'p> {
    type Value = Attribute;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "type `{}`", self.struct_op.typename)
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        let type_def_id = self.struct_op.def.def_id;
        let deserialized_map = deserialize_struct(
            self.processor,
            map,
            self.buffered_attrs,
            &self.struct_op.properties,
            self.struct_op
                .required_count(self.processor.mode, self.processor.ctx.parent_property_id),
            SpecialAddrs {
                rel_params: self.ctx.rel_params_addr,
                id: None,
            },
        )?;
        Ok(Attribute {
            value: Value {
                data: Data::Struct(deserialized_map.attributes),
                type_def_id,
            },
            rel_params: deserialized_map.rel_params,
        })
    }
}

pub(super) fn deserialize_struct<'on, 'p, 'de, A: MapAccess<'de>>(
    processor: SerdeProcessor<'on, 'p>,
    mut map: A,
    buffered_attrs: Vec<(String, serde_value::Value)>,
    properties: &IndexMap<String, SerdeProperty>,
    expected_required_count: usize,
    special_addrs: SpecialAddrs,
) -> Result<DeserializedStruct, A::Error> {
    let mut attributes = BTreeMap::new();
    let mut rel_params = Value::unit();
    let mut id = None;

    let mut observed_required_count = 0;

    // first parse buffered attributes, if any
    for (serde_key, serde_value) in buffered_attrs {
        match PropertySet::new(
            properties,
            special_addrs,
            processor.mode,
            processor.profile,
            processor.ctx.parent_property_id,
        )
        .visit_str(&serde_key)?
        {
            PropertyKey::RelParams(addr) => {
                let Attribute { value, .. } = processor
                    .new_child(addr)
                    .map_err(RecursionLimitError::to_de_error)?
                    .deserialize(serde_value::ValueDeserializer::new(serde_value))?;

                rel_params = value;
            }
            PropertyKey::Id(addr) => {
                let Attribute { value, .. } = processor
                    .new_child(addr)
                    .map_err(RecursionLimitError::to_de_error)?
                    .deserialize(serde_value::ValueDeserializer::new(serde_value))?;
                id = Some(value);
            }
            PropertyKey::Property(serde_property) => {
                let Attribute { rel_params, value } = processor
                    .new_child_with_context(
                        serde_property.value_addr,
                        SubProcessorContext {
                            parent_property_id: Some(serde_property.property_id),
                            parent_property_flags: serde_property.flags,
                            rel_params_addr: serde_property.rel_params_addr,
                        },
                    )
                    .map_err(RecursionLimitError::to_de_error)?
                    .deserialize(serde_value::ValueDeserializer::new(serde_value))?;

                if !serde_property.is_optional() {
                    observed_required_count += 1;
                }

                attributes.insert(serde_property.property_id, Attribute { rel_params, value });
            }
            PropertyKey::Ignored => {}
        }
    }

    // parse rest of struct
    while let Some(map_key) = map.next_key_seed(PropertySet::new(
        properties,
        special_addrs,
        processor.mode,
        processor.profile,
        processor.ctx.parent_property_id,
    ))? {
        match map_key {
            PropertyKey::RelParams(addr) => {
                let Attribute { value, .. } = map.next_value_seed(
                    processor
                        .new_child(addr)
                        .map_err(RecursionLimitError::to_de_error)?,
                )?;

                rel_params = value;
            }
            PropertyKey::Id(addr) => {
                let Attribute { value, .. } = map.next_value_seed(
                    processor
                        .new_child(addr)
                        .map_err(RecursionLimitError::to_de_error)?,
                )?;

                id = Some(value);
            }
            PropertyKey::Property(serde_property) => {
                let attribute = map.next_value_seed(
                    processor
                        .new_child_with_context(
                            serde_property.value_addr,
                            SubProcessorContext {
                                parent_property_id: Some(serde_property.property_id),
                                parent_property_flags: serde_property.flags,
                                rel_params_addr: serde_property.rel_params_addr,
                            },
                        )
                        .map_err(RecursionLimitError::to_de_error)?,
                )?;

                if !serde_property.is_optional() {
                    observed_required_count += 1;
                }

                attributes.insert(serde_property.property_id, attribute);
            }
            PropertyKey::Ignored => {
                let _value: serde_value::Value = map.next_value()?;
            }
        }
    }

    if observed_required_count < expected_required_count {
        // Generate default values if missing
        for (_, property) in properties {
            // Only _default values_ are handled in the deserializer:
            if let Some(ValueGenerator::DefaultProc(address)) = property.value_generator {
                if !property.is_optional() && !attributes.contains_key(&property.property_id) {
                    let procedure = Procedure {
                        address,
                        n_params: NParams(0),
                    };
                    let value = processor.ontology.new_vm(procedure).run([]).unwrap();

                    // BUG: No support for rel_params:
                    attributes.insert(property.property_id, value.into());
                    observed_required_count += 1;
                }
            }
        }
    }

    if observed_required_count < expected_required_count
        || (rel_params.is_unit() != special_addrs.rel_params.is_none())
    {
        debug!(
            "Missing attributes(mode={:?}). Rel params match: {}, special_rel: {} parent_relationship: {:?}",
            processor.mode,
            rel_params.is_unit(),
            special_addrs.rel_params.is_none(),
            processor.ctx.parent_property_id,
        );
        for attr in &attributes {
            debug!("    attr {:?}", attr.0);
        }
        for prop in properties {
            debug!(
                "    prop {:?}('{}') {:?} visible={} optional={}",
                prop.1.property_id,
                prop.0,
                prop.1.flags,
                prop.1
                    .filter(processor.mode, processor.ctx.parent_property_id)
                    .is_some(),
                prop.1.is_optional()
            );
        }

        let mut items: Vec<DoubleQuote<String>> = properties
            .iter()
            .filter(|(_, property)| {
                property
                    .filter(processor.mode, processor.ctx.parent_property_id)
                    .is_some()
                    && !property.is_optional()
                    && !attributes.contains_key(&property.property_id)
            })
            .map(|(key, _)| DoubleQuote(key.clone()))
            .collect();

        if special_addrs.rel_params.is_some() && rel_params.type_def_id == DefId::unit() {
            items.push(DoubleQuote(EDGE_PROPERTY.into()));
        }

        debug!("    items len: {}", items.len());

        let missing_keys = Missing {
            items,
            logic_op: LogicOp::And,
        };

        return Err(serde::de::Error::custom(format!(
            "missing properties, expected {missing_keys}"
        )));
    }

    Ok(DeserializedStruct {
        attributes,
        id,
        rel_params,
    })
}

impl<'a> PropertySet<'a> {
    fn new(
        properties: &'a IndexMap<String, SerdeProperty>,
        special_addrs: SpecialAddrs<'a>,
        processor_mode: ProcessorMode,
        processor_profile: &'a ProcessorProfile,
        parent_property_id: Option<PropertyId>,
    ) -> Self {
        Self {
            properties,
            special_addrs,
            processor_mode,
            processor_profile,
            parent_property_id,
        }
    }
}

impl<'a, 'de> DeserializeSeed<'de> for PropertySet<'a> {
    type Value = PropertyKey;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'a, 'de> Visitor<'de> for PropertySet<'a> {
    type Value = PropertyKey;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "property identifier")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        match v {
            EDGE_PROPERTY => {
                if let Some(addr) = self.special_addrs.rel_params {
                    Ok(PropertyKey::RelParams(addr))
                } else {
                    Err(Error::custom("`_edge` property not accepted here"))
                }
            }
            _ => {
                if let Some((property_name, addr)) = self.special_addrs.id {
                    if v == property_name {
                        return Ok(PropertyKey::Id(addr));
                    }
                }

                if Some(v) == self.processor_profile.overridden_id_property_key {
                    for (_, prop) in self.properties {
                        if prop.is_entity_id() {
                            return Ok(PropertyKey::Property(*prop));
                        }
                    }

                    return Err(Error::custom(format!("unknown property `{v}`")));
                }

                if self.processor_profile.ignored_property_keys.contains(&v) {
                    return Ok(PropertyKey::Ignored);
                }

                let serde_property = self.properties.get(v).ok_or_else(|| {
                    // TODO: This error message could be improved to suggest valid fields.
                    // see OneOf in serde (this is a private struct)
                    Error::custom(format!("unknown property `{v}`"))
                })?;

                if serde_property
                    .filter(self.processor_mode, self.parent_property_id)
                    .is_some()
                {
                    Ok(PropertyKey::Property(*serde_property))
                } else if serde_property.is_read_only()
                    && !matches!(self.processor_mode, ProcessorMode::Read)
                {
                    Err(Error::custom(format!("property `{v}` is read-only")))
                } else {
                    Err(Error::custom(format!(
                        "property `{v}` not available in this context"
                    )))
                }
            }
        }
    }
}
