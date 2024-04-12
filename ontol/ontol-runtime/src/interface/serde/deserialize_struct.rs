use std::collections::{BTreeMap, HashMap};

use fnv::FnvHashMap;
use serde::de::{DeserializeSeed, MapAccess, Visitor};
use tracing::debug;

use crate::{
    format_utils::{DoubleQuote, LogicOp, Missing},
    interface::serde::deserialize_raw::RawVisitor,
    ontology::{
        domain::TypeKind,
        ontol::{TextConstant, ValueGenerator},
    },
    phf::PhfIndexMap,
    property::PropertyId,
    value::{Attribute, Value},
    vm::proc::{NParams, Procedure},
    DefId,
};

use super::{
    deserialize_property::{IdSingletonPropVisitor, PropKind, PropertyMapVisitor},
    matcher::{
        map_matchers::MapMatchMode, union_matcher::UnionMatcher, ExpectingMatching, ValueMatcher,
    },
    operator::{
        PossibleVariants, SerdeOperator, SerdeOperatorAddr, SerdeProperty, SerdeStructFlags,
        StructOperator,
    },
    processor::{RecursionLimitError, SerdeProcessor, SubProcessorContext},
    utils::BufferedAttrsReader,
};

/// The output of the struct deserializer.
/// Requires some post-processing before it can become an Attribute.
pub struct Struct {
    pub attributes: FnvHashMap<PropertyId, Attribute>,
    pub id: Option<Value>,
    pub rel_params: Value,
    observed_required_count: usize,

    /// Pre-discriminated flattened unions
    flattened_union_ops: FnvHashMap<PropertyId, SerdeOperatorAddr>,

    /// serde properties that may later be deserialized by the flattened unions above
    flattened_union_tmp_data: HashMap<Box<str>, serde_value::Value>,

    open_dict: BTreeMap<smartstring::alias::String, Value>,
}

/// A serde visitor for maps (i.e. JSON objects) that uses the [StructDeserializer] internally.
pub struct StructVisitor<'on, 'p> {
    pub processor: SerdeProcessor<'on, 'p>,

    // FIXME: Can make use of `Cow<'de, str>`?
    pub buffered_attrs: Vec<(Box<str>, serde_value::Value)>,

    pub struct_op: &'on StructOperator,
    pub ctx: SubProcessorContext,
    pub raw_dynamic_entity: bool,
}

/// The struct deserializer itself.
pub struct StructDeserializer<'on, 'p> {
    pub(super) type_def_id: DefId,
    pub(super) processor: SerdeProcessor<'on, 'p>,
    pub(super) flags: SerdeStructFlags,

    possible_props: PossibleProps<'on>,

    /// The number of expected properties
    expected_required_count: usize,

    /// The operator address for rel_params/edge
    pub(super) rel_params_addr: Option<SerdeOperatorAddr>,

    /// Whether to handle "raw dynamic entity" deserialization
    /// (i.e. dynamic ID/data detection)
    raw_dynamic_entity: Option<DefId>,
}

pub enum PossibleProps<'on> {
    Any(&'on PhfIndexMap<SerdeProperty>),
    IdSingleton {
        name: &'on str,
        addr: SerdeOperatorAddr,
    },
}

impl Default for Struct {
    fn default() -> Self {
        Self {
            attributes: FnvHashMap::default(),
            id: None,
            rel_params: Value::unit(),
            open_dict: BTreeMap::default(),
            flattened_union_ops: FnvHashMap::default(),
            flattened_union_tmp_data: HashMap::default(),
            observed_required_count: 0,
        }
    }
}

impl<'on, 'p, 'de> Visitor<'de> for StructVisitor<'on, 'p> {
    type Value = Attribute;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "type `{}`",
            &self.processor.ontology[self.struct_op.typename]
        )
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        let type_def_id = self.struct_op.def.def_id;
        let mut struct_deserializer = StructDeserializer::new(
            self.struct_op.def.def_id,
            self.processor,
            PossibleProps::Any(&self.struct_op.properties),
        )
        .with_struct_flags(self.struct_op.flags)
        .with_expected_required_count(self.struct_op.required_count(
            self.processor.mode,
            self.processor.ctx.parent_property_id,
            self.processor.profile.flags,
        ))
        .with_rel_params_addr(self.ctx.rel_params_addr);

        let output = if self.raw_dynamic_entity {
            struct_deserializer.raw_dynamic_entity = Some(type_def_id);
            let output = struct_deserializer.deserialize_struct(self.buffered_attrs, map)?;

            if output.id.is_some() {
                return Ok(Attribute {
                    rel: output.rel_params,
                    val: output.id.unwrap(),
                });
            } else {
                output
            }
        } else {
            struct_deserializer.deserialize_struct(self.buffered_attrs, map)?
        };

        let boxed_attrs = Box::new(output.attributes);
        Ok(Attribute {
            rel: output.rel_params,
            val: if self.ctx.is_update {
                Value::StructUpdate(boxed_attrs, type_def_id)
            } else {
                Value::Struct(boxed_attrs, type_def_id)
            },
        })
    }
}

impl<'on, 'p> StructDeserializer<'on, 'p> {
    pub fn new(
        type_def_id: DefId,
        processor: SerdeProcessor<'on, 'p>,
        possible_props: PossibleProps<'on>,
    ) -> Self {
        Self {
            type_def_id,
            processor,
            possible_props,
            flags: SerdeStructFlags::empty(),
            expected_required_count: 0,
            rel_params_addr: None,
            raw_dynamic_entity: None,
        }
    }

    fn with_struct_flags(mut self, flags: SerdeStructFlags) -> Self {
        self.flags = flags;
        self
    }

    pub fn with_expected_required_count(mut self, count: usize) -> Self {
        self.expected_required_count = count;
        self
    }

    pub fn with_rel_params_addr(mut self, rel_params_addr: Option<SerdeOperatorAddr>) -> Self {
        self.rel_params_addr = rel_params_addr;
        self
    }

    /// Perform the struct deserialization
    pub fn deserialize_struct<'de, A: MapAccess<'de>>(
        self,
        buffered_attrs: Vec<(Box<str>, serde_value::Value)>,
        map: A,
    ) -> Result<Struct, A::Error> {
        let mut output = Struct::default();

        let buf_reader = BufferedAttrsReader::new(buffered_attrs);

        match self.possible_props {
            PossibleProps::Any(properties) => {
                let prop_visitor = PropertyMapVisitor {
                    deserializer: &self,
                    properties,
                };

                self.consume(buf_reader, prop_visitor, &mut output)?;
                self.consume(map, prop_visitor, &mut output)?;
            }
            PossibleProps::IdSingleton { name, addr } => {
                let prop_visitor = IdSingletonPropVisitor {
                    deserializer: &self,
                    id_prop_name: name,
                    id_prop_addr: addr,
                };

                self.consume(buf_reader, prop_visitor, &mut output)?;
                self.consume(map, prop_visitor, &mut output)?;
            }
        }

        match self.try_convert_to_raw_dynamic_id(output) {
            Ok(id_struct) => Ok(id_struct),
            Err(mut output) => {
                self.generate_missing_attributes(&mut output)
                    .map_err(serde::de::Error::custom)?;

                self.deserialize_flattened_unions::<A::Error>(&mut output)?;

                self.report_missing_attributes(&output)
                    .map_err(serde::de::Error::custom)?;

                self.finalize_output(&mut output);
                Ok(output)
            }
        }
    }

    /// Deserialize an inner flattened struct.
    /// The strategy is to remove matching attributes from the passed HashMap,
    /// so that the remaining serde attributes can be used afterwards for other
    /// flattened structs or inherent properties.
    fn deserialize_inner_flattened_struct<E: serde::de::Error>(
        &self,
        all_attrs: &mut HashMap<Box<str>, serde_value::Value>,
    ) -> Result<Value, E> {
        let PossibleProps::Any(properties) = &self.possible_props else {
            panic!("expected any properties");
        };

        let mut output = Struct::default();

        for (key, serde_property) in properties.iter() {
            let property_processor = self
                .processor
                .new_child_with_context(
                    serde_property.value_addr,
                    SubProcessorContext {
                        is_update: false,
                        parent_property_id: Some(serde_property.property_id),
                        parent_property_flags: serde_property.flags,
                        rel_params_addr: None,
                    },
                )
                .map_err(RecursionLimitError::to_de_error)?;

            let is_optional =
                serde_property.is_optional_for(self.processor.mode, &self.processor.profile.flags);

            if let Some(serde_value) = all_attrs.remove(key.arc_str().as_str()) {
                let attr = property_processor
                    .deserialize(serde_value::ValueDeserializer::<E>::new(serde_value))?;
                output.attributes.insert(serde_property.property_id, attr);

                if !is_optional {
                    output.observed_required_count += 1;
                }
            } else if !is_optional {
                return Err(serde::de::Error::custom(format!(
                    "missing property `{}`",
                    key.arc_str()
                )));
            }
        }

        self.generate_missing_attributes(&mut output)
            .map_err(serde::de::Error::custom)?;

        self.report_missing_attributes(&output)
            .map_err(serde::de::Error::custom)?;

        let boxed_attrs = Box::new(output.attributes);
        Ok(if self.processor.ctx.is_update {
            Value::StructUpdate(boxed_attrs, self.type_def_id)
        } else {
            Value::Struct(boxed_attrs, self.type_def_id)
        })
    }

    /// Read from a serde MapAccess and copy resulting attributes into output
    fn consume<'de, A: MapAccess<'de>>(
        &self,
        mut map: A,
        property_visitor: impl DeserializeSeed<'de, Value = PropKind<'on>> + Copy,
        output: &mut Struct,
    ) -> Result<(), A::Error> {
        while let Some(prop_kind) = map.next_key_seed(property_visitor)? {
            match prop_kind {
                PropKind::RelParams(addr) => {
                    let Attribute { val, .. } = map.next_value_seed(
                        self.processor
                            .new_rel_params_child(addr)
                            .map_err(RecursionLimitError::to_de_error)?,
                    )?;

                    output.rel_params = val;
                }
                PropKind::SingletonId(addr) => {
                    let Attribute { val, .. } = map.next_value_seed(
                        self.processor
                            .new_child_with_context(addr, SubProcessorContext::entity_id())
                            .map_err(RecursionLimitError::to_de_error)?,
                    )?;

                    output.id = Some(val);
                }
                PropKind::OverriddenId(relationship_id, addr) => {
                    let attr = map.next_value_seed(
                        self.processor
                            .new_child_with_context(addr, SubProcessorContext::entity_id())
                            .map_err(RecursionLimitError::to_de_error)?,
                    )?;

                    output
                        .attributes
                        .insert(PropertyId::subject(relationship_id), attr);

                    if !self.flags.contains(SerdeStructFlags::ENTITY_ID_OPTIONAL) {
                        output.observed_required_count += 1;
                    }
                }
                PropKind::Property(serde_property, rel_params_addr) => {
                    let property_processor = self
                        .processor
                        .new_child_with_context(
                            serde_property.value_addr,
                            SubProcessorContext {
                                is_update: false,
                                parent_property_id: Some(serde_property.property_id),
                                parent_property_flags: serde_property.flags,
                                rel_params_addr: rel_params_addr.0,
                            },
                        )
                        .map_err(RecursionLimitError::to_de_error)?;

                    if serde_property
                        .is_optional_for(self.processor.mode, &self.processor.profile.flags)
                    {
                        if let Some(attr) =
                            map.next_value_seed(property_processor.to_option_processor())?
                        {
                            output.attributes.insert(serde_property.property_id, attr);
                        }
                    } else {
                        output.attributes.insert(
                            serde_property.property_id,
                            map.next_value_seed(property_processor)?,
                        );

                        output.observed_required_count += 1;
                    }
                }
                PropKind::FlatUnionDiscriminator(key, serde_property, variants) => {
                    let union_matcher = UnionMatcher {
                        typename: TextConstant(0),
                        ctx: self.processor.ctx,
                        possible_variants: PossibleVariants::new(
                            variants,
                            self.processor.mode,
                            self.processor.level,
                        ),
                        ontology: self.processor.ontology,
                        profile: self.processor.profile,
                        mode: self.processor.mode,
                        level: self.processor.level,
                    };
                    let map_matcher = union_matcher
                        .match_map()
                        .map_err(|_| serde::de::Error::custom("unmatchable"))?;
                    let value: serde_value::Value = map.next_value()?;

                    let map_match = map_matcher
                        .match_attribute(&key, &value)
                        .or_else(|_| map_matcher.match_fallback())
                        .map_err(|_indicisive| {
                            serde::de::Error::custom(format!(
                                "invalid map value, expected {}",
                                ExpectingMatching(&union_matcher)
                            ))
                        })?;

                    let MapMatchMode::Struct(addr, _) = map_match.mode else {
                        return Err(serde::de::Error::custom(
                            "flattened union error: not a struct",
                        ));
                    };

                    output
                        .flattened_union_ops
                        .insert(serde_property.property_id, addr);
                    output.flattened_union_tmp_data.insert(key, value);
                }
                PropKind::FlatUnionData(key) => {
                    output
                        .flattened_union_tmp_data
                        .insert(key, map.next_value()?);
                }
                PropKind::Open(key) => {
                    output.open_dict.insert(
                        key,
                        map.next_value_seed(
                            RawVisitor::new(self.processor.ontology, self.processor.level)
                                .map_err(RecursionLimitError::to_de_error)?,
                        )?,
                    );
                }
                PropKind::Ignored => {
                    let _value: serde_value::Value = map.next_value()?;
                }
            }
        }

        Ok(())
    }

    fn deserialize_flattened_unions<E: serde::de::Error>(
        &self,
        output: &mut Struct,
    ) -> Result<(), E> {
        for (property_id, addr) in std::mem::take(&mut output.flattened_union_ops) {
            let SerdeOperator::Struct(struct_op) = &self.processor.ontology[addr] else {
                return Err(E::custom("BUG: flattened union must use a struct operator"));
            };

            let struct_deserializer = StructDeserializer::new(
                struct_op.def.def_id,
                self.processor,
                PossibleProps::Any(&struct_op.properties),
            )
            .with_struct_flags(struct_op.flags)
            .with_expected_required_count(struct_op.required_count(
                self.processor.mode,
                self.processor.ctx.parent_property_id,
                self.processor.profile.flags,
            ));

            let ontol_value = struct_deserializer
                .deserialize_inner_flattened_struct(&mut output.flattened_union_tmp_data)?;

            output.attributes.insert(property_id, ontol_value.into());
        }

        // TODO/FIXME: The remaining flattened_union_tmp_data,
        // should it be converted to open data?

        Ok(())
    }

    /// If in dynamic raw entity mode, try to convert to a singleton ID struct
    fn try_convert_to_raw_dynamic_id(&self, mut output: Struct) -> Result<Struct, Struct> {
        let Some(entity_def_id) = self.raw_dynamic_entity else {
            return Err(output);
        };
        if output.attributes.len() != 1 {
            return Err(output);
        }

        let (prop_id, _) = output.attributes.iter().next().unwrap();
        let type_info = self.processor.ontology.get_type_info(entity_def_id);

        let TypeKind::Entity(entity_info) = &type_info.kind else {
            return Err(output);
        };
        if prop_id.relationship_id != entity_info.id_relationship_id {
            return Err(output);
        }

        let attributes = std::mem::take(&mut output.attributes);
        output.id = Some(attributes.into_iter().next().unwrap().1.val);
        Ok(output)
    }

    /// Generate default values if missing
    fn generate_missing_attributes(&self, output: &mut Struct) -> Result<(), std::string::String> {
        if output.observed_required_count >= self.expected_required_count {
            return Ok(());
        }

        if let PossibleProps::Any(properties) = &self.possible_props {
            for (_, property) in properties.iter() {
                // Only _default values_ are handled in the deserializer:
                if let Some(ValueGenerator::DefaultProc(address)) = property.value_generator {
                    if !property.is_optional_for(self.processor.mode, &self.processor.profile.flags)
                        && !output.attributes.contains_key(&property.property_id)
                    {
                        let procedure = Procedure {
                            address,
                            n_params: NParams(0),
                        };
                        let value = self
                            .processor
                            .ontology
                            .new_vm(procedure)
                            .run([])
                            .map_err(|vm_error| format!("{vm_error}"))?
                            .unwrap();

                        // BUG: No support for rel_params:
                        output.attributes.insert(property.property_id, value.into());
                        output.observed_required_count += 1;
                    }
                }
            }
        }

        Ok(())
    }

    fn report_missing_attributes(&self, output: &Struct) -> Result<(), std::string::String> {
        if output.observed_required_count >= self.expected_required_count
            && (output.rel_params.is_unit() == self.rel_params_addr.is_none())
        {
            return Ok(());
        }

        debug!(
            "Missing attributes(mode={:?}). Rel params match: {}, special_rel: {} parent_relationship: {:?} expected_required_count: {}",
            self.processor.mode,
            output.rel_params.is_unit(),
            self.rel_params_addr.is_none(),
            self.processor.ctx.parent_property_id,
            self.expected_required_count
        );
        for attr in &output.attributes {
            debug!("    attr {:?}", attr.0);
        }

        if let PossibleProps::Any(properties) = &self.possible_props {
            for prop in properties.iter() {
                debug!(
                    "    prop {:?}('{}') {:?} visible={} optional={}",
                    prop.1.property_id,
                    prop.0.arc_str(),
                    prop.1.flags,
                    prop.1
                        .filter(
                            self.processor.mode,
                            self.processor.ctx.parent_property_id,
                            self.processor.profile.flags
                        )
                        .is_some(),
                    prop.1.is_optional()
                );
            }
        };

        let mut items: Vec<DoubleQuote<String>> = match &self.possible_props {
            PossibleProps::Any(properties) => properties
                .iter()
                .filter(|(_, property)| {
                    property
                        .filter(
                            self.processor.mode,
                            self.processor.ctx.parent_property_id,
                            self.processor.profile.flags,
                        )
                        .is_some()
                        && !property.is_optional()
                        && !output.attributes.contains_key(&property.property_id)
                })
                .map(|(key, _)| DoubleQuote(key.arc_str().as_str().into()))
                .collect(),
            PossibleProps::IdSingleton { .. } => vec![],
        };

        if self.rel_params_addr.is_some() && output.rel_params.type_def_id() == DefId::unit() {
            items.push(DoubleQuote(
                self.processor
                    .ontology
                    .ontol_domain_meta()
                    .edge_property
                    .clone(),
            ));
        }

        debug!("    items len: {}", items.len());

        let missing_keys = Missing {
            items,
            logic_op: LogicOp::And,
        };

        Err(format!("missing properties, expected {missing_keys}"))
    }

    fn finalize_output(&self, output: &mut Struct) {
        if !output.open_dict.is_empty() {
            let open_dict = std::mem::take(&mut output.open_dict);

            output.attributes.insert(
                self.processor
                    .ontology
                    .ontol_domain_meta()
                    .open_data_property_id(),
                Value::Dict(Box::new(open_dict), DefId::unit()).into(),
            );
        }
    }
}
