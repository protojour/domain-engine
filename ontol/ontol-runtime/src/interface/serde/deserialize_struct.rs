use std::{
    collections::{BTreeMap, HashMap},
    ops::ControlFlow,
};

use bit_set::BitSet;
use fnv::FnvHashMap;
use serde::de::{DeserializeSeed, MapAccess, Visitor};
use tracing::{debug, trace, trace_span};

use crate::{
    attr::Attr,
    debug::OntolDebug,
    format_utils::{DoubleQuote, LogicOp, LogicalConcat},
    interface::serde::deserialize_raw::RawVisitor,
    ontology::{domain::DefKind, ontol::ValueGenerator},
    phf::PhfIndexMap,
    value::{Value, ValueTag},
    vm::proc::{NParams, Procedure},
    DefId, RelationshipId,
};

use super::{
    deserialize_property::{PropKind, PropertyMapVisitor},
    matcher::{union_matcher::UnionMatcher, ExpectingMatching, ValueMatcher},
    operator::{
        PossibleVariants, SerdeOperator, SerdeOperatorAddr, SerdeProperty, SerdeStructFlags,
        StructOperator,
    },
    processor::{
        ProcessorLevel, ProcessorMode, ProcessorProfileFlags, RecursionLimitError, SerdeProcessor,
        SubProcessorContext,
    },
    utils::BufferedAttrsReader,
};

/// The output of the struct deserializer.
/// Requires some post-processing before it can become an Attribute.
pub struct Struct {
    pub attributes: FnvHashMap<RelationshipId, Attr>,
    pub id: Option<Value>,
    pub rel_params: Value,

    /// Whether the struct was interpreted as only the ID of an existing entity
    pub resolved_to_id: bool,

    observed_props_bitset: BitSet,

    /// Pre-discriminated flattened unions
    flattened_union_ops: FnvHashMap<RelationshipId, SerdeOperatorAddr>,

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
}

/// The struct deserializer itself.
pub struct StructDeserializer<'on, 'p> {
    pub(super) type_def_id: DefId,
    pub(super) processor: SerdeProcessor<'on, 'p>,
    pub(super) flags: SerdeStructFlags,
    level: ProcessorLevel,

    properties: &'on PhfIndexMap<SerdeProperty>,

    /// The required props
    required_props_bitset: BitSet,

    /// The operator address for rel_params/edge
    pub(super) rel_params_addr: Option<SerdeOperatorAddr>,

    pub(super) dynamic_id_unchecked: bool,
}

impl Default for Struct {
    fn default() -> Self {
        Self {
            attributes: FnvHashMap::default(),
            id: None,
            resolved_to_id: false,
            rel_params: Value::unit(),
            open_dict: BTreeMap::default(),
            flattened_union_ops: FnvHashMap::default(),
            flattened_union_tmp_data: HashMap::default(),
            observed_props_bitset: BitSet::default(),
        }
    }
}

impl<'on, 'p, 'de> Visitor<'de> for StructVisitor<'on, 'p> {
    type Value = Attr;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "type `{}`",
            &self.processor.ontology[self.struct_op.typename]
        )
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        let type_def_id = self.struct_op.def.def_id;
        let struct_deserializer = StructDeserializer::new(
            self.struct_op.def.def_id,
            self.processor,
            &self.struct_op.properties,
            self.struct_op.flags,
            self.processor.level,
        )
        .with_required_props_bitset(self.struct_op.required_props_bitset(
            self.processor.mode,
            self.processor.ctx.parent_property_id,
            self.processor.profile.flags,
        ))
        .with_rel_params_addr(self.ctx.rel_params_addr);

        let output = struct_deserializer.deserialize_struct(self.buffered_attrs, map)?;

        if self
            .struct_op
            .flags
            .contains(SerdeStructFlags::PROPER_ENTITY)
            && output.id.is_some()
            && output.resolved_to_id
        {
            Ok(Attr::unit_or_tuple(output.id.unwrap(), output.rel_params))
        } else {
            Ok(Attr::unit_or_tuple(
                Value::Struct(
                    Box::new(output.attributes),
                    ValueTag::from(type_def_id).with_is_update(self.ctx.is_update),
                ),
                output.rel_params,
            ))
        }
    }
}

impl<'on, 'p> StructDeserializer<'on, 'p> {
    pub fn new(
        type_def_id: DefId,
        processor: SerdeProcessor<'on, 'p>,
        properties: &'on PhfIndexMap<SerdeProperty>,
        flags: SerdeStructFlags,
        level: ProcessorLevel,
    ) -> Self {
        Self {
            type_def_id,
            processor,
            properties,
            flags,
            level,
            required_props_bitset: BitSet::default(),
            rel_params_addr: None,
            dynamic_id_unchecked: matches!(
                processor.mode,
                ProcessorMode::Raw | ProcessorMode::RawTreeOnly
            ) && flags.contains(SerdeStructFlags::PROPER_ENTITY),
        }
    }

    pub fn with_required_props_bitset(mut self, required: BitSet) -> Self {
        self.required_props_bitset = required;
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

        let prop_visitor = PropertyMapVisitor {
            deserializer: &self,
            properties: self.properties,
        };

        trace!(
            "deserialize properties {:?} {:#?}",
            self.processor.mode.debug(&()),
            self.properties.raw_map().debug(self.processor.ontology)
        );

        self.consume(buf_reader, prop_visitor, &mut output)?;
        self.consume(map, prop_visitor, &mut output)?;

        match self
            .try_convert_to_raw_dynamic_id(output)
            .map_err(serde::de::Error::custom)?
        {
            IdOrStruct::Id(id_struct) => Ok(id_struct),
            IdOrStruct::Struct(mut output) => {
                debug!("not a dynamic id");

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
        let mut output = Struct::default();

        for (prop_idx, (key, serde_property)) in self.properties.raw_map().iter().enumerate() {
            let property_processor = self
                .processor
                .new_child_with_context(
                    serde_property.value_addr,
                    SubProcessorContext {
                        is_update: false,
                        parent_property_id: Some(serde_property.rel_id),
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
                output.attributes.insert(serde_property.rel_id, attr);
                output.observed_props_bitset.insert(prop_idx);
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

        let mut tag = ValueTag::from(self.type_def_id);
        if self.processor.ctx.is_update {
            tag.set_is_update();
        }

        let boxed_attrs = Box::new(output.attributes);
        Ok(Value::Struct(boxed_attrs, tag))
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
                    let Attr::Unit(rel_params) = map.next_value_seed(
                        self.processor
                            .new_rel_params_child(addr)
                            .map_err(RecursionLimitError::to_de_error)?,
                    )?
                    else {
                        return Err(serde::de::Error::custom("invalid relation parameters"));
                    };

                    output.rel_params = rel_params;
                }
                PropKind::SingletonId(addr) => {
                    let Attr::Unit(id) = map.next_value_seed(
                        self.processor
                            .new_child_with_context(addr, SubProcessorContext::entity_id())
                            .map_err(RecursionLimitError::to_de_error)?,
                    )?
                    else {
                        panic!("expected singleton attribute");
                    };

                    output.id = Some(id);
                }
                PropKind::OverriddenId(relationship_id, addr) => {
                    let attr = map.next_value_seed(
                        self.processor
                            .new_child_with_context(addr, SubProcessorContext::entity_id())
                            .map_err(RecursionLimitError::to_de_error)?,
                    )?;

                    output.attributes.insert(relationship_id, attr);

                    if !self.flags.contains(SerdeStructFlags::ENTITY_ID_OPTIONAL) {
                        let (prop_idx, _) = self
                            .properties
                            .raw_map()
                            .iter()
                            .enumerate()
                            .find(|(_, (_, prop))| prop.is_entity_id())
                            .ok_or_else(|| {
                                serde::de::Error::custom(
                                    "corresponding id property for overridden id not found",
                                )
                            })?;

                        output.observed_props_bitset.insert(prop_idx);
                    }
                }
                PropKind::Property(prop_idx, serde_property, rel_params_addr) => {
                    let _entered = trace_span!(
                        "prop",
                        rel = ?serde_property.rel_id,
                        addr = serde_property.value_addr.0
                    )
                    .entered();

                    let property_processor = self
                        .processor
                        .new_child_with_context(
                            serde_property.value_addr,
                            SubProcessorContext {
                                is_update: false,
                                parent_property_id: Some(serde_property.rel_id),
                                parent_property_flags: serde_property.flags,
                                rel_params_addr: rel_params_addr.0,
                            },
                        )
                        .map_err(RecursionLimitError::to_de_error)?;

                    output.observed_props_bitset.insert(prop_idx);

                    if serde_property
                        .is_optional_for(self.processor.mode, &self.processor.profile.flags)
                    {
                        if let Some(attr) =
                            map.next_value_seed(property_processor.to_option_processor())?
                        {
                            output.attributes.insert(serde_property.rel_id, attr);
                        }
                    } else {
                        output.attributes.insert(
                            serde_property.rel_id,
                            map.next_value_seed(property_processor)?,
                        );

                        debug!(
                            "observe required prop {prop_idx} {:?}",
                            serde_property.rel_id
                        );
                    }
                }
                PropKind::FlatUnionDiscriminator(prop_idx, key, serde_property, union_addr) => {
                    let SerdeOperator::Union(union_op) = &self.processor.ontology[union_addr]
                    else {
                        panic!("expected a union operator");
                    };
                    let union_matcher = UnionMatcher {
                        typename: union_op.typename(),
                        ctx: self.processor.ctx,
                        possible_variants: PossibleVariants::new(
                            union_op.unfiltered_variants(),
                            self.processor.mode,
                            self.processor.level,
                        ),
                        ontology: self.processor.ontology,
                        profile: self.processor.profile,
                        mode: self.processor.mode,
                        level: self.processor.level,
                    };
                    let mut map_matcher = union_matcher
                        .match_map()
                        .map_err(|_| serde::de::Error::custom("unmatchable"))?;
                    let value: serde_value::Value = map.next_value()?;

                    let buffer = vec![(key, value)];

                    let match_ok = match map_matcher.consume_next_attr(&buffer) {
                        ControlFlow::Break(match_ok) => match_ok,
                        ControlFlow::Continue(()) => match map_matcher.consume_end(&buffer) {
                            Ok(match_ok) => match_ok,
                            Err(()) => {
                                return Err(serde::de::Error::custom(format!(
                                    "property \"{key}\": invalid value, expected {expected}",
                                    key = buffer[0].0,
                                    expected = ExpectingMatching(&union_matcher)
                                )))
                            }
                        },
                    };

                    output
                        .flattened_union_ops
                        .insert(serde_property.rel_id, match_ok.addr);
                    output.flattened_union_tmp_data.extend(buffer);
                    output.observed_props_bitset.insert(prop_idx);
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
        for (rel_id, addr) in std::mem::take(&mut output.flattened_union_ops) {
            let SerdeOperator::Struct(struct_op) = &self.processor.ontology[addr] else {
                return Err(E::custom("BUG: flattened union must use a struct operator"));
            };

            let struct_deserializer = StructDeserializer::new(
                struct_op.def.def_id,
                self.processor,
                &struct_op.properties,
                struct_op.flags,
                self.processor.level,
            )
            .with_required_props_bitset(struct_op.required_props_bitset(
                self.processor.mode,
                self.processor.ctx.parent_property_id,
                self.processor.profile.flags,
            ));

            let ontol_value = struct_deserializer
                .deserialize_inner_flattened_struct(&mut output.flattened_union_tmp_data)?;

            output.attributes.insert(rel_id, ontol_value.into());
        }

        if self.flags.contains(SerdeStructFlags::OPEN_DATA)
            && self
                .processor
                .profile
                .flags
                .contains(ProcessorProfileFlags::DESERIALIZE_OPEN_DATA)
        {
            debug!(
                "remaining flattened union tmp data: {:#?}",
                output.flattened_union_tmp_data
            );

            for (key, serde_value) in std::mem::take(&mut output.flattened_union_tmp_data) {
                output.open_dict.insert(
                    key.into(),
                    RawVisitor::new(self.processor.ontology, self.processor.level)
                        .map_err(RecursionLimitError::to_de_error)?
                        .deserialize(serde_value::ValueDeserializer::new(serde_value))?,
                );
            }
        }

        Ok(())
    }

    /// If in dynamic raw entity mode, try to convert to a singleton ID struct
    fn try_convert_to_raw_dynamic_id(&self, mut output: Struct) -> Result<IdOrStruct, String> {
        if output.attributes.len() != 1 {
            return Ok(IdOrStruct::Struct(output));
        }

        let (rel_id, _) = output.attributes.iter().next().unwrap();
        let def = self.processor.ontology.def(self.type_def_id);

        let DefKind::Entity(entity) = &def.kind else {
            return Ok(IdOrStruct::Struct(output));
        };
        if *rel_id != entity.id_relationship_id {
            return Ok(IdOrStruct::Struct(output));
        }

        if self.flags.contains(SerdeStructFlags::PROPER_ENTITY) && !self.level.is_global_root() {
            let mut items = vec![];
            if self.try_report_missing_edge(&output, &mut items) {
                return Err(format_missing_attrs_error(items));
            }
        } else if !self.dynamic_id_unchecked {
            return Ok(IdOrStruct::Struct(output));
        }

        let attributes = std::mem::take(&mut output.attributes);
        output.id = match attributes.into_iter().next().unwrap().1 {
            Attr::Unit(id) => Some(id),
            _ => None,
        };
        output.resolved_to_id = true;
        debug!("  made a dynamic entity id: {:?}", output.id);

        Ok(IdOrStruct::Id(output))
    }

    /// Generate default values if missing
    fn generate_missing_attributes(&self, output: &mut Struct) -> Result<(), std::string::String> {
        if output
            .observed_props_bitset
            .is_superset(&self.required_props_bitset)
        {
            // early return: nothing needs to be generated
            return Ok(());
        }

        for (prop_idx, (_, property)) in self.properties.raw_map().iter().enumerate() {
            // Only _default values_ are handled in the deserializer:
            if let Some(ValueGenerator::DefaultProc(address)) = property.value_generator {
                if !property.is_optional_for(self.processor.mode, &self.processor.profile.flags)
                    && !output.attributes.contains_key(&property.rel_id)
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
                    output.attributes.insert(property.rel_id, value.into());
                    output.observed_props_bitset.insert(prop_idx);
                }
            }
        }

        Ok(())
    }

    fn report_missing_attributes(&self, output: &Struct) -> Result<(), std::string::String> {
        if output
            .observed_props_bitset
            .is_superset(&self.required_props_bitset)
            && (output.rel_params.is_unit() == self.rel_params_addr.is_none())
        {
            // nothing is missing
            return Ok(());
        }

        debug!(
            "Missing attributes(mode={:?}). Rel params match: {}, special_rel: {}, parent_relationship: {:?}, observed/expected: {:?}/{:?}",
            self.processor.mode.debug(&()),
            output.rel_params.is_unit(),
            self.rel_params_addr.is_none(),
            self.processor.ctx.parent_property_id,
            output.observed_props_bitset,
            self.required_props_bitset
        );
        for attr in &output.attributes {
            debug!("    attr {:?}", attr.0);
        }

        for (key, prop) in self.properties.raw_map().iter() {
            debug!(
                "    prop {:?}('{}') {:?} visible={} optional={}",
                prop.rel_id,
                key.arc_str(),
                prop.flags,
                prop.filter(
                    self.processor.mode,
                    self.processor.ctx.parent_property_id,
                    self.processor.profile.flags
                )
                .is_some(),
                prop.is_optional()
            );
        }

        let mut items = self
            .properties
            .iter()
            .filter(|(_, property)| {
                !output.attributes.contains_key(&property.rel_id)
                    && property
                        .filter(
                            self.processor.mode,
                            self.processor.ctx.parent_property_id,
                            self.processor.profile.flags,
                        )
                        .is_some()
                    && !property.is_optional_for(self.processor.mode, &self.processor.profile.flags)
            })
            .map(|(key, _)| DoubleQuote(key.arc_str().as_str().into()))
            .collect();

        self.try_report_missing_edge(output, &mut items);

        debug!("    items len: {}", items.len());

        Err(format_missing_attrs_error(items))
    }

    fn try_report_missing_edge(
        &self,
        output: &Struct,
        items: &mut Vec<DoubleQuote<String>>,
    ) -> bool {
        if self.rel_params_addr.is_some()
            && output.rel_params.type_def_id() == DefId::unit()
            && !matches!(self.processor.mode, ProcessorMode::Delete)
        {
            items.push(DoubleQuote(
                self.processor
                    .ontology
                    .ontol_domain_meta()
                    .edge_property
                    .clone(),
            ));
            true
        } else {
            false
        }
    }

    fn finalize_output(&self, output: &mut Struct) {
        if !output.open_dict.is_empty() {
            let open_dict = std::mem::take(&mut output.open_dict);

            output.attributes.insert(
                self.processor
                    .ontology
                    .ontol_domain_meta()
                    .open_data_rel_id(),
                Value::Dict(Box::new(open_dict), ValueTag::unit()).into(),
            );
        }
    }
}

fn format_missing_attrs_error(items: Vec<DoubleQuote<String>>) -> String {
    let missing_keys = LogicalConcat {
        items,
        logic_op: LogicOp::And,
    };

    format!("missing properties, expected {missing_keys}")
}

enum IdOrStruct {
    Id(Struct),
    Struct(Struct),
}
