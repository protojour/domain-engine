use fnv::FnvHashMap;
use serde::{
    ser::{Error, SerializeMap, SerializeSeq},
    Serializer,
};
use std::{fmt::Write, slice};
use tracing::{trace, warn};

use crate::{
    cast::Cast,
    interface::serde::{
        operator::AppliedVariants,
        processor::{RecursionLimitError, ScalarFormat},
    },
    ontology::ontol::text_pattern::{FormatPattern, TextPatternConstantPart},
    sequence::Sequence,
    value::{Attr, AttrRef, FormatValueAsText, Value},
    DefId, RelationshipId,
};

use super::{
    operator::{SequenceRange, SerdeOperator, SerdePropertyKind, SerdeStructFlags},
    processor::{ProcessorProfileFlags, SerdeProcessor, SpecialProperty, SubProcessorContext},
    serialize_raw::RawProxy,
    StructOperator,
};

type Res<S> = Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>;

const UNIT_ATTR: Attr = Attr::Unit(Value::unit());

impl<'on, 'p> SerdeProcessor<'on, 'p> {
    /// Serialize a value using this processor.
    pub fn serialize_attr<S: Serializer>(&self, attr: AttrRef, serializer: S) -> Res<S> {
        trace!(
            "serializing op={:?}",
            self.ontology.debug(self.value_operator)
        );

        let attr = attr.coerce_to_unit();

        match (self.value_operator, self.scalar_format(), attr) {
            (SerdeOperator::AnyPlaceholder, ..) => {
                warn!("serializatoin of AnyPlaceholder");
                Err(Error::custom("unknown type"))
            }
            (SerdeOperator::Unit, _, AttrRef::Unit(v)) => {
                cast_ref::<()>(v);
                serializer.serialize_unit()
            }
            (SerdeOperator::False(_), ..) => serializer.serialize_bool(false),
            (SerdeOperator::True(_), ..) => serializer.serialize_bool(true),
            (SerdeOperator::Boolean(_), _, AttrRef::Unit(v)) => {
                serializer.serialize_bool(*cast_ref::<bool>(v))
            }
            (SerdeOperator::I64(..), ScalarFormat::DomainTransparent, AttrRef::Unit(v)) => {
                match v {
                    Value::I64(int, _) => serializer.serialize_i64(*int),
                    Value::F64(f, _) => serializer.serialize_i64((*f).round() as i64),
                    other => panic!("BUG: Serialize expected number, got {other:?}"),
                }
            }
            (SerdeOperator::I32(..), ScalarFormat::DomainTransparent, AttrRef::Unit(v)) => {
                let int_i64: i64 = match v {
                    Value::I64(int, _) => *int,
                    Value::F64(f, _) => (*f).round() as i64,
                    other => panic!("BUG: Serialize expected number, got {other:?}"),
                };

                serializer.serialize_i32(int_i64.try_into().map_err(|err| {
                    S::Error::custom(format!("overflow when converting to i32: {err:?}"))
                })?)
            }
            (SerdeOperator::F64(..), ScalarFormat::DomainTransparent, AttrRef::Unit(v)) => {
                match v {
                    Value::I64(num, _) => serializer.serialize_f64(*num as f64),
                    Value::F64(f, _) => serializer.serialize_f64(*f),
                    other => panic!("BUG: Serialize expected number, got {other:?}"),
                }
            }
            (SerdeOperator::Serial(def_id), ScalarFormat::DomainTransparent, AttrRef::Unit(v)) => {
                self.serialize_as_text_formatted(v, *def_id, serializer)
            }
            (
                SerdeOperator::I32(def_id, ..)
                | SerdeOperator::I64(def_id, ..)
                | SerdeOperator::F64(def_id, ..)
                | SerdeOperator::Serial(def_id, ..),
                ScalarFormat::RawText,
                AttrRef::Unit(v),
            ) => self.serialize_as_text_formatted(v, *def_id, serializer),
            (
                SerdeOperator::String(def_id)
                | SerdeOperator::StringConstant(_, def_id)
                | SerdeOperator::TextPattern(def_id),
                _,
                AttrRef::Unit(v),
            ) => match v {
                Value::Text(s, _) => serializer.serialize_str(s),
                _ => self.serialize_as_text_formatted(v, *def_id, serializer),
            },
            (
                SerdeOperator::CapturingTextPattern(pattern_def_id),
                ScalarFormat::DomainTransparent,
                AttrRef::Unit(v),
            ) => self.serialize_as_pattern_formatted(v, *pattern_def_id, serializer),
            (
                SerdeOperator::CapturingTextPattern(pattern_def_id),
                ScalarFormat::RawText,
                AttrRef::Unit(v),
            ) => self.serialize_pattern_as_raw_text(v, *pattern_def_id, serializer),
            (SerdeOperator::DynamicSequence, _, AttrRef::Unit(v)) => match v {
                Value::Sequence(seq, _) => {
                    self.serialize_dynamic_sequence(&seq.elements, serializer)
                }
                _ => panic!("Not a sequence"),
            },
            (
                SerdeOperator::RelationList(seq_op) | SerdeOperator::RelationIndexSet(seq_op),
                _,
                AttrRef::Matrix(tuple),
            ) => self.serialize_matrix(tuple, slice::from_ref(&seq_op.range), serializer),
            (
                SerdeOperator::ConstructorSequence(seq_op),
                _,
                AttrRef::Unit(Value::Sequence(seq, _)),
            ) => self.serialize_sequence(
                seq.elements.iter().map(|value| AttrRef::Unit(value)),
                seq.elements.len(),
                &seq_op.ranges,
                serializer,
            ),
            (SerdeOperator::Alias(value_op), _, _) => self
                .narrow(value_op.inner_addr)
                .serialize_attr(attr, serializer),
            (SerdeOperator::Union(union_op), _, _) => {
                let val = attr.first_unit().expect("matrix union");

                match union_op.applied_variants(self.mode, self.level) {
                    AppliedVariants::Unambiguous(addr) => {
                        self.narrow(addr).serialize_attr(attr, serializer)
                    }
                    AppliedVariants::OneOf(possible_variants) => {
                        let variant = possible_variants
                            .into_iter()
                            .find(|variant| val.type_def_id() == variant.serde_def.def_id);

                        if let Some(variant) = variant {
                            let processor = self.narrow(variant.addr);
                            trace!(
                                "serializing union variant with {:?} {processor:}",
                                variant.addr
                            );

                            processor.serialize_attr(attr, serializer)
                        } else {
                            panic!(
                                "Discriminator not found while serializing union type {:?}: {:#?}",
                                val.type_def_id(),
                                possible_variants.into_iter().collect::<Vec<_>>()
                            );
                        }
                    }
                }
            }
            (
                SerdeOperator::IdSingletonStruct(_, name_constant, inner_addr),
                _,
                AttrRef::Unit(_),
            ) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(
                    &self.ontology[*name_constant],
                    &Proxy {
                        attr,
                        processor: self
                            .new_child(*inner_addr)
                            .map_err(RecursionLimitError::to_ser_error)?,
                    },
                )?;

                map.end()
            }
            (
                SerdeOperator::IdSingletonStruct(_, name_constant, inner_addr),
                _,
                AttrRef::Tuple(t),
            ) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(
                    &self.ontology[*name_constant],
                    &Proxy {
                        attr: AttrRef::Unit(&t[0]),
                        processor: self
                            .new_child(*inner_addr)
                            .map_err(RecursionLimitError::to_ser_error)?,
                    },
                )?;
                self.serialize_rel_params::<S>(
                    &self.ontology.ontol_domain_meta().edge_property,
                    t.get(1),
                    &mut map,
                )?;

                map.end()
            }
            (SerdeOperator::Struct(struct_op), _, _) => {
                self.serialize_struct(struct_op, attr, serializer)
            }
            (_, _, attr) => panic!("unknown serializer combination: {self:?} attr: {attr:?}"),
        }
    }

    fn serialize_as_text_formatted<S: Serializer>(
        &self,
        value: &Value,
        operator_def_id: DefId,
        serializer: S,
    ) -> Res<S> {
        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            FormatValueAsText {
                value,
                type_def_id: operator_def_id,
                ontology: self.ontology
            }
        )
        .map_err(|_| S::Error::custom("Conversion to text failed"))?;

        serializer.serialize_str(&buf)
    }

    fn serialize_as_pattern_formatted<S: Serializer>(
        &self,
        value: &Value,
        pattern_def_id: DefId,
        serializer: S,
    ) -> Res<S> {
        let pattern = &self
            .ontology
            .data
            .text_patterns
            .get(&pattern_def_id)
            .unwrap();
        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            FormatPattern {
                pattern,
                value,
                ontology: self.ontology
            }
        )
        .map_err(|_| S::Error::custom("Failed to serialize capturing text pattern"))?;

        serializer.serialize_str(&buf)
    }

    fn serialize_pattern_as_raw_text<S: Serializer>(
        &self,
        value: &Value,
        pattern_def_id: DefId,
        serializer: S,
    ) -> Res<S> {
        match value {
            Value::Struct(attrs, _) => {
                let pattern = &self
                    .ontology
                    .data
                    .text_patterns
                    .get(&pattern_def_id)
                    .unwrap();
                let mut pattern_property = None;

                for part in &pattern.constant_parts {
                    if let TextPatternConstantPart::Property(property) = part {
                        if pattern_property.is_some() {
                            return self.serialize_as_pattern_formatted(
                                value,
                                pattern_def_id,
                                serializer,
                            );
                        }

                        pattern_property = Some(property);
                    }
                }

                match pattern_property {
                    Some(pattern_property) => {
                        let attr = attrs.get(&pattern_property.rel_id).ok_or_else(|| {
                            S::Error::custom("property not present in pattern struct")
                        })?;
                        let Attr::Unit(value) = attr else { panic!() };

                        self.serialize_as_text_formatted(
                            value,
                            pattern_property.type_def_id,
                            serializer,
                        )
                    }
                    None => self.serialize_as_text_formatted(value, pattern_def_id, serializer),
                }
            }
            _ => self.serialize_as_text_formatted(value, pattern_def_id, serializer),
        }
    }

    fn serialize_sequence<'v, S: Serializer>(
        &self,
        mut elements: impl Iterator<Item = AttrRef<'v>>,
        len: usize,
        ranges: &[SequenceRange],
        serializer: S,
    ) -> Res<S> {
        let mut seq = serializer.serialize_seq(Some(len))?;

        for range in ranges {
            if let Some(finite_repetition) = range.finite_repetition {
                for _ in 0..finite_repetition {
                    let attr = elements.next().unwrap();

                    seq.serialize_element(&Proxy {
                        attr,
                        processor: self.narrow(range.addr),
                    })?;
                }
            } else {
                while let Some(attr) = elements.next() {
                    seq.serialize_element(&Proxy {
                        attr,
                        processor: self.narrow(range.addr),
                    })?;
                }
            }
        }

        seq.end()
    }

    fn serialize_matrix<'v, S: Serializer>(
        &self,
        tuple: &[Sequence<Value>],
        ranges: &[SequenceRange],
        serializer: S,
    ) -> Res<S> {
        let mut seq =
            serializer.serialize_seq(tuple.iter().next().map(|seq| seq.elements.len()))?;

        let mut work_tuple: Vec<&Value> = Vec::with_capacity(tuple.len());
        let mut index = 0;

        #[inline]
        fn next_item<'v>(
            tuple: &'v [Sequence<Value>],
            output: &mut Vec<&'v Value>,
            index: &mut usize,
        ) -> Option<()> {
            output.clear();

            for e in tuple {
                if e.elements.len() <= *index {
                    return None;
                }

                output.push(&e.elements[*index]);
            }

            *index += 1;

            Some(())
        }

        for range in ranges {
            if let Some(finite_repetition) = range.finite_repetition {
                for _ in 0..finite_repetition {
                    next_item(tuple, &mut work_tuple, &mut index).unwrap();
                    let attr = AttrRef::RowTuple(&work_tuple).coerce_to_unit();

                    seq.serialize_element(&Proxy {
                        attr,
                        processor: self.narrow(range.addr),
                    })?;
                }
            } else {
                while let Some(()) = next_item(tuple, &mut work_tuple, &mut index) {
                    let attr = AttrRef::RowTuple(&work_tuple).coerce_to_unit();

                    seq.serialize_element(&Proxy {
                        attr,
                        processor: self.narrow(range.addr),
                    })?;
                }
            }
        }

        seq.end()
    }

    fn serialize_dynamic_sequence<S: Serializer>(
        &self,
        elements: &[Value],
        serializer: S,
    ) -> Res<S> {
        let mut seq = serializer.serialize_seq(Some(elements.len()))?;

        for value in elements {
            let def_id = value.type_def_id();
            match self.ontology.get_type_info(def_id).operator_addr {
                Some(addr) => seq.serialize_element(&Proxy {
                    attr: AttrRef::Unit(value),
                    processor: self.narrow(addr),
                })?,
                None => {
                    panic!("No processor found for {def_id:?}");
                }
            }
        }

        seq.end()
    }

    fn serialize_struct<S: Serializer>(
        &self,
        struct_op: &StructOperator,
        attr: AttrRef,
        serializer: S,
    ) -> Res<S> {
        let (value, rel_params) = match attr {
            AttrRef::Unit(value) => (value, None),
            AttrRef::Tuple(elements) => (&elements[0], elements.get(1)),
            AttrRef::RowTuple(elements) => (elements[0], elements.get(1).map(|v| *v)),
            AttrRef::Matrix(..) => {
                panic!("attr matrix")
            }
        };

        let attributes = match value {
            Value::Struct(attributes, _) => attributes,
            Value::StructUpdate(attributes, _) => attributes,
            // Support for empty structs that are Unit encoded:
            Value::Unit(_) => return serializer.serialize_map(Some(0))?.end(),
            other => panic!("BUG: Serialize expected map attributes, got {other:?}"),
        };

        let mut map = serializer.serialize_map(Some(attributes.len() + option_len(&rel_params)))?;

        let overridden_id_property_key = self
            .profile
            .api
            .find_special_property_name(SpecialProperty::IdOverride);

        self.serialize_struct_properties::<S>(
            struct_op,
            value,
            attributes,
            rel_params,
            overridden_id_property_key,
            &mut map,
        )?;

        if struct_op.flags.contains(SerdeStructFlags::OPEN_DATA)
            && self
                .profile
                .flags
                .contains(ProcessorProfileFlags::SERIALIZE_OPEN_DATA)
        {
            if let Some(open_data_attr) =
                attributes.get(&self.ontology.ontol_domain_meta().open_data_rel_id())
            {
                let Some(Value::Dict(dict, _)) = &open_data_attr.as_unit() else {
                    panic!("Open data must be a dict");
                };

                for (key, value) in dict.iter() {
                    if struct_op.properties.contains_key(key) {
                        warn!("Open key `{key}` is shadowed in domain. Ignoring!");
                        continue;
                    }

                    map.serialize_entry(
                        key,
                        &RawProxy::new_as_child(value, self.ontology, self.level)
                            .map_err(RecursionLimitError::to_ser_error)?,
                    )?;
                }
            }
        }

        map.end()
    }

    fn serialize_struct_properties<S: Serializer>(
        &self,
        struct_op: &StructOperator,
        value: &Value,
        attrs: &FnvHashMap<RelationshipId, Attr>,
        rel_params: Option<&Value>,
        overridden_id_property_key: Option<&str>,
        map: &mut S::SerializeMap,
    ) -> Result<(), S::Error> {
        for (phf_key, serde_prop) in
            struct_op.filter_properties(self.mode, self.ctx.parent_property_id, self.profile.flags)
        {
            if serde_prop.is_rel_params() {
                self.serialize_rel_params::<S>(phf_key.arc_str(), rel_params, map)?;
            } else {
                let unit_attr = UNIT_ATTR;
                let attr = match attrs.get(&serde_prop.rel_id) {
                    Some(attr) => attr,
                    None => {
                        if serde_prop.is_optional_for(self.mode, &self.profile.flags) {
                            continue;
                        } else {
                            match &self.ontology[serde_prop.value_addr] {
                                SerdeOperator::Struct(struct_op) => {
                                    if struct_op.is_struct_properties_empty() {
                                        &unit_attr
                                    } else {
                                        panic!(
                                            "While serializing value {:?} with `{}`, the expected value was a non-empty struct, but found unit",
                                            value, &self.ontology[struct_op.typename]
                                        )
                                    }
                                }
                                _ => {
                                    panic!(
                                        "While serializing value {:?} with `{}`, property `{}` was not found.",
                                        value, &self.ontology[struct_op.typename], phf_key.arc_str()
                                    )
                                }
                            }
                        }
                    }
                };

                match &serde_prop.kind {
                    SerdePropertyKind::Plain { rel_params_addr } => {
                        let is_entity_id = serde_prop.is_entity_id();

                        let name = match (is_entity_id, overridden_id_property_key) {
                            (true, Some(id_key)) => id_key,
                            _ => phf_key.arc_str().as_str(),
                        };

                        map.serialize_entry(
                            name,
                            &Proxy {
                                attr: attr.as_ref(),
                                processor: self
                                    .new_child_with_context(
                                        serde_prop.value_addr,
                                        SubProcessorContext {
                                            is_update: false,
                                            parent_property_id: Some(serde_prop.rel_id),
                                            parent_property_flags: serde_prop.flags,
                                            rel_params_addr: *rel_params_addr,
                                        },
                                    )
                                    .map_err(RecursionLimitError::to_ser_error)?,
                            },
                        )?;
                    }
                    SerdePropertyKind::FlatUnionDiscriminator { union_addr } => {
                        let SerdeOperator::Union(union_op) = &self.ontology[*union_addr] else {
                            panic!("expected union operator");
                        };
                        let value = attr.as_unit().unwrap();
                        let addr = match union_op.applied_variants(self.mode, self.level) {
                            AppliedVariants::Unambiguous(addr) => addr,
                            AppliedVariants::OneOf(possible_variants) => {
                                possible_variants
                                    .into_iter()
                                    .find(|variant| value.type_def_id() == variant.serde_def.def_id)
                                    .ok_or(serde::ser::Error::custom(
                                        "flattened union variant not found",
                                    ))?
                                    .addr
                            }
                        };

                        let flattened_attrs = match value {
                            Value::Struct(attrs, _) | Value::StructUpdate(attrs, _) => attrs,
                            _ => {
                                return Err(serde::ser::Error::custom(
                                    "flattened union variant not found",
                                ))
                            }
                        };

                        match &self.ontology[addr] {
                            SerdeOperator::Struct(struct_op) => {
                                self.serialize_struct_properties::<S>(
                                    struct_op,
                                    value,
                                    flattened_attrs,
                                    None,
                                    None,
                                    map,
                                )?;
                            }
                            _ => {
                                return Err(serde::ser::Error::custom(
                                    "invalid flattened sub-operator",
                                ))
                            }
                        }
                    }
                    SerdePropertyKind::FlatUnionData => {}
                }
            }
        }

        Ok(())
    }

    fn serialize_rel_params<S: Serializer>(
        &self,
        property_name: &str,
        rel_params: Option<&Value>,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), <S as Serializer>::Error> {
        match (rel_params, self.ctx.rel_params_addr) {
            (None, None) => {}
            (Some(rel_params), Some(addr)) => {
                map.serialize_entry(
                    property_name,
                    &Proxy {
                        attr: AttrRef::Unit(rel_params),
                        processor: self
                            .new_child(addr)
                            .map_err(RecursionLimitError::to_ser_error)?,
                    },
                )?;
            }
            (None, Some(_)) => {
                panic!("Must serialize edge params, but was not present in attribute")
            }
            (Some(rel_params), None) => {
                panic!("Attribute had rel params {rel_params:#?}, but no serializer operator available: {self:#?}")
            }
        }

        Ok(())
    }
}

fn option_len<T>(opt: &Option<T>) -> usize {
    match opt {
        Some(_) => 1,
        None => 0,
    }
}

struct Proxy<'v, 'on, 'p> {
    attr: AttrRef<'v>,
    processor: SerdeProcessor<'on, 'p>,
}

impl<'v, 'on, 'p> serde::Serialize for Proxy<'v, 'on, 'p> {
    fn serialize<S>(&self, serializer: S) -> Res<S>
    where
        S: serde::Serializer,
    {
        self.processor.serialize_attr(self.attr, serializer)
    }
}

fn cast_ref<T>(value: &Value) -> &<Value as Cast<T>>::Ref
where
    Value: Cast<T>,
{
    value.cast_ref()
}
