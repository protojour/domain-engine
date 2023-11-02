use std::fmt::Display;

use juniper::{graphql_value, parser::SourcePosition, LookAheadArgument, LookAheadValue, Spanning};
use ontol_runtime::{
    interface::{
        graphql::argument::{self, DefaultArg},
        serde::processor::ProcessorProfile,
    },
    smart_format,
};
use serde::{
    de::{self, DeserializeSeed, IntoDeserializer},
    Deserialize,
};

use crate::{
    context::{SchemaCtx, ServiceCtx},
    gql_scalar::GqlScalar,
};

pub(crate) struct ArgsWrapper<'a> {
    arguments: &'a [LookAheadArgument<'a, GqlScalar>],
}

impl<'a> ArgsWrapper<'a> {
    pub fn new(arguments: &'a [LookAheadArgument<'a, GqlScalar>]) -> Self {
        Self { arguments }
    }

    pub fn deserialize_optional<'de, T: serde::Deserialize<'de>>(
        &self,
        name: &str,
    ) -> Result<Option<T>, juniper::FieldError<GqlScalar>> {
        match self.find_argument(name) {
            None => Ok(None),
            Some(argument) => {
                let value = Option::<T>::deserialize(LookAheadValueDeserializer {
                    value: argument.spanned_value(),
                })
                .map_err(|error| {
                    juniper::FieldError::new(
                        smart_format!("`{name}`: {error}"),
                        graphql_value!(None),
                    )
                })?;

                Ok(value)
            }
        }
    }

    pub fn deserialize_domain_field_arg_as_attribute(
        &self,
        field_arg: &dyn argument::DomainFieldArg,
        (schema_ctx, service_ctx): (&SchemaCtx, &ServiceCtx),
    ) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
        let arg_name = field_arg.name();
        let (mode, level) = field_arg.typing_purpose().mode_and_level();

        let processor_profile =
            ProcessorProfile::default().with_flags(service_ctx.serde_processor_profile_flags);
        let serde_processor = schema_ctx
            .ontology
            .new_serde_processor(field_arg.operator_addr(), mode)
            .with_level(level)
            .with_profile(&processor_profile);

        let result = match self.find_argument(arg_name) {
            Some(argument) => serde_processor.deserialize(LookAheadValueDeserializer {
                value: argument.spanned_value(),
            }),
            None => serde_processor.deserialize(LookAheadValueDeserializer {
                value: &Spanning {
                    item: match field_arg.default_arg() {
                        Some(DefaultArg::EmptyObject) => LookAheadValue::Object(vec![]),
                        None => {
                            return Err(juniper::FieldError::new(
                                smart_format!("argument `{arg_name}` is missing"),
                                graphql_value!(None),
                            ))
                        }
                    },
                    start: SourcePosition::new_origin(),
                    end: SourcePosition::new_origin(),
                },
            }),
        };

        result.map_err(|error| juniper::FieldError::new(error, graphql_value!(None)))
    }

    fn find_argument(&self, name: &str) -> Option<&'a LookAheadArgument<'a, GqlScalar>> {
        self.arguments.iter().find(|arg| arg.name() == name)
    }
}

#[derive(Debug)]
struct Error {
    msg: smartstring::alias::String,
    start: Option<juniper::parser::SourcePosition>,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = &self.msg;
        if let Some(start) = &self.start {
            write!(
                f,
                "{msg} in input at line {} column {}",
                start.line(),
                start.column()
            )
        } else {
            write!(f, "{msg}")
        }
    }
}

impl de::StdError for Error {}

impl de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self {
            msg: smart_format!("{}", msg),
            start: None,
        }
    }
}

trait ErrorContext {
    fn context(self, pos: &SourcePosition) -> Self;
}

impl<T> ErrorContext for Result<T, Error> {
    fn context(self, pos: &SourcePosition) -> Self {
        self.map_err(|mut error| {
            if error.start.is_none() {
                error.start = Some(*pos);
            }
            error
        })
    }
}

struct LookAheadValueDeserializer<'a> {
    value: &'a Spanning<LookAheadValue<'a, GqlScalar>>,
}

impl<'a, 'de> de::Deserializer<'de> for LookAheadValueDeserializer<'a> {
    type Error = Error;

    fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match &self.value.item {
            LookAheadValue::Null => visitor.visit_none::<Error>().context(&self.value.start),
            LookAheadValue::Scalar(GqlScalar::I32(value)) => {
                visitor.visit_i32(*value).context(&self.value.start)
            }
            LookAheadValue::Scalar(GqlScalar::I64(value)) => {
                visitor.visit_i64(*value).context(&self.value.start)
            }
            LookAheadValue::Scalar(GqlScalar::F64(value)) => {
                visitor.visit_f64(*value).context(&self.value.start)
            }
            LookAheadValue::Scalar(GqlScalar::Boolean(value)) => {
                visitor.visit_bool(*value).context(&self.value.start)
            }
            LookAheadValue::Scalar(GqlScalar::String(value)) => {
                visitor.visit_str(value).context(&self.value.start)
            }
            LookAheadValue::Enum(value) => visitor.visit_str(value),
            LookAheadValue::List(vec) => {
                let mut iterator = vec.iter().fuse();
                let value = visitor
                    .visit_seq(SeqDeserializer::<_>::new(&mut iterator))
                    .context(&self.value.start)?;
                match iterator.next() {
                    Some(item) => Err(Error {
                        msg: "trailing characters".into(),
                        start: Some(item.start),
                    }),
                    None => Ok(value),
                }
            }
            LookAheadValue::Object(vec) => {
                let mut iterator = vec.iter().fuse();
                let value = visitor
                    .visit_map(ObjectDeserializer::<_>::new(&mut iterator))
                    .context(&self.value.start)?;
                match iterator.next() {
                    Some((key, _)) => Err(Error {
                        msg: "trailing characters".into(),
                        start: Some(key.start),
                    }),
                    None => Ok(value),
                }
            }
        }
    }

    fn deserialize_option<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match &self.value.item {
            LookAheadValue::Null => visitor.visit_none::<Error>().context(&self.value.start),
            _ => {
                let pos = self.value.start;
                visitor.visit_some(self).context(&pos)
            }
        }
    }

    serde::forward_to_deserialize_any!(
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit
        seq bytes byte_buf map unit_struct
        tuple_struct struct tuple ignored_any identifier newtype_struct enum
    );
}

struct SeqDeserializer<'i, I> {
    iter: &'i mut std::iter::Fuse<I>,
    count: usize,
}

impl<'i, I: Iterator> SeqDeserializer<'i, I> {
    fn new(iter: &'i mut std::iter::Fuse<I>) -> Self {
        Self { iter, count: 0 }
    }
}

impl<'a, 'i, 'de, I> de::SeqAccess<'de> for SeqDeserializer<'i, I>
where
    I: Iterator<Item = &'a Spanning<LookAheadValue<'a, GqlScalar>>>,
{
    type Error = Error;

    fn next_element_seed<V>(&mut self, seed: V) -> Result<Option<V::Value>, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some(value) => {
                self.count += 1;
                seed.deserialize(LookAheadValueDeserializer { value })
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        match self.iter.size_hint() {
            (lower, Some(upper)) if lower == upper => Some(upper),
            _ => None,
        }
    }
}

struct ObjectDeserializer<'a, 'i, I> {
    iter: &'i mut std::iter::Fuse<I>,
    state: MapState<&'a Spanning<LookAheadValue<'a, GqlScalar>>>,
    _count: usize,
}

impl<'a, 'i, I: Iterator> ObjectDeserializer<'a, 'i, I> {
    fn new(iter: &'i mut std::iter::Fuse<I>) -> Self {
        Self {
            iter,
            state: MapState::NextKey,
            _count: 0,
        }
    }
}

impl<'a, 'i, 'de, I> de::MapAccess<'de> for ObjectDeserializer<'a, 'i, I>
where
    I: Iterator<Item = &'a (Spanning<&'a str>, Spanning<LookAheadValue<'a, GqlScalar>>)>,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        match (self.iter.next(), &self.state) {
            (Some((key, value)), MapState::NextKey) => {
                self.state = MapState::NextValue(value);
                seed.deserialize(key.item.into_deserializer()).map(Some)
            }
            (None, MapState::NextKey) => Ok(None),
            (_, MapState::NextValue(_)) => panic!("should call next_value"),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        match std::mem::take(&mut self.state) {
            MapState::NextValue(value) => seed.deserialize(LookAheadValueDeserializer { value }),
            MapState::NextKey => panic!("should call next_key"),
        }
    }
}

#[derive(Default)]
enum MapState<T> {
    #[default]
    NextKey,
    NextValue(T),
}
