use std::fmt::Display;

use juniper::{graphql_value, LookAheadArgument, LookAheadValue, Spanning};
use ontol_runtime::{
    interface::graphql::{argument::DomainFieldArg, schema::TypingPurpose},
    interface::serde::{operator::SerdeOperatorAddr, processor::DOMAIN_PROFILE},
    ontology::Ontology,
    smart_format,
    value::Attribute,
};
use serde::de::{self, DeserializeSeed, IntoDeserializer};

use crate::gql_scalar::GqlScalar;

pub struct ArgsWrapper<'a> {
    arguments: &'a [LookAheadArgument<'a, GqlScalar>],
}

impl<'a> ArgsWrapper<'a> {
    pub fn new(arguments: &'a [LookAheadArgument<'a, GqlScalar>]) -> Self {
        Self { arguments }
    }

    pub fn domain_deserialize(
        &self,
        operator_addr: SerdeOperatorAddr,
        scalar_input_name: Option<&str>,
        typing_purpose: TypingPurpose,
        ontology: &Ontology,
    ) -> Result<Attribute, juniper::FieldError<GqlScalar>> {
        let (mode, level) = typing_purpose.mode_and_level();

        if let Some(_scalar_input_name) = scalar_input_name {
            let spanned_arg = self
                .arguments
                .first()
                .ok_or_else(|| {
                    juniper::FieldError::new("Expected single argument", graphql_value!(None))
                })?
                .spanned_value();

            ontology
                .new_serde_processor(operator_addr, mode, level, &DOMAIN_PROFILE)
                .deserialize(LookAheadValueDeserializer { value: spanned_arg })
                .map_err(|error| juniper::FieldError::new(error, graphql_value!(None)))
        } else {
            ontology
                .new_serde_processor(operator_addr, mode, level, &DOMAIN_PROFILE)
                .deserialize(LookAheadArgumentsDeserializer {
                    arguments: self.arguments,
                })
                .map_err(|error| juniper::FieldError::new(error, graphql_value!(None)))
        }
    }

    pub fn deserialize<'de, T: serde::Deserialize<'de>>(
        &self,
        name: &str,
    ) -> Result<Option<T>, juniper::FieldError<GqlScalar>> {
        match self.find_argument(name) {
            None => Ok(None),
            Some(argument) => {
                let value = T::deserialize(LookAheadValueDeserializer {
                    value: argument.spanned_value(),
                })
                .map_err(|error| juniper::FieldError::new(error, graphql_value!(None)))?;

                Ok(Some(value))
            }
        }
    }

    pub fn deserialize_domain_field_arg_as_attribute(
        &self,
        field_arg: &dyn DomainFieldArg,
        ontology: &Ontology,
    ) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
        let name = field_arg.name();
        let operator_addr = field_arg.operator_addr();
        self.deserialize_operator_attribute(
            name,
            operator_addr,
            ontology,
            field_arg.typing_purpose(),
        )
    }

    pub fn deserialize_operator_attribute(
        &self,
        name: &str,
        operator_addr: SerdeOperatorAddr,
        ontology: &Ontology,
        typing_purpose: TypingPurpose,
    ) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
        let argument = self.find_argument(name).unwrap();

        // debug!("deserializing {value:?}");

        let (mode, level) = typing_purpose.mode_and_level();

        ontology
            .new_serde_processor(operator_addr, mode, level, &DOMAIN_PROFILE)
            .deserialize(LookAheadValueDeserializer {
                value: argument.spanned_value(),
            })
            .map_err(|error| juniper::FieldError::new(error, graphql_value!(None)))
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
    fn context<'a>(self, ctx_value: &'a Spanning<LookAheadValue<'a, GqlScalar>>) -> Self;
}

impl<T> ErrorContext for Result<T, Error> {
    fn context<'a>(self, ctx_value: &'a Spanning<LookAheadValue<'a, GqlScalar>>) -> Self {
        self.map_err(|mut error| {
            if error.start.is_none() {
                error.start = Some(ctx_value.start);
            }
            error
        })
    }
}

struct LookAheadArgumentsDeserializer<'a> {
    arguments: &'a [LookAheadArgument<'a, GqlScalar>],
}

impl<'a, 'de> de::Deserializer<'de> for LookAheadArgumentsDeserializer<'a> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        let mut iterator = self.arguments.iter().fuse();
        let value = visitor.visit_map(ArgsDeserializer::<_>::new(&mut iterator))?;
        match iterator.next() {
            Some(arg) => Err(Error {
                msg: "trailing characters".into(),
                start: Some(arg.spanned_value().start),
            }),
            None => Ok(value),
        }
    }

    serde::forward_to_deserialize_any!(
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit
        seq bytes byte_buf map unit_struct
        tuple_struct struct tuple ignored_any identifier option newtype_struct enum
    );
}

struct LookAheadValueDeserializer<'a> {
    value: &'a Spanning<LookAheadValue<'a, GqlScalar>>,
}

impl<'a, 'de> de::Deserializer<'de> for LookAheadValueDeserializer<'a> {
    type Error = Error;

    fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match &self.value.item {
            LookAheadValue::Null => visitor.visit_none::<Error>().context(self.value),
            LookAheadValue::Scalar(GqlScalar::Unit) => visitor.visit_unit().context(self.value),
            LookAheadValue::Scalar(GqlScalar::I32(value)) => {
                visitor.visit_i32(*value).context(self.value)
            }
            LookAheadValue::Scalar(GqlScalar::I64(value)) => {
                visitor.visit_i64(*value).context(self.value)
            }
            LookAheadValue::Scalar(GqlScalar::F64(value)) => {
                visitor.visit_f64(*value).context(self.value)
            }
            LookAheadValue::Scalar(GqlScalar::Boolean(value)) => {
                visitor.visit_bool(*value).context(self.value)
            }
            LookAheadValue::Scalar(GqlScalar::String(value)) => {
                visitor.visit_str(value).context(self.value)
            }
            LookAheadValue::Enum(value) => visitor.visit_str(value),
            LookAheadValue::List(vec) => {
                let mut iterator = vec.iter().fuse();
                let value = visitor
                    .visit_seq(SeqDeserializer::<_>::new(&mut iterator))
                    .context(self.value)?;
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
                    .context(self.value)?;
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

    serde::forward_to_deserialize_any!(
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit
        seq bytes byte_buf map unit_struct
        tuple_struct struct tuple ignored_any identifier option newtype_struct enum
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

struct ArgsDeserializer<'a, 'i, I> {
    iter: &'i mut std::iter::Fuse<I>,
    state: MapState<&'a Spanning<LookAheadValue<'a, GqlScalar>>>,
    _count: usize,
}

impl<'a, 'i, I: Iterator> ArgsDeserializer<'a, 'i, I> {
    fn new(iter: &'i mut std::iter::Fuse<I>) -> Self {
        Self {
            iter,
            state: MapState::NextKey,
            _count: 0,
        }
    }
}

impl<'a, 'i, 'de, I> de::MapAccess<'de> for ArgsDeserializer<'a, 'i, I>
where
    I: Iterator<Item = &'a LookAheadArgument<'a, GqlScalar>>,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        match (self.iter.next(), &self.state) {
            (Some(arg), MapState::NextKey) => {
                self.state = MapState::NextValue(arg.spanned_value());
                seed.deserialize(arg.name().into_deserializer()).map(Some)
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
