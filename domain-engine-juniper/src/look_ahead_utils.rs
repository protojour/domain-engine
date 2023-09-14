use std::fmt::Display;

use juniper::{graphql_value, LookAheadArgument, LookAheadValue, Spanning};
use ontol_runtime::{
    graphql::{
        argument::{ArgKind, DomainFieldArg},
        TypingPurpose,
    },
    ontology::Ontology,
    serde::operator::SerdeOperatorId,
    smart_format, DefId,
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

    pub fn deserialize_attribute(
        &self,
        field_arg: &dyn DomainFieldArg,
        ontology: &Ontology,
    ) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
        let name = field_arg.name();
        match field_arg.kind() {
            ArgKind::Def(_, def_id) => {
                self.deserialize_def_attribute(name, def_id, ontology, field_arg.typing_purpose())
            }
            ArgKind::Operator(operator_id) => self.deserialize_operator_attribute(
                name,
                operator_id,
                ontology,
                field_arg.typing_purpose(),
            ),
        }
    }

    /// Deserialize some named juniper input argument using the given SerdeOperatorId.
    pub fn deserialize_def_attribute(
        &self,
        name: &str,
        def_id: DefId,
        ontology: &Ontology,
        typing_purpose: TypingPurpose,
    ) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
        let type_info = ontology.get_type_info(def_id);
        self.deserialize_operator_attribute(
            name,
            type_info
                .operator_id
                .expect("This type cannot be deserialized"),
            ontology,
            typing_purpose,
        )
    }

    pub fn deserialize_operator_attribute(
        &self,
        name: &str,
        operator_id: SerdeOperatorId,
        ontology: &Ontology,
        typing_purpose: TypingPurpose,
    ) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
        let argument = self.find_argument(name).unwrap();

        // debug!("deserializing {value:?}");

        let (mode, level) = typing_purpose.mode_and_level();

        ontology
            .new_serde_processor(operator_id, mode, level)
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
    start: juniper::parser::SourcePosition,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} in input at line {} column {}",
            self.msg,
            self.start.line(),
            self.start.column()
        )
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
            start: juniper::parser::SourcePosition::new_origin(),
        }
    }
}

struct LookAheadValueDeserializer<'a> {
    value: &'a Spanning<LookAheadValue<'a, GqlScalar>>,
}

impl<'a, 'de> de::Deserializer<'de> for LookAheadValueDeserializer<'a> {
    type Error = Error;

    fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let result = match &self.value.item {
            LookAheadValue::Null => visitor.visit_none::<Error>(),
            LookAheadValue::Scalar(GqlScalar::Unit) => visitor.visit_unit(),
            LookAheadValue::Scalar(GqlScalar::I32(value)) => visitor.visit_i32(*value),
            LookAheadValue::Scalar(GqlScalar::F64(value)) => visitor.visit_f64(*value),
            LookAheadValue::Scalar(GqlScalar::Boolean(value)) => visitor.visit_bool(*value),
            LookAheadValue::Scalar(GqlScalar::String(value)) => visitor.visit_str(value),
            LookAheadValue::Enum(value) => visitor.visit_str(value),
            LookAheadValue::List(vec) => {
                let mut iterator = vec.iter().fuse();
                let value = visitor.visit_seq(SeqDeserializer::<_>::new(&mut iterator))?;
                match iterator.next() {
                    Some(item) => Err(Error {
                        msg: "trailing characters".into(),
                        start: item.start,
                    }),
                    None => Ok(value),
                }
            }
            LookAheadValue::Object(vec) => {
                let mut iterator = vec.iter().fuse();
                let value = visitor.visit_map(MapDeserializer::<_>::new(&mut iterator))?;
                match iterator.next() {
                    Some((key, _)) => Err(Error {
                        msg: "trailing characters".into(),
                        start: key.start,
                    }),
                    None => Ok(value),
                }
            }
        };

        match result {
            Ok(value) => Ok(value),
            Err(mut error) => {
                if error.start.line() == 0 {
                    error.start = self.value.start;
                }

                Err(error)
            }
        }
    }

    fn deserialize_option<V: de::Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V: de::Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        unimplemented!()
    }

    fn deserialize_enum<V: de::Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        unimplemented!()
    }

    serde::forward_to_deserialize_any!(
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit
        seq bytes byte_buf map unit_struct
        tuple_struct struct tuple ignored_any identifier
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

struct MapDeserializer<'a, 'i, I> {
    iter: &'i mut std::iter::Fuse<I>,
    state: MapState<'a>,
    _count: usize,
}

#[derive(Default)]
enum MapState<'a> {
    #[default]
    NextKey,
    NextValue(&'a Spanning<LookAheadValue<'a, GqlScalar>>),
}

impl<'a, 'i, I: Iterator> MapDeserializer<'a, 'i, I> {
    fn new(iter: &'i mut std::iter::Fuse<I>) -> Self {
        Self {
            iter,
            state: MapState::NextKey,
            _count: 0,
        }
    }
}

impl<'a, 'i, 'de, I> de::MapAccess<'de> for MapDeserializer<'a, 'i, I>
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
