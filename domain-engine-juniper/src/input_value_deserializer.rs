use std::fmt::Display;

use juniper::{graphql_value, InputValue, Spanning};
use ontol_runtime::{
    env::Env,
    serde::{
        operator::SerdeOperatorId,
        processor::{ProcessorLevel, ProcessorMode},
    },
    smart_format, DefId,
};
use serde::de::{self, DeserializeSeed, IntoDeserializer};
use tracing::debug;

use crate::{gql_scalar::GqlScalar, virtual_schema::data::Argument};

pub fn deserialize_argument(
    argument: &Argument,
    arguments: &juniper::Arguments<GqlScalar>,
    env: &Env,
) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
    let name = argument.name();
    match argument {
        Argument::Input(_, def_id) => deserialize_def_argument(arguments, name, *def_id, env),
        Argument::Id(operator_id) => {
            deserialize_operator_argument(arguments, name, *operator_id, env)
        }
    }
}

/// Deserialize some named juniper input argument using the given SerdeOperatorId.
pub fn deserialize_def_argument(
    arguments: &juniper::Arguments<GqlScalar>,
    name: &str,
    def_id: DefId,
    env: &Env,
) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
    let type_info = env.get_type_info(def_id);
    deserialize_operator_argument(
        arguments,
        name,
        type_info
            .create_operator_id
            .expect("This type cannot be deserialized"),
        env,
    )
}

pub fn deserialize_operator_argument(
    arguments: &juniper::Arguments<GqlScalar>,
    name: &str,
    operator_id: SerdeOperatorId,
    env: &Env,
) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
    let value = arguments.get_input_value(name).unwrap();

    debug!("deserializing {value:?}");

    env.new_serde_processor(
        operator_id,
        None,
        ProcessorMode::Create,
        ProcessorLevel::Root,
    )
    .deserialize(InputValueDeserializer { value })
    .map_err(|error| juniper::FieldError::new(error, graphql_value!(None)))
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

struct InputValueDeserializer<'v> {
    value: &'v Spanning<juniper::InputValue<GqlScalar>>,
}

impl<'v, 'de> de::Deserializer<'de> for InputValueDeserializer<'v> {
    type Error = Error;

    fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let result = match &self.value.item {
            InputValue::Null => visitor.visit_none::<Error>(),
            InputValue::Scalar(GqlScalar::I32(value)) => visitor.visit_i32(*value),
            InputValue::Scalar(GqlScalar::F64(value)) => visitor.visit_f64(*value),
            InputValue::Scalar(GqlScalar::Bool(value)) => visitor.visit_bool(*value),
            InputValue::Scalar(GqlScalar::String(value)) => visitor.visit_str(value),
            InputValue::Enum(value) => visitor.visit_str(value),
            InputValue::Variable(var) => Err(de::Error::invalid_value(
                de::Unexpected::Str(var),
                &"variable",
            )),
            InputValue::List(vec) => {
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
            InputValue::Object(vec) => {
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

impl<'v, 'i, 'de, I> de::SeqAccess<'de> for SeqDeserializer<'i, I>
where
    I: Iterator<Item = &'v Spanning<juniper::InputValue<GqlScalar>>>,
{
    type Error = Error;

    fn next_element_seed<V>(&mut self, seed: V) -> Result<Option<V::Value>, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some(value) => {
                self.count += 1;
                seed.deserialize(InputValueDeserializer { value }).map(Some)
            }
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        serde::__private::size_hint::from_bounds(&self.iter)
    }
}

struct MapDeserializer<'v, 'i, I> {
    iter: &'i mut std::iter::Fuse<I>,
    state: MapState<'v>,
    _count: usize,
}

#[derive(Default)]
enum MapState<'v> {
    #[default]
    NextKey,
    NextValue(&'v Spanning<juniper::InputValue<GqlScalar>>),
}

impl<'v, 'i, I: Iterator> MapDeserializer<'v, 'i, I> {
    fn new(iter: &'i mut std::iter::Fuse<I>) -> Self {
        Self {
            iter,
            state: MapState::NextKey,
            _count: 0,
        }
    }
}

impl<'v, 'i, 'de, I> de::MapAccess<'de> for MapDeserializer<'v, 'i, I>
where
    I: Iterator<Item = &'v (Spanning<String>, Spanning<juniper::InputValue<GqlScalar>>)>,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        match (self.iter.next(), &self.state) {
            (Some((key, value)), MapState::NextKey) => {
                self.state = MapState::NextValue(value);
                seed.deserialize(key.item.as_str().into_deserializer())
                    .map(Some)
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
            MapState::NextValue(value) => seed.deserialize(InputValueDeserializer { value }),
            MapState::NextKey => panic!("should call next_key"),
        }
    }
}
