use juniper::{InputValue, Spanning};
use serde::de;
use serde::de::IntoDeserializer;

use crate::gql_scalar::GqlScalar;

pub struct InputValueDeserializer<'v, E> {
    pub value: &'v juniper::InputValue<GqlScalar>,
    pub error: std::marker::PhantomData<fn() -> E>,
}

impl<'v, 'de, E: de::Error> de::Deserializer<'de> for InputValueDeserializer<'v, E> {
    type Error = E;

    fn deserialize_any<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            InputValue::Null => visitor.visit_none(),
            InputValue::Scalar(GqlScalar::I32(value)) => visitor.visit_i32(*value),
            InputValue::Scalar(GqlScalar::F64(value)) => visitor.visit_f64(*value),
            InputValue::Scalar(GqlScalar::Bool(value)) => visitor.visit_bool(*value),
            InputValue::Scalar(GqlScalar::String(value)) => visitor.visit_str(value),
            InputValue::Enum(value) => visitor.visit_str(&value),
            InputValue::Variable(var) => Err(de::Error::invalid_value(
                de::Unexpected::Str(&var),
                &"variable",
            )),
            InputValue::List(vec) => {
                visitor.visit_seq(SeqDeserializer::<E, _>::new(vec.into_iter()))
            }
            InputValue::Object(vec) => {
                visitor.visit_map(MapDeserializer::<E, _>::new(vec.into_iter()))
            }
        }
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    serde::forward_to_deserialize_any!(
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit
        seq bytes byte_buf map unit_struct
        tuple_struct struct tuple ignored_any identifier
    );
}

struct SeqDeserializer<E, I> {
    iter: std::iter::Fuse<I>,
    count: usize,
    error: std::marker::PhantomData<fn() -> E>,
}

impl<E, I: Iterator> SeqDeserializer<E, I> {
    fn new(iterator: I) -> Self {
        Self {
            iter: iterator.fuse(),
            count: 0,
            error: std::marker::PhantomData,
        }
    }
}

impl<'v, 'de, E: de::Error, I> de::SeqAccess<'de> for SeqDeserializer<E, I>
where
    E: de::Error,
    I: Iterator<Item = &'v Spanning<juniper::InputValue<GqlScalar>>>,
{
    type Error = E;

    fn next_element_seed<V>(&mut self, seed: V) -> Result<Option<V::Value>, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some(value) => {
                self.count += 1;
                seed.deserialize(InputValueDeserializer {
                    value: &value.item,
                    error: std::marker::PhantomData,
                })
                .map(Some)
            }
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        serde::__private::size_hint::from_bounds(&self.iter)
    }
}

struct MapDeserializer<'v, E, I> {
    iter: std::iter::Fuse<I>,
    state: MapState<'v>,
    _count: usize,
    error: std::marker::PhantomData<fn() -> E>,
}

#[derive(Default)]
enum MapState<'v> {
    #[default]
    NextKey,
    NextValue(&'v Spanning<juniper::InputValue<GqlScalar>>),
}

impl<'v, E, I: Iterator> MapDeserializer<'v, E, I> {
    fn new(iterator: I) -> Self {
        Self {
            iter: iterator.fuse(),
            state: MapState::NextKey,
            _count: 0,
            error: std::marker::PhantomData,
        }
    }
}

impl<'v, 'de, E: de::Error, I> de::MapAccess<'de> for MapDeserializer<'v, E, I>
where
    E: de::Error,
    I: Iterator<Item = &'v (Spanning<String>, Spanning<juniper::InputValue<GqlScalar>>)>,
{
    type Error = E;

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
            MapState::NextValue(value) => seed.deserialize(InputValueDeserializer {
                value: &value.item,
                error: std::marker::PhantomData,
            }),
            MapState::NextKey => panic!("should call next_key"),
        }
    }
}
