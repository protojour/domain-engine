use std::marker::PhantomData;

use serde::de::{Error, IntoDeserializer, MapAccess};

pub struct BufferedAttrsReader<E> {
    iterator: std::vec::IntoIter<(String, serde_value::Value)>,
    next_value: Option<serde_value::Value>,
    error: PhantomData<E>,
}

impl<E> BufferedAttrsReader<E> {
    pub fn new(buffered_attrs: Vec<(String, serde_value::Value)>) -> Self {
        Self {
            iterator: buffered_attrs.into_iter(),
            next_value: None,
            error: PhantomData,
        }
    }

    fn take_next_value(&mut self) -> serde_value::Value {
        self.next_value.take().expect("no value")
    }
}

impl<'de, E: Error> MapAccess<'de> for BufferedAttrsReader<E> {
    type Error = E;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        let Some((key, value)) = self.iterator.next() else {
            return Ok(None);
        };
        self.next_value = Some(value);

        let key = seed.deserialize(key.into_deserializer())?;
        Ok(Some(key))
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        seed.deserialize(serde_value::ValueDeserializer::new(self.take_next_value()))
    }
}
