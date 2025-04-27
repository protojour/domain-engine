use std::fmt::Debug;

use serde::{Deserialize, Serialize, de::Visitor};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Tag(pub u32);

#[derive(Clone, Default, Debug)]
pub struct TagBump {
    next: u32,
}

impl TagBump {
    pub fn bump(&mut self) -> Tag {
        let tag = self.next;
        self.next += 1;
        Tag(tag)
    }
}

#[derive(Clone, Default, Debug)]
pub struct TagAllocator {
    pub next_tag: TagBump,
}

impl Serialize for Tag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = Default::default();
        let encoded = unsigned_varint::encode::u32(self.0, &mut buf);
        serializer.serialize_bytes(encoded)
    }
}

impl<'de> Deserialize<'de> for Tag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visit;

        impl Visitor<'_> for Visit {
            type Value = Tag;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "tag")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let (value, _) = unsigned_varint::decode::u32(v)
                    .map_err(|_| serde::de::Error::custom("invalid tag encoding"))?;

                Ok(Tag(value))
            }
        }

        deserializer.deserialize_bytes(Visit)
    }
}
