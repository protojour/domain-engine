use base64::Engine;

use crate::gql_scalar::GqlScalar;

pub fn serialize_cursor(cursor: &[u8]) -> GqlScalar {
    GqlScalar::String(
        base64::engine::general_purpose::STANDARD
            .encode(cursor)
            .into(),
    )
}

pub struct GraphQLCursor(pub Box<[u8]>);

impl<'de> serde::de::Deserialize<'de> for GraphQLCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CursorVisitor)
    }
}

pub struct CursorVisitor;

impl<'de> serde::de::Visitor<'de> for CursorVisitor {
    type Value = GraphQLCursor;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "cursor")
    }

    fn visit_str<E: serde::de::Error>(self, str: &str) -> Result<Self::Value, E> {
        let decoded: Box<[u8]> = base64::engine::general_purpose::STANDARD
            .decode(str)
            .map_err(E::custom)?
            .into();

        Ok(GraphQLCursor(decoded))
    }
}
