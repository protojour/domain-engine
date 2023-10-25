use base64::Engine;
use ontol_runtime::{sequence::Cursor, smart_format};
use smartstring::alias::String;

use crate::gql_scalar::GqlScalar;

pub fn serialize_cursor(cursor: &Cursor) -> GqlScalar {
    let string = cursor_to_string(cursor);
    GqlScalar::String(
        base64::engine::general_purpose::STANDARD
            .encode(string)
            .into(),
    )
}

fn cursor_to_string(cursor: &Cursor) -> String {
    match cursor {
        Cursor::Offset(offset) => smart_format!("o={offset}"),
        Cursor::Custom(bytes) => smart_format!("b={bytes:x?}"),
    }
}

pub struct GraphQLCursor(pub Cursor);

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
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(str)
            .map_err(E::custom)?;
        let decoded = std::str::from_utf8(&decoded).map_err(E::custom)?;

        if let Some(offset) = decoded.strip_prefix("o=") {
            Ok(GraphQLCursor(Cursor::Offset(
                offset.parse().map_err(E::custom)?,
            )))
        } else if let Some(hex) = decoded.strip_prefix("b=") {
            let bytes = hex::decode(hex).map_err(E::custom)?;
            Ok(GraphQLCursor(Cursor::Custom(bytes.into_boxed_slice())))
        } else {
            Err(E::custom("Unrecognized cursor format"))
        }
    }
}
