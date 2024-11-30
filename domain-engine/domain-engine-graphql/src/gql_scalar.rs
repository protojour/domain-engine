use std::fmt::Display;

use compact_str::CompactString;
use juniper::ScalarValue;
use serde::{de, ser};

#[derive(Clone, PartialEq, Debug)]
pub enum GqlScalar {
    /// The standard Int type for GraphQL is an i32.
    I32(i32),
    /// Memoriam supports the i64 custom scalar in GraphQL.
    I64(i64),
    F64(f64),
    Boolean(bool),
    String(CompactString),
}

impl From<i32> for GqlScalar {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}

impl From<f64> for GqlScalar {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<bool> for GqlScalar {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<std::string::String> for GqlScalar {
    fn from(value: std::string::String) -> Self {
        Self::String(value.into())
    }
}

impl From<CompactString> for GqlScalar {
    fn from(value: CompactString) -> Self {
        Self::String(value)
    }
}

impl From<&str> for GqlScalar {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<GqlScalar> for juniper::Value<GqlScalar> {
    fn from(value: GqlScalar) -> Self {
        juniper::Value::Scalar(value)
    }
}

impl Display for GqlScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::I32(i) => write!(f, "{i}"),
            Self::I64(i) => write!(f, "{i}"),
            Self::F64(f64) => write!(f, "{f64}"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::String(s) => write!(f, "{s}"),
        }
    }
}

impl ScalarValue for GqlScalar {
    fn as_int(&self) -> Option<i32> {
        match self {
            Self::I32(i) => Some(*i),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    fn as_float(&self) -> Option<f64> {
        match self {
            Self::I32(i) => Some(*i as f64),
            Self::F64(f) => Some(*f),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    fn as_string(&self) -> Option<std::string::String> {
        match self {
            Self::String(s) => Some(s.as_str().into()),
            _ => None,
        }
    }

    fn into_string(self) -> Option<std::string::String> {
        match self {
            Self::String(s) => Some(s.into()),
            _ => None,
        }
    }
}

impl<'de> de::Deserialize<'de> for GqlScalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(ScalarDeserializeVisitor)
    }
}

impl ser::Serialize for GqlScalar {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            Self::I32(i) => serializer.serialize_i32(*i),
            Self::I64(i) => serializer.serialize_i64(*i),
            Self::F64(f) => serializer.serialize_f64(*f),
            Self::Boolean(b) => serializer.serialize_bool(*b),
            Self::String(s) => serializer.serialize_str(s),
        }
    }
}

#[derive(Default)]
pub struct ScalarDeserializeVisitor;

impl de::Visitor<'_> for ScalarDeserializeVisitor {
    type Value = GqlScalar;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a valid input value")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<GqlScalar, E> {
        Ok(GqlScalar::Boolean(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<GqlScalar, E> {
        Ok(GqlScalar::I64(v))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<GqlScalar, E> {
        if v <= i64::MAX as u64 {
            self.visit_i64(v as i64)
        } else {
            Err(E::custom("Overflow converting u64 to i64"))
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<GqlScalar, E> {
        Ok(GqlScalar::F64(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<GqlScalar, E> {
        Ok(GqlScalar::String(v.into()))
    }
}
