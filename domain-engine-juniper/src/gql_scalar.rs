use std::fmt::Display;

use juniper::ScalarValue;
use smartstring::alias::String;

#[derive(Clone, PartialEq, Debug)]
pub enum GqlScalar {
    I32(i32),
    F64(f64),
    Bool(bool),
    String(String),
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
        Self::Bool(value)
    }
}

impl From<std::string::String> for GqlScalar {
    fn from(value: std::string::String) -> Self {
        Self::String(value.into())
    }
}

impl Display for GqlScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::I32(i) => write!(f, "{i}"),
            Self::F64(f64) => write!(f, "{f64}"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::String(s) => write!(f, "{s}"),
        }
    }
}

impl ScalarValue for GqlScalar {
    type Visitor = ScalarDeserializeVisitor;

    fn as_int(&self) -> Option<i32> {
        match self {
            Self::I32(i) => Some(*i),
            _ => None,
        }
    }

    fn as_boolean(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
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

impl serde::ser::Serialize for GqlScalar {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::I32(i) => serializer.serialize_i32(*i),
            Self::F64(f) => serializer.serialize_f64(*f),
            Self::Bool(b) => serializer.serialize_bool(*b),
            Self::String(s) => serializer.serialize_str(s),
        }
    }
}

#[derive(Default)]
pub struct ScalarDeserializeVisitor;

impl<'de> serde::de::Visitor<'de> for ScalarDeserializeVisitor {
    type Value = GqlScalar;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a valid input value")
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<GqlScalar, E> {
        Ok(GqlScalar::Bool(v))
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<GqlScalar, E> {
        if v >= i64::from(i32::min_value()) && v <= i64::from(i32::max_value()) {
            Ok(GqlScalar::I32(v as i32))
        } else {
            // Browser's JSON.stringify serialize all numbers having no
            // fractional part as integers (no decimal point), so we
            // must parse large integers as floating point otherwise
            // we would error on transferring large floating point
            // numbers.
            Ok(GqlScalar::F64(v as f64))
        }
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<GqlScalar, E> {
        if v <= i32::max_value() as u64 {
            self.visit_i64(v as i64)
        } else {
            // Browser's JSON.stringify serialize all numbers having no
            // fractional part as integers (no decimal point), so we
            // must parse large integers as floating point otherwise
            // we would error on transferring large floating point
            // numbers.
            Ok(GqlScalar::F64(v as f64))
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<GqlScalar, E> {
        Ok(GqlScalar::F64(v))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<GqlScalar, E> {
        Ok(GqlScalar::String(v.into()))
    }
}
