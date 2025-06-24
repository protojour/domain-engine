use std::fmt::Display;

use compact_str::CompactString;
use juniper::{
    AnyExt, FieldError, FromScalarValue, InputValue, IntoFieldError, IntoValue, ScalarValue,
    TryToPrimitive,
};
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

impl TryFrom<GqlScalar> for String {
    type Error = ();

    fn try_from(value: GqlScalar) -> Result<Self, Self::Error> {
        match value {
            GqlScalar::String(s) => Ok(s.into()),
            _ => Err(()),
        }
    }
}

impl ScalarValue for GqlScalar {
    fn is_type<T: std::any::Any + ?Sized>(&self) -> bool {
        match self {
            GqlScalar::I32(i) => AnyExt::is::<T>(i),
            GqlScalar::I64(i) => AnyExt::is::<T>(i),
            GqlScalar::F64(f) => AnyExt::is::<T>(f),
            GqlScalar::Boolean(b) => AnyExt::is::<T>(b),
            GqlScalar::String(s) => AnyExt::is::<T>(s),
        }
    }

    fn downcast_type<T: std::any::Any>(&self) -> Option<&T> {
        match self {
            GqlScalar::I32(i) => AnyExt::downcast_ref(i),
            GqlScalar::I64(i) => AnyExt::downcast_ref(i),
            GqlScalar::F64(f) => AnyExt::downcast_ref(f),
            GqlScalar::Boolean(b) => AnyExt::downcast_ref(b),
            GqlScalar::String(s) => AnyExt::downcast_ref(s),
        }
    }
}

impl IntoValue<GqlScalar> for GqlScalar {
    fn into_value(self) -> juniper::Value<GqlScalar> {
        juniper::Value::Scalar(self)
    }
}

pub struct ToPrimitiveError;

impl IntoFieldError<GqlScalar> for ToPrimitiveError {
    fn into_field_error(self) -> FieldError<GqlScalar> {
        FieldError::new("prmitive conversion failed", juniper::Value::Null)
    }
}

impl Display for ToPrimitiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to convert to primitive")
    }
}

impl<'a> TryToPrimitive<'a, bool> for GqlScalar {
    type Error = ToPrimitiveError;

    fn try_to_primitive(&'a self) -> Result<bool, Self::Error> {
        match self {
            Self::Boolean(b) => Ok(*b),
            _ => Err(ToPrimitiveError),
        }
    }
}

impl<'a> TryToPrimitive<'a, i32> for GqlScalar {
    type Error = ToPrimitiveError;

    fn try_to_primitive(&'a self) -> Result<i32, Self::Error> {
        match self {
            Self::I32(i) => Ok(*i),
            _ => Err(ToPrimitiveError),
        }
    }
}

impl<'a> TryToPrimitive<'a, f64> for GqlScalar {
    type Error = ToPrimitiveError;

    fn try_to_primitive(&'a self) -> Result<f64, Self::Error> {
        match self {
            Self::F64(f) => Ok(*f),
            _ => Err(ToPrimitiveError),
        }
    }
}

impl<'a> TryToPrimitive<'a, String> for GqlScalar {
    type Error = ToPrimitiveError;

    fn try_to_primitive(&'a self) -> Result<String, Self::Error> {
        match self {
            Self::String(s) => Ok(String::from(s.as_str())),
            _ => Err(ToPrimitiveError),
        }
    }
}

impl<'a> TryToPrimitive<'a, &'a str> for GqlScalar {
    type Error = ToPrimitiveError;

    fn try_to_primitive(&'a self) -> Result<&'a str, Self::Error> {
        match self {
            Self::String(s) => Ok(s),
            _ => Err(ToPrimitiveError),
        }
    }
}

pub struct FromScalarValueError;

impl<'a> FromScalarValue<'a, GqlScalar> for InputValue<GqlScalar> {
    type Error = FromScalarValueError;

    fn from_scalar_value(v: &'a GqlScalar) -> Result<Self, Self::Error> {
        Ok(InputValue::Scalar(v.clone()))
    }
}

impl IntoFieldError<GqlScalar> for FromScalarValueError {
    fn into_field_error(self) -> FieldError<GqlScalar> {
        FieldError::new("scalar value error", juniper::Value::Null)
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
