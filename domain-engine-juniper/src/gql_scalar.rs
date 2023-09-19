use std::fmt::Display;

use juniper::ScalarValue;
use ontol_runtime::smart_format;
use serde::{de, ser};
use smartstring::alias::String;

#[derive(Clone, PartialEq, Debug)]
pub enum GqlScalar {
    Unit,
    /// The standard Int type for GraphQL is an i32.
    I32(i32),
    /// Memoriam supports the i64 custom scalar in GraphQL.
    I64(i64),
    F64(f64),
    Boolean(bool),
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
        Self::Boolean(value)
    }
}

impl From<std::string::String> for GqlScalar {
    fn from(value: std::string::String) -> Self {
        Self::String(value.into())
    }
}

impl From<smartstring::alias::String> for GqlScalar {
    fn from(value: smartstring::alias::String) -> Self {
        Self::String(value)
    }
}

impl Display for GqlScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unit => write!(f, "{{}}"),
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
            Self::Unit => serializer.serialize_unit(),
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

impl<'de> de::Visitor<'de> for ScalarDeserializeVisitor {
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
        if v <= i64::max_value() as u64 {
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

pub struct GqlScalarSerializer;

#[derive(Debug)]
pub struct GqlScalarError(String);

impl Display for GqlScalarError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ser::StdError for GqlScalarError {}
impl ser::Error for GqlScalarError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self(smart_format!("{}", msg))
    }
}

type SerResult = Result<GqlScalar, GqlScalarError>;
type Impossible = ser::Impossible<GqlScalar, GqlScalarError>;

impl ser::Serializer for GqlScalarSerializer {
    type Ok = GqlScalar;
    type Error = GqlScalarError;

    type SerializeSeq = Impossible;
    type SerializeTuple = Impossible;
    type SerializeTupleStruct = Impossible;
    type SerializeTupleVariant = Impossible;
    type SerializeMap = Impossible;
    type SerializeStruct = Impossible;
    type SerializeStructVariant = Impossible;

    fn serialize_bool(self, v: bool) -> SerResult {
        Ok(GqlScalar::Boolean(v))
    }

    fn serialize_i8(self, value: i8) -> SerResult {
        Ok(GqlScalar::I32(value as i32))
    }

    fn serialize_i16(self, value: i16) -> SerResult {
        Ok(GqlScalar::I32(value as i32))
    }

    fn serialize_i32(self, value: i32) -> SerResult {
        Ok(GqlScalar::I32(value))
    }

    fn serialize_i64(self, value: i64) -> SerResult {
        if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
            Ok(GqlScalar::I32(value as i32))
        } else {
            Ok(GqlScalar::I64(value))
        }
    }

    fn serialize_i128(self, value: i128) -> SerResult {
        if value >= i32::MIN as i128 && value <= i32::MAX as i128 {
            Ok(GqlScalar::I32(value as i32))
        } else {
            Ok(GqlScalar::F64(value as f64))
        }
    }

    fn serialize_u8(self, value: u8) -> SerResult {
        Ok(GqlScalar::I32(value as i32))
    }

    fn serialize_u16(self, value: u16) -> SerResult {
        Ok(GqlScalar::I32(value as i32))
    }

    fn serialize_u32(self, value: u32) -> SerResult {
        let int_i64 = value as i64;
        if int_i64 < (i32::MAX as i64) {
            Ok(GqlScalar::I32(int_i64 as i32))
        } else {
            Ok(GqlScalar::I64(int_i64))
        }
    }

    fn serialize_u64(self, value: u64) -> SerResult {
        let int_i64: i64 = value
            .try_into()
            .map_err(|_| GqlScalarError(smart_format!("u64 -> i64 overflow")))?;
        self.serialize_i64(int_i64)
    }

    fn serialize_u128(self, value: u128) -> SerResult {
        let int_i64: i64 = value
            .try_into()
            .map_err(|_| GqlScalarError(smart_format!("u128 -> i64 overflow")))?;
        self.serialize_i64(int_i64)
    }

    fn serialize_f32(self, v: f32) -> SerResult {
        Ok(GqlScalar::F64(v as f64))
    }

    fn serialize_f64(self, v: f64) -> SerResult {
        Ok(GqlScalar::F64(v))
    }

    fn serialize_char(self, v: char) -> SerResult {
        Ok(GqlScalar::String(smart_format!("{v}")))
    }

    fn serialize_str(self, v: &str) -> SerResult {
        Ok(GqlScalar::String(v.into()))
    }

    fn serialize_bytes(self, _value: &[u8]) -> SerResult {
        todo!()
    }

    fn serialize_unit(self) -> SerResult {
        Ok(GqlScalar::Unit)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> SerResult {
        panic!("not used")
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> SerResult {
        panic!("not used")
    }

    fn serialize_newtype_struct<T: ?Sized + ser::Serialize>(
        self,
        _: &'static str,
        _: &T,
    ) -> SerResult {
        panic!("not used")
    }

    fn serialize_newtype_variant<T: ?Sized + ser::Serialize>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> SerResult {
        panic!("not used")
    }

    fn serialize_some<T: ?Sized + ser::Serialize>(self, _: &T) -> SerResult {
        panic!("not used")
    }

    fn serialize_none(self) -> SerResult {
        panic!("not used")
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        panic!("not a scalar")
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        panic!("not a scalar")
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        panic!("not a scalar")
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        panic!("not a scalar")
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        panic!("not a scalar")
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        panic!("not a scalar")
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        panic!("not a scalar")
    }
}
