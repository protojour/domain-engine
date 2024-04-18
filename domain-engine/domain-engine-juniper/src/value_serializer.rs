use serde::ser;
use std::fmt::Display;

use crate::gql_scalar::GqlScalar;

pub struct JuniperValueSerializer;

#[derive(Debug)]
pub struct SerializeError(String);

impl SerializeError {
    fn not_implemented() -> Self {
        Self("not implemented".into())
    }
}

impl Display for SerializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ser::StdError for SerializeError {}
impl ser::Error for SerializeError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self(format!("{}", msg))
    }
}

type SerResult = Result<juniper::Value<GqlScalar>, SerializeError>;
type Impossible = ser::Impossible<juniper::Value<GqlScalar>, SerializeError>;

impl ser::Serializer for JuniperValueSerializer {
    type Ok = juniper::Value<GqlScalar>;
    type Error = SerializeError;

    type SerializeSeq = JuniperListSerializer;
    type SerializeTuple = Impossible;
    type SerializeTupleStruct = Impossible;
    type SerializeTupleVariant = Impossible;
    type SerializeMap = JuniperObjectSerializer;
    type SerializeStruct = Impossible;
    type SerializeStructVariant = Impossible;

    fn serialize_bool(self, v: bool) -> SerResult {
        Ok(GqlScalar::Boolean(v).into())
    }

    fn serialize_i8(self, value: i8) -> SerResult {
        Ok(GqlScalar::I32(value as i32).into())
    }

    fn serialize_i16(self, value: i16) -> SerResult {
        Ok(GqlScalar::I32(value as i32).into())
    }

    fn serialize_i32(self, value: i32) -> SerResult {
        Ok(GqlScalar::I32(value).into())
    }

    fn serialize_i64(self, value: i64) -> SerResult {
        if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
            Ok(GqlScalar::I32(value as i32).into())
        } else {
            Ok(GqlScalar::I64(value).into())
        }
    }

    fn serialize_i128(self, value: i128) -> SerResult {
        if value >= i32::MIN as i128 && value <= i32::MAX as i128 {
            Ok(GqlScalar::I32(value as i32).into())
        } else {
            Ok(GqlScalar::F64(value as f64).into())
        }
    }

    fn serialize_u8(self, value: u8) -> SerResult {
        Ok(GqlScalar::I32(value as i32).into())
    }

    fn serialize_u16(self, value: u16) -> SerResult {
        Ok(GqlScalar::I32(value as i32).into())
    }

    fn serialize_u32(self, value: u32) -> SerResult {
        let int_i64 = value as i64;
        if int_i64 < (i32::MAX as i64) {
            Ok(GqlScalar::I32(int_i64 as i32).into())
        } else {
            Ok(GqlScalar::I64(int_i64).into())
        }
    }

    fn serialize_u64(self, value: u64) -> SerResult {
        let int_i64: i64 = value
            .try_into()
            .map_err(|_| SerializeError("u64 -> i64 overflow".to_string()))?;
        self.serialize_i64(int_i64)
    }

    fn serialize_u128(self, value: u128) -> SerResult {
        let int_i64: i64 = value
            .try_into()
            .map_err(|_| SerializeError("u128 -> i64 overflow".to_string()))?;
        self.serialize_i64(int_i64)
    }

    fn serialize_f32(self, v: f32) -> SerResult {
        Ok(GqlScalar::F64(v as f64).into())
    }

    fn serialize_f64(self, v: f64) -> SerResult {
        Ok(GqlScalar::F64(v).into())
    }

    fn serialize_char(self, v: char) -> SerResult {
        Ok(GqlScalar::String(format!("{v}").into()).into())
    }

    fn serialize_str(self, v: &str) -> SerResult {
        Ok(GqlScalar::String(v.into()).into())
    }

    fn serialize_bytes(self, _value: &[u8]) -> SerResult {
        Err(SerializeError::not_implemented())
    }

    fn serialize_unit(self) -> SerResult {
        Ok(juniper::Value::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> SerResult {
        Ok(juniper::Value::Null)
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> SerResult {
        Err(SerializeError::not_implemented())
    }

    fn serialize_newtype_struct<T: ?Sized + ser::Serialize>(
        self,
        _: &'static str,
        _: &T,
    ) -> SerResult {
        Err(SerializeError::not_implemented())
    }

    fn serialize_newtype_variant<T: ?Sized + ser::Serialize>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> SerResult {
        Err(SerializeError::not_implemented())
    }

    fn serialize_some<T: ?Sized + ser::Serialize>(self, some: &T) -> SerResult {
        some.serialize(self)
    }

    fn serialize_none(self) -> SerResult {
        Ok(juniper::Value::Null)
    }

    fn serialize_seq(self, cap: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(JuniperListSerializer {
            elements: Vec::with_capacity(cap.unwrap_or(0)),
        })
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerializeError::not_implemented())
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(SerializeError::not_implemented())
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(SerializeError::not_implemented())
    }

    fn serialize_map(self, cap: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(JuniperObjectSerializer {
            pending_key: None,
            object: juniper::Object::with_capacity(cap.unwrap_or(0)),
        })
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(SerializeError::not_implemented())
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(SerializeError::not_implemented())
    }
}

pub struct JuniperListSerializer {
    elements: Vec<juniper::Value<GqlScalar>>,
}

impl ser::SerializeSeq for JuniperListSerializer {
    type Ok = juniper::Value<GqlScalar>;
    type Error = SerializeError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        self.elements.push(value.serialize(JuniperValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(juniper::Value::List(self.elements))
    }
}

pub struct JuniperObjectSerializer {
    pending_key: Option<smartstring::alias::String>,
    object: juniper::Object<GqlScalar>,
}

impl ser::SerializeMap for JuniperObjectSerializer {
    type Ok = juniper::Value<GqlScalar>;
    type Error = SerializeError;

    fn serialize_key<T: ?Sized + serde::Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        let juniper::Value::Scalar(GqlScalar::String(key)) =
            key.serialize(JuniperValueSerializer)?
        else {
            return Err(SerializeError("key must be a string".to_string()));
        };

        self.pending_key = Some(key);
        Ok(())
    }

    fn serialize_value<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        let key = self.pending_key.take().unwrap();
        let value = value.serialize(JuniperValueSerializer)?;

        self.object.add_field(key, value);

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.object.into())
    }
}
