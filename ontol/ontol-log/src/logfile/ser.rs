use std::{error::Error, fmt::Display, fmt::Write};

use serde::{
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};

pub fn to_string(value: &[impl Serialize]) -> Result<String, SerError> {
    let mut s = LogfileSerializer {
        out: String::new(),
        level: 0,
        trailing: false,
    };
    value.serialize(&mut s)?;
    s.out.push('\n');
    Ok(s.out)
}

pub struct LogfileSerializer {
    out: String,
    level: usize,
    trailing: bool,
}

#[derive(Debug)]
pub struct SerError(String);

impl Display for SerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for SerError {}

impl serde::ser::Error for SerError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self(format!("{msg}"))
    }
}

impl LogfileSerializer {
    fn push_seq(&mut self) {
        if self.level > 0 {
            self.out.push('(');
        }
        self.level += 1;
        self.trailing = false;
    }

    fn pop_seq(&mut self) {
        self.level -= 1;
        if self.level > 0 {
            self.out.push(')');
        }
        self.trailing = true;
    }

    fn pre_element(&mut self) {
        match (self.level, self.trailing) {
            (0 | 1, true) => {
                self.out.push('\n');
            }
            (_, true) => {
                self.out.push(' ');
            }
            _ => {}
        }

        self.trailing = true;
    }
}

impl Serializer for &mut LogfileSerializer {
    type Ok = ();
    type Error = SerError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.out.push_str(if v { "true" } else { "false" });
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        write!(&mut self.out, "{v}").unwrap();
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        write!(&mut self.out, "{v}").unwrap();
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        write!(&mut self.out, "{v}").unwrap();
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        write!(&mut self.out, "{v}").unwrap();
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        write!(&mut self.out, "{v}").unwrap();
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        write!(&mut self.out, "{v}").unwrap();
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        write!(&mut self.out, "{v}").unwrap();
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        write!(&mut self.out, "{v}").unwrap();
        Ok(())
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(SerError("float not supported".to_owned()))
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(SerError("float not supported".to_owned()))
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(SerError("char not supported".to_owned()))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.out.push('"');
        // FIXME: escape sequences
        self.out.push_str(v);
        self.out.push('"');
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.out.push('#');
        for byte in v {
            write!(&mut self.out, "{byte:02x}").unwrap();
        }
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.out.push_str("()");
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        // warning: potential ambiguity with e.g. Option<Vec>
        value.serialize(&mut *self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.out.push_str("()");
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.out.push_str("()");
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.out.push_str(variant);
        Ok(())
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        value.serialize(&mut *self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.push_seq();
        self.out.push_str(variant);
        self.trailing = true;
        self.pre_element();
        value.serialize(&mut *self)?;
        self.pop_seq();
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.push_seq();
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.push_seq();
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.push_seq();
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.push_seq();
        self.out.push_str(variant);
        self.trailing = true;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(SerError("map not supported".to_string()))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(SerError("struct not supported".to_string()))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(SerError("struct not supported".to_string()))
    }
}

impl SerializeSeq for &mut LogfileSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.pre_element();
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.pop_seq();
        Ok(())
    }
}

impl SerializeTuple for &mut LogfileSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.pre_element();
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.pop_seq();
        Ok(())
    }
}

impl SerializeTupleStruct for &mut LogfileSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.pre_element();
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.pop_seq();
        Ok(())
    }
}

impl SerializeTupleVariant for &mut LogfileSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.pre_element();
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.pop_seq();
        Ok(())
    }
}

impl SerializeMap for &mut LogfileSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        Err(SerError("map not supported".to_string()))
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        Err(SerError("map not supported".to_string()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(SerError("map not supported".to_string()))
    }
}

impl SerializeStruct for &mut LogfileSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        Err(SerError("struct not supported".to_string()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.pop_seq();
        Ok(())
    }
}

impl SerializeStructVariant for &mut LogfileSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        Err(SerError("struct not supported".to_string()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.pop_seq();
        Ok(())
    }
}
