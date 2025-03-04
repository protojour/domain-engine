use std::ops::RangeInclusive;

use crate::{
    DefId,
    interface::serde::matcher::OptWithinRangeDisplay,
    value::{Serial, Value, ValueTag},
};

use super::ValueMatcher;

pub struct UnitMatcher;

impl ValueMatcher for UnitMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "unit")
    }

    fn match_unit(&self) -> Result<Value, ()> {
        Ok(Value::Unit(ValueTag::unit()))
    }
}

pub enum BooleanMatcher {
    False(DefId),
    True(DefId),
    Boolean(DefId),
}

impl ValueMatcher for BooleanMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::False(_) => write!(f, "false"),
            Self::True(_) => write!(f, "true"),
            Self::Boolean(_) => write!(f, "boolean"),
        }
    }

    fn match_boolean(&self, val: bool) -> Result<Value, ()> {
        let int = if val { 1 } else { 0 };
        match (self, val) {
            (Self::False(def_id), false) => Ok(Value::I64(int, (*def_id).into())),
            (Self::True(def_id), true) => Ok(Value::I64(int, (*def_id).into())),
            (Self::Boolean(def_id), _) => Ok(Value::I64(int, (*def_id).into())),
            _ => Err(()),
        }
    }
}

pub struct NumberMatcher<T> {
    pub def_id: DefId,
    pub range: Option<RangeInclusive<T>>,
}

impl ValueMatcher for NumberMatcher<i64> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "integer{}", OptWithinRangeDisplay(&self.range))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        u64_to_i64(value).and_then(|value| self.match_i64(value))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        if let Some(range) = &self.range {
            if !range.contains(&value) {
                return Err(());
            }
        }

        Ok(Value::I64(value, self.def_id.into()))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        self.match_i64(str.parse().map_err(|_| ())?)
    }
}

impl ValueMatcher for NumberMatcher<i32> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "integer{}", OptWithinRangeDisplay(&self.range))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        u64_to_i64(value).and_then(|value| self.match_i64(value))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        let value: i32 = value.try_into().map_err(|_| ())?;

        if let Some(range) = &self.range {
            if !range.contains(&value) {
                return Err(());
            }
        }

        Ok(Value::I64(value as i64, self.def_id.into()))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        self.match_i64(str.parse().map_err(|_| ())?)
    }
}

impl ValueMatcher for NumberMatcher<f64> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "float{}", OptWithinRangeDisplay(&self.range))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        self.match_f64(value as f64)
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        self.match_f64(value as f64)
    }

    fn match_f64(&self, value: f64) -> Result<Value, ()> {
        if let Some(range) = &self.range {
            if !range.contains(&value) {
                return Err(());
            }
        }

        Ok(Value::F64(value, self.def_id.into()))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        self.match_f64(str.parse().map_err(|_| ())?)
    }
}

impl ValueMatcher for NumberMatcher<Serial> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "serial")
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        Ok(Value::Serial(Serial(value), self.def_id.into()))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        self.match_u64(value as u64)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        self.match_u64(str.parse().map_err(|_| ())?)
    }
}

pub fn u64_to_i64(value: u64) -> Result<i64, ()> {
    i64::try_from(value).map_err(|_| ())
}
