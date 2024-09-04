use std::{fmt::Display, ops::RangeInclusive};

use map_matcher::MapMatcher;

use crate::value::Value;

use self::sequence_matcher::SequenceRangesMatcher;

pub mod map_matcher;
pub mod octets_matcher;
pub mod primitive_matchers;
pub mod sequence_matcher;
pub mod text_matchers;
pub mod union_matcher;

pub struct ExpectingMatching<'v>(pub &'v dyn ValueMatcher);

impl<'v> Display for ExpectingMatching<'v> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.expecting(f)
    }
}

/// Trait for matching incoming types for deserialization
pub trait ValueMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result;

    fn match_unit(&self) -> Result<Value, ()> {
        Err(())
    }

    fn match_boolean(&self, _: bool) -> Result<Value, ()> {
        Err(())
    }

    fn match_u64(&self, _: u64) -> Result<Value, ()> {
        Err(())
    }

    fn match_i64(&self, _: i64) -> Result<Value, ()> {
        Err(())
    }

    fn match_f64(&self, _: f64) -> Result<Value, ()> {
        Err(())
    }

    fn match_str(&self, _: &str) -> Result<Value, ()> {
        Err(())
    }

    fn match_sequence(&self) -> Result<SequenceRangesMatcher, ()> {
        Err(())
    }

    fn match_map(&self) -> Result<MapMatcher, ()> {
        Err(())
    }
}

struct OptWithinRangeDisplay<'a, T>(&'a Option<RangeInclusive<T>>);

impl<'a, T: Display> Display for OptWithinRangeDisplay<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(range) = self.0 {
            write!(f, " in range {}..={}", range.start(), range.end())
        } else {
            Ok(())
        }
    }
}
