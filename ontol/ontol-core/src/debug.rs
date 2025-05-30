use std::{
    fmt::{Formatter, Result},
    ops::{Range, RangeInclusive},
};

use compact_str::CompactString;
use thin_vec::ThinVec;

/// A debug trait that has access to a [OntolFormatter]
///
/// This trait can be derived using [ontol_macros].
pub trait OntolDebug {
    /// Format the this value with the help of an [OntolFormatter],
    /// write output to the [core::fmt::Formatter].
    fn fmt(&self, ontol_fmt: &dyn OntolFormatter, f: &mut core::fmt::Formatter<'_>) -> Result;

    /// Convert a reference of this value into a type that implemens Debug
    fn debug<'a>(&'a self, formatter: &'a dyn OntolFormatter) -> impl std::fmt::Debug + Sized {
        Fmt(formatter, self)
    }
}

/// A debugging context for [OntolDebug].
///
/// Its purpose is to debug various deduplicated types that uses some kind of lookup index that can be resolved using the context.
pub trait OntolFormatter {
    fn fmt_text_constant(&self, constant: u32, f: &mut Formatter<'_>) -> Result;
}

impl OntolFormatter for () {
    fn fmt_text_constant(&self, constant: u32, f: &mut Formatter<'_>) -> Result {
        write!(f, "{constant:?}")
    }
}

/// A type that combines type T with an [OntolFormatter].
///
/// This type implements [Debug], and can use the ontol formatter.
pub struct Fmt<'f, T>(pub &'f dyn OntolFormatter, pub T);

impl<'f, T> std::fmt::Debug for Fmt<'f, &'f T>
where
    T: ?Sized + OntolDebug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        <T as OntolDebug>::fmt(self.1, self.0, f)
    }
}

/// Shorthand macro for implementing [OntolDebug] for a type that already implements [Debug].
#[macro_export]
macro_rules! impl_ontol_debug {
    ($t:path) => {
        impl $crate::debug::OntolDebug for $t {
            fn fmt(
                &self,
                _: &dyn $crate::debug::OntolFormatter,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                <Self as std::fmt::Debug>::fmt(self, f)
            }
        }
    };
}

impl_ontol_debug!(bool);
impl_ontol_debug!(u8);
impl_ontol_debug!(u16);
impl_ontol_debug!(u32);
impl_ontol_debug!(u64);
impl_ontol_debug!(i8);
impl_ontol_debug!(i16);
impl_ontol_debug!(i32);
impl_ontol_debug!(i64);
impl_ontol_debug!(f32);
impl_ontol_debug!(f64);
impl_ontol_debug!(usize);
impl_ontol_debug!(std::string::String);
impl_ontol_debug!(CompactString);
impl_ontol_debug!(arcstr::ArcStr);
impl_ontol_debug!(bit_vec::BitVec);

impl<T: ?Sized + OntolDebug> OntolDebug for &T {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        (*self).fmt(ofmt, f)
    }
}

impl OntolDebug for &str {
    fn fmt(&self, _: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        write!(f, "{self:?}")
    }
}

impl<T: OntolDebug> OntolDebug for Option<T> {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        match self {
            Some(value) => f.debug_tuple("Some").field(&Fmt(ofmt, value)).finish(),
            None => f.debug_tuple("None").finish(),
        }
    }
}

impl<T: OntolDebug> OntolDebug for Vec<T> {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        f.debug_list()
            .entries(self.iter().map(|el| Fmt(ofmt, el)))
            .finish()
    }
}

impl<T: OntolDebug> OntolDebug for ThinVec<T> {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        f.debug_list()
            .entries(self.iter().map(|el| Fmt(ofmt, el)))
            .finish()
    }
}

impl<const N: usize, T: OntolDebug> OntolDebug for [T; N] {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        f.debug_list()
            .entries(self.iter().map(|el| Fmt(ofmt, el)))
            .finish()
    }
}

impl<T: OntolDebug> OntolDebug for [T] {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        f.debug_list()
            .entries(self.iter().map(|el| Fmt(ofmt, el)))
            .finish()
    }
}

impl<T: ?Sized + OntolDebug> OntolDebug for Box<T> {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        <T as OntolDebug>::fmt(self.as_ref(), ofmt, f)
    }
}

impl<T: OntolDebug> OntolDebug for Range<T> {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        self.start.fmt(ofmt, f)?;
        write!(f, "..")?;
        self.end.fmt(ofmt, f)?;
        Ok(())
    }
}

impl<T: OntolDebug> OntolDebug for RangeInclusive<T> {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        self.start().fmt(ofmt, f)?;
        write!(f, "..=")?;
        self.end().fmt(ofmt, f)?;
        Ok(())
    }
}

impl<A: OntolDebug, B: OntolDebug> OntolDebug for (A, B) {
    fn fmt(&self, ofmt: &dyn OntolFormatter, f: &mut Formatter<'_>) -> Result {
        f.debug_tuple("")
            .field(&Fmt(ofmt, &self.0))
            .field(&Fmt(ofmt, &self.1))
            .finish()
    }
}
