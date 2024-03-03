use std::{
    fmt::{Formatter, Result},
    ops::{Range, RangeInclusive},
};

use indexmap::IndexMap;
use smallvec::SmallVec;

pub trait OntolDebug {
    fn fmt(&self, c: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result;
}

pub trait OntolDebugCtx {}

pub struct Debug<'on, T>(pub &'on dyn OntolDebugCtx, pub T);

impl<'on, T: ?Sized> std::fmt::Debug for Debug<'on, &'on T>
where
    T: OntolDebug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        <T as OntolDebug>::fmt(&self.1, self.0, f)
    }
}

#[macro_export]
macro_rules! impl_ontol_debug {
    ($t:path) => {
        impl crate::debug::OntolDebug for $t {
            fn fmt(
                &self,
                _: &dyn crate::debug::OntolDebugCtx,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                write!(f, "{self:?}")
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
impl_ontol_debug!(smartstring::alias::String);

impl<'a, T: OntolDebug> OntolDebug for &'a T {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        (*self).fmt(ctx, f)
    }
}

impl<'a> OntolDebug for &'a str {
    fn fmt(&self, _: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        write!(f, "{self:?}")
    }
}

impl<T: OntolDebug> OntolDebug for Option<T> {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        match self {
            Some(value) => f.debug_tuple("Some").field(&Debug(ctx, value)).finish(),
            None => f.debug_tuple("None").finish(),
        }
    }
}

impl<T: OntolDebug> OntolDebug for Vec<T> {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        f.debug_list()
            .entries(self.iter().map(|el| Debug(ctx, el)))
            .finish()
    }
}

impl<T: OntolDebug> OntolDebug for SmallVec<[T; 3]> {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        f.debug_list()
            .entries(self.iter().map(|el| Debug(ctx, el)))
            .finish()
    }
}

impl<const N: usize, T: OntolDebug> OntolDebug for [T; N] {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        f.debug_list()
            .entries(self.iter().map(|el| Debug(ctx, el)))
            .finish()
    }
}

impl<T: OntolDebug> OntolDebug for [T] {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        f.debug_list()
            .entries(self.iter().map(|el| Debug(ctx, el)))
            .finish()
    }
}

impl<T: OntolDebug> OntolDebug for Range<T> {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        self.start.fmt(ctx, f)?;
        write!(f, "..")?;
        self.end.fmt(ctx, f)?;
        Ok(())
    }
}

impl<T: OntolDebug> OntolDebug for RangeInclusive<T> {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        self.start().fmt(ctx, f)?;
        write!(f, "..=")?;
        self.end().fmt(ctx, f)?;
        Ok(())
    }
}

impl<A: OntolDebug, B: OntolDebug> OntolDebug for (A, B) {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        f.debug_tuple("")
            .field(&Debug(ctx, &self.0))
            .field(&Debug(ctx, &self.1))
            .finish()
    }
}

impl<K: OntolDebug, V: OntolDebug, S> OntolDebug for IndexMap<K, V, S> {
    fn fmt(&self, ctx: &dyn OntolDebugCtx, f: &mut Formatter<'_>) -> Result {
        f.debug_map()
            .entries(self.iter().map(|(k, v)| (Debug(ctx, k), Debug(ctx, v))))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(dead_code)]

    use super::*;
    use crate::ontology::Ontology;
    use ontol_macros::OntolDebug;

    struct Custom;

    impl OntolDebug for Custom {
        fn fmt(
            &self,
            _ctx: &dyn OntolDebugCtx,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "Custom!")
        }
    }

    #[derive(OntolDebug)]
    struct Unit;

    #[derive(OntolDebug)]
    struct Tuple(Unit, Custom);

    #[derive(OntolDebug)]
    struct Named {
        tuple: Tuple,
        int: usize,
    }

    #[derive(OntolDebug)]
    enum Enum {
        A { f: Named },
        B,
        C(usize),
    }

    #[test]
    fn test_debug() {
        let value = Enum::A {
            f: Named {
                tuple: Tuple(Unit, Custom),
                int: 3,
            },
        };
        let ontology = Ontology::builder().build();
        let str = format!("{:?}", Debug(&ontology, &value));

        assert_eq!(
            str,
            "A { f: Named { tuple: Tuple(Unit, Custom!), int: 3 } }"
        );
    }
}
