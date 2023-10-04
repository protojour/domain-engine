use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use bit_set::BitSet;

use crate::format_utils::{try_alpha_to_u32, AsAlpha, DebugViaDisplay};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Var(pub u32);

impl From<u32> for Var {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl FromStr for Var {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let next = s.strip_prefix('$').ok_or(())?;
        try_alpha_to_u32(next).map(Var)
    }
}

impl Debug for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Var({})", AsAlpha(self.0))
    }
}

impl Display for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", AsAlpha(self.0))
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct VarSet(pub BitSet);

impl VarSet {
    pub fn iter(&self) -> VarSetIter {
        VarSetIter(self.0.iter())
    }

    #[inline]
    pub fn contains(&self, var: Var) -> bool {
        self.0.contains(var.0 as usize)
    }

    #[inline]
    pub fn insert(&mut self, var: Var) -> bool {
        self.0.insert(var.0 as usize)
    }

    #[inline]
    pub fn remove(&mut self, var: Var) -> bool {
        self.0.remove(var.0 as usize)
    }

    #[inline]
    pub fn union_with(&mut self, other: &Self) {
        self.0.union_with(&other.0);
    }

    #[inline]
    pub fn union(&self, other: &Self) -> Self {
        Self(self.0.union(&other.0).collect())
    }

    #[inline]
    pub fn union_one(&self, var: Var) -> Self {
        let mut clone = self.clone();
        clone.insert(var);
        clone
    }
}

impl FromIterator<Var> for VarSet {
    fn from_iter<T: IntoIterator<Item = Var>>(iter: T) -> Self {
        Self(iter.into_iter().map(|var| var.0 as usize).collect())
    }
}

impl Debug for VarSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut set = f.debug_set();
        for bit in &self.0 {
            set.entry(&DebugViaDisplay(&Var(bit as u32)));
        }

        set.finish()
    }
}

impl<I> From<I> for VarSet
where
    I: IntoIterator<Item = Var>,
{
    fn from(value: I) -> Self {
        Self(value.into_iter().map(|var| var.0 as usize).collect())
    }
}

impl<'a> IntoIterator for &'a VarSet {
    type Item = Var;
    type IntoIter = VarSetIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        VarSetIter(self.0.iter())
    }
}

pub struct VarSetIter<'b>(bit_set::Iter<'b, u32>);

impl<'b> Iterator for VarSetIter<'b> {
    type Item = Var;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;
        Some(Var(next.try_into().unwrap()))
    }
}
