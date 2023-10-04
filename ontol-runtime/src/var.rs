use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use crate::format_utils::{try_alpha_to_u32, AsAlpha};

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
