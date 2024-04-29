use std::{fmt::Display, str::FromStr};

#[allow(dead_code)]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Modifier {
    Private,
    Open,
    Extern,
    Symbol,
    Match,
    In,
    AllIn,
    ContainsAll,
    Intersects,
    Equals,
}

impl Display for Modifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Modifier::Private => "private",
            Modifier::Open => "open",
            Modifier::Extern => "extern",
            Modifier::Symbol => "symbol",
            Modifier::Match => "match",
            Modifier::In => "in",
            Modifier::AllIn => "all_in",
            Modifier::ContainsAll => "contains_all",
            Modifier::Intersects => "intersects",
            Modifier::Equals => "equals",
        };

        write!(f, "@{str}")
    }
}

impl FromStr for Modifier {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "@private" => Ok(Modifier::Private),
            "@open" => Ok(Modifier::Open),
            "@extern" => Ok(Modifier::Extern),
            "@symbol" => Ok(Modifier::Symbol),
            "@match" => Ok(Modifier::Match),
            "@in" => Ok(Modifier::In),
            "@all_in" => Ok(Modifier::AllIn),
            "@contains_all" => Ok(Modifier::ContainsAll),
            "@intersects" => Ok(Modifier::Intersects),
            "@equals" => Ok(Modifier::Equals),
            _ => Err(()),
        }
    }
}
