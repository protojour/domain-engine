use std::fmt::Display;

use super::modifier::Modifier;

#[allow(dead_code)]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Token {
    Open(char),
    Close(char),
    Sigil(char),
    Use,
    Def,
    Rel,
    Fmt,
    Map,
    FatArrow,
    DotDot,
    Number(String),
    TextLiteral(String),
    Regex(String),
    Sym(String),
    Modifier(Modifier),
    UnknownModifer(String),
    DocComment(String),
    /// Used in error reporting only
    Expected(&'static str),
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(c) | Self::Close(c) | Self::Sigil(c) => write!(f, "`{c}`"),
            Self::Use => write!(f, "`use`"),
            Self::Def => write!(f, "`def`"),
            Self::Rel => write!(f, "`rel`"),
            Self::Fmt => write!(f, "`fmt`"),
            Self::Map => write!(f, "`map`"),
            Self::DotDot => write!(f, "`..`"),
            Self::FatArrow => write!(f, "`=>`"),
            Self::Number(_) => write!(f, "`number`"),
            Self::TextLiteral(_) => write!(f, "`string`"),
            Self::Regex(_) => write!(f, "`regex`"),
            Self::Sym(sym) => write!(f, "`{sym}`"),
            Self::Modifier(m) => write!(f, "`{m}`"),
            Self::UnknownModifer(m) => write!(f, "`{m}`"),
            Self::DocComment(_) => write!(f, "`doc_comment`"),
            Self::Expected(expected) => write!(f, "{expected}"),
        }
    }
}
