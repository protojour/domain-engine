use std::{borrow::Borrow, fmt::Debug};

use ontol_parser::{cst::view::TokenView, lexer::kind::Kind};
use ontol_syntax::{
    OntolLang,
    rowan::{GreenToken, Language, SyntaxKind},
    syntax_view::RowanTokenView,
};
use serde::{Deserialize, Serialize, ser::SerializeTuple};

use crate::with_span::WithSpan;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Token(pub GreenToken);

impl Token {
    pub fn symbol(text: &'static str) -> Self {
        Self::new(Kind::Symbol, text)
    }

    pub fn new(kind: Kind, text: &'static str) -> Self {
        Self(GreenToken::new(SyntaxKind(kind as u16), text))
    }

    pub fn text(&self) -> &str {
        self.0.text()
    }
}

impl PartialOrd<Token> for Token {
    fn partial_cmp(&self, other: &Token) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Token {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .kind()
            .cmp(&other.0.kind())
            .then_with(|| self.0.text().cmp(other.0.text()))
    }
}

impl From<RowanTokenView> for Token {
    fn from(value: RowanTokenView) -> Self {
        Self(value.0.green().to_owned())
    }
}

impl From<RowanTokenView> for WithSpan<Token> {
    fn from(value: RowanTokenView) -> Self {
        WithSpan(Token(value.0.green().to_owned()), value.span())
    }
}

impl From<&RowanTokenView> for Token {
    fn from(value: &RowanTokenView) -> Self {
        Self(value.0.green().to_owned())
    }
}

impl Borrow<GreenToken> for Token {
    fn borrow(&self) -> &GreenToken {
        &self.0
    }
}

impl Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\"", self.0.text())
    }
}

impl Serialize for Token {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let kind = OntolLang::kind_from_raw(self.0.kind());
        if kind == Kind::Symbol {
            serializer.serialize_str(self.0.text())
        } else {
            let mut tup = serializer.serialize_tuple(2)?;
            tup.serialize_element(&kind)?;
            tup.serialize_element(self.0.text())?;
            tup.end()
        }
    }
}

impl<'de> Deserialize<'de> for Token {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor {}

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Token;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "token")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Token(GreenToken::new(SyntaxKind(Kind::Symbol as u16), v)))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let Some(kind) = seq.next_element::<Kind>()? else {
                    return Err(serde::de::Error::custom("missing token kind"));
                };
                let Some(text) = seq.next_element::<String>()? else {
                    return Err(serde::de::Error::custom("missing token text"));
                };

                Ok(Token(GreenToken::new(SyntaxKind(kind as u16), &text)))
            }
        }

        deserializer.deserialize_any(Visitor {})
    }
}
