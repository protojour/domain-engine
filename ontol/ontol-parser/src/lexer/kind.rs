//! A logos-based lexer.

use std::fmt::Display;

use crate::K;
use logos::{Lexer, Logos};

/// The Kind of token the lexer can produce.
///
/// The lexer will slice the full document into tokens.
/// The produced Spans will need some post-processing
/// for extracting useful data (such as escape codes, etc)
#[derive(Logos, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(u16)]
pub enum Kind {
    #[regex(r"([ \t\n\r])+")]
    Whitespace = 0,

    #[regex(r"///[^\n\r]*")]
    DocComment,

    #[regex(r"//[^\n\r]*", priority = 2)]
    Comment,

    #[token(r"(")]
    ParenOpen,

    #[token(r")")]
    ParenClose,

    #[token(r"{")]
    CurlyOpen,

    #[token(r"}")]
    CurlyClose,

    #[token(r"[")]
    SquareOpen,

    #[token(r"]")]
    SquareClose,

    #[token(r".")]
    Dot,

    #[token(r",")]
    Comma,

    #[token(r":")]
    Colon,

    #[token(r"?")]
    Question,

    #[token(r"_")]
    Underscore,

    #[token(r"+")]
    Plus,

    #[token(r"-")]
    Minus,

    #[token(r"*")]
    Star,

    #[token(r"/", priority = 3)]
    Div,

    #[token(r"=")]
    Equals,

    #[token(r"<")]
    Lt,

    #[token(r">")]
    Gt,

    #[token(r"|")]
    Pipe,

    #[token(r"..")]
    DotDot,

    #[token(r"=>")]
    FatArrow,

    #[token("use")]
    KwUse,

    #[token("def")]
    KwDef,

    #[token("rel")]
    KwRel,

    #[token("fmt")]
    KwFmt,

    #[token("map")]
    KwMap,

    #[regex(r"@[a-zA-Z_]*")]
    Modifier,

    #[regex(r"-?[0-9]+(\.[0-9]+)?")]
    Number,

    #[regex(r#"""#, lex_double_quote_text)]
    DoubleQuoteText,

    #[regex(r"'", lex_single_quote_text)]
    SingleQuoteText,

    #[regex(r"/[^ /]", lex_regex, priority = 1)]
    Regex,

    #[regex(
        // note: may not start with a number
        // may not start with `-` or `_` or any other token that may be a sigil
        // may contain `-` or `_` internally for kebab/snake-case naming
        r##"[^ \t\r\n_\(\)\{\}\[\]\.+\-\?\*,=<>\|'"/:;@0-9][^ \t\r\n\(\)\{\}\[\]\.\?\*,+=<>\|"'/:;]*"##,
    )]
    Sym,

    //
    // Syntax-level nodes:
    //
    /// use statement
    UseStatement,

    /// def statement
    DefStatement,
    DefBody,

    /// rel statement
    RelStatement,
    RelFwdSet,
    RelBackwdSet,
    Relation,
    RelSubject,
    RelObject,
    RelParams,
    PropCardinality,

    /// fmt statement
    FmtStatement,

    /// map statement
    MapStatement,
    MapArm,

    // types
    /// `x` in `rel x 'foo': y`
    TypeModUnit,
    /// `{x}` in `rel {x} 'foo': y`
    TypeModSet,
    /// `[x]` in `rel [x] 'foo': y`
    TypeModList,
    /// `.` in `rel .'foo': y`
    This,
    /// span of any literal value
    Literal,
    /// `1..2`
    NumberRange,
    /// `1` in `1..2`
    RangeStart,
    /// `2` in `1..2`
    RangeEnd,
    /// `'file'` in `use 'file' as foo`
    Name,
    /// `foo`, `foo.bar`
    IdentPath,

    // patterns
    /// `struct(..)`
    PatStruct,
    /// `set { }`
    PatSet,
    /// `variable`, `42`, `'text'`, `/regex/`
    PatAtom,
    /// `a + b`
    PatBinary,

    // pattern elements
    /// `'prop': x` in `struct('prop': x)`
    StructParamAttrProp,
    /// `unit` in `struct(unit)`
    StructParamAttrUnit,
    /// ['a': b] in `struct('prop'['a': b]: x)`
    RelArgs,
    /// `..a` in `{ ..a }`
    SetElement,

    // container for `..`
    Spread,

    // Synthetic token representing end of file
    Eof,
    // Synthetic node representing a grammatical error
    Error,

    /// The root node. This should be the last entry in the enum.
    Ontol,
}

fn lex_double_quote_text(lexer: &mut Lexer<Kind>) -> Option<()> {
    let remainder: &str = lexer.remainder();
    let len = terminate::<'"'>(remainder.chars())?;
    lexer.bump(remainder[0..len].as_bytes().len());
    Some(())
}

fn lex_single_quote_text(lexer: &mut Lexer<Kind>) -> Option<()> {
    let remainder: &str = lexer.remainder();
    let len = terminate::<'\''>(remainder.chars())?;
    lexer.bump(remainder[0..len].as_bytes().len());
    Some(())
}

// Note: The leading regex already lexed `/` and another character
fn lex_regex(lexer: &mut Lexer<Kind>) -> Option<()> {
    let remainder: &str = lexer.remainder();

    let len = terminate::<'/'>(lexer.slice().chars().skip(1).chain(remainder.chars()))?;
    lexer.bump(remainder[0..len - 1].as_bytes().len());
    Some(())
}

fn terminate<const END: char>(it: impl Iterator<Item = char>) -> Option<usize> {
    let mut total_len = 0;
    let mut escaped = false;

    for c in it {
        total_len += c.len_utf8();

        if c == '\\' {
            escaped = !escaped;
            continue;
        }

        if c == END && !escaped {
            return Some(total_len);
        }

        escaped = false;
    }
    None
}

impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Kind::Whitespace => write!(f, "whitespace"),
            Kind::DocComment => write!(f, "doc comment"),
            Kind::Comment => write!(f, "comment"),
            K!['('] => write!(f, "`(`"),
            K![')'] => write!(f, "`)`"),
            K!['{'] => write!(f, "`{{`"),
            K!['}'] => write!(f, "`}}`"),
            K!['['] => write!(f, "`[`"),
            K![']'] => write!(f, "`]`"),
            K![.] => write!(f, "`.`"),
            K![,] => write!(f, "`,`"),
            K![:] => write!(f, "`:`"),
            K![?] => write!(f, "`?`"),
            K![_] => write!(f, "`_`"),
            K![+] => write!(f, "`+`"),
            K![-] => write!(f, "`-`"),
            K![*] => write!(f, "`*`"),
            K![/] => write!(f, "`/`"),
            K![=] => write!(f, "`=`"),
            K![<] => write!(f, "`<`"),
            K![>] => write!(f, "`>`"),
            K![|] => write!(f, "`|`"),
            K![..] => write!(f, "`..`"),
            K![=>] => write!(f, "`=>`"),
            K![use] => write!(f, "`use`"),
            K![def] => write!(f, "`def`"),
            K![rel] => write!(f, "`rel`"),
            K![fmt] => write!(f, "`fmt`"),
            K![map] => write!(f, "`map`"),
            Kind::Modifier => write!(f, "modifier"),
            Kind::Number => write!(f, "number"),
            Kind::DoubleQuoteText => write!(f, "text literal"),
            Kind::SingleQuoteText => write!(f, "text literal"),
            Kind::Regex => write!(f, "regex"),
            Kind::Sym => write!(f, "symbol"),
            Kind::Eof => write!(f, "end of file"),
            Kind::Error => write!(f, "error"),
            Kind::Ontol => write!(f, "ontol"),
            Kind::UseStatement => write!(f, "use statement"),
            Kind::DefStatement => write!(f, "def statement"),
            Kind::DefBody => write!(f, "def body"),
            Kind::RelStatement => write!(f, "rel statement"),
            Kind::RelFwdSet => write!(f, "rel forward set"),
            Kind::RelBackwdSet => write!(f, "rel backward set"),
            Kind::Relation => write!(f, "relation"),
            Kind::RelSubject => write!(f, "subject"),
            Kind::RelObject => write!(f, "object"),
            Kind::RelParams => write!(f, "relation parameters"),
            Kind::PropCardinality => write!(f, "property cardinality"),
            Kind::FmtStatement => write!(f, "fmt statement"),
            Kind::MapStatement => write!(f, "map statement"),
            Kind::MapArm => write!(f, "map arm"),
            Kind::TypeModUnit => write!(f, "unit type modifier"),
            Kind::TypeModSet => write!(f, "set type modifier"),
            Kind::TypeModList => write!(f, "list type modifier"),
            Kind::This => write!(f, "this"),
            Kind::Literal => write!(f, "literal"),
            Kind::NumberRange => write!(f, "number range"),
            Kind::RangeStart => write!(f, "start of range"),
            Kind::RangeEnd => write!(f, "end of range"),
            Kind::Name => write!(f, "name"),
            Kind::IdentPath => write!(f, "ident path"),
            Kind::PatStruct => write!(f, "struct pattern"),
            Kind::PatSet => write!(f, "set pattern"),
            Kind::PatAtom => write!(f, "atom patterm"),
            Kind::PatBinary => write!(f, "binary pattern"),
            Kind::StructParamAttrProp => write!(f, "property attribute"),
            Kind::StructParamAttrUnit => write!(f, "unit attribute"),
            Kind::RelArgs => write!(f, "relation arguments"),
            Kind::SetElement => write!(f, "set element"),
            Kind::Spread => write!(f, "spread"),
        }
    }
}

#[macro_export]
macro_rules! K {
    [use] => {
        Kind::KwUse
    };
    [def] => {
        Kind::KwDef
    };
    [rel] => {
        Kind::KwRel
    };
    [fmt] => {
        Kind::KwFmt
    };
    [map] => {
        Kind::KwMap
    };
    ['('] => {
        Kind::ParenOpen
    };
    [')'] => {
        Kind::ParenClose
    };
    ['{'] => {
        Kind::CurlyOpen
    };
    ['}'] => {
        Kind::CurlyClose
    };
    ['['] => {
        Kind::SquareOpen
    };
    [']'] => {
        Kind::SquareClose
    };
    [:] => {
        Kind::Colon
    };
    [|] => {
        Kind::Pipe
    };
    [.] => {
        Kind::Dot
    };
    [..] => {
        Kind::DotDot
    };
    [,] => {
        Kind::Comma
    };
    [?] => {
        Kind::Question
    };
    [+] => {
        Kind::Plus
    };
    [-] => {
        Kind::Minus
    };
    [*] => {
        Kind::Star
    };
    [/] => {
        Kind::Div
    };
    [=] => {
        Kind::Equals
    };
    [_] => {
        Kind::Underscore
    };
    [<] => {
        Kind::Lt
    };
    [>] => {
        Kind::Gt
    };
    [=>] => {
        Kind::FatArrow
    };
}

#[cfg(test)]
mod tests {
    use super::Kind::*;
    use super::*;

    fn lex(input: &str) -> Result<Vec<Kind>, usize> {
        let mut tokens = vec![];
        let mut error_count = 0;

        for result in Kind::lexer(input) {
            match result {
                Ok(kind) => {
                    tokens.push(kind);
                }
                Err(_error) => {
                    error_count += 1;
                }
            }
        }

        if error_count > 0 {
            Err(error_count)
        } else {
            Ok(tokens)
        }
    }

    #[track_caller]
    fn lex_ok(input: &str) -> Vec<Kind> {
        lex(input).unwrap()
    }

    #[test]
    fn test_empty() {
        lex_ok("");
    }

    #[test]
    fn test_whitespace() {
        assert_eq!(lex_ok(" "), vec![Whitespace]);
    }

    #[test]
    fn test_comment() {
        assert_eq!(lex_ok("  // comment"), &[Whitespace, Comment]);
    }

    #[test]
    fn test_paren() {
        assert_eq!(lex_ok("()"), &[ParenOpen, ParenClose]);
    }

    #[test]
    fn test_doc_comment() {
        let source = "
        ///      Over here
        def @open PubType {}
        ";
        assert_eq!(
            lex_ok(source),
            &[
                Whitespace, DocComment, Whitespace, KwDef, Whitespace, Modifier, Whitespace, Sym,
                Whitespace, CurlyOpen, CurlyClose, Whitespace
            ]
        );
    }

    #[test]
    fn test_integer() {
        assert_eq!(&lex_ok("42"), &[Number]);
        assert_eq!(&lex_ok("-42"), &[Number]);
        assert_eq!(&lex_ok("--42"), &[Minus, Number]);
        assert_eq!(&lex_ok("42x"), &[Number, Sym]);
        assert_eq!(&lex_ok("42 "), &[Number, Whitespace]);
        assert_eq!(&lex_ok("4-2"), &[Number, Number]);
        assert_eq!(&lex_ok("4--2"), &[Number, Minus, Number]);
    }

    #[test]
    fn test_decimal() {
        assert_eq!(&lex_ok(".42"), &[Dot, Number]);
        assert_eq!(&lex_ok("42."), &[Number, Dot]);
        assert_eq!(&lex_ok("4.2"), &[Number]);
        assert_eq!(&lex_ok("42.42"), &[Number]);
        assert_eq!(&lex_ok("-0.42"), &[Number]);
        assert_eq!(&lex_ok("-.42"), &[Minus, Dot, Number]);
        assert_eq!(&lex_ok("4..2"), &[Number, DotDot, Number]);
    }

    #[test]
    fn test_regex() {
        assert_eq!(&lex_ok(r"/"), &[Div]);
        assert_eq!(&lex_ok(r"/ "), &[Div, Whitespace]);
        assert_eq!(&lex_ok("//"), &[Comment]);
        assert_eq!(&lex_ok(r"/ /"), &[Div, Whitespace, Div]);
        assert_eq!(&lex_ok(r"/\ /"), &[Regex]);
        assert_eq!(&lex_ok(r"/\  /"), &[Regex]);
        assert_eq!(&lex_ok(r"/\//"), &[Regex]);
        assert_eq!(&lex_ok(r"/a/"), &[Regex]);
        assert_eq!(&lex_ok(r"/Hello (?<name>\w+)!/"), &[Regex]);
    }

    #[test]
    fn test_sym() {
        assert_eq!(&lex_ok("abc"), &[Sym]);
        assert_eq!(&lex_ok("a0"), &[Sym]);
        assert_eq!(&lex_ok("a-b"), &[Sym]);
        assert_eq!(&lex_ok("a_b"), &[Sym]);
        assert_eq!(&lex_ok("a b"), &[Sym, Whitespace, Sym]);
    }

    #[test]
    fn test_text() {
        assert_eq!(&lex_ok("'abc'"), &[SingleQuoteText]);
        assert_eq!(&lex_ok("'abc' "), &[SingleQuoteText, Whitespace]);
        assert_eq!(&lex_ok("'abc'? "), &[SingleQuoteText, Question, Whitespace]);
    }
}
