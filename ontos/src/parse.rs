use ontol_runtime::vm::proc::BuiltinProc;

use crate::{
    kind::{MatchArm, NodeKind, PatternBinding, PropPattern, PropVariant},
    Binder, Lang, Variable,
};

#[derive(Debug)]
pub enum Error<'s> {
    Unexpected(Class),
    Expected(Class),
    InvalidChar(char),
    InvalidToken(Token<'s>),
    InvalidIdent(&'s str),
    InvalidNumber,
    InvalidVariableNumber,
}

#[derive(Debug)]
pub enum Class {
    Eof,
    LParen,
    RParen,
    Ident,
    Hash,
    Property,
}

#[derive(Debug)]
pub enum Token<'s> {
    LParen,
    RParen,
    Ident(&'s str),
    Int(i64),
    Hash,
    Underscore,
}

type ParseResult<'s, T> = Result<(T, &'s str), Error<'s>>;

pub struct Parser<L: Lang> {
    lang: L,
}

impl<L: Lang> Parser<L> {
    pub fn new(lang: L) -> Self {
        Self { lang }
    }

    fn make_node<'a>(&self, kind: NodeKind<'a, L>) -> L::Node<'a> {
        self.lang.make_node(kind)
    }

    pub fn parse<'a, 's>(&self, next: &'s str) -> ParseResult<'s, L::Node<'a>> {
        let next = next.trim_start();
        match parse_token(next)? {
            (Token::LParen, next) => {
                let (node, next) = match parse_ident(next)? {
                    ("+", next) => self.parse_binary_call(BuiltinProc::Add, next)?,
                    ("-", next) => self.parse_binary_call(BuiltinProc::Sub, next)?,
                    ("*", next) => self.parse_binary_call(BuiltinProc::Mul, next)?,
                    ("/", next) => self.parse_binary_call(BuiltinProc::Div, next)?,
                    ("let", next) => {
                        let (_, next) = parse_lparen(next)?;
                        let (bind_var, next) = parse_hash_var(next)?;
                        let (def, next) = self.parse(next)?;
                        let (_, next) = parse_rparen(next)?;
                        let (body, next) = self.parse_many(next, Self::parse)?;
                        (
                            self.make_node(NodeKind::Let(Binder(bind_var), Box::new(def), body)),
                            next,
                        )
                    }
                    ("struct", next) => {
                        let (binder, next) = parse_binder(next)?;
                        let (children, next) = self.parse_many(next, Self::parse)?;
                        (self.make_node(NodeKind::Struct(binder, children)), next)
                    }
                    ("prop", next) => {
                        let (var, next) = parse_hash_var(next)?;
                        let (prop, next) = parse_ident(next)?;
                        let (_, next) = parse_lparen(next)?;
                        let (rel, next) = self.parse(next)?;
                        let (value, next) = self.parse(next)?;
                        let (_, next) = parse_rparen(next)?;
                        (
                            self.make_node(NodeKind::Prop(
                                var,
                                prop.parse().map_err(|_| Error::Expected(Class::Property))?,
                                PropVariant {
                                    rel: Box::new(rel),
                                    val: Box::new(value),
                                },
                            )),
                            next,
                        )
                    }
                    ("seq", next) => {
                        let (binder, next) = parse_binder(next)?;
                        let (children, next) = self.parse_many(next, Self::parse)?;
                        (self.make_node(NodeKind::Seq(binder, children)), next)
                    }
                    ("match-prop", next) => {
                        let (struct_var, next) = parse_hash_var(next)?;
                        let (prop, next) = parse_ident(next)?;
                        let (arms, next) = self.parse_many(next, Self::parse_prop_match_arm)?;
                        (
                            self.make_node(NodeKind::MatchProp(
                                struct_var,
                                prop.parse().map_err(|_| Error::Expected(Class::Property))?,
                                arms,
                            )),
                            next,
                        )
                    }
                    ("map-seq", next) => {
                        let (var, next) = parse_hash_var(next)?;
                        let (binder, next) = parse_binder(next)?;
                        let (children, next) = self.parse_many(next, Self::parse)?;
                        (
                            self.make_node(NodeKind::MapSeq(var, binder, children)),
                            next,
                        )
                    }
                    (ident, _) => return Err(Error::InvalidIdent(ident)),
                };
                let (_, next) = parse_rparen(next)?;
                Ok((node, next))
            }
            (Token::Hash, next) => match parse_token(next)? {
                (Token::Int(int), next) => {
                    Ok((self.make_node(NodeKind::VariableRef(mk_var(int)?)), next))
                }
                (Token::Ident(ident), next) => match ident {
                    "u" => Ok((self.make_node(NodeKind::Unit), next)),
                    other => Err(Error::InvalidIdent(other)),
                },
                (token, _) => Err(Error::InvalidToken(token)),
            },
            (Token::Int(num), next) => Ok((self.make_node(NodeKind::Int(num)), next)),
            (token, _) => Err(Error::InvalidToken(token)),
        }
    }

    fn parse_many<'p, 's, T>(
        &'p self,
        mut next: &'s str,
        item_fn: impl Fn(&'p Self, &'s str) -> ParseResult<'s, T>,
    ) -> ParseResult<'s, Vec<T>> {
        let mut nodes = vec![];
        loop {
            match item_fn(self, next) {
                Ok((node, next_next)) => {
                    next = next_next;
                    nodes.push(node);
                }
                Err(Error::Unexpected(Class::Eof | Class::RParen)) => {
                    return Ok((nodes, next));
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn parse_prop_match_arm<'a, 's>(&self, next: &'s str) -> ParseResult<'s, MatchArm<'a, L>> {
        let (_, next) = parse_lparen(next)?;
        let (_, next) = parse_lparen(next)?;
        let (pattern, next) = match self.parse_pattern_binding(next) {
            Ok((rel_binding, next)) => {
                let (val_binding, next) = self.parse_pattern_binding(next)?;
                let (_, next) = parse_rparen(next)?;

                (PropPattern::Present(rel_binding, val_binding), next)
            }
            Err(Error::Unexpected(Class::RParen)) => {
                let (_, next) = parse_rparen(next)?;
                (PropPattern::NotPresent, next)
            }
            Err(other) => return Err(other),
        };
        let (nodes, next) = self.parse_many(next, Self::parse)?;
        let (_, next) = parse_rparen(next)?;

        Ok((MatchArm { pattern, nodes }, next))
    }

    fn parse_pattern_binding<'s>(&self, next: &'s str) -> ParseResult<'s, PatternBinding> {
        let (_, next) = parse_hash(next)?;
        match parse_token(next)? {
            (Token::Int(int), next) => Ok((PatternBinding::Binder(mk_var(int)?), next)),
            (Token::Underscore, next) => Ok((PatternBinding::Wildcard, next)),
            (token, _) => Err(Error::InvalidToken(token)),
        }
    }

    fn parse_binary_call<'a, 's>(
        &self,
        proc: BuiltinProc,
        next: &'s str,
    ) -> ParseResult<'s, L::Node<'a>> {
        let (a, next) = self.parse(next)?;
        let (b, next) = self.parse(next)?;

        Ok((self.make_node(NodeKind::Call(proc, [a, b].into())), next))
    }
}

fn parse_ident(next: &str) -> ParseResult<'_, &str> {
    match parse_token(next)? {
        (Token::Ident(ident), next) => Ok((ident, next)),
        _ => Err(Error::Expected(Class::Ident)),
    }
}

fn parse_hash(next: &str) -> ParseResult<'_, ()> {
    match parse_token(next)? {
        (Token::Hash, next) => Ok(((), next)),
        _ => Err(Error::Expected(Class::Hash)),
    }
}

fn parse_hash_var(next: &str) -> ParseResult<'_, Variable> {
    let (_, next) = parse_hash(next)?;
    match parse_token(next)? {
        (Token::Int(int), next) => Ok((mk_var(int)?, next)),
        (token, _) => Err(Error::InvalidToken(token)),
    }
}

fn parse_binder(next: &str) -> ParseResult<'_, Binder> {
    let (_, next) = parse_lparen(next)?;
    let (var, next) = parse_hash_var(next)?;
    let (_, next) = parse_rparen(next)?;
    Ok((Binder(var), next))
}

fn parse_lparen(next: &str) -> ParseResult<'_, ()> {
    match parse_token(next)? {
        (Token::LParen, next) => Ok(((), next)),
        _ => Err(Error::Expected(Class::LParen)),
    }
}

fn parse_rparen(next: &str) -> ParseResult<'_, ()> {
    match parse_raw_token(next)? {
        (Token::RParen, next) => Ok(((), next)),
        _ => Err(Error::Expected(Class::RParen)),
    }
}

fn parse_token(next: &str) -> ParseResult<'_, Token<'_>> {
    match parse_raw_token(next)? {
        (Token::RParen, _) => Err(Error::Unexpected(Class::RParen)),
        (token, next) => Ok((token, next)),
    }
}

fn parse_raw_token(next: &str) -> ParseResult<'_, Token<'_>> {
    let next = next.trim_start();
    let mut chars = next.char_indices();

    match chars.next() {
        None => Err(Error::Unexpected(Class::Eof)),
        Some((_, '(')) => Ok((Token::LParen, chars.as_str())),
        Some((_, ')')) => Ok((Token::RParen, chars.as_str())),
        Some((_, '#')) => Ok((Token::Hash, chars.as_str())),
        Some((_, '_')) => Ok((Token::Underscore, chars.as_str())),
        Some((_, char)) if char.is_numeric() => {
            for (index, char) in chars.by_ref() {
                if !char.is_numeric() {
                    return Ok((parse_int(&next[0..index])?, &next[index..]));
                }
            }

            Ok((parse_int(next)?, chars.as_str()))
        }
        Some((_, char)) if char.is_ascii() => {
            for (index, char) in chars.by_ref() {
                if char.is_whitespace() || char == '(' || char == ')' {
                    return Ok((Token::Ident(&next[0..index]), &next[index..]));
                }
            }

            Ok((Token::Ident(next), chars.as_str()))
        }
        Some((_, char)) => Err(Error::InvalidChar(char)),
    }
}

fn parse_int(num: &str) -> Result<Token<'_>, Error<'_>> {
    let result: Result<i64, _> = num.parse();
    match result {
        Ok(num) => Ok(Token::Int(num)),
        Err(_) => Err(Error::InvalidNumber),
    }
}

fn mk_var(num: i64) -> Result<Variable, Error<'static>> {
    let num: u32 = num.try_into().map_err(|_| Error::InvalidVariableNumber)?;
    Ok(Variable(num))
}
