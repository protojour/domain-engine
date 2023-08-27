use std::str::FromStr;

use ontol_runtime::vm::proc::BuiltinProc;

use crate::*;

impl FromStr for Var {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        try_alpha_to_u32(s).map(Var).map_err(|_| ())
    }
}

#[derive(Debug)]
pub enum Error<'s> {
    Unexpected(Class),
    Expected(Class, Found<Token<'s>>),
    InvalidChar(char),
    InvalidToken(Token<'s>),
    InvalidSymbol(&'s str),
    InvalidNumber,
    InvalidVariableNumber,
}

#[derive(Debug)]
pub struct Found<T>(pub T);

#[derive(Debug)]
pub enum Class {
    Eof,
    LParen,
    RParen,
    Symbol,
    Seq,
    Dollar,
    Hash,
    At,
    Property,
}

#[derive(Debug)]
pub enum Token<'s> {
    LParen,
    RParen,
    Symbol(&'s str),
    I64(i64),
    F64(f64),
    Dollar,
    Hash,
    At,
    Underscore,
    Questionmark,
}

type ParseResult<'s, T> = Result<(T, &'s str), Error<'s>>;

pub struct Parser<L: Lang> {
    lang: L,
}

impl<L: Lang> Parser<L> {
    pub fn new(lang: L) -> Self {
        Self { lang }
    }

    pub fn parse<'a, 's>(&self, next: &'s str) -> ParseResult<'s, L::Node<'a>> {
        match parse_token(next)? {
            (Token::LParen, next) => {
                let (node, next) = self.parse_parenthesized(next)?;
                let (_, next) = parse_rparen(next)?;
                Ok((node, next))
            }
            (Token::Dollar, next) => match parse_token(next)? {
                (Token::Symbol(sym), next) => {
                    Ok((self.make_node(Kind::Var(Var(try_alpha_to_u32(sym)?))), next))
                }
                (token, _) => Err(Error::InvalidToken(token)),
            },
            (Token::Hash, next) => match parse_token(next)? {
                (Token::Symbol(sym), next) => match sym {
                    "u" => Ok((self.make_node(Kind::Unit), next)),
                    sym => Err(Error::InvalidSymbol(sym)),
                },
                (token, _) => Err(Error::InvalidToken(token)),
            },
            (Token::I64(num), next) => Ok((self.make_node(Kind::I64(num)), next)),
            (Token::F64(num), next) => Ok((self.make_node(Kind::F64(num)), next)),
            (token, _) => Err(Error::InvalidToken(token)),
        }
    }

    pub fn parse_parenthesized<'a, 's>(&self, next: &'s str) -> ParseResult<'s, L::Node<'a>> {
        match parse_symbol(next)? {
            ("+", next) => self.parse_binary_call(BuiltinProc::Add, next),
            ("-", next) => self.parse_binary_call(BuiltinProc::Sub, next),
            ("*", next) => self.parse_binary_call(BuiltinProc::Mul, next),
            ("/", next) => self.parse_binary_call(BuiltinProc::Div, next),
            ("let", next) => {
                let (_, next) = parse_lparen(next)?;
                let (bind_var, next) = parse_dollar_var(next)?;
                let (def, next) = self.parse(next)?;
                let (_, next) = parse_rparen(next)?;
                let (body, next) = self.parse_many(next, Self::parse)?;
                Ok((
                    self.make_node(Kind::Let(self.make_binder(bind_var), Box::new(def), body)),
                    next,
                ))
            }
            ("map", next) => {
                let (arg, next) = self.parse(next)?;
                Ok((self.make_node(Kind::Map(Box::new(arg))), next))
            }
            ("struct", next) => self.parse_struct_inner(next, StructFlags::empty()),
            ("match-struct", next) => self.parse_struct_inner(next, StructFlags::MATCH),
            ("prop", next) => self.parse_prop(Optional(false), next),
            ("prop?", next) => self.parse_prop(Optional(true), next),
            ("decl-seq", next) => {
                let (_, next) = parse_lparen(next)?;
                let (label, next) = parse_at_label(next)?;
                let (_, next) = parse_rparen(next)?;
                let (rel, next) = self.parse(next)?;
                let (val, next) = self.parse(next)?;
                Ok((
                    self.make_node(Kind::DeclSeq(
                        self.make_label(label),
                        Attribute {
                            rel: Box::new(rel),
                            val: Box::new(val),
                        },
                    )),
                    next,
                ))
            }
            ("match-prop", next) => {
                let (struct_var, next) = parse_dollar_var(next)?;
                let (prop, next) = parse_symbol(next)?;
                let (arms, next) = self.parse_many(next, Self::parse_prop_match_arm)?;
                Ok((
                    self.make_node(Kind::MatchProp(
                        struct_var,
                        prop.parse().map_err(|_| {
                            Error::Expected(Class::Property, Found(Token::Symbol(prop)))
                        })?,
                        arms,
                    )),
                    next,
                ))
            }
            ("sequence", next) => {
                let (binder, next) = self.parse_binder(next)?;
                let (children, next) = self.parse_many(next, Self::parse)?;
                Ok((self.make_node(Kind::Sequence(binder, children)), next))
            }
            ("for-each", next) => {
                let (seq_var, next) = parse_dollar_var(next)?;
                let ((rel, val), next) = parse_paren_delimited(next, |next| {
                    let (rel, next) = self.parse_pattern_binding(next)?;
                    let (val, next) = self.parse_pattern_binding(next)?;
                    Ok(((rel, val), next))
                })?;
                let (body, next) = self.parse_many(next, Self::parse)?;
                Ok((
                    self.make_node(Kind::ForEach(seq_var, (rel, val), body)),
                    next,
                ))
            }
            ("push", next) => {
                let (seq_var, next) = parse_dollar_var(next)?;
                let (rel, next) = self.parse(next)?;
                let (val, next) = self.parse(next)?;
                Ok((
                    self.make_node(Kind::Push(
                        seq_var,
                        Attribute {
                            rel: Box::new(rel),
                            val: Box::new(val),
                        },
                    )),
                    next,
                ))
            }
            (sym, _) => Err(Error::InvalidSymbol(sym)),
        }
    }

    pub fn parse_prop<'a, 's>(
        &self,
        optional: Optional,
        next: &'s str,
    ) -> ParseResult<'s, L::Node<'a>> {
        let (var, next) = parse_dollar_var(next)?;
        let (prop, next) = parse_symbol(next)?;
        let (variants, next) = self.parse_many(next, Self::parse_prop_variant)?;
        Ok((
            self.make_node(Kind::Prop(
                optional,
                var,
                prop.parse()
                    .map_err(|_| Error::Expected(Class::Property, Found(Token::Symbol(prop))))?,
                variants,
            )),
            next,
        ))
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

    fn parse_struct_inner<'a, 's>(
        &self,
        next: &'s str,
        flags: StructFlags,
    ) -> ParseResult<'s, L::Node<'a>> {
        let (binder, next) = self.parse_binder(next)?;
        let (children, next) = self.parse_many(next, Self::parse)?;
        Ok((self.make_node(Kind::Struct(binder, flags, children)), next))
    }

    fn parse_prop_variant<'a, 's>(&self, next: &'s str) -> ParseResult<'s, PropVariant<'a, L>> {
        parse_paren_delimited(next, |next| match parse_symbol(next) {
            Ok((sym @ ("seq" | "seq-default"), next)) => {
                let (label, next) = parse_paren_delimited(next, parse_at_label)?;
                let (elements, next) = self.parse_many(next, Self::parse_seq_property_element)?;
                Ok((
                    PropVariant::Seq(SeqPropertyVariant {
                        label: self.make_label(label),
                        has_default: HasDefault(sym != "seq"),
                        elements,
                    }),
                    next,
                ))
            }
            Ok((sym, _)) => return Err(Error::Expected(Class::Seq, Found(Token::Symbol(sym)))),
            Err(_) => {
                let (rel, next) = self.parse(next)?;
                let (value, next) = self.parse(next)?;

                Ok((
                    PropVariant::Singleton(Attribute {
                        rel: Box::new(rel),
                        val: Box::new(value),
                    }),
                    next,
                ))
            }
        })
    }

    fn parse_seq_property_element<'a, 's>(
        &self,
        next: &'s str,
    ) -> ParseResult<'s, SeqPropertyElement<'a, L>> {
        parse_paren_delimited(next, |next| {
            let (iter, next) = match parse_symbol(next) {
                Ok(("iter", next)) => (true, next),
                Ok((sym, _)) => {
                    return Err(Error::Expected(Class::Symbol, Found(Token::Symbol(sym))))
                }
                Err(_) => (false, next),
            };

            let (rel, next) = self.parse(next)?;
            let (val, next) = self.parse(next)?;

            Ok((
                SeqPropertyElement {
                    iter,
                    attribute: Attribute { rel, val },
                },
                next,
            ))
        })
    }

    fn parse_prop_match_arm<'a, 's>(&self, next: &'s str) -> ParseResult<'s, MatchArm<'a, L>> {
        parse_paren_delimited(next, |next| {
            let (_, next) = parse_lparen(next)?;
            let (seq_default, next) = match parse_symbol(next) {
                Ok(("seq", next)) => (Some(HasDefault(false)), next),
                Ok(("seq-default", next)) => (Some(HasDefault(true)), next),
                Ok((sym, _)) => return Err(Error::Expected(Class::Seq, Found(Token::Symbol(sym)))),
                _ => (None, next),
            };
            let (pattern, next) = match self.parse_pattern_binding(next) {
                Ok((first_binding, next)) => {
                    match (self.parse_pattern_binding(next), seq_default) {
                        (Ok((val_binding, next)), seq_default) => {
                            let (_, next) = parse_rparen(next)?;
                            (
                                if seq_default.is_some() {
                                    return Err(Error::Unexpected(Class::Seq));
                                } else {
                                    PropPattern::Attr(first_binding, val_binding)
                                },
                                next,
                            )
                        }
                        (Err(Error::Unexpected(Class::RParen)), Some(has_default)) => {
                            let (_, next) = parse_rparen(next)?;
                            (PropPattern::Seq(first_binding, has_default), next)
                        }
                        (Err(err), _) => return Err(err),
                    }
                }
                Err(Error::Unexpected(Class::RParen)) => {
                    let (_, next) = parse_rparen(next)?;
                    (PropPattern::Absent, next)
                }
                Err(other) => return Err(other),
            };
            let (nodes, next) = self.parse_many(next, Self::parse)?;

            Ok((MatchArm { pattern, nodes }, next))
        })
    }

    fn parse_binary_call<'a, 's>(
        &self,
        proc: BuiltinProc,
        next: &'s str,
    ) -> ParseResult<'s, L::Node<'a>> {
        let (a, next) = self.parse(next)?;
        let (b, next) = self.parse(next)?;

        Ok((self.make_node(Kind::Call(proc, [a, b].into())), next))
    }

    fn parse_binder<'a, 's>(&self, next: &'s str) -> ParseResult<'s, L::Binder<'a>> {
        parse_paren_delimited(next, |next| {
            let (var, next) = parse_dollar_var(next)?;
            Ok((self.make_binder(var), next))
        })
    }

    fn parse_pattern_binding<'a, 's>(&self, next: &'s str) -> ParseResult<'s, Binding<'a, L>> {
        let (_, next) = parse_dollar(next)?;
        match parse_token(next)? {
            (Token::Symbol(sym), next) => Ok((
                Binding::Binder(self.make_binder(Var(try_alpha_to_u32(sym)?))),
                next,
            )),
            (Token::Underscore, next) => Ok((Binding::Wildcard, next)),
            (token, _) => Err(Error::InvalidToken(token)),
        }
    }

    fn make_node<'a>(&self, kind: Kind<'a, L>) -> L::Node<'a> {
        self.lang.make_node(kind)
    }

    fn make_binder<'a>(&self, var: Var) -> L::Binder<'a> {
        self.lang.make_binder(var)
    }

    fn make_label<'a>(&self, label: Label) -> L::Label<'a> {
        self.lang.make_label(label)
    }
}

fn parse_symbol(next: &str) -> ParseResult<&str> {
    match parse_token(next)? {
        (Token::Symbol(sym), next) => Ok((sym, next)),
        (token, _) => Err(Error::Expected(Class::Symbol, Found(token))),
    }
}

fn parse_dollar(next: &str) -> ParseResult<()> {
    match parse_token(next)? {
        (Token::Dollar, next) => Ok(((), next)),
        (token, _) => Err(Error::Expected(Class::Dollar, Found(token))),
    }
}

fn parse_dollar_var(next: &str) -> ParseResult<Var> {
    let (_, next) = parse_dollar(next)?;
    match parse_token(next)? {
        (Token::Symbol(sym), next) => Ok((Var(try_alpha_to_u32(sym)?), next)),
        (token, _) => Err(Error::InvalidToken(token)),
    }
}

/// Parse `(`, then closure, then `)`
fn parse_paren_delimited<'s, T>(
    next: &'s str,
    item_fn: impl Fn(&'s str) -> ParseResult<'s, T>,
) -> ParseResult<'s, T> {
    let (_, next) = parse_lparen(next)?;
    let (value, next) = item_fn(next)?;
    let (_, next) = parse_rparen(next)?;
    Ok((value, next))
}

fn parse_at_label(next: &str) -> ParseResult<Label> {
    let next = match parse_token(next)? {
        (Token::At, next) => next,
        (token, _) => return Err(Error::Expected(Class::At, Found(token))),
    };
    match parse_token(next)? {
        (Token::Symbol(sym), next) => Ok((Label(try_alpha_to_u32(sym)?), next)),
        (token, _) => Err(Error::InvalidToken(token)),
    }
}

fn parse_lparen(next: &str) -> ParseResult<()> {
    match parse_token(next)? {
        (Token::LParen, next) => Ok(((), next)),
        (token, _) => Err(Error::Expected(Class::LParen, Found(token))),
    }
}

fn parse_rparen(next: &str) -> ParseResult<()> {
    match parse_raw_token(next)? {
        (Token::RParen, next) => Ok(((), next)),
        (token, _) => Err(Error::Expected(Class::RParen, Found(token))),
    }
}

fn parse_token(next: &str) -> ParseResult<Token> {
    match parse_raw_token(next)? {
        (Token::RParen, _) => Err(Error::Unexpected(Class::RParen)),
        (token, next) => Ok((token, next)),
    }
}

fn parse_raw_token(next: &str) -> ParseResult<Token> {
    let next = next.trim_start();
    let mut chars = next.char_indices();

    match chars.next() {
        None => Err(Error::Unexpected(Class::Eof)),
        Some((_, '(')) => Ok((Token::LParen, chars.as_str())),
        Some((_, ')')) => Ok((Token::RParen, chars.as_str())),
        Some((_, '$')) => Ok((Token::Dollar, chars.as_str())),
        Some((_, '#')) => Ok((Token::Hash, chars.as_str())),
        Some((_, '@')) => Ok((Token::At, chars.as_str())),
        Some((_, '_')) => Ok((Token::Underscore, chars.as_str())),
        Some((_, char)) if char.is_numeric() => {
            for (index, char) in chars.by_ref() {
                if !char.is_numeric() && char != '.' {
                    return Ok((parse_number(&next[0..index])?, &next[index..]));
                }
            }

            Ok((parse_number(next)?, chars.as_str()))
        }
        Some((_, char)) if char.is_ascii() => {
            for (index, char) in chars.by_ref() {
                if char.is_whitespace() || char == '(' || char == ')' {
                    return Ok((Token::Symbol(&next[0..index]), &next[index..]));
                }
            }

            Ok((Token::Symbol(next), chars.as_str()))
        }
        Some((_, char)) => Err(Error::InvalidChar(char)),
    }
}

fn parse_number(num: &str) -> Result<Token, Error> {
    if num.contains('.') {
        let result: Result<f64, _> = num.parse();
        match result {
            Ok(num) => Ok(Token::F64(num)),
            Err(_) => Err(Error::InvalidNumber),
        }
    } else {
        let result: Result<i64, _> = num.parse();
        match result {
            Ok(num) => Ok(Token::I64(num)),
            Err(_) => Err(Error::InvalidNumber),
        }
    }
}

pub(super) fn try_alpha_to_u32(sym: &str) -> Result<u32, Error> {
    if sym.is_empty() {
        return Err(Error::InvalidSymbol(sym));
    }

    let mut num: u32 = 0;
    let mut iterator = sym.chars().peekable();

    while let Some(char) = iterator.next() {
        if !char.is_ascii_lowercase() {
            return Err(Error::InvalidVariableNumber);
        }

        let value = u32::from(char) - u32::from('a');
        num += value;

        if iterator.peek().is_some() {
            num = (num + 1) * 26;
        }
    }

    Ok(num)
}
