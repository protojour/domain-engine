use ontol_runtime::{vm::proc::BuiltinProc, PackageId};

use crate::*;

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
    Set,
    Dollar,
    Hash,
    At,
    Property,
    Number,
    Colon,
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
    Colon,
}

type ParseResult<'s, T> = Result<(T, &'s str), Error<'s>>;

pub struct Parser<'a, L: Lang> {
    lang: L,
    arena: Arena<'a, L>,
}

impl<'a, L: Lang> Parser<'a, L> {
    pub fn new(lang: L) -> Self {
        Self {
            lang,
            arena: Default::default(),
        }
    }

    pub fn parse_root<'s>(mut self, next: &'s str) -> ParseResult<'s, RootNode<'a, L>> {
        let (root_node, next) = self.parse(next)?;
        Ok((RootNode::new(root_node, self.arena), next))
    }

    fn parse<'s>(&mut self, next: &'s str) -> ParseResult<'s, Node> {
        match parse_token(next)? {
            (Token::LParen, next) => {
                let (node, next) = self.parse_parenthesized(next)?;
                let (_, next) = parse_rparen(next)?;
                Ok((node, next))
            }
            (Token::Dollar, next) => match parse_token(next)? {
                (Token::Symbol(sym), next) => Ok((
                    self.make_node(Kind::Var(Var(parse_alpha_to_u32(sym)?))),
                    next,
                )),
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

    pub fn parse_parenthesized<'s>(&mut self, next: &'s str) -> ParseResult<'s, Node> {
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
                    self.make_node(Kind::Let(self.make_binder(bind_var), def, body.into())),
                    next,
                ))
            }
            ("map", next) => {
                let (arg, next) = self.parse(next)?;
                Ok((self.make_node(Kind::Map(arg)), next))
            }
            ("struct", next) => self.parse_struct_inner(next, StructFlags::empty()),
            ("match-struct", next) => self.parse_struct_inner(next, StructFlags::MATCH),
            ("prop", next) => self.parse_prop(Optional(false), next),
            ("prop?", next) => self.parse_prop(Optional(true), next),
            ("decl-set", next) => {
                let (_, next) = parse_lparen(next)?;
                let (label, next) = parse_at_label(next)?;
                let (_, next) = parse_rparen(next)?;
                let (rel, next) = self.parse(next)?;
                let (val, next) = self.parse(next)?;
                Ok((
                    self.make_node(Kind::DeclSet(
                        self.make_label(label),
                        Attribute { rel, val },
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
                        arms.into(),
                    )),
                    next,
                ))
            }
            ("sequence", next) => {
                let (binder, next) = self.parse_binder(next)?;
                let (children, next) = self.parse_many(next, Self::parse)?;
                Ok((
                    self.make_node(Kind::Sequence(binder, children.into())),
                    next,
                ))
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
                    self.make_node(Kind::ForEach(seq_var, (rel, val), body.into())),
                    next,
                ))
            }
            ("insert", next) => {
                let (seq_var, next) = parse_dollar_var(next)?;
                let (rel, next) = self.parse(next)?;
                let (val, next) = self.parse(next)?;
                Ok((
                    self.make_node(Kind::Insert(seq_var, Attribute { rel, val })),
                    next,
                ))
            }
            (sym @ ("regex" | "regex-seq"), next) => {
                let (label, next) = if sym.contains("seq") {
                    let (label, next) = parse_paren_delimited(next, parse_at_label)?;
                    (Some(self.make_label(label)), next)
                } else {
                    (None, next)
                };
                let (regex_def_id, next) = parse_def_id(next)?;
                let (match_arms, next) = self.parse_many(next, |zelf, next| {
                    parse_paren_delimited(next, |next| {
                        zelf.parse_many(next, Self::parse_capture_group)
                    })
                })?;

                Ok((
                    self.make_node(Kind::Regex(label, regex_def_id, match_arms)),
                    next,
                ))
            }
            (sym @ ("match-regex" | "match-regex-iter"), next) => {
                let iter = Iter(sym.contains("iter"));
                let (text_var, next) = parse_dollar_var(next)?;
                let (regex_def_id, next) = parse_def_id(next)?;
                let (match_arms, next) = self.parse_many(next, Self::parse_capture_match_arm)?;
                Ok((
                    self.make_node(Kind::MatchRegex(iter, text_var, regex_def_id, match_arms)),
                    next,
                ))
            }
            (sym, _) => Err(Error::InvalidSymbol(sym)),
        }
    }

    pub fn parse_prop<'s>(&mut self, optional: Optional, next: &'s str) -> ParseResult<'s, Node> {
        let (var, next) = parse_dollar_var(next)?;
        let (prop, next) = parse_symbol(next)?;
        let (variants, next) = self.parse_many(next, Self::parse_prop_variant)?;
        Ok((
            self.make_node(Kind::Prop(
                optional,
                var,
                prop.parse()
                    .map_err(|_| Error::Expected(Class::Property, Found(Token::Symbol(prop))))?,
                variants.into(),
            )),
            next,
        ))
    }

    fn parse_many<'p, 's, T>(
        &'p mut self,
        mut next: &'s str,
        mut item_fn: impl FnMut(&mut Self, &'s str) -> ParseResult<'s, T>,
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

    fn parse_struct_inner<'s>(
        &mut self,
        next: &'s str,
        flags: StructFlags,
    ) -> ParseResult<'s, Node> {
        let (binder, next) = self.parse_binder(next)?;
        let (children, next) = self.parse_many(next, Self::parse)?;
        Ok((
            self.make_node(Kind::Struct(binder, flags, children.into())),
            next,
        ))
    }

    fn parse_prop_variant<'s>(&mut self, next: &'s str) -> ParseResult<'s, PropVariant<'a, L>> {
        parse_paren_delimited(next, |next| match parse_symbol(next) {
            Ok((sym @ (".." | "..default"), next)) => {
                let (label, next) = parse_paren_delimited(next, parse_at_label)?;
                let (elements, next) = self.parse_many(next, Self::parse_seq_property_element)?;
                Ok((
                    PropVariant::Set(SetPropertyVariant {
                        label: self.make_label(label),
                        has_default: HasDefault(sym != ".."),
                        elements: elements.into(),
                    }),
                    next,
                ))
            }
            Ok((sym, _)) => return Err(Error::Expected(Class::Set, Found(Token::Symbol(sym)))),
            Err(_) => {
                let (rel, next) = self.parse(next)?;
                let (val, next) = self.parse(next)?;

                Ok((PropVariant::Singleton(Attribute { rel, val }), next))
            }
        })
    }

    fn parse_seq_property_element<'s>(
        &mut self,
        next: &'s str,
    ) -> ParseResult<'s, (Iter, Attribute<Node>)> {
        parse_paren_delimited(next, |next| {
            let (iter, next) = match parse_symbol(next) {
                Ok(("iter", next)) => (Iter(true), next),
                Ok((sym, _)) => {
                    return Err(Error::Expected(Class::Symbol, Found(Token::Symbol(sym))))
                }
                Err(_) => (Iter(false), next),
            };

            let (rel, next) = self.parse(next)?;
            let (val, next) = self.parse(next)?;

            Ok(((iter, Attribute { rel, val }), next))
        })
    }

    fn parse_prop_match_arm<'s>(
        &mut self,
        next: &'s str,
    ) -> ParseResult<'s, (PropPattern<'a, L>, Nodes)> {
        parse_paren_delimited(next, |next| {
            let (_, next) = parse_lparen(next)?;
            let (seq_default, next) = match parse_symbol(next) {
                Ok(("seq", next)) => (Some(HasDefault(false)), next),
                Ok(("seq-default", next)) => (Some(HasDefault(true)), next),
                Ok((sym, _)) => return Err(Error::Expected(Class::Set, Found(Token::Symbol(sym)))),
                _ => (None, next),
            };
            let (pattern, next) = match self.parse_pattern_binding(next) {
                Ok((first_binding, next)) => {
                    match (self.parse_pattern_binding(next), seq_default) {
                        (Ok((val_binding, next)), seq_default) => {
                            let (_, next) = parse_rparen(next)?;
                            (
                                if seq_default.is_some() {
                                    return Err(Error::Unexpected(Class::Set));
                                } else {
                                    PropPattern::Attr(first_binding, val_binding)
                                },
                                next,
                            )
                        }
                        (Err(Error::Unexpected(Class::RParen)), Some(has_default)) => {
                            let (_, next) = parse_rparen(next)?;
                            (PropPattern::Set(first_binding, has_default), next)
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

            Ok(((pattern, nodes.into()), next))
        })
    }

    fn parse_capture_match_arm<'s>(
        &mut self,
        next: &'s str,
    ) -> ParseResult<'s, CaptureMatchArm<'a, L>> {
        parse_paren_delimited(next, |next| {
            let (capture_groups, next) = parse_paren_delimited(next, |next| {
                self.parse_many(next, Self::parse_capture_group)
            })?;
            let (nodes, next) = self.parse_many(next, Self::parse)?;
            Ok((
                CaptureMatchArm {
                    capture_groups,
                    nodes: nodes.into(),
                },
                next,
            ))
        })
    }

    fn parse_binary_call<'s>(&mut self, proc: BuiltinProc, next: &'s str) -> ParseResult<'s, Node> {
        let (a, next) = self.parse(next)?;
        let (b, next) = self.parse(next)?;

        Ok((self.make_node(Kind::Call(proc, [a, b].into())), next))
    }

    fn parse_binder<'s>(&self, next: &'s str) -> ParseResult<'s, L::Data<'a, Binder>> {
        parse_paren_delimited(next, |next| {
            let (var, next) = parse_dollar_var(next)?;
            Ok((self.make_binder(var), next))
        })
    }

    fn parse_pattern_binding<'s>(&self, next: &'s str) -> ParseResult<'s, Binding<'a, L>> {
        let (_, next) = parse_dollar(next)?;
        match parse_token(next)? {
            (Token::Symbol(sym), next) => Ok((
                Binding::Binder(self.make_binder(Var(parse_alpha_to_u32(sym)?))),
                next,
            )),
            (Token::Underscore, next) => Ok((Binding::Wildcard, next)),
            (token, _) => Err(Error::InvalidToken(token)),
        }
    }

    fn parse_capture_group<'s>(&mut self, next: &'s str) -> ParseResult<'s, CaptureGroup<'a, L>> {
        parse_paren_delimited(next, |next| {
            let (index, next) = parse_i64(next)?;
            let (var, next) = parse_dollar_var(next)?;

            Ok((
                CaptureGroup {
                    index: index as u32,
                    binder: self.make_binder(var),
                },
                next,
            ))
        })
    }

    fn make_node(&mut self, kind: Kind<'a, L>) -> Node {
        self.arena.add(self.lang.default_data(kind))
    }

    fn make_binder(&self, var: Var) -> L::Data<'a, Binder> {
        self.lang.default_data(Binder { var })
    }

    fn make_label(&self, label: Label) -> L::Data<'a, Label> {
        self.lang.default_data(label)
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
        (Token::Symbol(sym), next) => Ok((Var(parse_alpha_to_u32(sym)?), next)),
        (token, _) => Err(Error::InvalidToken(token)),
    }
}

/// Parse `(`, then closure, then `)`
fn parse_paren_delimited<'s, T>(
    next: &'s str,
    mut item_fn: impl FnMut(&'s str) -> ParseResult<'s, T>,
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
        (Token::Symbol(sym), next) => Ok((Label(parse_alpha_to_u32(sym)?), next)),
        (token, _) => Err(Error::InvalidToken(token)),
    }
}

fn parse_alpha_to_u32(next: &str) -> Result<u32, Error<'_>> {
    ontol_runtime::format_utils::try_alpha_to_u32(next).ok_or(Error::InvalidVariableNumber)
}

fn parse_def_id(next: &str) -> ParseResult<DefId> {
    let (sym, next_outer) = parse_symbol(next)?;
    let Some(next) = sym.strip_prefix("def") else {
        return Err(Error::Expected(Class::Symbol, Found(Token::Symbol(sym))));
    };
    let next = match parse_token(next)? {
        (Token::At, next) => next,
        (token, _) => return Err(Error::Expected(Class::At, Found(token))),
    };
    let (pkg, next) = parse_i64(next)?;
    let (_, next) = parse_colon(next)?;
    let (idx, _) = parse_i64(next)?;

    Ok((DefId(PackageId(pkg as u16), idx as u16), next_outer))
}

fn parse_i64(next: &str) -> ParseResult<i64> {
    match parse_token(next)? {
        (Token::I64(n), next) => Ok((n, next)),
        (token, _) => Err(Error::Expected(Class::Number, Found(token))),
    }
}

fn parse_colon(next: &str) -> ParseResult<()> {
    match parse_token(next)? {
        (Token::Colon, next) => Ok(((), next)),
        (token, _) => Err(Error::Expected(Class::Colon, Found(token))),
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
        Some((_, ':')) => Ok((Token::Colon, chars.as_str())),
        Some((_, char)) if char.is_numeric() => {
            for (index, char) in chars.by_ref() {
                if !char.is_numeric() && char != '.' {
                    return Ok((interpret_number(&next[0..index])?, &next[index..]));
                }
            }

            Ok((interpret_number(next)?, chars.as_str()))
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

fn interpret_number(num: &str) -> Result<Token, Error> {
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
