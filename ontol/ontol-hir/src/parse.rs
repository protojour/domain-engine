use ontol_core::tag::DomainIndex;
use thin_vec::thin_vec;

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
    ElementMismatch,
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

#[derive(PartialEq, Debug)]
pub enum Token<'s> {
    LParen,
    RParen,
    LBracket,
    RBracket,
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
                let (_, next) = parse_expect(next, Token::RParen, Class::RParen)?;
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
            ("+", next) => self.parse_binary_call(OverloadFunc::Add, next),
            ("-", next) => self.parse_binary_call(OverloadFunc::Sub, next),
            ("*", next) => self.parse_binary_call(OverloadFunc::Mul, next),
            ("/", next) => self.parse_binary_call(OverloadFunc::Div, next),
            ("with", next) => {
                let (_, next) = parse_expect(next, Token::LParen, Class::LParen)?;
                let (bind_var, next) = parse_dollar_var(next)?;
                let (def, next) = self.parse(next)?;
                let (_, next) = parse_expect(next, Token::RParen, Class::RParen)?;
                let (body, next) = self.parse_many(next, Self::parse)?;
                Ok((
                    self.make_node(Kind::With(self.make_binder(bind_var), def, body.into())),
                    next,
                ))
            }
            ("block", next) => {
                let (body, next) = self.parse_many(next, Self::parse)?;
                Ok((self.make_node(Kind::Block(body.into())), next))
            }
            ("catch", next) => {
                let (label, next) = parse_paren_delimited(next, parse_at_label)?;
                let (body, next) = self.parse_many(next, Self::parse)?;
                Ok((self.make_node(Kind::Catch(label, body.into())), next))
            }
            ("try?", next) => {
                let (label, next) = parse_at_label(next)?;
                let (var, next) = parse_dollar_var(next)?;
                Ok((self.make_node(Kind::Try(label, var)), next))
            }
            ("let", next) => {
                let (var, next) = parse_dollar_var(next)?;
                let (node, next) = self.parse(next)?;
                Ok((self.make_node(Kind::Let(self.make_binder(var), node)), next))
            }
            ("let-prop", next) => {
                let (bind_pack, next) =
                    self.parse_pack(next, |zelf, next| zelf.parse_pattern_binding(next))?;

                let (_, next) = parse_expect(next, Token::LParen, Class::LParen)?;
                let (var, next) = parse_dollar_var(next)?;
                let (prop_id, next) = parse_prop_id(next)?;
                let (_, next) = parse_expect(next, Token::RParen, Class::RParen)?;
                Ok((
                    self.make_node(Kind::LetProp(bind_pack, (var, prop_id))),
                    next,
                ))
            }
            ("let-prop-default", next) => {
                let (bind_pack, next) =
                    self.parse_pack(next, |zelf, next| zelf.parse_pattern_binding(next))?;

                let (_, next) = parse_expect(next, Token::LParen, Class::LParen)?;
                let (var, next) = parse_dollar_var(next)?;
                let (prop_id, next) = parse_prop_id(next)?;
                let (_, next) = parse_expect(next, Token::RParen, Class::RParen)?;
                let (default, next) = self.parse_many(next, Self::parse)?;
                Ok((
                    self.make_node(Kind::LetPropDefault(
                        bind_pack,
                        (var, prop_id),
                        default.into(),
                    )),
                    next,
                ))
            }
            ("map", next) => {
                let (arg, next) = self.parse(next)?;
                Ok((self.make_node(Kind::Map(arg)), next))
            }
            ("struct", next) => self.parse_struct_inner(next, StructFlags::empty()),
            ("match-struct", next) => self.parse_struct_inner(next, StructFlags::MATCH),
            ("prop" | "prop-", next) => self.parse_prop(
                PropFlags::REL_DOWN_OPTIONAL | PropFlags::REL_UP_OPTIONAL,
                next,
            ),
            ("prop?" | "prop?-", next) => self.parse_prop(
                PropFlags::PAT_OPTIONAL | PropFlags::REL_DOWN_OPTIONAL | PropFlags::REL_UP_OPTIONAL,
                next,
            ),
            ("prop!", next) => self.parse_prop(PropFlags::empty(), next),
            ("prop-!", next) => self.parse_prop(PropFlags::REL_DOWN_OPTIONAL, next),
            ("prop?!", next) => self.parse_prop(PropFlags::PAT_OPTIONAL, next),
            ("prop?-!", next) => {
                self.parse_prop(PropFlags::PAT_OPTIONAL | PropFlags::REL_DOWN_OPTIONAL, next)
            }
            ("matrix", next) => {
                let (entries, next) = self.parse_many(next, Self::parse_set_entry)?;
                Ok((self.make_node(Kind::Matrix(entries.into())), next))
            }
            ("make-seq", next) => {
                let (binder, next) = match parse_raw_token(next) {
                    Ok((Token::LParen, _)) => {
                        let (binder, next) = self.parse_binder(next)?;
                        (Some(binder), next)
                    }
                    _ => (None, next),
                };
                let (children, next) = self.parse_many(next, Self::parse)?;
                Ok((self.make_node(Kind::MakeSeq(binder, children.into())), next))
            }
            ("for-each", mut next) => {
                let mut elements: ThinVec<(Var, Binding<'a, L>)> = Default::default();

                loop {
                    let (seq_var, next_next) = parse_dollar_var(next)?;

                    next = next_next;
                    elements.push((seq_var, Binding::Wildcard));

                    if matches!(parse_raw_token(next), Ok((Token::LParen, _))) {
                        break;
                    }
                }

                let (bindings, next) = parse_paren_delimited(next, |next| {
                    self.parse_many(next, |zelf, next| zelf.parse_pattern_binding(next))
                })?;

                if bindings.len() != elements.len() {
                    return Err(Error::ElementMismatch);
                }

                for (element, binding) in elements.iter_mut().zip(bindings.into_iter()) {
                    element.1 = binding;
                }

                let (body, next) = self.parse_many(next, Self::parse)?;
                Ok((self.make_node(Kind::ForEach(elements, body.into())), next))
            }
            ("insert", next) => {
                let (seq_var, next) = parse_dollar_var(next)?;
                let (node, next) = self.parse(next)?;
                Ok((self.make_node(Kind::Insert(seq_var, node)), next))
            }
            ("let-regex", mut next) => {
                let mut groups_list = thin_vec![];
                while parse_expect(next, Token::LParen, Class::LParen).is_ok() {
                    let (groups, next_next) = parse_paren_delimited(next, |next| {
                        self.parse_many(next, Self::parse_capture_group)
                            .map(|(vec, next)| (ThinVec::from(vec), next))
                    })?;
                    groups_list.push(groups);

                    next = next_next;
                }
                let (regex_def_id, next) = parse_def_id(next)?;
                let (var, next) = parse_dollar_var(next)?;

                Ok((
                    self.make_node(Kind::LetRegex(groups_list, regex_def_id, var)),
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
                            .map(|(vec, next)| (ThinVec::from(vec), next))
                    })
                })?;

                Ok((
                    self.make_node(Kind::Regex(label, regex_def_id, ThinVec::from(match_arms))),
                    next,
                ))
            }
            ("move-rest-attrs", next) => {
                let (dest, next) = parse_dollar_var(next)?;
                let (src, next) = parse_dollar_var(next)?;
                Ok((self.make_node(Kind::MoveRestAttrs(dest, src)), next))
            }
            ("let-cond-var", next) => {
                let (a, next) = parse_dollar_var(next)?;
                let (b, next) = parse_dollar_var(next)?;
                Ok((self.make_node(Kind::LetCondVar(a, b)), next))
            }
            (sym, _) => Err(Error::InvalidSymbol(sym)),
        }
    }

    pub fn parse_prop<'s>(&mut self, optional: PropFlags, next: &'s str) -> ParseResult<'s, Node> {
        let (var, next) = parse_dollar_var(next)?;
        let (prop, next) = parse_symbol(next)?;
        let (variant, next) = self.parse_prop_variant(next)?;
        Ok((
            self.make_node(Kind::Prop(
                optional,
                var,
                prop.parse()
                    .map_err(|_| Error::Expected(Class::Property, Found(Token::Symbol(prop))))?,
                variant,
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
                Err(error) => {
                    return Err(error);
                }
            }
        }
    }

    fn parse_pack<'s, T>(
        &mut self,
        next: &'s str,
        mut item_fn: impl FnMut(&mut Self, &'s str) -> ParseResult<'s, T>,
    ) -> ParseResult<'s, Pack<T>> {
        match parse_raw_token(next)? {
            (Token::LBracket, _) => {
                let (items, next) = parse_bracket_delimited(next, |next| {
                    self.parse_many(next, |zelf, next| item_fn(zelf, next))
                })?;
                Ok((Pack::Tuple(items.into_iter().collect()), next))
            }
            _ => {
                let (unit, next) = item_fn(self, next)?;
                Ok((Pack::Unit(unit), next))
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

    fn parse_prop_variant<'s>(&mut self, next: &'s str) -> ParseResult<'s, PropVariant> {
        match parse_raw_token(next)? {
            (Token::LBracket, _) => parse_bracket_delimited(next, |next| {
                let (nodes, next) = self.parse_many(next, |zelf, next| zelf.parse(next))?;

                Ok((PropVariant::Tuple(nodes.into_iter().collect()), next))
            }),
            _ => match parse_symbol(next) {
                Ok(("element-in", next)) => {
                    let (node, next) = self.parse(next)?;
                    Ok((PropVariant::Predicate(SetOperator::ElementIn, node), next))
                }
                _ => {
                    let (unit, next) = self.parse(next)?;

                    Ok((PropVariant::Unit(unit), next))
                }
            },
        }
    }

    fn parse_set_entry<'s>(&mut self, next: &'s str) -> ParseResult<'s, MatrixRow<'a, L>> {
        parse_paren_delimited(next, |next| {
            let (label, next) = match parse_symbol(next) {
                Ok(("..", next)) => {
                    let (label, next) = parse_at_label(next)?;
                    (Some(label), next)
                }
                _ => (None, next),
            };

            let (elements, next) = self.parse_many(next, |zelf, next| zelf.parse(next))?;

            Ok((
                MatrixRow(
                    label.map(|label| self.make_label(label)),
                    elements.into_iter().collect(),
                ),
                next,
            ))
        })
    }

    fn parse_binary_call<'s>(
        &mut self,
        func: OverloadFunc,
        next: &'s str,
    ) -> ParseResult<'s, Node> {
        let (a, next) = self.parse(next)?;
        let (b, next) = self.parse(next)?;

        Ok((self.make_node(Kind::Call(func, [a, b].into())), next))
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

fn parse_prop_id(next: &str) -> ParseResult<PropId> {
    let (sym, next) = parse_symbol(next)?;
    let prop_id: PropId = sym
        .parse()
        .map_err(|_| Error::Expected(Class::Property, Found(Token::Symbol(sym))))?;
    Ok((prop_id, next))
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
    let (_, next) = parse_expect(next, Token::LParen, Class::LParen)?;
    let (value, next) = item_fn(next)?;
    let (_, next) = parse_expect(next, Token::RParen, Class::RParen)?;
    Ok((value, next))
}

fn parse_bracket_delimited<'s, T>(
    next: &'s str,
    mut item_fn: impl FnMut(&'s str) -> ParseResult<'s, T>,
) -> ParseResult<'s, T> {
    let (_, next) = parse_expect(next, Token::LBracket, Class::LParen)?;
    let (value, next) = item_fn(next)?;
    let (_, next) = parse_expect(next, Token::RBracket, Class::RParen)?;
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
    ontol_runtime::format_utils::try_alpha_to_u32(next, 'a').ok_or(Error::InvalidVariableNumber)
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
    let (_, next) = parse_expect(next, Token::Colon, Class::Colon)?;
    let (tag, _) = parse_i64(next)?;

    let Ok(domain_index) = DomainIndex::from_u16(pkg.try_into().unwrap()) else {
        panic!("invalid domain index");
    };

    Ok((DefId(domain_index, tag as u16), next_outer))
}

fn parse_i64(next: &str) -> ParseResult<i64> {
    match parse_token(next)? {
        (Token::I64(n), next) => Ok((n, next)),
        (token, _) => Err(Error::Expected(Class::Number, Found(token))),
    }
}

fn parse_expect<'s>(next: &'s str, expected: Token, class: Class) -> ParseResult<'s, ()> {
    let (token, next_next) = parse_raw_token(next)?;

    if token == expected {
        Ok(((), next_next))
    } else {
        match token {
            Token::RParen | Token::RBracket => Err(Error::Unexpected(Class::RParen)),
            _ => Err(Error::Expected(class, Found(token))),
        }
    }
}

fn parse_token(next: &str) -> ParseResult<Token> {
    match parse_raw_token(next)? {
        (Token::RParen | Token::RBracket, _) => Err(Error::Unexpected(Class::RParen)),
        (token, next) => Ok((token, next)),
    }
}

fn parse_raw_token(next: &str) -> ParseResult<Token> {
    let next = next.trim_start();
    let mut chars = next.char_indices();

    let token = match chars.next() {
        None => Err(Error::Unexpected(Class::Eof)),
        Some((_, '(')) => Ok((Token::LParen, chars.as_str())),
        Some((_, ')')) => Ok((Token::RParen, chars.as_str())),
        Some((_, '[')) => Ok((Token::LBracket, chars.as_str())),
        Some((_, ']')) => Ok((Token::RBracket, chars.as_str())),
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
                if char.is_whitespace() || matches!(char, '(' | ')' | '[' | ']') {
                    return Ok((Token::Symbol(&next[0..index]), &next[index..]));
                }
            }

            Ok((Token::Symbol(next), chars.as_str()))
        }
        Some((_, char)) => Err(Error::InvalidChar(char)),
    };

    token
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
