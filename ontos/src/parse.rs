use ontol_runtime::vm::proc::BuiltinProc;

use super::ast::{
    Hir2Ast, Hir2AstPatternBinding, Hir2AstPropMatchArm, Hir2AstPropPattern, Hir2AstVariable,
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

pub fn parse(next: &str) -> ParseResult<'_, Hir2Ast> {
    let next = next.trim_start();
    match parse_token(next)? {
        (Token::LParen, next) => {
            let (node, next) = match parse_ident(next)? {
                ("destruct", next) => {
                    let (_, next) = parse_hash(next)?;
                    let (var, next) = parse_var_after_hash(next)?;
                    let (children, next) = parse_many(next, parse)?;
                    (Hir2Ast::Destruct(var, children), next)
                }
                ("construct", next) => {
                    let (children, next) = parse_many(next, parse)?;
                    (Hir2Ast::Construct(children), next)
                }
                ("destruct-prop", next) => {
                    let (prop, next) = parse_ident(next)?;
                    let (arms, next) = parse_many(next, parse_prop_match_arm)?;
                    (Hir2Ast::DestructProp(String::from(prop), arms), next)
                }
                ("construct-prop", next) => {
                    let (prop, next) = parse_ident(next)?;
                    let (rel, next) = parse(next)?;
                    let (value, next) = parse(next)?;
                    (
                        Hir2Ast::ConstructProp(String::from(prop), Box::new(rel), Box::new(value)),
                        next,
                    )
                }
                ("+", next) => parse_binary_call(BuiltinProc::Add, next)?,
                ("-", next) => parse_binary_call(BuiltinProc::Sub, next)?,
                ("*", next) => parse_binary_call(BuiltinProc::Mul, next)?,
                ("/", next) => parse_binary_call(BuiltinProc::Div, next)?,
                (ident, _) => return Err(Error::InvalidIdent(ident)),
            };
            let (_, next) = parse_rparen(next)?;
            Ok((node, next))
        }
        (Token::Hash, next) => match parse_token(next)? {
            (Token::Int(int), next) => Ok((Hir2Ast::VariableRef(mk_var(int)?), next)),
            (Token::Ident(ident), next) => match ident {
                "u" => Ok((Hir2Ast::Unit, next)),
                other => Err(Error::InvalidIdent(other)),
            },
            (token, _) => Err(Error::InvalidToken(token)),
        },
        (Token::Int(num), next) => Ok((Hir2Ast::Int(num), next)),
        (token, _) => Err(Error::InvalidToken(token)),
    }
}

fn parse_many<'s, T>(
    mut next: &'s str,
    item_fn: impl Fn(&'s str) -> ParseResult<'s, T>,
) -> ParseResult<'s, Vec<T>> {
    let mut nodes = vec![];
    loop {
        match item_fn(next) {
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

fn parse_prop_match_arm(next: &str) -> ParseResult<'_, Hir2AstPropMatchArm> {
    let (_, next) = parse_lparen(next)?;
    let (_, next) = parse_lparen(next)?;
    let (pattern, next) = match parse_pattern_binding(next) {
        Ok((rel_binding, next)) => {
            let (val_binding, next) = parse_pattern_binding(next)?;
            let (_, next) = parse_rparen(next)?;

            (Hir2AstPropPattern::Present(rel_binding, val_binding), next)
        }
        Err(Error::Unexpected(Class::RParen)) => {
            let (_, next) = parse_rparen(next)?;
            (Hir2AstPropPattern::NotPresent, next)
        }
        Err(other) => return Err(other),
    };
    let (node, next) = parse(next)?;
    let (_, next) = parse_rparen(next)?;

    Ok((Hir2AstPropMatchArm { pattern, node }, next))
}

fn parse_pattern_binding(next: &str) -> ParseResult<'_, Hir2AstPatternBinding> {
    let (_, next) = parse_hash(next)?;
    match parse_token(next)? {
        (Token::Int(int), next) => Ok((Hir2AstPatternBinding::Binder(mk_var(int)?), next)),
        (Token::Underscore, next) => Ok((Hir2AstPatternBinding::Wildcard, next)),
        (token, _) => Err(Error::InvalidToken(token)),
    }
}

fn parse_binary_call(proc: BuiltinProc, next: &str) -> ParseResult<'_, Hir2Ast> {
    let (a, next) = parse(next)?;
    let (b, next) = parse(next)?;

    Ok((Hir2Ast::Call(proc, vec![a, b]), next))
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

fn parse_var_after_hash(next: &str) -> ParseResult<'_, Hir2AstVariable> {
    match parse_token(next)? {
        (Token::Int(int), next) => Ok((mk_var(int)?, next)),
        (token, _) => Err(Error::InvalidToken(token)),
    }
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

fn mk_var(num: i64) -> Result<Hir2AstVariable, Error<'static>> {
    let num: u32 = num.try_into().map_err(|_| Error::InvalidVariableNumber)?;
    Ok(Hir2AstVariable(num))
}
