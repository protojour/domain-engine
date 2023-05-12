use ontol_runtime::vm::proc::BuiltinProc;

use super::ast::{
    Hir2Ast, Hir2AstPatternBinding, Hir2AstPropMatchArm, Hir2AstPropPattern, Hir2AstVariable,
};

#[derive(Debug)]
pub enum Hir2ParseError<'s> {
    Eof,
    EndOfExpression,
    InvalidChar(char),
    InvalidToken(Token<'s>),
    InvalidIdent(&'s str),
    InvalidNumber,
    ExpectedIdent,
    ExpectedEndOfExpression,
    ExpectedHash(Token<'s>),
}

#[derive(Debug)]
pub enum Token<'s> {
    LParen,
    RParen,
    Ident(&'s str),
    Number(u32),
    Hash,
    Underscore,
}

type ParseResult<'s, T> = Result<(T, &'s str), Hir2ParseError<'s>>;

pub fn parse(input: &str) -> ParseResult<'_, Hir2Ast> {
    let input = input.trim_start();
    let (token, input) = next_proper_token(input)?;
    match token {
        Token::LParen => {
            let (node, input) = match next_ident(input)? {
                ("destruct", input) => {
                    let (_, input) = next_var_hash(input)?;
                    let (var, input) = next_var_after_hash(input)?;
                    let (children, input) = parse_many(input, parse)?;
                    (Hir2Ast::Destruct(var, children), input)
                }
                ("construct", input) => {
                    let (children, input) = parse_many(input, parse)?;
                    (Hir2Ast::Construct(children), input)
                }
                ("destruct-prop", input) => {
                    let (prop, input) = next_ident(input)?;
                    let (arms, input) = parse_many(input, parse_prop_match_arm)?;
                    (Hir2Ast::DestructProp(String::from(prop), arms), input)
                }
                ("construct-prop", input) => {
                    let (prop, input) = next_ident(input)?;
                    let (rel, input) = parse(input)?;
                    let (value, input) = parse(input)?;
                    (
                        Hir2Ast::ConstructProp(String::from(prop), Box::new(rel), Box::new(value)),
                        input,
                    )
                }
                ("+", input) => parse_binary_call(BuiltinProc::Add, input)?,
                ("-", input) => parse_binary_call(BuiltinProc::Sub, input)?,
                ("*", input) => parse_binary_call(BuiltinProc::Mul, input)?,
                ("/", input) => parse_binary_call(BuiltinProc::Div, input)?,
                (ident, _) => return Err(Hir2ParseError::InvalidIdent(ident)),
            };
            let (_, input) = next_rparen(input)?;
            Ok((node, input))
        }
        Token::Hash => match next_proper_token(input)? {
            (Token::Number(num), input) => Ok((Hir2Ast::VariableRef(Hir2AstVariable(num)), input)),
            (Token::Ident(ident), input) => match ident {
                "u" => Ok((Hir2Ast::Unit, input)),
                other => Err(Hir2ParseError::InvalidIdent(other)),
            },
            (token, _) => Err(Hir2ParseError::InvalidToken(token)),
        },
        Token::Number(num) => Ok((Hir2Ast::Int(num as i64), input)),
        token => Err(Hir2ParseError::InvalidToken(token)),
    }
}

fn parse_many<'s, T>(
    mut input: &'s str,
    item_fn: impl Fn(&'s str) -> ParseResult<'s, T>,
) -> ParseResult<'s, Vec<T>> {
    let mut nodes = vec![];
    loop {
        match item_fn(input) {
            Ok((node, next_input)) => {
                input = next_input;
                nodes.push(node);
            }
            Err(Hir2ParseError::Eof | Hir2ParseError::EndOfExpression) => {
                return Ok((nodes, input));
            }
            Err(error) => return Err(error),
        }
    }
}

fn parse_prop_match_arm(input: &str) -> ParseResult<'_, Hir2AstPropMatchArm> {
    let input = input.trim_start();
    let input = match next_proper_token(input)? {
        (Token::LParen, input) => input,
        (token, _) => return Err(Hir2ParseError::InvalidToken(token)),
    };
    let input = match next_proper_token(input)? {
        (Token::LParen, input) => input,
        (token, _) => return Err(Hir2ParseError::InvalidToken(token)),
    };
    let (pattern, input) = match parse_pattern_binding(input) {
        Ok((rel_binding, input)) => {
            let (val_binding, input) = parse_pattern_binding(input)?;
            let (_, input) = next_rparen(input)?;

            (Hir2AstPropPattern::Present(rel_binding, val_binding), input)
        }
        Err(Hir2ParseError::EndOfExpression) => {
            let (_, input) = next_rparen(input)?;
            (Hir2AstPropPattern::NotPresent, input)
        }
        Err(other) => return Err(other),
    };
    let (node, input) = parse(input)?;
    let (_, input) = next_rparen(input)?;

    Ok((Hir2AstPropMatchArm { pattern, node }, input))
}

fn parse_pattern_binding(input: &str) -> ParseResult<'_, Hir2AstPatternBinding> {
    let (_, input) = next_var_hash(input)?;
    match next_proper_token(input)? {
        (Token::Number(num), input) => Ok((Hir2AstPatternBinding::Binder(num), input)),
        (Token::Underscore, input) => Ok((Hir2AstPatternBinding::Wildcard, input)),
        (token, _) => Err(Hir2ParseError::InvalidToken(token)),
    }
}

fn parse_binary_call(proc: BuiltinProc, input: &str) -> ParseResult<'_, Hir2Ast> {
    let (a, input) = parse(input)?;
    let (b, input) = parse(input)?;

    Ok((Hir2Ast::Call(proc, vec![a, b]), input))
}

fn next_ident(input: &str) -> ParseResult<'_, &str> {
    match next_proper_token(input)? {
        (Token::Ident(ident), input) => Ok((ident, input)),
        _ => Err(Hir2ParseError::ExpectedIdent),
    }
}

fn next_var_hash(input: &str) -> ParseResult<'_, ()> {
    match next_proper_token(input)? {
        (Token::Hash, input) => Ok(((), input)),
        (token, _) => Err(Hir2ParseError::ExpectedHash(token)),
    }
}

fn next_var_after_hash(input: &str) -> ParseResult<'_, Hir2AstVariable> {
    match next_proper_token(input)? {
        (Token::Number(num), input) => Ok((Hir2AstVariable(num), input)),
        (token, _) => Err(Hir2ParseError::InvalidToken(token)),
    }
}

fn next_rparen(input: &str) -> ParseResult<'_, ()> {
    match next_token(input)? {
        (Token::RParen, input) => Ok(((), input)),
        _ => Err(Hir2ParseError::ExpectedEndOfExpression),
    }
}

fn next_proper_token(input: &str) -> ParseResult<'_, Token<'_>> {
    match next_token(input)? {
        (Token::RParen, _) => Err(Hir2ParseError::EndOfExpression),
        (token, input) => Ok((token, input)),
    }
}

fn next_token(input: &str) -> ParseResult<'_, Token<'_>> {
    let input = input.trim_start();

    let mut chars = input.char_indices();
    match chars.next() {
        None => Err(Hir2ParseError::Eof),
        Some((_, '(')) => Ok((Token::LParen, chars.as_str())),
        Some((_, ')')) => Ok((Token::RParen, chars.as_str())),
        Some((_, '#')) => Ok((Token::Hash, chars.as_str())),
        Some((_, '_')) => Ok((Token::Underscore, chars.as_str())),
        Some((_, char)) if char.is_numeric() => {
            let mut remain = chars.as_str();

            while let Some((index, char)) = chars.next() {
                if !char.is_numeric() {
                    let str = &input[0..index];
                    return parse_number(str, remain);
                } else {
                    remain = chars.as_str();
                }
            }

            parse_number(input, remain)
        }
        Some((_, char)) if !char.is_whitespace() => {
            let mut remain = chars.as_str();
            while let Some((index, char)) = chars.next() {
                if char.is_whitespace() || char == '(' || char == ')' {
                    let ident = &input[0..index];
                    return Ok((Token::Ident(ident), remain));
                } else {
                    remain = chars.as_str();
                }
            }

            Ok((Token::Ident(input), remain))
        }
        Some((_, char)) => Err(Hir2ParseError::InvalidChar(char)),
    }
}

fn parse_number<'r>(
    num: &str,
    remain: &'r str,
) -> Result<(Token<'static>, &'r str), Hir2ParseError<'r>> {
    let result: Result<u32, _> = num.parse();
    match result {
        Ok(num) => Ok((Token::Number(num), remain)),
        Err(_) => Err(Hir2ParseError::InvalidNumber),
    }
}
