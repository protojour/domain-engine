use std::ops::Range;

use chumsky::prelude::*;
use either::Either;

use crate::{
    ast::{
        AnyPattern, Dot, ExprPattern, FmtStatement, MapArm, Path, SetPattern, SetPatternElement,
        SetPatternModifier, StructPattern, StructPatternAttr, StructPatternAttributeKind,
        StructPatternModifier, StructPatternParameter, TypeOrPattern, UseStatement,
    },
    join_doc_lines,
    modifier::Modifier,
};

use super::{
    ast::{
        BinaryOp, Cardinality, DefStatement, MapStatement, RelStatement, RelType, Relation,
        Statement, Type,
    },
    lexer::Token,
    Span, Spanned,
};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum RawCardinality {
    Unit,
    Set,
    List,
}

/// AstParser parses Tokens into AST nodes.
///
/// This is an extension trait that simplifies the use of chumsky's Parser trait in this module.
pub trait AstParser<O>: Parser<Token, O, Error = Simple<Token>> + Clone {}

impl<T, O> AstParser<O> for T where T: Parser<Token, O, Error = Simple<Token>> + Clone {}

/// A parser for a sequence of statements (i.e. a source file)
#[allow(dead_code)]
pub fn statement_sequence() -> impl AstParser<Vec<Spanned<Statement>>> {
    header_statement()
        .repeated()
        .chain(domain_statement().repeated())
        .then_ignore(end())
}

/// Statement allowed only at the top/header section of a file
fn header_statement() -> impl AstParser<Spanned<Statement>> {
    spanned(use_statement()).map(span_map(Statement::Use))
}

fn use_statement() -> impl AstParser<UseStatement> {
    keyword(Token::Use)
        .then(spanned(text_literal()))
        .then_ignore(sym("as", "`as`"))
        .then(spanned(ident()))
        .map(|((kw, source), as_ident)| UseStatement {
            kw,
            reference: source,
            as_ident,
        })
}

fn domain_statement() -> impl AstParser<Spanned<Statement>> {
    recursive(|stmt_parser| {
        let def_stmt = spanned(def_statement(stmt_parser.clone())).map(span_map(Statement::Def));
        let rel_stmt = spanned(rel_statement(stmt_parser)).map(span_map(Statement::Rel));
        let fmt_stmt = spanned(fmt_statement()).map(span_map(Statement::Fmt));
        let map_stmt = spanned(map_statement()).map(span_map(Statement::Map));

        def_stmt.or(rel_stmt).or(fmt_stmt).or(map_stmt)
    })
}

fn def_statement(stmt_parser: impl AstParser<Spanned<Statement>>) -> impl AstParser<DefStatement> {
    doc_comment()
        .then(keyword(Token::Def))
        .then(
            spanned(
                modifier(Modifier::Private)
                    .or(modifier(Modifier::Open))
                    .or(modifier(Modifier::Extern))
                    .or(modifier(Modifier::Symbol)),
            )
            .repeated()
            .collect::<Vec<_>>(),
        )
        .then(spanned(ident()))
        .then(spanned(
            stmt_parser.repeated().delimited_by(open('('), close(')')),
        ))
        .map(|((((docs, kw), modifiers), ident), ctx_block)| {
            let mut private = None;
            let mut open = None;
            let mut extern_ = None;
            let mut symbol = None;

            for (modifier, span) in modifiers {
                match modifier {
                    Modifier::Private => private = Some(span),
                    Modifier::Open => open = Some(span),
                    Modifier::Extern => extern_ = Some(span),
                    Modifier::Symbol => symbol = Some(span),
                    _ => unreachable!(),
                }
            }

            DefStatement {
                docs,
                private,
                open,
                extern_,
                symbol,
                kw,
                ident,
                block: ctx_block,
            }
        })
}

fn rel_statement(
    stmt_parser: impl AstParser<Spanned<Statement>> + 'static,
) -> impl AstParser<RelStatement> {
    let object_type_or_pattern =
        with_raw_cardinality(spanned_named_or_anonymous_type_or_dot(stmt_parser.clone()))
            .map(|(unit_or_seq, (opt_ty, span))| {
                (unit_or_seq, (opt_ty.map_right(TypeOrPattern::Type), span))
            })
            .or(
                sigil('=').ignore_then(spanned(any_pattern()).map(|(pat, span)| {
                    (
                        RawCardinality::Unit,
                        (Either::Right(TypeOrPattern::Pattern(pat)), span),
                    )
                })),
            );

    doc_comment()
        .then(keyword(Token::Rel))
        // subject:
        .then(with_raw_cardinality(
            spanned_named_or_anonymous_type_or_dot(stmt_parser.clone()),
        ))
        // forward relations:
        .then(
            forward_relation(stmt_parser)
                .separated_by(sigil('|'))
                .at_least(1),
        )
        // colon separator:
        .then_ignore(colon())
        // backward relations:
        .then(
            colon()
                .ignore_then(backward_relation().separated_by(sigil('|')))
                .or_not(),
        )
        // object:
        .then(object_type_or_pattern)
        .map(
            |(
                ((((docs, kw), (obj_unit_or_seq, subject)), forward_relations), backward_relations),
                (subj_unit_or_seq, object),
            )| {
                let relations = forward_relations
                    .into_iter()
                    .enumerate()
                    .map(|(idx, forward): (usize, ForwardRelation)| {
                        let backward_relation = backward_relations
                            .as_ref()
                            .and_then(|backward_vec| backward_vec.get(idx));

                        Relation {
                            ty: forward.ty,
                            subject_cardinality: compose_cardinality(
                                forward.subject_cardinality,
                                subj_unit_or_seq,
                            ),
                            ctx_block: forward.ctx_block,
                            object_prop_ident: backward_relation
                                .map(|backward| backward.object_prop_ident.clone()),
                            object_cardinality: compose_cardinality(
                                backward_relation
                                    .and_then(|backward| backward.object_cardinality.clone()),
                                obj_unit_or_seq,
                            ),
                        }
                    })
                    .collect();

                RelStatement {
                    docs,
                    kw,
                    subject,
                    relations,
                    object,
                }
            },
        )
}

fn compose_cardinality(
    cardinality: Option<Cardinality>,
    raw: RawCardinality,
) -> Option<Cardinality> {
    match (cardinality, raw) {
        (None, RawCardinality::Unit) => None,
        (None, RawCardinality::Set) => Some(Cardinality::Set),
        (None, RawCardinality::List) => Some(Cardinality::List),
        (Some(_), RawCardinality::Unit) => Some(Cardinality::Optional),
        (Some(_), RawCardinality::Set) => Some(Cardinality::OptionalSet),
        (Some(_), RawCardinality::List) => Some(Cardinality::OptionalList),
    }
}

struct ForwardRelation {
    pub ty: RelType,
    pub ctx_block: Option<Spanned<Vec<Spanned<Statement>>>>,
    pub subject_cardinality: Option<Cardinality>,
}

struct BackwardRelation {
    pub object_prop_ident: Spanned<String>,
    pub object_cardinality: Option<Cardinality>,
}

fn forward_relation(
    stmt_parser: impl AstParser<Spanned<Statement>> + 'static,
) -> impl AstParser<ForwardRelation> {
    let rel_ty_int_range = spanned(u16_range()).map(RelType::IntRange);
    let rel_ty_type =
        spanned(named_type().or(anonymous_type(stmt_parser.clone()))).map(RelType::Type);

    let rel_ty = rel_ty_int_range.or(rel_ty_type);

    let rel_params_block = stmt_parser.repeated().delimited_by(open('['), close(']'));

    rel_ty
        .then(spanned(rel_params_block).or_not())
        .then(sigil('?').or_not())
        .map(|((ty, ctx_block), subject_optional)| ForwardRelation {
            ty,
            ctx_block,
            subject_cardinality: subject_optional.map(|_| Cardinality::Optional),
        })
}

fn backward_relation() -> impl AstParser<BackwardRelation> {
    spanned(text_literal())
        .then(sigil('?').or_not())
        .map(|(object_prop_ident, object_optional)| {
            let object_cardinality = object_optional.map(|_| Cardinality::Optional);

            BackwardRelation {
                object_prop_ident,
                object_cardinality,
            }
        })
}

fn fmt_statement() -> impl AstParser<FmtStatement> {
    doc_comment()
        .then(keyword(Token::Fmt))
        // origin
        .then(spanned(named_type()))
        // transitions
        .then(
            just(Token::FatArrow)
                .ignore_then(spanned_named_type_or_dot())
                .repeated(),
        )
        .map(|(((docs, kw), origin), transitions)| FmtStatement {
            docs,
            kw,
            origin,
            transitions,
        })
}

fn with_raw_cardinality<T>(
    inner: impl AstParser<T> + Clone,
) -> impl AstParser<(RawCardinality, T)> {
    inner
        .clone()
        .map(|unit| (RawCardinality::Unit, unit))
        .or(inner
            .clone()
            .delimited_by(open('{'), close('}'))
            .map(|val| (RawCardinality::Set, val)))
        .or(inner
            .delimited_by(open('['), close(']'))
            .map(|val| (RawCardinality::List, val)))
}

fn map_statement() -> impl AstParser<MapStatement> {
    doc_comment()
        .then(keyword(Token::Map))
        .then(spanned(ident()).or_not())
        .then(
            spanned(map_arm())
                .then_ignore(sigil(','))
                .then(spanned(map_arm()))
                .then_ignore(sigil(',').or_not())
                .delimited_by(open('('), close(')')),
        )
        .map(|(((docs, kw), ident), (first, second))| MapStatement {
            docs,
            kw,
            ident,
            first,
            second,
        })
}

fn map_arm() -> impl AstParser<MapArm> {
    let struct_arm = parenthesized_struct_pattern(any_pattern()).map(MapArm::Struct);
    let set_arm = set_pattern(any_pattern()).map(MapArm::Set);

    struct_arm.or(set_arm)
}

fn any_pattern() -> impl AstParser<AnyPattern> {
    recursive(|this| {
        spanned(parenthesized_struct_pattern(this.clone()))
            .map(AnyPattern::Struct)
            .or(spanned(set_pattern(this.clone())).map(AnyPattern::Set))
            .or(expr_pattern().map(AnyPattern::Expr))
    })
}

fn parenthesized_struct_pattern(
    any_pattern: impl AstParser<AnyPattern> + Clone + 'static,
) -> impl AstParser<StructPattern> {
    let modifier = modifier(Modifier::Match).to(StructPatternModifier::Match);

    spanned(modifier)
        .or_not()
        .then(spanned(path()).or_not())
        .then(struct_pattern_param(any_pattern).delimited_by(open('('), close(')')))
        .map(|((modifier, path), param)| StructPattern {
            path,
            modifier,
            param,
        })
        .labelled("struct pattern")
}

fn struct_pattern_param(
    any_pattern: impl AstParser<AnyPattern> + Clone + 'static,
) -> impl AstParser<StructPatternParameter> {
    let attributes = spanned(
        struct_pattern_attr(any_pattern.clone())
            .map(StructPatternAttributeKind::Attr)
            .or(struct_pattern_spread()),
    )
    .separated_by(sigil(','))
    .allow_trailing()
    .at_least(1)
    .map(StructPatternParameter::Attributes);
    let pattern = any_pattern.map(|pat| StructPatternParameter::Pattern(Box::new(pat)));
    let nothing = close(')')
        .rewind()
        .map(|_| StructPatternParameter::Attributes(vec![]));

    attributes.or(pattern).or(nothing)
}

fn struct_pattern_attr(
    any_pattern: impl AstParser<AnyPattern> + Clone + 'static,
) -> impl AstParser<StructPatternAttr> {
    recursive(|struct_pattern_attr| {
        spanned(named_type())
            .then(
                spanned(
                    spanned(struct_pattern_attr.map(StructPatternAttributeKind::Attr))
                        .separated_by(sigil(','))
                        .delimited_by(open('['), close(']')),
                )
                .or_not(),
            )
            .then(spanned(sigil('?')).or_not())
            .then_ignore(colon())
            .then(spanned(any_pattern))
            .map_with_span(|(((relation, relation_args), option), object), _span| {
                StructPatternAttr {
                    relation,
                    relation_args,
                    option: option.map(|(_, span)| ((), span)),
                    object,
                }
            })
    })
}

fn struct_pattern_spread() -> impl AstParser<StructPatternAttributeKind> {
    dot_dot()
        .ignore_then(ident())
        .map(StructPatternAttributeKind::Spread)
}

fn set_pattern(
    any_pattern: impl AstParser<AnyPattern> + Clone + 'static,
) -> impl AstParser<SetPattern> {
    let modifier = spanned(
        modifier(Modifier::In)
            .to(SetPatternModifier::In)
            .or(modifier(Modifier::ContainsAll).to(SetPatternModifier::ContainsAll))
            .or(modifier(Modifier::AllIn).to(SetPatternModifier::AllIn))
            .or(modifier(Modifier::Intersects).to(SetPatternModifier::Intersects))
            .or(modifier(Modifier::Equals).to(SetPatternModifier::Equals)),
    );

    let elements = spanned(
        spanned(dot_dot())
            .or_not()
            .then(
                spanned(
                    spanned(struct_pattern_attr(any_pattern.clone()))
                        .separated_by(sigil(','))
                        .delimited_by(open('['), close(']')),
                )
                .or_not(),
            )
            .then(spanned(any_pattern))
            .map(|((spread, relation_attrs), pattern)| SetPatternElement {
                spread: spread.map(|(_, span)| span),
                relation_attrs,
                pattern,
            }),
    )
    .separated_by(sigil(','))
    .allow_trailing();

    modifier
        .or_not()
        .then(spanned(path()).or_not())
        .then(elements.delimited_by(open('{'), close('}')))
        .map(|((modifier, path), elements)| SetPattern {
            modifier,
            path,
            elements,
        })
}

fn expr_pattern() -> impl AstParser<Spanned<ExprPattern>> {
    recursive(|expr_pattern| {
        let symbol_pattern = symbol_pattern();
        let number_literal = number_literal().map(ExprPattern::NumberLiteral);
        let text_literal = text_literal().map(ExprPattern::TextLiteral);
        let regex_literal =
            select! { Token::Regex(string) => string }.map(ExprPattern::RegexLiteral);
        let group = expr_pattern.delimited_by(open('('), close(')'));

        let atom = spanned(symbol_pattern)
            .or(spanned(number_literal))
            .or(spanned(text_literal))
            .or(spanned(regex_literal))
            .or(group);

        // Consume operators by precedende
        // TODO: Migrate to Pratt parse when Chumsky supports it.
        // these are large types and thus pretty slow to compile :(

        let add = sigil('+').to(BinaryOp::Add);
        let sub = sigil('-').to(BinaryOp::Sub);
        let mul = sigil('*').to(BinaryOp::Mul);
        let div = sigil('/').to(BinaryOp::Div);

        // infix precedence for multiplication and division:
        let product =
            atom.clone()
                .then(mul.or(div).then(atom).repeated())
                .foldl(|left, (op, right)| {
                    let span = left.1.start..right.1.end;
                    (
                        ExprPattern::Binary(Box::new(left), op, Box::new(right)),
                        span,
                    )
                });

        // infix precedence for addition and subtraction:
        product
            .clone()
            .then(add.or(sub).then(product).repeated())
            .foldl(|left, (op, right)| {
                let span = left.1.start..right.1.end;
                (
                    ExprPattern::Binary(Box::new(left), op, Box::new(right)),
                    span,
                )
            })
    })
    .labelled("expression pattern")
}

fn spanned_named_type_or_dot() -> impl AstParser<Spanned<Either<Dot, Type>>> {
    spanned(
        named_type()
            .map(Either::Right)
            .or(sigil('.').map(|_| Either::Left(Dot))),
    )
}

fn spanned_named_or_anonymous_type_or_dot(
    stmt_parser: impl AstParser<Spanned<Statement>> + 'static,
) -> impl AstParser<Spanned<Either<Dot, Type>>> {
    spanned(
        (named_type().or(anonymous_type(stmt_parser)))
            .map(Either::Right)
            .or(sigil('.').map(|_| Either::Left(Dot))),
    )
}

/// Type parser
fn named_type() -> impl AstParser<Type> {
    let unit = just(Token::Open('('))
        .then(just(Token::Close(')')))
        .map(|_| Type::Unit);
    let path = path().map(Type::Path);
    let number_literal = number_literal().map(Type::NumberLiteral);
    let text_literal = text_literal().map(Type::TextLiteral);
    let regex = select! { Token::Regex(pattern) => pattern }.map(Type::Regex);

    unit.or(path).or(number_literal).or(text_literal).or(regex)
}

fn anonymous_type(
    stmt_parser: impl AstParser<Spanned<Statement>> + 'static,
) -> impl AstParser<Type> {
    spanned(stmt_parser.repeated().delimited_by(open('('), close(')')))
        .boxed()
        .map(Type::AnonymousStruct)
}

fn doc_comment() -> impl AstParser<Option<std::string::String>> {
    select! { Token::DocComment(comment) => comment }
        .repeated()
        .map(|lines| join_doc_lines(lines.iter().map(|line| line.as_str())))
}

fn open(char: char) -> impl AstParser<Token> {
    just(Token::Open(char))
}

fn close(char: char) -> impl AstParser<Token> {
    just(Token::Close(char))
}

fn sigil(char: char) -> impl AstParser<Token> {
    just(Token::Sigil(char))
}

fn keyword(token: Token) -> impl AstParser<Span> {
    just(token).map_with_span(|_, span| span)
}

fn path() -> impl AstParser<Path> {
    enum SpannedPath {
        Ident(Spanned<String>),
        Path(Vec<Spanned<String>>),
    }

    spanned(any_sym())
        .map(SpannedPath::Ident)
        .then(dot().ignore_then(spanned(any_sym())).repeated())
        .foldl(|prev, next| match prev {
            SpannedPath::Ident(ident) => SpannedPath::Path(vec![ident, next]),
            SpannedPath::Path(mut path) => {
                path.push(next);
                SpannedPath::Path(path)
            }
        })
        .map(|tmp_path| match tmp_path {
            // Remove the span from the ident case,
            // this span will always be the same as the Path itself:
            SpannedPath::Ident(spanned_sym) => Path::Ident(spanned_sym.0),
            SpannedPath::Path(path) => Path::Path(path),
        })
        .labelled("path")
}

fn any_sym() -> impl AstParser<String> {
    select! { Token::Sym(ident) => ident }.map_err(expected("symbol"))
}

fn sym(str: &'static str, label: &'static str) -> impl AstParser<String> {
    select! { Token::Sym(sym) if sym == str => sym }.map_err(expected(label))
}

fn ident() -> impl AstParser<String> {
    select! { Token::Sym(ident) => ident }.map_err(expected("identifier"))
}

fn modifier(modifier: Modifier) -> impl AstParser<Modifier> {
    just(Token::Modifier(modifier)).to(modifier)
}

fn symbol_pattern() -> impl AstParser<ExprPattern> {
    select! {
    Token::Sym(ident) if ident == "true" => ExprPattern::BooleanLiteral(true),
    Token::Sym(ident) if ident == "false" => ExprPattern::BooleanLiteral(false),
    Token::Sym(ident) => ExprPattern::Variable(ident) }
    .map_err(expected("symbol"))
}

fn number_literal() -> impl AstParser<String> {
    select! { Token::Number(literal) => literal }.map_err(expected("number"))
}

fn u16() -> impl AstParser<u16> {
    number_literal().try_map(|lit: String, span| {
        let u: u16 = lit
            .parse()
            .map_err(|_| Simple::custom(span, "unable to parse number"))?;
        Ok(u)
    })
}

fn u16_range() -> impl AstParser<Range<Option<u16>>> {
    let simple = u16()
        .then(dot_dot().ignore_then(u16().or_not()).or_not())
        .map(move |(start, end)| Range {
            start: Some(start),
            end: match end {
                Some(end) => end,
                None => Some(start + 1),
            },
        });
    let dot_dot_following = dot_dot().ignore_then(u16()).map(|end| Range {
        start: None,
        end: Some(end),
    });

    simple.or(dot_dot_following)
}

fn dot() -> impl AstParser<()> {
    just(Token::Sigil('.')).ignored()
}

fn dot_dot() -> impl AstParser<()> {
    just(Token::DotDot).ignored()
}

fn colon() -> impl AstParser<()> {
    just(Token::Sigil(':')).ignored()
}

fn text_literal() -> impl AstParser<String> {
    select! { Token::TextLiteral(string) => string }
}

/// Parser combinator that transforms an output type O into Spanned<O>.
fn spanned<P: AstParser<O>, O>(parser: P) -> impl AstParser<Spanned<O>> {
    parser.map_with_span(|t, span| (t, span))
}

/// Map the inner type T in Spanned<T> into U.
fn span_map<T, U, F>(f: F) -> impl (Fn(Spanned<T>) -> Spanned<U>) + Clone
where
    F: (Fn(T) -> U) + Clone,
{
    move |(data, span)| (f(data), span)
}

fn expected(label: &'static str) -> impl Fn(Simple<Token>) -> Simple<Token> + Clone {
    move |error| {
        Simple::expected_input_found(
            error.span(),
            Some(Some(Token::Expected(label))),
            error.found().cloned(),
        )
    }
}
