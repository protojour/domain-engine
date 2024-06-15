use crate::{lexer::kind::Kind, K};

use super::parser::{CstParser, SyntaxCursor};

#[derive(PartialEq, Eq, Debug)]
pub enum DetectedPattern {
    Struct,
    Set,
    Expr,
}

#[derive(Clone, Copy)]
enum Delimiter {
    Eof,
    Paren,
}

#[derive(Clone, Copy, Default)]
struct AllowedType {
    dot: bool,
    anonymous: bool,
    int_range: bool,
}

#[derive(Clone, Copy, Default)]
struct AllowedPattern {
    expr: bool,
}

pub fn ontol(p: &mut CstParser) {
    let ontol = p.start_exact(Kind::Ontol);

    while p.at() != Kind::Eof {
        statement(p);
    }

    p.eat_trivia();

    p.end(ontol);
}

fn statement(p: &mut CstParser) {
    let mut stmt = p.start(Kind::Error);
    p.eat_trivia();

    if p.has_error_state() {
        p.eat_while(|kind| {
            !matches!(
                kind,
                Kind::Eof | K![use] | K![def] | K![sym] | K![rel] | K![fmt] | K![map]
            )
        });

        if p.at() == Kind::Eof {
            return;
        }

        p.clear_error_state();
    }

    let kind = match p.at() {
        K![domain] => {
            def_like_statement(K![domain], p);
            Kind::DomainStatement
        }
        K![use] => {
            use_statement(p);
            Kind::UseStatement
        }
        K![def] => {
            def_like_statement(K![def], p);
            Kind::DefStatement
        }
        K![rel] => {
            rel::rel(p);
            Kind::RelStatement
        }
        K![sym] => {
            sym::statement(p);
            Kind::SymStatement
        }
        K![fmt] => {
            fmt_statement(p);
            Kind::FmtStatement
        }
        K![map] => {
            map::statement(p);
            Kind::MapStatement
        }
        _ => {
            p.eat_error(|kind| format!("expected keyword, found {kind}"));
            Kind::Error
        }
    };

    stmt.set_kind(kind);
    p.end(stmt);
}

fn use_statement(p: &mut CstParser) {
    p.eat(K![use]);
    p.eat_trivia();

    let location = p.start(Kind::Name);
    p.eat_text_literal();
    p.end(location);

    p.eat_sym_value("as");
    p.eat_trivia();

    let path = p.start(Kind::IdentPath);
    p.eat(Kind::Symbol);
    p.end(path);
}

fn def_like_statement(keyword: Kind, p: &mut CstParser) {
    p.eat(keyword);

    p.eat_modifiers();
    p.eat_trivia();

    let ident = p.start(Kind::IdentPath);
    p.eat(Kind::Symbol);
    p.end(ident);

    p.eat_trivia();

    let body = p.start(Kind::DefBody);
    p.eat(K!['(']);

    while p.not_peekforward(|kind| matches!(kind, K![')'])) {
        statement(p);
    }

    p.eat(K![')']);
    p.end(body);
}

mod rel {
    use super::*;

    /// `rel` statement entrypoint
    pub fn rel(p: &mut CstParser) {
        p.eat(K![rel]);
        p.eat_trivia();

        let subject = p.start(Kind::RelSubject);
        rel_type_reference(p);
        p.end(subject);

        let fwd = p.start(Kind::RelFwdSet);
        loop {
            let relation = p.start(Kind::Relation);
            forward_relation(p);
            p.end(relation);

            if matches!(p.at(), K![|]) {
                p.eat(K![|]);
            } else {
                break;
            }
        }
        p.end(fwd);

        p.eat(K![:]);

        if matches!(p.at(), K![:]) {
            let backwd = p.start(Kind::RelBackwdSet);
            p.eat(K![:]);

            loop {
                let ident = p.start(Kind::Name);
                p.eat_text_literal();
                p.end(ident);

                prop_cardinality(p);

                if matches!(p.at(), K![|]) {
                    p.eat(K![|]);
                } else {
                    break;
                }
            }

            p.end(backwd);
        }

        p.eat_trivia();

        {
            let object = p.start(Kind::RelObject);

            if p.at() == K![=] {
                p.eat(K![=]);
                pattern_with_expr(p);
            } else {
                rel_type_reference(p);
            }

            p.end(object);
        }
    }

    fn forward_relation(p: &mut CstParser) {
        p.eat_trivia();
        rel_type_reference(p);

        if p.at() == K!['['] {
            let rel_params = p.start(Kind::RelParams);
            p.eat(K!['[']);

            while p.not_peekforward(|kind| matches!(kind, K![']'])) {
                statement(p);
            }

            p.eat(K![']']);
            p.end(rel_params);
        }

        p.eat_trivia();

        prop_cardinality(p);
    }

    fn rel_type_reference(p: &mut CstParser) {
        let allowed = AllowedType {
            dot: true,
            anonymous: true,
            int_range: true,
        };

        p.eat_trivia();

        let label = "relation type";

        match p.at() {
            K!['{'] => {
                let set = p.start(Kind::TypeModSet);
                p.eat(K!['{']);
                type_ref_inner(p, allowed, label);
                p.eat(K!['}']);
                p.end(set);
            }
            K!['['] => {
                let seq = p.start(Kind::TypeModList);
                p.eat(K!['[']);
                type_ref_inner(p, allowed, label);
                p.eat(K![']']);
                p.end(seq);
            }
            _ => {
                let unit = p.start(Kind::TypeModUnit);
                type_ref_inner(p, allowed, label);
                p.end(unit);
            }
        }
    }

    fn prop_cardinality(p: &mut CstParser) {
        let cardinality = p.start(Kind::PropCardinality);
        if p.at() == K![?] {
            p.eat(K![?]);
        }
        p.end(cardinality);
    }
}

fn type_ref_inner(p: &mut CstParser, allowed: AllowedType, label: &'static str) {
    p.eat_trivia();

    match p.at() {
        Kind::Symbol => {
            ident_path(p);
        }
        Kind::Number => {
            let mut node = p.start(Kind::Literal);
            let cursor = p.syntax_cursor();
            p.eat(Kind::Number);

            if allowed.int_range && p.at() == K![..] {
                p.insert_node(cursor, Kind::RangeStart);

                node.set_kind(Kind::NumberRange);
                p.eat(K![..]);

                if p.at() == Kind::Number {
                    let end = p.start(Kind::RangeEnd);
                    p.eat(Kind::Number);
                    p.end(end);
                }
            }

            p.end(node);
        }
        K![..] if allowed.int_range => {
            let range = p.start(Kind::NumberRange);
            p.eat(K![..]);
            let end = p.start(Kind::RangeEnd);
            p.eat(Kind::Number);
            p.end(end);
            p.end(range);
        }
        kind @ (Kind::DoubleQuoteText | Kind::SingleQuoteText | Kind::Regex) => {
            let literal = p.start(Kind::Literal);
            p.eat(kind);
            p.end(literal);
        }
        K!['('] if allowed.anonymous => {
            let def_body = p.start(Kind::DefBody);
            p.eat(K!['(']);

            while p.not_peekforward(|kind| matches!(kind, K![')'])) {
                statement(p);
            }

            p.eat(K![')']);
            p.end(def_body);
        }
        K![.] if allowed.dot => {
            let this = p.start(Kind::This);
            p.eat(K![.]);
            p.end(this);
        }
        _ => {
            p.eat_error(|kind| format!("expected {label}, found {kind}"));
        }
    }
}

fn fmt_statement(p: &mut CstParser) {
    p.eat(K![fmt]);

    loop {
        let type_ref = p.start(Kind::TypeModUnit);
        type_ref_inner(
            p,
            AllowedType {
                dot: true,
                anonymous: true,
                int_range: false,
            },
            "type",
        );
        p.end(type_ref);

        if p.at() == Kind::FatArrow {
            p.eat(Kind::FatArrow);
        } else {
            break;
        }
    }
}

mod sym {
    use super::*;

    pub fn statement(p: &mut CstParser) {
        p.eat(K![sym]);

        p.eat_trivia();

        delimited_comma_separated(p, K!['{'], &sym_relation, K!['}']);
    }

    fn sym_relation(p: &mut CstParser) {
        let relation = p.start(Kind::SymRelation);

        while p.not_peekforward(|kind| matches!(kind, K![,])) {
            match lookahead_variant(p) {
                Some(Kind::SymVar) => {
                    let var = p.start(Kind::SymVar);
                    p.eat_trivia();
                    p.eat(K!['(']);
                    p.eat(Kind::Symbol);
                    p.eat(K![')']);

                    p.end(var);
                }
                Some(Kind::SymDecl) => {
                    let decl = p.start(Kind::SymDecl);
                    p.eat_trivia();
                    p.eat(Kind::Symbol);
                    p.end(decl);
                }
                _ => break,
            }
        }

        p.end(relation);
    }

    fn lookahead_variant(p: &CstParser) -> Option<Kind> {
        for kind in p.peek_tokens() {
            match kind {
                K!['('] => return Some(Kind::SymVar),
                Kind::Symbol => return Some(Kind::SymDecl),
                Kind::Whitespace | Kind::Comment | Kind::DocComment => {}
                _ => break,
            }
        }

        None
    }
}

mod map {
    use super::*;

    pub fn statement(p: &mut CstParser) {
        p.eat(K![map]);

        p.eat_modifiers();

        if p.at() == Kind::Symbol {
            let ident = p.start(Kind::IdentPath);
            p.eat(Kind::Symbol);
            p.end(ident);
        }

        p.eat_trivia();

        {
            p.eat(K!['(']);

            arm(p);
            p.eat(K![,]);
            arm(p);

            if p.at() == K![,] {
                p.eat(K![,]);
            }

            p.eat(K![')']);
        }
    }

    fn arm(p: &mut CstParser) {
        let arm = p.start(Kind::MapArm);

        pattern(p, AllowedPattern { expr: false });

        p.end(arm);
    }
}

pub fn pattern_with_expr(p: &mut CstParser) {
    pattern(p, AllowedPattern { expr: true })
}

fn pattern(p: &mut CstParser, allowed: AllowedPattern) {
    p.eat_space();

    match lookahead_detect_pattern(p) {
        DetectedPattern::Struct => struct_pattern::entry(p),
        DetectedPattern::Set => set_pattern::entry(p),
        DetectedPattern::Expr if allowed.expr => expr_pattern::entry(p),
        _ => {
            p.eat_error(|kind| format!("expected pattern, found {kind}"));
        }
    }
}

/// There's slight ambiguity between:
/// 1. anonymous struct:   `(a: b)`
/// 2. parenthesized expr: `(1 + 2)`
///
/// TODO: improve this algorithm?
pub fn lookahead_detect_pattern(p: &CstParser) -> DetectedPattern {
    let detected = DetectedPattern::Expr;
    let mut param_open = false;
    let mut param_offset = 0;
    let mut name_before_param = false;

    for kind in p.peek_tokens().iter().copied() {
        if param_open {
            param_offset += 1;
        }

        match kind {
            Kind::Symbol => {
                if !param_open {
                    name_before_param = true;
                }
            }
            K!['('] => {
                if name_before_param {
                    return DetectedPattern::Struct;
                }
                param_open = true;
            }
            K!['{'] => return DetectedPattern::Set,
            K![')'] => {
                if param_offset == 1 {
                    // empty parentheses
                    return DetectedPattern::Struct;
                }
            }
            K![:] | K![..] => return DetectedPattern::Struct,
            Kind::Modifier
            | Kind::Whitespace
            | Kind::Comment
            | Kind::DoubleQuoteText
            | Kind::SingleQuoteText
            | K![.]
            | K![?] => {}
            _ => break,
        }
    }

    detected
}

mod struct_pattern {
    use super::*;

    pub fn entry(p: &mut CstParser) {
        let pat = p.start(Kind::PatStruct);

        p.eat_modifiers();

        if p.at() == Kind::Symbol {
            ident_path(p);
        }

        delimited_comma_separated(p, K!['('], &param, K![')']);

        p.end(pat);
    }

    fn param(p: &mut CstParser) {
        if p.at() == K![..] {
            let spread = p.start(Kind::Spread);
            p.eat(K![..]);
            p.eat(Kind::Symbol);
            p.end(spread);
        } else {
            fn lookahead_colon(p: &CstParser) -> bool {
                for kind in p.peek_tokens() {
                    match kind {
                        K![:] => return true,
                        K![,] | K!['('] | K![')'] | K!['}'] | K![']'] => return false,
                        _ => {}
                    }
                }

                false
            }

            if lookahead_colon(p) {
                property(p);
            } else {
                let unit = p.start(Kind::StructParamAttrUnit);
                pattern_with_expr(p);
                p.end(unit);
            }
        }
    }

    pub fn property(p: &mut CstParser) {
        p.eat_trivia();

        let prop = p.start(Kind::StructParamAttrProp);

        {
            let type_ref = p.start(Kind::TypeModUnit);
            type_ref_inner(
                p,
                AllowedType {
                    dot: false,
                    anonymous: false,
                    int_range: false,
                },
                "property type",
            );
            p.end(type_ref);

            if p.at() == K!['['] {
                p.eat_space();
                let rel_args = p.start(Kind::RelArgs);
                delimited_comma_separated(p, K!['['], &param, K![']']);
                p.end(rel_args);
            }

            if p.at() == K![?] {
                let card = p.start(Kind::PropCardinality);
                p.eat(K![?]);
                p.end(card);
            }
        }

        p.eat(K![:]);

        pattern(p, AllowedPattern { expr: true });

        p.end(prop);
    }
}

mod set_pattern {
    use super::*;

    pub fn entry(p: &mut CstParser) {
        let pat = p.start(Kind::PatSet);

        p.eat_modifiers();

        if p.at() == Kind::Symbol {
            ident_path(p);
        }

        delimited_comma_separated(p, K!['{'], &element, K!['}']);

        p.end(pat);
    }

    fn element(p: &mut CstParser) {
        let element = p.start(Kind::SetElement);

        if p.at() == K![..] {
            let spread = p.start(Kind::Spread);
            p.eat(K![..]);
            p.end(spread);
        }

        if p.at() == K!['['] {
            let rel_params = p.start(Kind::RelArgs);
            delimited_comma_separated(p, K!['['], &super::struct_pattern::property, K![']']);
            p.end(rel_params);
        }

        super::pattern(p, AllowedPattern { expr: true });

        p.end(element);
    }
}

pub mod expr_pattern {
    use super::*;

    pub fn entry(p: &mut CstParser) {
        expr_pattern_shunting_yard(p, Delimiter::Eof);
    }

    fn expr_pattern_delimited(p: &mut CstParser) {
        match p.at() {
            kind @ (Kind::Symbol
            | Kind::Number
            | Kind::DoubleQuoteText
            | Kind::SingleQuoteText
            | Kind::Regex) => {
                let atom = p.start(Kind::PatAtom);
                p.eat(kind);
                p.end(atom);
            }
            K!['('] => {
                expr_pattern_shunting_yard(p, Delimiter::Paren);
            }
            _ => {
                p.eat_error(|kind| format!("expected expression pattern, found {kind}"));
            }
        };
    }

    fn expr_pattern_shunting_yard(p: &mut CstParser, delimiter: Delimiter) {
        p.eat_trivia();

        let mut cursor = p.syntax_cursor();

        if matches!(delimiter, Delimiter::Paren) {
            p.eat(K!['(']);
        }
        p.eat_trivia();

        expr_pattern_delimited(p);

        let mut op_stack: Vec<(u8, SyntaxCursor)> = vec![];

        // handle infix wrapping using "shunting yard" stack with saved syntax cursor pointer
        loop {
            let op_kind = match p.at() {
                kind @ (K![-] | K![+] | K![*] | K![/]) => kind,
                Kind::Eof | K![')'] | K!['}'] | K![']'] | K![,] => break,
                _ => {
                    p.eat_error(|kind| {
                        format!("{kind} is an invalid continuation of an expression")
                    });
                    break;
                }
            };

            let (left_bp, _right_bp) = infix_binding_power(op_kind);

            let mut push_cursor = cursor;

            while left_bp <= op_stack.last().copied().map(|(bp, _)| bp).unwrap_or(0) {
                let (_, stack_cursor) = op_stack.pop().unwrap();

                p.insert_node(stack_cursor, Kind::PatBinary);
                push_cursor = stack_cursor;
            }

            op_stack.push((left_bp, push_cursor));
            p.eat(op_kind);

            cursor = p.syntax_cursor();

            expr_pattern_delimited(p);
        }

        if matches!(delimiter, Delimiter::Paren) {
            p.eat(K![')']);
        }

        while let Some((_, stack_cursor)) = op_stack.pop() {
            p.insert_node(stack_cursor, Kind::PatBinary);
        }
    }

    fn infix_binding_power(kind: Kind) -> (u8, u8) {
        match kind {
            K![-] | K![+] => (1, 2),
            K![*] | K![/] => (3, 4),
            _ => panic!(),
        }
    }
}

pub fn ident_path(p: &mut CstParser) {
    p.eat_trivia();

    let ident_path = p.start(Kind::IdentPath);

    p.eat(Kind::Symbol);

    while p.at() == K![.] {
        p.eat(K![.]);
        p.eat(Kind::Symbol);
    }

    p.end(ident_path);
}

fn delimited_comma_separated(
    p: &mut CstParser,
    open: Kind,
    inner: &dyn Fn(&mut CstParser),
    close: Kind,
) {
    p.eat(open);

    if p.at() != close {
        loop {
            inner(p);

            if p.at() == K![,] {
                p.eat(K![,]);

                if p.at() == close {
                    // it was a trailing comma
                    break;
                }
            } else {
                break;
            }
        }
    }
    p.eat(close);
}
