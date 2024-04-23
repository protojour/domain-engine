use crate::{lexer::kind::Kind, K};

use super::parser::{CstParser, SyntaxCursor};

fn map_stmt(p: &mut CstParser) {
    p.eat(K![map]);
    p.eat(Kind::Sym);

    p.eat(K!['(']);
    p.eat(K![')']);
}

pub fn expr_pattern(p: &mut CstParser) {
    expr_pattern_shunting_yard(p, false);
}

pub fn expr_pattern_shunting_yard(p: &mut CstParser, parenthesized: bool) {
    p.eat_trivia();
    if parenthesized {
        p.eat(K!['(']);
    }
    p.eat_trivia();

    let mut cursor = p.syntax_cursor();

    expr_pattern_delimited(p);

    let mut op_stack: Vec<(u8, SyntaxCursor)> = vec![];

    // handle infix wrapping using "shunting yard" stack with saved syntax cursor pointer
    loop {
        let op_kind = match p.at() {
            kind @ (K![-] | K![+] | K![*] | K![/]) => kind,
            Kind::Eof | K![')'] => break,
            other => {
                p.error();
                break;
            }
        };

        let (left_bp, right_bp) = infix_binding_power(op_kind);

        let mut push_cursor = cursor;

        while left_bp < op_stack.last().copied().map(|(bp, _)| bp).unwrap_or(0) {
            let (_, stack_cursor) = op_stack.pop().unwrap();

            p.insert_node(stack_cursor, Kind::ExprPatternBinary);
            push_cursor = stack_cursor;
        }

        op_stack.push((left_bp, push_cursor));
        p.eat(op_kind);

        cursor = p.syntax_cursor();

        expr_pattern_delimited(p);
    }

    while let Some((_, stack_cursor)) = op_stack.pop() {
        p.insert_node(stack_cursor, Kind::ExprPatternBinary);
    }

    if parenthesized {
        p.eat(K![')']);
    }
}

fn infix_binding_power(kind: Kind) -> (u8, u8) {
    match kind {
        K![-] | K![+] => (1, 2),
        K![*] | K![/] => (3, 4),
        _ => panic!(),
    }
}

fn expr_pattern_delimited(p: &mut CstParser) {
    p.eat_trivia();
    match p.at() {
        kind @ (Kind::Sym
        | Kind::Number
        | Kind::DoubleQuoteText
        | Kind::SingleQuoteText
        | Kind::Regex) => {
            let cursor = p.start_node();
            p.eat(kind);
            p.commit_node(cursor, Kind::ExprPatternAtom);
        }
        K!['('] => {
            expr_pattern_shunting_yard(p, true);
        }
        _ => {
            p.error();
            todo!()
        }
    };
}
