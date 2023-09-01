use std::{collections::HashMap, str::Chars};

use ontol_parser::Span;
use ontol_runtime::{smart_format, DefId};
use regex_syntax::{
    ast::{Ast, GroupKind},
    hir::{Class, ClassUnicode, ClassUnicodeRange, Hir, HirKind, Literal, Look, Repetition},
    Parser,
};
use smartstring::alias::String;

use crate::{
    def::RegexMeta,
    lowering::MapVarTable,
    pattern::{Patterns, RegexPattern, RegexPatternCaptureNode},
    SourceSpan, Src,
};

pub fn uuid() -> Hir {
    let hex = Hir::class(Class::Unicode(ClassUnicode::new([
        ClassUnicodeRange::new('0', '9'),
        ClassUnicodeRange::new('a', 'f'),
        ClassUnicodeRange::new('A', 'F'),
    ])));
    let dash = Hir::literal("-".as_bytes());

    fn repeat_exact(hir: Hir, n: u32) -> Hir {
        Hir::repetition(Repetition {
            min: n,
            max: Some(n),
            greedy: true,
            sub: Box::new(hir),
        })
    }

    Hir::alternation(vec![
        repeat_exact(hex.clone(), 32),
        Hir::concat(vec![
            repeat_exact(hex.clone(), 8),
            dash.clone(),
            repeat_exact(hex.clone(), 4),
            dash.clone(),
            repeat_exact(hex.clone(), 4),
            dash.clone(),
            repeat_exact(hex.clone(), 4),
            dash,
            repeat_exact(hex, 12),
        ]),
    ])
}

pub fn datetime_rfc3339() -> Hir {
    Parser::new()
        .parse(r"((?:([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?))(Z|[\+-][0-9]{2}:[0-9]{2})?)")
        .unwrap()
}

pub fn empty_string() -> Hir {
    Hir::concat(vec![Hir::look(Look::Start), Hir::look(Look::End)])
}

pub fn set_of_all_strings() -> Hir {
    Parser::new().parse(r#".*"#).unwrap()
}

pub fn collect_hir_constant_parts(hir: &Hir, parts: &mut String) {
    match hir.kind() {
        HirKind::Literal(Literal(bytes)) => parts.push_str(std::str::from_utf8(bytes).unwrap()),
        HirKind::Concat(hirs) => {
            for child in hirs {
                collect_hir_constant_parts(child, parts);
            }
        }
        _ => {}
    }
}

pub fn constant_prefix(hir: &Hir) -> Option<String> {
    if let HirKind::Concat(hirs) = hir.kind() {
        let mut iterator = hirs.iter();
        let first = iterator.next()?;

        match first.kind() {
            HirKind::Look(Look::Start | Look::StartLF | Look::StartCRLF) => {}
            _ => return None,
        }

        let mut prefix = String::new();

        for next in iterator {
            match next.kind() {
                HirKind::Literal(Literal(bytes)) => {
                    prefix.push_str(std::str::from_utf8(bytes).unwrap());
                }
                _ => {
                    break;
                }
            }
        }

        if prefix.is_empty() {
            None
        } else {
            Some(prefix)
        }
    } else {
        None
    }
}

pub fn parse_literal_regex<'m>(
    pattern: &'m str,
    pattern_span: &Span,
) -> Result<RegexMeta<'m>, (String, Span)> {
    let mut ast_parser = regex_syntax::ast::parse::Parser::new();
    let ast = match ast_parser.parse(pattern) {
        Ok(ast) => ast,
        Err(err) => {
            return Err((
                smart_format!("{}", err.kind()),
                project_regex_span(pattern, pattern_span, err.span()),
            ))
        }
    };

    let mut translator = regex_syntax::hir::translate::Translator::new();
    let hir = match translator.translate(pattern, &ast) {
        Ok(hir) => hir,
        Err(err) => {
            return Err((
                smart_format!("{}", err.kind()),
                project_regex_span(pattern, pattern_span, err.span()),
            ))
        }
    };

    Ok(RegexMeta { pattern, ast, hir })
}

pub struct RegexToPatternLowerer<'a> {
    named_capture_spans: HashMap<String, SourceSpan>,
    current_nodes: Vec<RegexPatternCaptureNode>,
    pushback: Vec<StackNode>,

    pattern_literal: &'a str,
    pattern_span: &'a Span,
    src: &'a Src,
    var_table: &'a mut MapVarTable,
    patterns: &'a mut Patterns,
}

impl<'a> RegexToPatternLowerer<'a> {
    pub fn new(
        pattern_literal: &'a str,
        pattern_span: &'a Span,
        src: &'a Src,
        var_table: &'a mut MapVarTable,
        patterns: &'a mut Patterns,
    ) -> Self {
        Self {
            named_capture_spans: Default::default(),
            current_nodes: vec![],
            pushback: vec![],
            pattern_literal,
            pattern_span,
            src,
            var_table,
            patterns,
        }
    }

    pub fn syntax_visitor(&mut self) -> RegexSyntaxVisitor<'_, 'a> {
        RegexSyntaxVisitor(self)
    }

    pub fn into_expr(self, regex_def_id: DefId) -> RegexPattern {
        let capture_node = Self::into_single_node(self.current_nodes);

        RegexPattern {
            regex_def_id,
            capture_node,
        }
    }

    fn pushback(&mut self, combinator: RegexCombinator) {
        let pushed = std::mem::take(&mut self.current_nodes);
        self.pushback.push(StackNode { combinator, pushed });
    }

    fn pop(&mut self) -> Vec<RegexPatternCaptureNode> {
        let mut stack_node = self.pushback.pop().unwrap();
        std::mem::swap(&mut stack_node.pushed, &mut self.current_nodes);
        stack_node.pushed
    }

    fn into_single_node(mut nodes: Vec<RegexPatternCaptureNode>) -> RegexPatternCaptureNode {
        loop {
            match nodes.len() {
                0 => {
                    return RegexPatternCaptureNode::Concat { nodes: vec![] };
                }
                1 => match nodes.into_iter().next().unwrap() {
                    node @ RegexPatternCaptureNode::Capture { .. } => return node,
                    RegexPatternCaptureNode::Concat {
                        nodes: concat_nodes,
                    } => {
                        nodes = concat_nodes;
                    }
                    RegexPatternCaptureNode::Alternation { variants } => {
                        nodes = variants;
                    }
                    node @ RegexPatternCaptureNode::Repetition { .. } => {
                        return node;
                    }
                },
                _ => {
                    return RegexPatternCaptureNode::Concat { nodes };
                }
            }
        }
    }
}

struct StackNode {
    combinator: RegexCombinator,
    pushed: Vec<RegexPatternCaptureNode>,
}

enum RegexCombinator {
    Concat,
    Alt,
    Rep,
}

pub struct RegexSyntaxVisitor<'l, 'a>(pub &'l mut RegexToPatternLowerer<'a>);

/// This is the first pass of regex for pattern/expr analysis.
/// The AST visitor figures out variable spans of named capture groups.
impl<'l, 'a> regex_syntax::ast::Visitor for RegexSyntaxVisitor<'l, 'a> {
    type Output = ();
    type Err = ();

    fn finish(self) -> Result<Self::Output, Self::Err> {
        Ok(())
    }

    fn visit_pre(&mut self, ast: &Ast) -> Result<(), Self::Err> {
        if let Ast::Group(group) = ast {
            if let GroupKind::CaptureName { name, .. } = &group.kind {
                self.0.named_capture_spans.insert(
                    name.name.as_str().into(),
                    self.0.src.span(&project_regex_span(
                        self.0.pattern_literal,
                        self.0.pattern_span,
                        &name.span,
                    )),
                );
            }
        }

        Ok(())
    }
}

/// Second pass of regex for pattern/expr analysis.
impl<'l, 'a> regex_syntax::hir::Visitor for RegexSyntaxVisitor<'l, 'a> {
    type Output = ();
    type Err = ();

    fn finish(self) -> Result<Self::Output, Self::Err> {
        Ok(())
    }

    fn visit_pre(&mut self, hir: &Hir) -> Result<(), Self::Err> {
        match hir.kind() {
            HirKind::Capture(capture) => {
                if let Some(name) = &capture.name {
                    let span = self.0.named_capture_spans.get(name.as_ref()).unwrap();
                    let var = self.0.var_table.get_or_create_var(name.as_ref().into());

                    self.0.current_nodes.push(RegexPatternCaptureNode::Capture {
                        var,
                        capture_index: capture.index,
                        name_span: *span,
                    });
                }
            }
            HirKind::Repetition(_) => {
                self.0.pushback(RegexCombinator::Rep);
            }
            HirKind::Concat(_) => {
                self.0.pushback(RegexCombinator::Concat);
            }
            HirKind::Alternation(_) => {
                self.0.pushback(RegexCombinator::Alt);
            }
            _ => {}
        }

        Ok(())
    }

    fn visit_post(&mut self, hir: &Hir) -> Result<(), Self::Err> {
        match hir.kind() {
            HirKind::Repetition(_) => {
                let nodes = self.0.pop();
                if !nodes.is_empty() {
                    let pat_id = self.0.patterns.alloc_pat_id();
                    if nodes.len() == 1 {
                        self.0
                            .current_nodes
                            .push(RegexPatternCaptureNode::Repetition {
                                pat_id,
                                node: Box::new(nodes.into_iter().next().unwrap()),
                            });
                    } else {
                        self.0
                            .current_nodes
                            .push(RegexPatternCaptureNode::Repetition {
                                pat_id,
                                node: Box::new(RegexPatternCaptureNode::Concat { nodes }),
                            });
                    }
                }
            }
            HirKind::Concat(_) => {
                let nodes = self.0.pop();
                if !nodes.is_empty() {
                    if let Some(RegexCombinator::Concat) = self
                        .0
                        .pushback
                        .last()
                        .map(|stack_node| &stack_node.combinator)
                    {
                        // Merge into existing concat
                        self.0.current_nodes.extend(nodes);
                    } else {
                        self.0
                            .current_nodes
                            .push(RegexPatternCaptureNode::Concat { nodes });
                    }
                }
            }
            HirKind::Alternation(_) => {
                let variants = self.0.pop();
                if !variants.is_empty() {
                    if let Some(RegexCombinator::Alt) = self
                        .0
                        .pushback
                        .last()
                        .map(|stack_node| &stack_node.combinator)
                    {
                        // Merge into existing alternation
                        self.0.current_nodes.extend(variants);
                    } else {
                        self.0
                            .current_nodes
                            .push(RegexPatternCaptureNode::Alternation { variants });
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

fn project_regex_span(
    pattern: &str,
    pattern_span: &Span,
    ast_span: &regex_syntax::ast::Span,
) -> Span {
    // literal regexes start with '/' so that's part of the ontol span,
    // but regex-syntax never sees that, so add 1.

    struct Scanner {
        source_cursor: usize,
        regex_cursor: usize,
    }

    impl Scanner {
        fn advance_to_regex_pos(&mut self, chars: &mut Chars, regex_pos: usize) -> usize {
            while self.regex_cursor < regex_pos {
                let char = chars.next().unwrap();
                match char {
                    // slash is the only escaped character in literal regexes
                    '/' => self.source_cursor += 2,
                    _ => self.source_cursor += 1,
                }
                self.regex_cursor += 1;
            }
            self.source_cursor
        }
    }

    let mut scanner = Scanner {
        source_cursor: pattern_span.start + 1,
        regex_cursor: 0,
    };

    let mut chars = pattern.chars();
    let start = scanner.advance_to_regex_pos(&mut chars, ast_span.start.offset);
    let end = scanner.advance_to_regex_pos(&mut chars, ast_span.end.offset);

    Span { start, end }
}
