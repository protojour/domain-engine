use fnv::FnvHashMap;
use ontol_runtime::DefId;
use smartstring::alias::String;

use crate::{source::SourceSpan, Compiler};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct ExprId(pub u32);

#[derive(Debug)]
pub struct Expr {
    pub id: ExprId,
    pub kind: ExprKind,
    pub span: SourceSpan,
}

type PropertyKey = (DefId, SourceSpan);

#[derive(Debug)]
pub enum ExprKind {
    /// Function call
    Call(DefId, Box<[Expr]>),
    Struct {
        /// The user-supplied type of the struct, None means anonymous
        type_path: Option<TypePath>,
        modifier: Option<ExprStructModifier>,
        attributes: Box<[ExprStructAttr]>,
    },
    /// Expression enclosed in sequence brackets: `[expr]`
    Seq(ExprId, Vec<ExprSeqElement>),
    Variable(ontol_hir::Var),
    ConstI64(i64),
    ConstString(String),
    Regex(ExprRegex),
}

#[derive(Clone, Copy, Debug)]
pub enum ExprStructModifier {
    Match,
}

#[derive(Debug)]
pub struct ExprStructAttr {
    pub key: PropertyKey,
    pub rel: Option<Expr>,
    pub bind_option: bool,
    pub value: Expr,
}

#[derive(Debug)]
pub struct ExprSeqElement {
    pub iter: bool,
    pub expr: Expr,
}

#[derive(Debug)]
pub struct ExprRegex {
    pub regex_def_id: DefId,
    pub capture_node: ExprRegexCaptureNode,
}

#[derive(Debug)]
pub enum ExprRegexCaptureNode {
    // Capture: Leaf node
    Capture {
        var: ontol_hir::Var,
        capture_index: u32,
        name_span: SourceSpan,
    },
    /// "AND"
    Concat { nodes: Vec<ExprRegexCaptureNode> },
    /// "OR"
    Alternation { variants: Vec<ExprRegexCaptureNode> },
    /// "QUANTIFY"
    Repetition {
        expr_id: ExprId,
        node: Box<ExprRegexCaptureNode>,
    },
}

impl ExprRegexCaptureNode {
    /// This method changes the input span to the sum of the named capture groups (if any found).
    pub fn constrain_span(&self, mut full_span: SourceSpan) -> SourceSpan {
        self.constrain_span_inner(&mut full_span);
        full_span
    }

    fn constrain_span_inner(&self, output: &mut SourceSpan) {
        match self {
            Self::Capture { name_span, .. } => {
                output.source_id = name_span.source_id;
                output.start = std::cmp::min(output.start, name_span.start);
                output.end = std::cmp::max(output.end, name_span.end);
            }
            Self::Concat { nodes } => {
                for node in nodes {
                    node.constrain_span_inner(output);
                }
            }
            Self::Alternation { variants } => {
                for variant in variants {
                    variant.constrain_span_inner(output);
                }
            }
            Self::Repetition { node, .. } => {
                node.constrain_span_inner(output);
            }
        }
    }
}

#[derive(Debug)]
pub struct TypePath {
    pub def_id: DefId,
    pub span: SourceSpan,
}

impl<'m> Compiler<'m> {
    pub fn expr(&mut self, kind: ExprKind, span: SourceSpan) -> Expr {
        Expr {
            id: self.expressions.alloc_expr_id(),
            kind,
            span,
        }
    }
}

#[derive(Debug)]
pub struct Expressions {
    next_expr_id: ExprId,
    pub table: FnvHashMap<ExprId, Expr>,
}

impl Default for Expressions {
    fn default() -> Self {
        Self {
            next_expr_id: ExprId(0),
            table: Default::default(),
        }
    }
}

impl Expressions {
    pub fn alloc_expr_id(&mut self) -> ExprId {
        let id = self.next_expr_id;
        self.next_expr_id.0 += 1;
        id
    }
}
