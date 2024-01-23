use fnv::FnvHashMap;
use ontol_runtime::{var::Var, DefId};
use smartstring::alias::String;

use crate::{source::SourceSpan, Compiler};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct PatId(pub u32);

#[derive(Debug)]
pub struct Pattern {
    pub id: PatId,
    pub kind: PatternKind,
    pub span: SourceSpan,
}

impl Pattern {
    pub fn kind(&self) -> &PatternKind {
        &self.kind
    }
}

#[derive(Debug)]
pub enum PatternKind {
    /// Function call
    Call(DefId, Box<[Pattern]>),
    /// Some kind of compound that is "unpacked" to expose inner variables
    Compound {
        /// The user-supplied type of the compound
        type_path: TypePath,
        modifier: Option<CompoundPatternModifier>,
        // The single attribute is a unit binding. I.e. `path: x` syntax
        is_unit_binding: bool,
        attributes: Box<[CompoundPatternAttr]>,
    },
    /// Expression enclosed in set-notation braces: `{expr..}`
    Set {
        val_type_def: Option<DefId>,
        elements: Vec<SetPatternElement>,
    },
    Variable(Var),
    ConstI64(i64),
    ConstText(String),
    Regex(RegexPattern),
    ContainsElement(Box<Pattern>),
    SetOperator {
        operator: SetBinaryOperator,
        element: Box<SetPatternElement>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum CompoundPatternModifier {
    Match,
}

#[derive(Debug)]
pub struct CompoundPatternAttr {
    pub key: (DefId, SourceSpan),
    pub rel: Option<Pattern>,
    pub bind_option: bool,
    pub value: Pattern,
}

#[derive(Debug)]
pub struct SetPatternElement {
    pub iter: bool,
    pub pattern: Pattern,
}

/// Binary operator that accepts a set on its right hand side
#[derive(Debug)]
pub enum SetBinaryOperator {
    ElementIn,
    AllIn,
    ContainsAll,
    Intersects,
    SetEquals,
}

#[derive(Debug)]
pub struct RegexPattern {
    pub regex_def_id: DefId,
    pub capture_node: RegexPatternCaptureNode,
}

#[derive(Debug)]
pub enum RegexPatternCaptureNode {
    // Capture: Leaf node
    Capture {
        var: Var,
        capture_index: u32,
        name_span: SourceSpan,
    },
    /// "AND"
    Concat { nodes: Vec<RegexPatternCaptureNode> },
    /// "OR"
    Alternation {
        variants: Vec<RegexPatternCaptureNode>,
    },
    /// "QUANTIFY"
    Repetition {
        pat_id: PatId,
        node: Box<RegexPatternCaptureNode>,
    },
}

impl RegexPatternCaptureNode {
    pub fn flatten(input: Vec<RegexPatternCaptureNode>) -> Vec<RegexPatternCaptureNode> {
        if input.len() == 1 {
            match input.into_iter().next().unwrap() {
                RegexPatternCaptureNode::Concat { nodes } => {
                    if nodes.len() == 1 {
                        nodes
                    } else {
                        vec![RegexPatternCaptureNode::Concat { nodes }]
                    }
                }
                other => vec![other],
            }
        } else {
            input
        }
    }

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
pub enum TypePath {
    // The type path is specified, and resolved to the given DefId
    Specified { def_id: DefId, span: SourceSpan },
    // The type path is anonymous and its structure should be inferred by its fields
    Inferred { def_id: DefId },
    // The type path is contextual (relation parameter)
    RelContextual,
}

impl<'m> Compiler<'m> {
    pub fn expr(&mut self, kind: PatternKind, span: SourceSpan) -> Pattern {
        Pattern {
            id: self.patterns.alloc_pat_id(),
            kind,
            span,
        }
    }
}

#[derive(Debug)]
pub struct Patterns {
    next_pat_id: PatId,
    pub table: FnvHashMap<PatId, Pattern>,
}

impl Default for Patterns {
    fn default() -> Self {
        Self {
            next_pat_id: PatId(0),
            table: Default::default(),
        }
    }
}

impl Patterns {
    pub fn alloc_pat_id(&mut self) -> PatId {
        let id = self.next_pat_id;
        self.next_pat_id.0 += 1;
        id
    }
}
