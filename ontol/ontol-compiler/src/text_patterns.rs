use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{
    ontology::ontol::text_pattern::{
        Regex, TextPattern, TextPatternConstantPart, TextPatternProperty,
    },
    DefId, DefPropTag, PropId,
};
use regex_syntax::hir::{Capture, Hir, Look};
use std::fmt::Write;
use tracing::debug;

use crate::{
    primitive::Primitives,
    properties::Constructor,
    regex_util::{self, unsigned_integer_with_radix},
    strings::StringCtx,
    Compiler,
};

#[derive(Default)]
pub struct TextPatterns {
    pub text_patterns: FnvHashMap<DefId, TextPattern>,
}

#[derive(Clone, Default, Debug)]
pub enum TextPatternSegment {
    /// Matches only the empty string:
    #[default]
    EmptyString,
    AnyString,
    Literal(String),
    Serial {
        radix: u8,
    },
    Regex(Hir),
    Attribute {
        prop_id: PropId,
        type_def_id: DefId,
        segment: Box<TextPatternSegment>,
    },
    Concat(Vec<TextPatternSegment>),
    #[allow(unused)]
    Alternation(Vec<TextPatternSegment>),
}

impl TextPatternSegment {
    pub fn new_literal(string: &str) -> Self {
        Self::Literal(string.into())
    }

    pub fn concat(segments: impl IntoIterator<Item = Self>) -> Self {
        let mut output = vec![];
        let mut prev = None;

        for segment in segments {
            match (prev, segment) {
                (Some(Self::EmptyString), Self::EmptyString) => {
                    prev = None;
                }
                (Some(Self::EmptyString), segment) => {
                    prev = Some(segment);
                }
                (Some(previous), Self::EmptyString) => {
                    prev = Some(previous);
                }
                (None, segment) => {
                    prev = Some(segment);
                }
                (Some(Self::Literal(s1)), Self::Literal(s2)) => {
                    prev = Some(Self::Literal(format!("{s1}{s2}")))
                }
                (Some(Self::Regex(r1)), Self::Regex(r2)) => {
                    prev = Some(Self::Regex(Hir::concat(vec![r1, r2])));
                }
                (Some(Self::Concat(mut s1)), Self::Concat(mut s2)) => {
                    s1.append(&mut s2);
                    prev = Some(Self::Concat(s1));
                }
                (Some(prev_done), segment) => {
                    output.push(prev_done);
                    prev = Some(segment);
                }
            }
        }

        if let Some(prev) = prev {
            output.push(prev);
        }

        if output.len() == 1 {
            output.into_iter().next().unwrap()
        } else {
            TextPatternSegment::Concat(output)
        }
    }

    pub fn constant_prefix(&self) -> Option<String> {
        match self {
            Self::EmptyString => None,
            Self::AnyString => None,
            Self::Literal(string) => Some(string.clone()),
            Self::Serial { .. } => None,
            Self::Regex(hir) => regex_util::constant_prefix(hir),
            Self::Attribute { .. } => None,
            Self::Concat(segments) => segments.iter().next().and_then(Self::constant_prefix),
            Self::Alternation(_) => None,
        }
    }

    fn to_regex_hir(&self, capture_cursor: &mut CaptureCursor) -> Hir {
        match self {
            Self::EmptyString => regex_util::well_known::empty_string(),
            Self::AnyString => {
                let index = capture_cursor.increment();
                Hir::capture(Capture {
                    index,
                    name: None,
                    sub: Box::new(regex_util::well_known::set_of_all_strings()),
                })
            }
            Self::Literal(string) => Hir::literal(string.as_bytes()),
            Self::Serial { radix } => {
                let index = capture_cursor.increment();
                Hir::capture(Capture {
                    index,
                    name: None,
                    sub: Box::new(unsigned_integer_with_radix(*radix)),
                })
            }
            Self::Regex(hir) => hir.clone(),
            Self::Attribute { segment, .. } => {
                let index = capture_cursor.increment();
                debug!("creating capture group with index {index}");
                Hir::capture(Capture {
                    index,
                    name: None,
                    sub: Box::new(segment.to_regex_hir(capture_cursor)),
                })
            }
            Self::Concat(segments) => Hir::concat(
                segments
                    .iter()
                    .map(|segment| segment.to_regex_hir(capture_cursor))
                    .collect(),
            ),
            Self::Alternation(segments) => Hir::alternation(
                segments
                    .iter()
                    .map(|segment| segment.to_regex_hir(capture_cursor))
                    .collect(),
            ),
        }
    }

    pub fn collect_attributes(
        &self,
        output: &mut FnvHashSet<(PropId, DefId)>,
        primitives: &Primitives,
    ) {
        match self {
            Self::Attribute {
                prop_id,
                type_def_id,
                segment,
            } => {
                output.insert((*prop_id, *type_def_id));
                segment.collect_attributes(output, primitives);
            }
            Self::AnyString => {
                // note: should match what ontol-runtime/../text_matcher.rs does
                let prop_id = PropId(primitives.text, DefPropTag(0));
                output.insert((prop_id, primitives.text));
            }
            Self::Concat(segments) | Self::Alternation(segments) => {
                for segment in segments {
                    segment.collect_attributes(output, primitives);
                }
            }
            _ => {}
        }
    }

    pub fn collect_constant_parts(
        &self,
        parts: &mut Vec<TextPatternConstantPart>,
        capture_cursor: &mut CaptureCursor,
        strings: &mut StringCtx,
    ) {
        match self {
            Self::EmptyString => {}
            Self::AnyString => {
                parts.push(TextPatternConstantPart::AnyString {
                    capture_group: capture_cursor.increment() as usize,
                });
            }
            Self::Literal(string) => {
                let constant = strings.intern_constant(string);
                parts.push(TextPatternConstantPart::Literal(constant));
            }
            Self::Serial { .. } => {}
            Self::Regex(hir) => {
                let mut string = String::new();
                regex_util::collect_hir_constant_parts(hir, &mut string);
                if !string.is_empty() {
                    let constant = strings.intern_constant(&string);
                    parts.push(TextPatternConstantPart::Literal(constant));
                }
            }
            Self::Attribute {
                prop_id,
                type_def_id,
                ..
            } => {
                let index = capture_cursor.increment();
                parts.push(TextPatternConstantPart::Property(TextPatternProperty {
                    prop_id: *prop_id,
                    type_def_id: *type_def_id,
                    capture_group: index as usize,
                }));
            }
            Self::Concat(segments) => {
                for segment in segments {
                    segment.collect_constant_parts(parts, capture_cursor, strings);
                }
            }
            Self::Alternation(_) => {}
        }
    }
}

pub fn compile_all_text_patterns(compiler: &mut Compiler) {
    compile_regex_literals(compiler);
    compile_text_pattern_constructors(compiler);
}

/// note: This processes all regex literals even if not "needed"
/// by serializers etc
fn compile_regex_literals(compiler: &mut Compiler) {
    let literal_regex_asts = std::mem::take(&mut compiler.defs.literal_regex_meta_table);

    for (def_id, ast) in literal_regex_asts {
        compiler.text_patterns.text_patterns.insert(
            def_id,
            TextPattern {
                regex: compile_regex(ast.hir),
                constant_parts: vec![],
            },
        );
    }
}

fn compile_text_pattern_constructors(compiler: &mut Compiler) {
    let text_patterns = std::mem::take(&mut compiler.misc_ctx.text_pattern_constructors);

    for def_id in text_patterns {
        let segment = match compiler
            .prop_ctx
            .properties_by_def_id(def_id)
            .map(|p| &p.constructor)
        {
            Some(Constructor::TextFmt(segment)) => segment,
            _ => panic!("{def_id:?} does not have a string pattern constructor"),
        };

        store_text_pattern_segment(
            def_id,
            segment,
            &mut compiler.text_patterns,
            &mut compiler.str_ctx,
        );
    }
}

pub fn store_text_pattern_segment(
    def_id: DefId,
    segment: &TextPatternSegment,
    patterns: &mut TextPatterns,
    strings: &mut StringCtx,
) {
    let anchored_hir = Hir::concat(vec![
        Hir::look(Look::Start),
        segment.to_regex_hir(&mut CaptureCursor(1)),
        Hir::look(Look::End),
    ]);

    let mut constant_parts = vec![];
    segment.collect_constant_parts(&mut constant_parts, &mut CaptureCursor(1), strings);

    patterns.text_patterns.insert(
        def_id,
        TextPattern {
            regex: compile_regex(anchored_hir),
            constant_parts,
        },
    );
}

fn compile_regex(hir: Hir) -> Regex {
    let mut pattern = std::string::String::new();
    write!(&mut pattern, "{hir}").unwrap();

    Regex {
        regex_impl: regex_automata::meta::Regex::builder()
            .build_from_hir(&hir)
            .unwrap(),
        pattern,
    }
}

pub struct CaptureCursor(pub u32);

impl CaptureCursor {
    fn increment(&mut self) -> u32 {
        let cursor = self.0;
        self.0 += 1;
        cursor
    }
}
