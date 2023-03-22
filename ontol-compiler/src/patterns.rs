use fnv::FnvHashMap;
use ontol_runtime::{
    string_pattern::{StringPattern, StringPatternConstantPart, StringPatternProperty},
    value::PropertyId,
    DefId,
};
use regex::Regex;
use regex_syntax::hir::{Anchor, Group, GroupKind, Hir, Literal};
use smartstring::alias::String;
use std::fmt::Write;
use tracing::debug;

use crate::{
    regex::{collect_hir_constant_parts, constant_prefix, empty_string_regex},
    relation::Constructor,
    Compiler,
};

#[derive(Default, Debug)]
pub struct Patterns {
    pub string_patterns: FnvHashMap<DefId, StringPattern>,
}

#[derive(Clone, Default, Debug)]
pub enum StringPatternSegment {
    /// Matches only the empty string:
    #[default]
    EmptyString,
    Literal(String),
    Regex(Hir),
    Property {
        property_id: PropertyId,
        type_def_id: DefId,
        segment: Box<StringPatternSegment>,
    },
    Concat(Vec<StringPatternSegment>),
    Alternation(Vec<StringPatternSegment>),
}

impl StringPatternSegment {
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
                (Some(Self::Literal(s1)), Self::Literal(s2)) => prev = Some(Self::Literal(s1 + s2)),
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
            StringPatternSegment::Concat(output)
        }
    }

    pub fn constant_prefix(&self) -> Option<String> {
        match self {
            Self::EmptyString => None,
            Self::Literal(string) => Some(string.clone()),
            Self::Regex(hir) => constant_prefix(hir),
            Self::Property { .. } => None,
            Self::Concat(segments) => segments.iter().next().and_then(Self::constant_prefix),
            Self::Alternation(_) => None,
        }
    }

    fn to_regex_hir(&self, capture_cursor: &mut CaptureCursor) -> Hir {
        match self {
            Self::EmptyString => empty_string_regex(),
            Self::Literal(string) => Hir::concat(
                string
                    .chars()
                    .map(|char| Hir::literal(Literal::Unicode(char)))
                    .collect(),
            ),
            Self::Regex(hir) => hir.clone(),
            Self::Property { segment, .. } => {
                let index = capture_cursor.increment();
                debug!("creating capture group with index {index}");
                Hir::group(Group {
                    kind: GroupKind::CaptureIndex(index),
                    hir: Box::new(segment.to_regex_hir(capture_cursor)),
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

    pub fn collect_constant_parts(
        &self,
        parts: &mut Vec<StringPatternConstantPart>,
        capture_cursor: &mut CaptureCursor,
    ) {
        match self {
            Self::EmptyString => {}
            Self::Literal(string) => {
                parts.push(StringPatternConstantPart::Literal(string.clone()));
            }
            Self::Regex(hir) => {
                let mut string = String::new();
                collect_hir_constant_parts(hir, &mut string);
                if !string.is_empty() {
                    parts.push(StringPatternConstantPart::Literal(string));
                }
            }
            Self::Property {
                property_id,
                type_def_id,
                ..
            } => {
                let index = capture_cursor.increment();
                parts.push(StringPatternConstantPart::Property(StringPatternProperty {
                    property_id: *property_id,
                    type_def_id: *type_def_id,
                    capture_group: index as usize,
                }));
            }
            Self::Concat(segments) => {
                for segment in segments {
                    segment.collect_constant_parts(parts, capture_cursor);
                }
            }
            Self::Alternation(_) => {}
        }
    }
}

pub fn compile_all_patterns(compiler: &mut Compiler) {
    compile_regex_literals(compiler);
    compile_string_pattern_constructors(compiler);
}

/// note: This processes all regex literals even if not "needed"
/// by serializers etc
fn compile_regex_literals(compiler: &mut Compiler) {
    let literal_regex_hirs = std::mem::take(&mut compiler.defs.literal_regex_hirs);

    for (def_id, hir) in literal_regex_hirs {
        compiler.patterns.string_patterns.insert(
            def_id,
            StringPattern {
                regex: compile_regex(hir),
                constant_parts: vec![],
            },
        );
    }
}

fn compile_string_pattern_constructors(compiler: &mut Compiler) {
    let string_patterns = std::mem::take(&mut compiler.relations.string_pattern_constructors);

    for def_id in string_patterns {
        let segment = match compiler
            .relations
            .properties_by_type(def_id)
            .map(|p| &p.constructor)
        {
            Some(Constructor::StringPattern(segment)) => segment,
            _ => panic!("{def_id:?} does not have a string pattern constructor"),
        };

        let anchored_hir = Hir::concat(vec![
            Hir::anchor(Anchor::StartText),
            segment.to_regex_hir(&mut CaptureCursor(1)),
            Hir::anchor(Anchor::EndText),
        ]);
        debug!("{def_id:?} regex: {anchored_hir}");

        let mut constant_parts = vec![];
        segment.collect_constant_parts(&mut constant_parts, &mut CaptureCursor(1));

        compiler.patterns.string_patterns.insert(
            def_id,
            StringPattern {
                regex: compile_regex(anchored_hir),
                constant_parts,
            },
        );
    }
}

fn compile_regex(hir: Hir) -> Regex {
    let mut expression = String::new();
    write!(&mut expression, "{hir}").unwrap();

    Regex::new(&expression).unwrap()
}

pub struct CaptureCursor(pub u32);

impl CaptureCursor {
    fn increment(&mut self) -> u32 {
        let cursor = self.0;
        self.0 += 1;
        cursor
    }
}
