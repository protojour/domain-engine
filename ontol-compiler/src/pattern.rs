use ontol_runtime::{
    string_pattern::{StringPattern, StringPatternConstantPart},
    DefId,
};
use regex::Regex;
use regex_syntax::hir::{Anchor, Hir, Literal};
use smartstring::alias::String;
use std::{collections::HashMap, fmt::Write};
use tracing::debug;

use crate::{compiler::Compiler, relation::Constructor};

#[derive(Default, Debug)]
pub struct Patterns {
    pub string_patterns: HashMap<DefId, StringPattern>,
}

#[derive(Clone, Default, Debug)]
pub enum StringPatternSegment {
    #[default]
    Empty,
    Literal(String),
    Regex(Hir),
    Property,
    Concat(Vec<StringPatternSegment>),
    Alternation(Vec<StringPatternSegment>),
}

impl StringPatternSegment {
    pub fn literal(string: &str) -> Self {
        Self::Literal(string.into())
    }

    pub fn concat(segments: impl IntoIterator<Item = Self>) -> Self {
        let mut output = vec![];
        let mut prev = None;

        for segment in segments {
            match (prev, segment) {
                (None, segment) => {
                    prev = Some(segment);
                }
                (Some(Self::Empty), Self::Empty) => {
                    prev = Some(Self::Empty);
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

    fn to_regex_hir(&self) -> Hir {
        match self {
            Self::Empty => Hir::empty(),
            Self::Literal(string) => Hir::concat(
                string
                    .chars()
                    .map(|char| Hir::literal(Literal::Unicode(char)))
                    .collect(),
            ),
            Self::Regex(hir) => hir.clone(),
            Self::Property => todo!(),
            Self::Concat(segments) => Hir::concat(
                segments
                    .iter()
                    .map(|segment| segment.to_regex_hir())
                    .collect(),
            ),
            Self::Alternation(segments) => Hir::alternation(
                segments
                    .iter()
                    .map(|segment| segment.to_regex_hir())
                    .collect(),
            ),
        }
    }

    fn collect_constant_parts(&self, parts: &mut Vec<StringPatternConstantPart>) {
        match self {
            Self::Empty => {}
            Self::Literal(string) => {
                parts.push(StringPatternConstantPart::Literal(string.clone()));
            }
            Self::Regex(_) => {}
            Self::Property => todo!(),
            Self::Concat(segments) => {
                for segment in segments {
                    segment.collect_constant_parts(parts);
                }
            }
            Self::Alternation(_) => {}
        }
    }
}

pub fn process_patterns(compiler: &mut Compiler) {
    let string_patterns = std::mem::take(&mut compiler.relations.string_patterns);

    for def_id in string_patterns {
        let segment = match compiler
            .relations
            .properties_by_type(def_id)
            .map(|p| &p.constructor)
        {
            Some(Constructor::StringPattern(segment)) => segment,
            _ => panic!("{def_id:?} does not have a string pattern constructor"),
        };

        let hir = segment.to_regex_hir();
        let anchored_hir = Hir::concat(vec![
            Hir::anchor(Anchor::StartText),
            hir,
            Hir::anchor(Anchor::EndText),
        ]);
        debug!("{def_id:?} regex: {anchored_hir}");

        let mut regex_string = String::new();
        write!(&mut regex_string, "{anchored_hir}").unwrap();

        let regex = Regex::new(&regex_string).unwrap();

        let mut constant_parts = vec![];
        segment.collect_constant_parts(&mut constant_parts);

        compiler.patterns.string_patterns.insert(
            def_id,
            StringPattern {
                regex,
                constant_parts,
            },
        );
    }
}
