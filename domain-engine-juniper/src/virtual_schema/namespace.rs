use std::collections::HashMap;

use convert_case::{Case, Casing};
use ontol_runtime::smart_format;
use smartstring::alias::String;

#[derive(Default)]
pub struct Namespace {
    rewrites: HashMap<String, String>,
}

impl Namespace {
    pub fn unique_literal(&mut self, literal: &str) -> String {
        let mut string: String = literal.into();
        while self.rewrites.contains_key(&string) {
            string = string + "_";
        }
        self.rewrite(&string).into()
    }

    pub fn typename(&mut self, typename: &str) -> String {
        self.rewrite(typename).into()
    }

    pub fn list(&mut self, typename: &str) -> String {
        smart_format!("{}List", self.rewrite(typename))
    }

    pub fn create(&mut self, typename: &str) -> String {
        smart_format!("create{}", self.rewrite(typename))
    }

    pub fn update(&mut self, typename: &str) -> String {
        smart_format!("update{}", self.rewrite(typename))
    }

    pub fn delete(&mut self, typename: &str) -> String {
        smart_format!("delete{}", self.rewrite(typename))
    }

    pub fn connection(&mut self, typename: &str) -> String {
        smart_format!("{}Connection", self.rewrite(typename))
    }

    pub fn edge(&mut self, rel_typename: Option<&str>, typename: &str) -> String {
        if let Some(rel_typename) = rel_typename {
            let rel_typename: String = self.rewrite(rel_typename).into();
            smart_format!("{rel_typename}{}Edge", self.rewrite(typename))
        } else {
            smart_format!("{}Edge", self.rewrite(typename))
        }
    }

    pub fn input(&mut self, typename: &str) -> String {
        smart_format!("{}Input", self.rewrite(typename))
    }

    pub fn partial_input(&mut self, typename: &str) -> String {
        smart_format!("{}PartialInput", self.rewrite(typename))
    }

    pub fn union_input(&mut self, typename: &str) -> String {
        smart_format!("{}UnionInput", self.rewrite(typename))
    }

    pub fn union_partial_input(&mut self, typename: &str) -> String {
        smart_format!("{}UnionPartialInput", self.rewrite(typename))
    }

    pub fn edge_input(&mut self, rel_typename: Option<&str>, typename: &str) -> String {
        if let Some(rel_typename) = rel_typename {
            let rel_typename: String = self.rewrite(rel_typename).into();
            smart_format!("{rel_typename}{}EdgeInput", self.rewrite(typename))
        } else {
            smart_format!("{}EdgeInput", self.rewrite(typename))
        }
    }

    fn rewrite(&mut self, typename: &str) -> &str {
        if self.rewrites.contains_key(typename) {
            self.rewrites.get(typename).unwrap()
        } else {
            // FIXME: Better naming conflict resolution:
            let rewritten: String = if typename == "Query" {
                "Query_".into()
            } else if typename == "Mutation" {
                "Mutation_".into()
            } else if is_valid_name(typename) {
                typename.into()
            } else if typename.contains('-') {
                typename.to_case(Case::Snake).into()
            } else {
                typename.to_case(Case::Camel).into()
            };

            self.rewrites.insert(typename.into(), rewritten);
            self.rewrites.get(typename).unwrap()
        }
    }
}

// copied from juniper, where this is not public API
pub fn is_valid_name(input: &str) -> bool {
    for (i, c) in input.chars().enumerate() {
        let is_valid = c.is_ascii_alphabetic() || c == '_' || (i > 0 && c.is_ascii_digit());
        if !is_valid {
            return false;
        }
    }
    !input.is_empty()
}
