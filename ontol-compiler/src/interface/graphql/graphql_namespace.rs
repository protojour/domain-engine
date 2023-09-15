use std::collections::HashMap;

use convert_case::{Case, Casing};
use ontol_runtime::{
    ontology::{Ontology, TypeInfo},
    smart_format, DefId, PackageId,
};
use smartstring::alias::String;

pub struct GraphqlNamespace<'o> {
    rewrites: HashMap<String, String>,
    domain_disambiguation: Option<DomainDisambiguation<'o>>,
}

pub struct DomainDisambiguation<'o> {
    pub root_domain: PackageId,
    pub ontology: &'o Ontology,
}

impl<'o> GraphqlNamespace<'o> {
    pub fn new() -> Self {
        Self {
            rewrites: Default::default(),
            domain_disambiguation: None,
        }
    }

    pub fn with_domain_disambiguation(domain_disambiguation: DomainDisambiguation<'o>) -> Self {
        Self {
            rewrites: Default::default(),
            domain_disambiguation: Some(domain_disambiguation),
        }
    }

    pub fn unique_literal(&mut self, literal: &str) -> String {
        let mut string: String = literal.into();
        while self.rewrites.contains_key(&string) {
            string = string + "_";
        }
        self.rewrite(&string).into()
    }

    pub fn typename(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&Typename(type_info)])
    }

    pub fn list(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&Typename(type_info), &"List"])
    }

    pub fn create(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&"create", &Typename(type_info)])
    }

    pub fn update(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&"update", &Typename(type_info)])
    }

    pub fn delete(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&"delete", &Typename(type_info)])
    }

    pub fn connection(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&Typename(type_info), &"Connection"])
    }

    pub fn edge(&mut self, rel_type_info: Option<&TypeInfo>, type_info: &TypeInfo) -> String {
        if let Some(rel_type_info) = rel_type_info {
            self.concat(&[&Typename(rel_type_info), &Typename(type_info), &"Edge"])
        } else {
            self.concat(&[&Typename(type_info), &"Edge"])
        }
    }

    pub fn input(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&Typename(type_info), &"Input"])
    }

    pub fn partial_input(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&Typename(type_info), &"PartialInput"])
    }

    pub fn union_input(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&Typename(type_info), &"UnionInput"])
    }

    pub fn union_partial_input(&mut self, type_info: &TypeInfo) -> String {
        self.concat(&[&Typename(type_info), &"UnionPartialInput"])
    }

    pub fn edge_input(&mut self, rel_type_info: Option<&TypeInfo>, type_info: &TypeInfo) -> String {
        if let Some(rel_type_info) = rel_type_info {
            self.concat(&[&Typename(rel_type_info), &Typename(type_info), &"EdgeInput"])
        } else {
            self.concat(&[&Typename(type_info), &"EdgeInput"])
        }
    }

    fn concat(&mut self, elements: &[&dyn ProcessName]) -> String {
        let mut output: String = "".into();

        if let Some(domain_disambiguation) = &self.domain_disambiguation {
            if let Some(def_id) = elements.iter().find_map(|elem| elem.def_id()) {
                let package_id = def_id.0;
                if package_id != domain_disambiguation.root_domain {
                    // FIXME: Domains could have globally unique names that we can use here?
                    output.push_str(&format!("_domain{}_", package_id.0));
                }
            }
        }

        for element in elements {
            output.push_str(element.process(self));
        }
        output
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

trait ProcessName {
    fn def_id(&self) -> Option<DefId>;

    fn process<'n>(&self, namespace: &'n mut GraphqlNamespace) -> &'n str;
}

struct Typename<'a>(&'a TypeInfo);

impl<'a> ProcessName for Typename<'a> {
    fn def_id(&self) -> Option<DefId> {
        Some(self.0.def_id)
    }

    fn process<'n>(&self, namespace: &'n mut GraphqlNamespace) -> &'n str {
        let type_info = self.0;
        match &type_info.name {
            Some(name) => namespace.rewrite(name),
            None => namespace.rewrite(&smart_format!(
                "_anon{}_{}",
                type_info.def_id.0 .0,
                type_info.def_id.1
            )),
        }
    }
}

impl ProcessName for &'static str {
    fn def_id(&self) -> Option<DefId> {
        None
    }

    fn process<'n>(&self, _: &'n mut GraphqlNamespace) -> &'n str {
        self
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
