use convert_case::{Case, Casing};
use ontol_runtime::smart_format;
use smartstring::alias::String;

use super::data::EdgeNames;

#[derive(Default)]
pub struct Namespace;

impl Namespace {
    pub fn list(&mut self, type_name: &str) -> String {
        smart_format!("{type_name}List")
    }

    pub fn create(&mut self, type_name: &str) -> String {
        smart_format!("create{type_name}")
    }

    pub fn update(&mut self, type_name: &str) -> String {
        smart_format!("update{type_name}")
    }

    pub fn delete(&mut self, type_name: &str) -> String {
        smart_format!("delete{type_name}")
    }

    pub fn input(&mut self, type_name: &str) -> String {
        smart_format!("{type_name}Input")
    }

    pub fn edge_names(&mut self, type_name: &str, property_name: Option<&str>) -> EdgeNames {
        match property_name {
            Some(property_name) => {
                let property = property_name.to_case(Case::Pascal);

                EdgeNames {
                    edge: smart_format!("{type_name}{property}ConnectionEdge"),
                    connection: smart_format!("{type_name}{property}Connection"),
                }
            }
            None => EdgeNames {
                edge: smart_format!("{type_name}ConnectionEdge"),
                connection: smart_format!("{type_name}Connection"),
            },
        }
    }
}
