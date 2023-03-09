use convert_case::{Case, Casing};
use ontol_runtime::smart_format;
use smartstring::alias::String;

use super::data::EdgeData;

pub struct Names;

impl Names {
    pub fn new() -> Self {
        Self
    }

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

    pub fn root_edge_data(&mut self, type_name: &str) -> EdgeData {
        EdgeData {
            edge_type_name: smart_format!("{type_name}ConnectionEdge"),
            connection_type_name: smart_format!("{type_name}Connection"),
        }
    }

    pub fn edge_data(&mut self, type_name: &str, property_name: &str) -> EdgeData {
        let property = property_name.to_case(Case::Pascal);

        EdgeData {
            edge_type_name: smart_format!("{type_name}{property}ConnectionEdge"),
            connection_type_name: smart_format!("{type_name}{property}Connection"),
        }
    }
}
