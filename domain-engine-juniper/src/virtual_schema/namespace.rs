use ontol_runtime::smart_format;
use smartstring::alias::String;

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

    pub fn partial_input(&mut self, type_name: &str) -> String {
        smart_format!("{type_name}PartialInput")
    }
}
