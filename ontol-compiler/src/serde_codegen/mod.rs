use crate::Compiler;

use serde_generator::SerdeGenerator;

pub mod serde_generator;

mod sequence_range_builder;
mod union_builder;

impl<'m> Compiler<'m> {
    pub fn serde_generator(&self) -> SerdeGenerator<'_, 'm> {
        SerdeGenerator {
            defs: &self.defs,
            primitives: &self.primitives,
            def_types: &self.def_types,
            relations: &self.relations,
            patterns: &self.patterns,
            codegen_tasks: &self.codegen_tasks,
            operators_by_id: Default::default(),
            operators_by_key: Default::default(),
        }
    }
}
