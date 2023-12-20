use serde_generator::SerdeGenerator;

use crate::{relation::UnionMemberCache, Compiler};

pub mod serde_generator;

mod sequence_range_builder;
mod union_builder;

impl<'m> Compiler<'m> {
    pub fn serde_generator<'c>(
        &'c self,
        union_member_cache: &'c UnionMemberCache,
    ) -> SerdeGenerator<'_, 'm> {
        SerdeGenerator {
            defs: &self.defs,
            primitives: &self.primitives,
            def_types: &self.def_types,
            relations: &self.relations,
            seal_ctx: &self.seal_ctx,
            patterns: &self.text_patterns,
            codegen_tasks: &self.codegen_tasks,
            union_member_cache,
            operators_by_addr: Default::default(),
            operators_by_key: Default::default(),
        }
    }
}
