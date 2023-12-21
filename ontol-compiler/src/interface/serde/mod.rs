use std::collections::VecDeque;

use serde_generator::SerdeGenerator;

use crate::{relation::UnionMemberCache, Compiler};

pub mod serde_generator;

mod sequence_range_builder;
mod serde_generator_lazy;
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
            lazy_struct_op_tasks: VecDeque::new(),
            lazy_struct_intersection_tasks: VecDeque::new(),
            lazy_union_repr_tasks: VecDeque::new(),
            operators_by_addr: Default::default(),
            operators_by_key: Default::default(),
        }
    }
}
