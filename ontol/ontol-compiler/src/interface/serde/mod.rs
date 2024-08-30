use std::collections::VecDeque;

use ontol_runtime::interface::serde::{operator::SerdeOperator, SerdeDef};
use serde_generator::SerdeGenerator;

use crate::{relation::UnionMemberCache, strings::StringCtx, Compiler};

use self::serde_generator::DebugTaskState;

pub mod serde_generator;

mod sequence_range_builder;
mod serde_generator_lazy;
// mod union_builder;

pub const EDGE_PROPERTY: &str = "_edge";

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SerdeKey {
    Def(SerdeDef),
    Intersection(Box<SerdeIntersection>),
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SerdeIntersection {
    pub main: Option<SerdeDef>,
    pub defs: Vec<SerdeDef>,
}

impl<'m> Compiler<'m> {
    pub fn serde_generator<'c>(
        &'c self,
        str_ctx: &'c mut StringCtx<'m>,
        union_member_cache: &'c UnionMemberCache,
    ) -> SerdeGenerator<'_, 'm> {
        let operators_by_addr: Vec<SerdeOperator> = vec![SerdeOperator::AnyPlaceholder];

        SerdeGenerator {
            str_ctx,
            defs: &self.defs,
            def_ty_ctx: &self.def_ty_ctx,
            rel_ctx: &self.rel_ctx,
            prop_ctx: &self.prop_ctx,
            edge_ctx: &self.edge_ctx,
            repr_ctx: &self.repr_ctx,
            misc_ctx: &self.misc_ctx,
            patterns: &self.text_patterns,
            code_ctx: &self.code_ctx,
            union_member_cache,
            primitives: &self.primitives,
            lazy_struct_op_tasks: VecDeque::new(),
            lazy_struct_intersection_tasks: VecDeque::new(),
            lazy_union_repr_tasks: VecDeque::new(),
            lazy_union_flattener_tasks: VecDeque::new(),
            task_state: DebugTaskState::Unlocked,
            operators_by_addr,
            operators_by_key: Default::default(),
        }
    }
}
