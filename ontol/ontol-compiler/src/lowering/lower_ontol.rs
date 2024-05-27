use std::marker::PhantomData;

use ontol_parser::cst::{
    inspect::{self as insp},
    view::NodeView,
};
use ontol_runtime::DefId;

use crate::{Compiler, Src};

use super::context::{BlockContext, CstLowering, LoweringCtx};

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub fn new(compiler: &'c mut Compiler<'m>, src: Src) -> Self {
        Self {
            ctx: LoweringCtx {
                compiler,
                package_id: src.package_id,
                source_id: src.id,
                root_defs: Default::default(),
            },
            _phantom: PhantomData,
        }
    }

    pub fn finish(self) -> Vec<DefId> {
        self.ctx.root_defs
    }

    pub fn lower_ontol(mut self, ontol: insp::Node<V>) -> Self {
        let insp::Node::Ontol(ontol) = ontol else {
            return self;
        };

        let mut pre_defined_statements = vec![];

        // first pass: Register all named definitions into the namespace
        for statement in ontol.statements() {
            if let Some(pre_defined) = self.pre_define_statement(statement) {
                pre_defined_statements.push(pre_defined);
            }
        }

        // second pass: Handle inner bodies, etc
        for stmt in pre_defined_statements {
            if let Some(mut defs) = self.lower_pre_defined(stmt, BlockContext::NoContext) {
                self.ctx.root_defs.append(&mut defs);
            }
        }

        self
    }
}
