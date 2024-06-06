//! TODO: Optimize so that we don't unnecessarily Clone struct attributes
//!
use fnv::FnvHashSet;
use ontol_runtime::vm::proc::OpCode;

use super::{
    ir::{BlockLabel, Ir, Terminator},
    proc_builder::ProcBuilder,
};

enum LocalUsage {
    None,
    Once,
    Multi,
}

enum BlockKind {
    Linear,
    Looping,
}

pub fn optimize(builder: &mut ProcBuilder) {
    let Some(first_block) = builder.blocks.first() else {
        return;
    };

    let mut block_clone_optimizer = CloneToBumpOptimizer::default();
    block_clone_optimizer.optimize_block(builder, first_block.label(), BlockKind::Linear);
}

#[derive(Default)]
struct CloneToBumpOptimizer {
    locals: Vec<LocalUsage>,
    optimized_blocks: FnvHashSet<BlockLabel>,
}

impl CloneToBumpOptimizer {
    fn optimize_block(
        &mut self,
        builder: &mut ProcBuilder,
        block_label: BlockLabel,
        block_kind: BlockKind,
    ) {
        if self.optimized_blocks.contains(&block_label) {
            return;
        }
        self.optimized_blocks.insert(block_label);

        let block = builder.block_mut(block_label);
        let stack_start = block.stack_start();

        let mut sub_blocks = Vec::new();

        for (ir, _) in block.ir_mut() {
            match ir {
                Ir::Op(OpCode::Clone(local)) => {
                    if (local.0 as u32) < stack_start {
                        match block_kind {
                            BlockKind::Linear => {
                                // A reference to a local in the parent block
                                self.count_local_use(local.0 as usize);
                            }
                            BlockKind::Looping => {
                                self.set_local_multi(local.0 as usize);
                            }
                        }
                    } else {
                        self.count_local_use(local.0 as usize);
                    }
                }
                Ir::Iter(.., block_index) => {
                    sub_blocks.push((*block_index, BlockKind::Looping));
                }
                _ => {}
            }
        }

        for (block_index, block_kind) in sub_blocks {
            self.optimize_block(builder, block_index, block_kind);
        }

        let block = builder.block_mut(block_label);

        // optimize (Clone => Bump):
        for (ir, _) in block.ir_mut() {
            if let Ir::Op(OpCode::Clone(local)) = ir {
                let usage = self.local_usage_mut(local.0 as usize);
                if matches!(usage, LocalUsage::Once) {
                    *ir = Ir::Op(OpCode::Bump(*local));
                }
            }
        }

        if let Some(Terminator::PopGoto(..)) = block.terminator() {
            self.truncate(stack_start);
        }
    }

    fn truncate(&mut self, size: u32) {
        self.locals.truncate(size as usize);
    }

    fn count_local_use(&mut self, local: usize) {
        match self.local_usage_mut(local) {
            usage @ LocalUsage::None => {
                *usage = LocalUsage::Once;
            }
            usage @ LocalUsage::Once => *usage = LocalUsage::Multi,
            _ => {}
        }
    }

    fn set_local_multi(&mut self, local: usize) {
        *self.local_usage_mut(local) = LocalUsage::Multi;
    }

    fn local_usage_mut(&mut self, local: usize) -> &mut LocalUsage {
        while self.locals.len() <= local {
            self.locals.push(LocalUsage::None)
        }

        self.locals.get_mut(local).unwrap()
    }
}
