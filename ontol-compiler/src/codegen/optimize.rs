use fnv::FnvHashSet;

use super::{
    ir::{BlockIndex, Ir, Terminator},
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
    if builder.blocks.is_empty() {
        return;
    }

    let mut block_clone_optimizer = CloneToBumpOptimizer::default();
    block_clone_optimizer.optimize_block(builder, BlockIndex(0), BlockKind::Linear);
}

#[derive(Default)]
struct CloneToBumpOptimizer {
    locals: Vec<LocalUsage>,
    optimized_blocks: FnvHashSet<BlockIndex>,
}

impl CloneToBumpOptimizer {
    fn optimize_block(
        &mut self,
        builder: &mut ProcBuilder,
        block_index: BlockIndex,
        block_kind: BlockKind,
    ) {
        if self.optimized_blocks.contains(&block_index) {
            return;
        }
        self.optimized_blocks.insert(block_index);

        let block = builder.blocks.get_mut(block_index.0 as usize).unwrap();
        let stack_start = block.stack_start();

        let mut sub_blocks = Vec::new();

        for (ir, _) in block.ir_mut() {
            match ir {
                Ir::Clone(local) => {
                    if (local.0 as u32) < stack_start {
                        match block_kind {
                            BlockKind::Linear => {
                                // A reference to a local in the parent block
                                self.count_use(local.0 as usize);
                                panic!();
                            }
                            BlockKind::Looping => {
                                self.set_multi(local.0 as usize);
                            }
                        }
                    } else {
                        self.count_use(local.0 as usize);
                    }
                }
                Ir::Iter(_, _, block_index) => {
                    sub_blocks.push((*block_index, BlockKind::Looping));
                }
                _ => {}
            }
        }

        for (block_index, block_kind) in sub_blocks {
            self.optimize_block(builder, block_index, block_kind);
        }

        let block = builder.blocks.get_mut(block_index.0 as usize).unwrap();

        // optimize (Clone => Take):
        for (ir, _) in block.ir_mut() {
            match ir {
                Ir::Clone(local) => {
                    let usage = self.usage_mut(local.0 as usize);
                    if matches!(usage, LocalUsage::Once) {
                        *ir = Ir::Bump(*local);
                    }
                }
                _ => {}
            }
        }

        match block.terminator() {
            Some(Terminator::PopGoto(..)) => {
                self.truncate(stack_start);
            }
            _ => {}
        }
    }

    fn truncate(&mut self, size: u32) {
        self.locals.truncate(size as usize);
    }

    fn count_use(&mut self, local: usize) {
        match self.usage_mut(local) {
            usage @ LocalUsage::None => {
                *usage = LocalUsage::Once;
            }
            usage @ LocalUsage::Once => *usage = LocalUsage::Multi,
            _ => {}
        }
    }

    fn set_multi(&mut self, local: usize) {
        *self.usage_mut(local) = LocalUsage::Multi;
    }

    fn usage_mut(&mut self, local: usize) -> &mut LocalUsage {
        while self.locals.len() <= local {
            self.locals.push(LocalUsage::None)
        }

        self.locals.get_mut(local).unwrap()
    }
}
