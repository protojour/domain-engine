use fnv::FnvHashMap;
use ontol_runtime::vm::proc::{AddressOffset, Local, NParams, OpCode};
use smallvec::{smallvec, SmallVec};
use tracing::{debug, trace, Level};

use crate::{codegen::optimize::optimize, SourceSpan};

use super::ir::{BlockIndex, BlockOffset, Ir, Terminator};

/// How an instruction influences the size of the stack
pub struct Delta(pub i32);

#[derive(Default)]
pub struct Scope {
    pub in_scope: FnvHashMap<ontol_hir::Var, Local>,
}

pub struct ProcBuilder {
    pub n_params: NParams,
    pub blocks: SmallVec<[Block; 8]>,
    pub stack_size: i32,
}

impl ProcBuilder {
    pub fn new(n_params: NParams) -> Self {
        Self {
            n_params,
            blocks: Default::default(),
            stack_size: 0,
        }
    }

    /// Generate a new block, the stack_delta is the number of locals
    /// implicitly pushed as the block is transitioned to.
    /// For the first block, this is the number of parameters.
    /// This block "owns" these locals, and they are allowed to be cleared before the block ends.
    pub fn new_block(&mut self, stack_delta: Delta, span: SourceSpan) -> Block {
        let stack_start = self.stack_size as u32;
        self.stack_size += stack_delta.0;

        let index = BlockIndex(self.blocks.len() as u32);

        self.blocks.push(Block {
            index,
            stack_start,
            ir: Default::default(),
            terminator: None,
            terminator_span: span,
        });
        Block {
            index,
            stack_start,
            ir: Default::default(),
            terminator: None,
            terminator_span: span,
        }
    }

    /// Make a split of the given block, so that the block reference points
    /// at the continuation block, returning a block pointing _before_ the split.
    pub fn split_block(&mut self, block: &mut Block) -> Block {
        let mut block2 = self.new_block(Delta(0), block.terminator_span());
        std::mem::swap(block, &mut block2);
        block2
    }

    pub fn top(&self) -> Local {
        Local(self.stack_size as u16 - 1)
    }

    pub fn top_plus(&self, plus: u16) -> Local {
        Local(self.stack_size as u16 - 1 + plus)
    }

    #[allow(unused)]
    pub fn top_minus(&self, minus: u16) -> Local {
        Local(self.stack_size as u16 - 1 - minus)
    }

    pub fn prealloc_stack(&mut self, delta: Delta) {
        self.stack_size += delta.0;
    }

    pub fn build(mut self) -> SpannedOpCodes {
        let mut block_addresses: SmallVec<[u32; 8]> = smallvec![];

        // compute addresses
        let mut block_addr = 0;
        for (block_index, block) in self.blocks.iter_mut().enumerate() {
            let mut terminator_size = 1;
            if let Some(Terminator::Goto(dest_index, offset)) = &block.terminator {
                // If it's just entering the next block, an instruction is not needed:
                if dest_index.0 as usize == block_index + 1 && offset.0 == 0 {
                    block.terminator = Some(Terminator::GotoNext);
                    terminator_size = 0;
                }
            }

            block_addresses.push(block_addr);

            // account for the terminator:
            block_addr += block.ir.len() as u32 + terminator_size;
        }

        if tracing::enabled!(Level::DEBUG) {
            self.debug_blocks();
        }

        optimize(&mut self);

        let mut output = smallvec![];

        for block in self.blocks {
            for (ir, span) in block.ir {
                let opcode = match ir {
                    Ir::Iter(seq, counter, block_index) => OpCode::Iter(
                        seq,
                        counter,
                        AddressOffset(block_addresses[block_index.0 as usize]),
                    ),
                    Ir::Cond(predicate, block_index) => OpCode::Cond(
                        predicate,
                        AddressOffset(block_addresses[block_index.0 as usize]),
                    ),
                    Ir::Op(opcode) => opcode,
                };
                output.push((opcode, span));
            }

            let span = block.terminator_span;
            match block.terminator {
                Some(Terminator::Return) => output.push((OpCode::Return, span)),
                Some(
                    Terminator::PopGoto(block_index, offset)
                    | Terminator::Goto(block_index, offset),
                ) => output.push((
                    OpCode::Goto(AddressOffset(
                        block_addresses[block_index.0 as usize] + offset.0,
                    )),
                    span,
                )),
                Some(Terminator::GotoNext) => {}
                Some(Terminator::Panic(message)) => output.push((OpCode::Panic(message), span)),
                None => panic!("Block has no terminator!"),
            }
        }

        if tracing::enabled!(Level::TRACE) {
            debug_output(self.n_params, &output);
        }

        output
    }

    fn debug_blocks(&self) {
        debug!("Proc ({:?}):", self.n_params);
        for (block_index, block) in self.blocks.iter().enumerate() {
            debug!("  BlockIndex({block_index}):");

            for (index, (ir, _)) in block.ir.iter().enumerate() {
                debug!("    {index}: {ir:?}");
            }

            debug!("    T: {:?}", block.terminator);
        }
    }
}

fn debug_output(n_params: NParams, output: &SpannedOpCodes) {
    trace!(
        "Built proc: {:?} {:#?}",
        n_params,
        output
            .iter()
            .enumerate()
            .map(|(index, (opcode, _))| format!("{index}: {opcode:?}"))
            .collect::<Vec<_>>()
    );
}

pub struct Block {
    index: BlockIndex,
    stack_start: u32,
    ir: SmallVec<[(Ir, SourceSpan); 32]>,
    terminator: Option<Terminator>,
    terminator_span: SourceSpan,
}

impl Block {
    /// Append an Ir (potentially escaped) opcode
    #[inline]
    pub fn ir(
        &mut self,
        ir: Ir,
        stack_delta: Delta,
        span: SourceSpan,
        builder: &mut ProcBuilder,
    ) -> Local {
        let local = Local(builder.stack_size as u16);
        builder.stack_size += stack_delta.0;
        self.ir.push((ir, span));
        local
    }

    /// Append an ontol-vm OpCode directly
    #[inline]
    pub fn op(
        &mut self,
        opcode: OpCode,
        stack_delta: Delta,
        span: SourceSpan,
        builder: &mut ProcBuilder,
    ) -> Local {
        self.ir(Ir::Op(opcode), stack_delta, span, builder)
    }

    /// Append OpCode::PopUntil instruction (shrinks stack up to (not including) the given local)
    pub fn pop_until(&mut self, local: Local, span: SourceSpan, builder: &mut ProcBuilder) {
        let stack_delta = Delta(local.0 as i32 - builder.top().0 as i32);
        if stack_delta.0 != 0 {
            if let Some((Ir::Op(OpCode::PopUntil(last_local)), _)) = self.ir.last_mut() {
                // peephole optimization: No need for consecutive PopUntil
                builder.stack_size += stack_delta.0;
                *last_local = local;
            } else {
                self.op(OpCode::PopUntil(local), stack_delta, span, builder);
            }
        }
    }

    pub fn commit(self, terminator: Terminator, builder: &mut ProcBuilder) -> BlockIndex {
        let index = self.index;
        builder.blocks[index.0 as usize].ir = self.ir;
        builder.blocks[index.0 as usize].terminator = Some(terminator);
        index
    }

    pub fn index(&self) -> BlockIndex {
        self.index
    }

    pub fn stack_start(&self) -> u32 {
        self.stack_start
    }

    pub fn current_offset(&self) -> BlockOffset {
        BlockOffset(self.ir.len() as u32)
    }

    pub fn ir_mut(&mut self) -> &mut SmallVec<[(Ir, SourceSpan); 32]> {
        &mut self.ir
    }

    pub fn terminator(&self) -> Option<&Terminator> {
        self.terminator.as_ref()
    }

    pub fn terminator_span(&self) -> SourceSpan {
        self.terminator_span
    }
}

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;
