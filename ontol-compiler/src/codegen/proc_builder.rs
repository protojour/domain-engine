use ontol_runtime::proc::{AddressOffset, Local, NParams, OpCode};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::{codegen::optimize::optimize, SourceSpan};

use super::ir::{BlockIndex, BlockOffset, Ir, Terminator};

/// How an instruction influences the size of the stack
pub struct Stack(pub i32);

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
    /// implicitly pushed as the block is transitioned to
    /// For the first block, this is the number of parameters.
    /// This block "owns" these locals, and they are allowed to be cleared before the block ends.
    pub fn new_block(&mut self, stack_delta: Stack, span: SourceSpan) -> Block {
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

    pub fn commit(&mut self, block: Block, terminator: Terminator) -> BlockIndex {
        let index = block.index;
        self.blocks[index.0 as usize].ir = block.ir;
        self.blocks[index.0 as usize].terminator = Some(terminator);
        index
    }

    /// Generate one instruction in the given block
    pub fn push(
        &mut self,
        block: &mut Block,
        ir: Ir,
        stack_delta: Stack,
        span: SourceSpan,
    ) -> Local {
        let local = Local(self.stack_size as u16);
        self.stack_size += stack_delta.0;
        block.ir.push((ir, span));
        local
    }

    pub fn build(mut self) -> SpannedOpCodes {
        let mut block_addresses: SmallVec<[u32; 8]> = smallvec![];

        // compute addresses
        let mut block_addr = 0;
        for block in &self.blocks {
            block_addresses.push(block_addr);

            // account for the terminator:
            block_addr += block.ir.len() as u32 + 1;
        }

        let mut index = 0;
        debug!("Proc ({:?}):", self.n_params);
        for (block_index, block) in self.blocks.iter().enumerate() {
            debug!("  BlockIndex({block_index}):");

            for (ir, _) in &block.ir {
                debug!("    {index}: {ir:?}");
                index += 1;
            }

            debug!("    T: {:?}", block.terminator);

            index += 1;
        }

        optimize(&mut self);

        // assert_eq!(0, self.depth);

        let mut output = smallvec![];

        for block in self.blocks {
            for (ir, span) in block.ir {
                let opcode = match ir {
                    Ir::Call(proc) => OpCode::Call(proc),
                    Ir::CallBuiltin(proc, def_id) => OpCode::CallBuiltin(proc, def_id),
                    Ir::PopUntil(_) => todo!(),
                    Ir::Remove(local) => OpCode::Remove(local),
                    Ir::Clone(local) => OpCode::Clone(local),
                    Ir::Bump(local) => OpCode::Bump(local),
                    Ir::Iter(seq, counter, block_index) => OpCode::Iter(
                        seq,
                        counter,
                        AddressOffset(block_addresses[block_index.0 as usize]),
                    ),
                    Ir::TakeAttr2(local, property_id) => OpCode::TakeAttr2(local, property_id),
                    Ir::PutAttrValue(local, property_id) => OpCode::PutUnitAttr(local, property_id),
                    Ir::AppendAttr2(local) => OpCode::AppendAttr2(local),
                    Ir::Constant(value, def_id) => OpCode::PushConstant(value, def_id),
                };
                output.push((opcode, span));
            }

            let span = block.terminator_span;
            match block.terminator {
                Some(Terminator::Return(Local(0))) => output.push((OpCode::Return0, span)),
                Some(Terminator::Return(local)) => output.push((OpCode::Return(local), span)),
                Some(Terminator::PopGoto(block_index, offset)) => output.push((
                    OpCode::Goto(AddressOffset(
                        block_addresses[block_index.0 as usize] + offset.0,
                    )),
                    span,
                )),
                None => panic!("Block has no terminator!"),
            }
        }

        debug!(
            "Built proc: {:?} {:#?}",
            self.n_params,
            output
                .iter()
                .enumerate()
                .map(|(index, (opcode, _))| format!("{index}: {opcode:?}"))
                .collect::<Vec<_>>()
        );

        output
    }
}

#[allow(unused)]
pub struct Block {
    index: BlockIndex,
    stack_start: u32,
    ir: SmallVec<[(Ir, SourceSpan); 32]>,
    terminator: Option<Terminator>,
    terminator_span: SourceSpan,
}

impl Block {
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
}

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;
