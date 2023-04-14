use ontol_runtime::proc::{Local, NParams, OpCode};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::SourceSpan;

use super::ir::{BlockIndex, Instr, Ir, Terminator};

pub struct ProcBuilder {
    pub n_params: NParams,
    pub blocks: SmallVec<[Block; 8]>,
    pub stack_size: u16,
    pub depth: u16,
}

impl ProcBuilder {
    pub fn new(n_params: NParams) -> Self {
        Self {
            n_params,
            blocks: Default::default(),
            stack_size: n_params.0 as u16,
            depth: n_params.0 as u16,
        }
    }

    pub fn new_block(&mut self, terminator: Terminator, span: SourceSpan) -> Block {
        let index = BlockIndex(self.blocks.len() as u32);
        self.blocks.push(Block {
            index,
            opcodes: Default::default(),
            ir: Default::default(),
            terminator: terminator.clone(),
            terminator_span: span,
        });
        Block {
            index,
            opcodes: Default::default(),
            ir: Default::default(),
            terminator,
            terminator_span: span,
        }
    }

    pub fn top(&self) -> Local {
        Local(self.depth - 1)
    }

    pub fn top_plus(&self, plus: u16) -> Local {
        Local(self.depth - 1 + plus)
    }

    pub fn commit(&mut self, block: Block) -> BlockIndex {
        let index = block.index;
        self.blocks[index.0 as usize].ir = block.ir;
        index
    }

    pub fn ir_push(&mut self, n: u16, ir: Ir, span: SourceSpan, block: &mut Block) -> Local {
        let local = Local(self.depth);
        self.depth += n;
        block.ir.push(Instr(ir, span));
        local
    }

    pub fn ir_pop(&mut self, n: u16, ir: Ir, span: SourceSpan, block: &mut Block) {
        self.depth += n;
        block.ir.push(Instr(ir, span));
    }

    pub fn push_stack_old(
        &mut self,
        n: u16,
        _spanned_opcode: (OpCode, SourceSpan),
        _block: &mut Block,
    ) -> Local {
        let local = self.stack_size;
        self.stack_size += n;
        // block.opcodes.push(spanned_opcode);
        Local(local)
    }

    pub fn pop_stack_old(
        &mut self,
        n: u16,
        _spanned_opcode: (OpCode, SourceSpan),
        _block: &mut Block,
    ) {
        self.stack_size -= n;
        // block.opcodes.push(spanned_opcode);
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

        // update addresses
        for block in &mut self.blocks {
            for Instr(ir, _) in &mut block.ir {
                // Important: Handle all instructions with AddressOffset
                if let Ir::Iter(_, _, addr_offset) = ir {
                    addr_offset.0 = block_addresses[addr_offset.0 as usize];
                }
            }
        }

        let mut index = 0;
        debug!("Proc:");
        for (block_index, block) in self.blocks.iter().enumerate() {
            debug!("  BlockIndex({block_index}):");

            for Instr(ir, _) in &block.ir {
                debug!("    {index}: {ir:?}");
                index += 1;
            }

            debug!("    {index}: {:?}", block.terminator);

            index += 1;
        }

        /*
        let mut output = smallvec![];
        for block in self.blocks {
            for spanned_opcode in block.opcodes {
                output.push(spanned_opcode);
            }

            let span = block.terminator_span;
            match block.terminator {
                Terminator::Return(Local(0)) => output.push((OpCode::Return0, span)),
                Terminator::Return(local) => output.push((OpCode::Return(local), span)),
                Terminator::Goto(block_index, offset) => output.push((
                    OpCode::Goto(AddressOffset(
                        block_addresses[block_index.0 as usize] + offset,
                    )),
                    span,
                )),
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
        */

        smallvec![]
    }
}

pub struct Block {
    pub index: BlockIndex,
    pub opcodes: SmallVec<[(OpCode, SourceSpan); 32]>,
    pub ir: SmallVec<[Instr; 32]>,
    pub terminator: Terminator,
    pub terminator_span: SourceSpan,
}

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;
