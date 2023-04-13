use ontol_runtime::proc::{AddressOffset, Local, NParams, OpCode};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::SourceSpan;

pub struct ProcBuilder {
    pub n_params: NParams,
    pub blocks: SmallVec<[Block; 8]>,
    pub stack_size: u32,
}

impl ProcBuilder {
    pub fn new(n_params: NParams) -> Self {
        Self {
            n_params,
            blocks: Default::default(),
            stack_size: n_params.0 as u32,
        }
    }

    pub fn new_block(&mut self, terminator: Terminator, span: SourceSpan) -> Block {
        let address = self.blocks.len() as u32;
        self.blocks.push(Block {
            index: address,
            opcodes: Default::default(),
            terminator: terminator.clone(),
            terminator_span: span,
        });
        Block {
            index: address,
            opcodes: Default::default(),
            terminator,
            terminator_span: span,
        }
    }

    pub fn commit(&mut self, block: Block) {
        let index = block.index;
        self.blocks[index as usize].opcodes = block.opcodes;
    }

    pub fn push_stack(
        &mut self,
        n: u32,
        spanned_opcode: (OpCode, SourceSpan),
        block: &mut Block,
    ) -> Local {
        let local = self.stack_size;
        self.stack_size += n;
        block.opcodes.push(spanned_opcode);
        Local(local)
    }

    pub fn pop_stack(&mut self, n: u32, spanned_opcode: (OpCode, SourceSpan), block: &mut Block) {
        self.stack_size -= n;
        block.opcodes.push(spanned_opcode);
    }

    pub fn build(mut self) -> SpannedOpCodes {
        let mut block_addresses: SmallVec<[u32; 8]> = smallvec![];

        // compute addresses
        let mut block_addr = 0;
        for block in &self.blocks {
            block_addresses.push(block_addr);
            block_addr += block.opcodes.len() as u32;
            // account for the terminator:
            block_addr += 1;
        }

        // update addresses
        for block in &mut self.blocks {
            for (opcode, _) in &mut block.opcodes {
                // Important: Handle all opcodes with AddressOffset
                match opcode {
                    OpCode::Goto(addr_offset) => {
                        addr_offset.0 = block_addresses[addr_offset.0 as usize];
                    }
                    OpCode::ForEach(_, _, addr_offset) => {
                        addr_offset.0 = block_addresses[addr_offset.0 as usize];
                    }
                    _ => {}
                }
            }
        }

        let mut output = smallvec![];
        for block in self.blocks {
            for spanned_opcode in block.opcodes {
                output.push(spanned_opcode);
            }

            let span = block.terminator_span;
            match block.terminator {
                Terminator::Return(Local(0)) => output.push((OpCode::Return0, span)),
                Terminator::Return(local) => output.push((OpCode::Return(local), span)),
                Terminator::Goto { block, offset } => output.push((
                    OpCode::Goto(AddressOffset(block_addresses[block as usize] + offset)),
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
    }
}

#[derive(Clone)]
pub enum Terminator {
    Return(Local),
    Goto { block: u32, offset: u32 },
}

pub struct Block {
    pub index: u32,
    pub opcodes: SmallVec<[(OpCode, SourceSpan); 32]>,
    pub terminator: Terminator,
    pub terminator_span: SourceSpan,
}

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;
