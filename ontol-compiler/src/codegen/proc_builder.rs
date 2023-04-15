use ontol_runtime::proc::{AddressOffset, Local, NParams, OpCode};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::SourceSpan;

use super::ir::{BlockIndex, BlockOffset, Ir, Terminator};

pub struct ProcBuilder {
    pub n_params: NParams,
    pub blocks: SmallVec<[Block; 8]>,
    pub depth: i32,
}

impl ProcBuilder {
    pub fn new(n_params: NParams) -> Self {
        Self {
            n_params,
            blocks: Default::default(),
            depth: n_params.0 as i32,
        }
    }

    pub fn new_block(&mut self, span: SourceSpan) -> Block {
        let index = BlockIndex(self.blocks.len() as u32);
        self.blocks.push(Block {
            index,
            ir: Default::default(),
            terminator: None,
            terminator_span: span,
        });
        Block {
            index,
            ir: Default::default(),
            terminator: None,
            terminator_span: span,
        }
    }

    pub fn top(&self) -> Local {
        Local(self.depth as u16 - 1)
    }

    pub fn top_plus(&self, plus: u16) -> Local {
        Local(self.depth as u16 - 1 + plus)
    }

    pub fn commit(&mut self, block: Block, terminator: Terminator) -> BlockIndex {
        let index = block.index;
        self.blocks[index.0 as usize].ir = block.ir;
        self.blocks[index.0 as usize].terminator = Some(terminator);
        index
    }

    pub fn push(&mut self, stack_delta: i32, ir: Ir, span: SourceSpan, block: &mut Block) -> Local {
        let local = Local(self.depth as u16);
        self.depth += stack_delta;
        block.ir.push((ir, span));
        local
    }

    pub fn build(self) -> SpannedOpCodes {
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

        // assert_eq!(0, self.depth);

        let mut output = smallvec![];

        for block in self.blocks {
            for (ir, span) in block.ir {
                let opcode = match ir {
                    Ir::Call(proc) => OpCode::Call(proc),
                    Ir::CallBuiltin(proc, def_id) => OpCode::CallBuiltin(proc, def_id),
                    Ir::Remove(local) => OpCode::Remove(local),
                    Ir::Clone(local) => OpCode::Clone(local),
                    Ir::Take(local) => OpCode::Take(local),
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
                Some(Terminator::Goto(block_index, offset)) => output.push((
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

pub struct Block {
    index: BlockIndex,
    ir: SmallVec<[(Ir, SourceSpan); 32]>,
    terminator: Option<Terminator>,
    terminator_span: SourceSpan,
}

impl Block {
    pub fn index(&self) -> BlockIndex {
        self.index
    }

    pub fn current_offset(&self) -> BlockOffset {
        BlockOffset(self.ir.len() as u32)
    }
}

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;
