use std::collections::HashMap;

use ontol_runtime::{
    proc::{Lib, OpCode, Procedure},
    DefId,
};

use crate::{compiler::Compiler, error::CompileError, SourceSpan};

use super::codegen::ProcTable;

pub struct LinkResult {
    pub lib: Lib,
    pub translations: HashMap<(DefId, DefId), Procedure>,
}

pub(super) fn link(compiler: &mut Compiler, proc_table: &mut ProcTable) -> LinkResult {
    let mut translations: HashMap<(DefId, DefId), Procedure> = Default::default();
    let mut lib = Lib::default();
    let mut spans: Vec<SourceSpan> = vec![];

    for ((from, to), unlinked_proc) in std::mem::take(&mut proc_table.procs) {
        spans.extend(unlinked_proc.opcodes.iter().map(|(_, span)| span));

        let procedure = lib.add_procedure(
            unlinked_proc.n_params,
            unlinked_proc
                .opcodes
                .into_iter()
                .map(|(opcode, _span)| opcode),
        );
        translations.insert((from, to), procedure);
    }

    // correct "call" opcodes to point to correct address
    for (index, opcode) in lib.opcodes.iter_mut().enumerate() {
        if let OpCode::Call(call_procedure) = opcode {
            let translate_call = &proc_table.translate_calls[call_procedure.start as usize];

            match translations.get(&translate_call.translation) {
                Some(translation_procedure) => {
                    call_procedure.start = translation_procedure.start;
                }
                None => {
                    compiler.push_error(
                        CompileError::CannotEquate.spanned(&compiler.sources, &spans[index]),
                    );
                }
            }
        }
    }

    LinkResult { lib, translations }
}
