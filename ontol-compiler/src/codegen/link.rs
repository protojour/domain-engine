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

    for ((from, to), unlinked_proc) in std::mem::take(&mut proc_table.procs) {
        let procedure = lib.add_procedure(unlinked_proc.n_params, unlinked_proc.opcodes);
        translations.insert((from, to), procedure);
    }

    // correct "call" opcodes to point to correct address
    for opcode in &mut lib.opcodes {
        if let OpCode::Call(call_procedure) = opcode {
            match proc_table.translate_calls.get(&call_procedure.start) {
                Some(translate_call) => match translations.get(&translate_call.translation) {
                    Some(translation_procedure) => {
                        call_procedure.start = translation_procedure.start;
                    }
                    None => {
                        compiler.push_error(
                            CompileError::CannotEquate
                                .spanned(&compiler.sources, &translate_call.span),
                        );
                    }
                },
                None => {
                    todo!("BUG! Something weird happened");
                }
            }
        }
    }

    LinkResult { lib, translations }
}
