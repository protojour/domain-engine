use std::collections::HashMap;

use ontol_runtime::{
    proc::{Lib, OpCode, Procedure},
    smart_format, DefId,
};
use smartstring::alias::String;

use crate::{compiler::Compiler, error::CompileError, types::FormatType, SourceSpan};

use super::ProcTable;

pub struct LinkResult {
    pub lib: Lib,
    pub translations: HashMap<(DefId, DefId), Procedure>,
}

pub(super) fn link(compiler: &mut Compiler, proc_table: &mut ProcTable) -> LinkResult {
    let mut translations: HashMap<(DefId, DefId), Procedure> = Default::default();
    let mut lib = Lib::default();
    // All the spans for each opcode
    let mut spans: Vec<SourceSpan> = vec![];

    for ((from, to), unlinked_proc) in std::mem::take(&mut proc_table.procedures) {
        spans.extend(unlinked_proc.opcodes.iter().map(|(_, span)| span));

        let procedure = lib.append_procedure(
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
            let translate_call = &proc_table.translate_calls[call_procedure.address as usize];

            match translations.get(&translate_call.translation) {
                Some(translation_procedure) => {
                    call_procedure.address = translation_procedure.address;
                }
                None => {
                    call_procedure.address = 0;
                    compiler.push_error(
                        CompileError::CannotConvertMissingEquation {
                            input: format_def(compiler, translate_call.translation.0),
                            output: format_def(compiler, translate_call.translation.1),
                        }
                        .spanned(&compiler.sources, &spans[index]),
                    );
                }
            }
        }
    }

    LinkResult { lib, translations }
}

fn format_def(compiler: &Compiler, def_id: DefId) -> String {
    let ty = compiler.def_types.map.get(&def_id).unwrap();

    smart_format!("{}", FormatType(ty, &compiler.defs))
}
