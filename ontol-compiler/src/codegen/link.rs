use std::collections::HashMap;

use ontol_runtime::{
    proc::{Lib, OpCode, Procedure},
    DefId,
};

use crate::compiler::Compiler;

use super::codegen::ProcTable;

pub(super) fn link(
    _compiler: &mut Compiler,
    proc_table: ProcTable,
) -> (Lib, HashMap<(DefId, DefId), Procedure>) {
    let mut translate_procs: HashMap<(DefId, DefId), Procedure> = Default::default();
    let mut lib = Lib::default();

    for ((from, to), unlinked_proc) in proc_table.procs {
        let procedure = lib.add_procedure(unlinked_proc.n_params, unlinked_proc.opcodes);
        translate_procs.insert((from, to), procedure);
    }

    for opcode in &mut lib.opcodes {
        if let OpCode::Call(procedure) = opcode {
            match proc_table.translate_calls.get(&procedure.start) {
                Some((from, to)) => match translate_procs.get(&(*from, *to)) {
                    Some(procedure_ref) => {
                        procedure.start = procedure_ref.start;
                    }
                    None => {
                        todo!("Cannot translate 1");
                    }
                },
                None => {
                    todo!("Cannot translate 2");
                }
            }
        }
    }

    (lib, translate_procs)
}
