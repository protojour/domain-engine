use fnv::FnvHashMap;
use ontol_runtime::{
    proc::{Address, Lib, OpCode, Procedure},
    smart_format,
};
use smartstring::alias::String;

use crate::{error::CompileError, types::FormatType, Compiler, SourceSpan};

use super::{MapKey, ProcTable};

pub struct LinkResult {
    pub lib: Lib,
    pub map_procs: FnvHashMap<(MapKey, MapKey), Procedure>,
}

pub(super) fn link(compiler: &mut Compiler, proc_table: &mut ProcTable) -> LinkResult {
    let mut mapping_procs: FnvHashMap<(MapKey, MapKey), Procedure> = Default::default();
    let mut lib = Lib::default();
    // All the spans for each opcode
    let mut spans: Vec<SourceSpan> = vec![];

    for ((from, to), proc_builder) in std::mem::take(&mut proc_table.procedures) {
        let n_params = proc_builder.n_params;
        let opcodes = proc_builder.build();
        spans.extend(opcodes.iter().map(|(_, span)| span));

        let procedure =
            lib.append_procedure(n_params, opcodes.into_iter().map(|(opcode, _span)| opcode));
        mapping_procs.insert((from, to), procedure);
    }

    // correct "call" opcodes to point to correct address
    for (index, opcode) in lib.opcodes.iter_mut().enumerate() {
        if let OpCode::Call(call_procedure) = opcode {
            let map_call = &proc_table.map_calls[call_procedure.address.0 as usize];

            match mapping_procs.get(&map_call.mapping) {
                Some(mapping_proc) => {
                    call_procedure.address = mapping_proc.address;
                }
                None => {
                    call_procedure.address = Address(0);
                    compiler.push_error(
                        CompileError::CannotConvertMissingEquation {
                            input: format_map_key(compiler, map_call.mapping.0),
                            output: format_map_key(compiler, map_call.mapping.1),
                        }
                        .spanned(&spans[index]),
                    );
                }
            }
        }
    }

    LinkResult {
        lib,
        map_procs: mapping_procs,
    }
}

fn format_map_key(compiler: &Compiler, map_key: MapKey) -> String {
    let ty = compiler.def_types.map.get(&map_key.def_id).unwrap();

    smart_format!("{}", FormatType(ty, &compiler.defs, &compiler.primitives))
}
