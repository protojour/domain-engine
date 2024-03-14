//! Generate maps between union types
//!
//! A union X (domain) maps to a union Y (codomain) if:
//!
//! 1. The mapping is injective, i.e. all members of X map to a member of Y
//! 2. There are no ambiguous mappings, i.e. there must be at most _one_ mapping
//!    for each element of the domain.

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{
    vm::proc::{Local, NParams, OpCode, Predicate, Procedure},
    DefId, MapDef, MapDefFlags, MapFlags, MapKey,
};
use tracing::debug;

use crate::{repr::repr_model::ReprKind, Compiler, NO_SPAN};

use super::{
    ir::{Ir, Terminator},
    proc_builder::{Delta, ProcBuilder},
    task::ProcTable,
};

/// Data that tracks one union definition
#[derive(Debug)]
struct UnionData {
    /// The members of this union
    members: Vec<DefId>,
    /// This tracks the set of unions that at least one member of this union maps to.
    ///
    /// If a union is a member of this set, AND all our members map to that union,
    /// then this UNION MAPS TO THAT UNION.
    partial_to_union_set: FnvHashSet<DefId>,
}

/// Perform the algorithm of detecting which unions map to each other,
/// and generate the code for each of these maps.
///
/// The generated code is equivalent to a case-switch
/// that checks the runtime type of the input, and when a match is found
/// delegates that map to the _member map_ that maps to a variant of the output union.
pub fn generate_union_maps(proc_table: &mut ProcTable, compiler: &mut Compiler) {
    // Union data, tracks each union:
    let mut union_data: FnvHashMap<DefId, UnionData> = Default::default();

    // Tracks which unions each member belongs to
    let mut reverse_union_map: FnvHashMap<DefId, FnvHashSet<DefId>> = Default::default();

    // What types map to other types (not unions!)
    let mut map_in_out: FnvHashMap<DefId, FnvHashSet<DefId>> = Default::default();

    // What types map to at least one variant of each union
    let mut maps_to_union_partial: FnvHashMap<DefId, FnvHashSet<DefId>> = Default::default();

    // populate union_data and reverse_union_map
    for (def_id, repr) in &compiler.seal_ctx.repr_table {
        if let ReprKind::Union(members) | ReprKind::StructUnion(members) = &repr.kind {
            union_data.insert(
                *def_id,
                UnionData {
                    members: members.iter().map(|(def_id, _)| *def_id).collect(),
                    partial_to_union_set: Default::default(),
                },
            );

            for (member_def_id, _) in members {
                reverse_union_map
                    .entry(*member_def_id)
                    .or_default()
                    .insert(*def_id);
            }
        }
    }

    // collect map_in_out
    for map_key in proc_table.map_procedures.keys() {
        if !map_key.input.flags.is_empty() || !map_key.output.flags.is_empty() {
            continue;
        }

        map_in_out
            .entry(map_key.input.def_id)
            .or_default()
            .insert(map_key.output.def_id);
    }

    // run the analysis
    for (member_input, member_outputs) in &map_in_out {
        let Some(input_unions) = reverse_union_map.get(member_input) else {
            continue;
        };

        for member_output in member_outputs {
            let Some(output_unions) = reverse_union_map.get(member_output) else {
                continue;
            };

            // each member_input map (partially) to each output_union
            maps_to_union_partial
                .entry(*member_input)
                .or_default()
                .extend(output_unions);

            // register that there is connection between input_unions and output_unions:
            for input_union in input_unions {
                let input_data = union_data.get_mut(input_union).unwrap();
                input_data.partial_to_union_set.extend(output_unions);
            }
        }
    }

    for (input_union, data) in union_data {
        for output_union in data.partial_to_union_set {
            // premise 1: Injection!
            // every member of the domain (input) maps to a member of the codomain (output)
            if data.members.iter().all(|member| {
                maps_to_union_partial
                    .get(member)
                    .map(|to_union| to_union.contains(&output_union))
                    .unwrap_or(false)
            }) {
                // premise 2:
                // there must be at most one output edge for every element in the domain (input)
                let Some(switch_pairs) =
                    get_switch_pairs(&data.members, output_union, &map_in_out, &reverse_union_map)
                else {
                    continue;
                };

                debug!("union {input_union:?} maps to union {output_union:?}");

                let map_key = MapKey {
                    input: MapDef {
                        def_id: input_union,
                        flags: MapDefFlags::default(),
                    },
                    output: MapDef {
                        def_id: output_union,
                        flags: MapDefFlags::default(),
                    },
                    flags: MapFlags::default(),
                };

                generate_union_switch_map(map_key, switch_pairs, proc_table, compiler);
            }
        }
    }
}

/// Generate the switch pairs if the mapping from input to output
/// where there are no ambiguous mappings from domain to codomain:
///
/// example of invalid mapping:
/// A -> X
/// A -> Y
fn get_switch_pairs(
    input_members: &[DefId],
    output_union: DefId,
    map_in_out: &FnvHashMap<DefId, FnvHashSet<DefId>>,
    reverse_union_map: &FnvHashMap<DefId, FnvHashSet<DefId>>,
) -> Option<Vec<(DefId, DefId)>> {
    let mut switch_pairs = vec![];

    for member in input_members {
        let outputs = map_in_out.get(member).unwrap();

        let mut targets: FnvHashSet<DefId> = Default::default();

        for target in outputs {
            let union_map = reverse_union_map.get(target).unwrap();

            if union_map.contains(&output_union) {
                targets.insert(*target);
            }
        }

        match targets.len() {
            0 => {
                panic!();
            }
            1 => {
                let target = targets.into_iter().next().unwrap();
                switch_pairs.push((*member, target));
            }
            _ => {
                // "duplicate arrow"
                return None;
            }
        }
    }

    Some(switch_pairs)
}

/// write the case switch
fn generate_union_switch_map(
    map_key: MapKey,
    switch_pairs: Vec<(DefId, DefId)>,
    proc_table: &mut ProcTable,
    compiler: &mut Compiler,
) {
    let mut builder = ProcBuilder::new(NParams(1));
    let mut block = builder.new_block(Delta(0), NO_SPAN);

    for (input_def_id, output_def_id) in switch_pairs {
        let mut case_block = builder.new_block(Delta(0), NO_SPAN);

        block.ir(
            Ir::Cond(
                Predicate::MatchesDiscriminant(Local(0), input_def_id),
                case_block.label(),
            ),
            Delta(0),
            NO_SPAN,
            &mut builder,
        );

        let proc = Procedure {
            address: proc_table.gen_mapping_addr(MapKey {
                input: MapDef {
                    def_id: input_def_id,
                    flags: Default::default(),
                },
                output: MapDef {
                    def_id: output_def_id,
                    flags: Default::default(),
                },
                flags: MapFlags::empty(),
            }),
            n_params: NParams(1),
        };

        case_block.op(OpCode::Call(proc), Delta(0), NO_SPAN, &mut builder);
        case_block.commit(Terminator::Return, &mut builder);
    }

    let msg = compiler.strings.intern_constant("no variant matched");
    block.op(OpCode::Panic(msg), Delta(0), NO_SPAN, &mut builder);
    block.commit(Terminator::Return, &mut builder);

    proc_table.map_procedures.insert(map_key, builder);
}
