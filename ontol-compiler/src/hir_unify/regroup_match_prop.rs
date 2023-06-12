use fnv::FnvHashMap;
use indexmap::{map::Entry, IndexMap};
use ontol_hir::Node;
use ontol_runtime::value::PropertyId;

use crate::typed_hir::{Meta, TypedHir, TypedHirNode};

pub fn regroup_match_prop(nodes: Vec<TypedHirNode>) -> Vec<TypedHirNode> {
    if !needs_regroup(&nodes) {
        return nodes;
    }

    let mut regroup_map: IndexMap<(ontol_hir::Var, PropertyId), Regrouped> = Default::default();
    let mut other_nodes = vec![];

    for TypedHirNode(kind, meta) in nodes {
        if let ontol_hir::Kind::MatchProp(struct_var, prop_id, match_arms) = kind {
            match regroup_map.entry((struct_var, prop_id)) {
                Entry::Occupied(mut regrouped) => {
                    regrouped.get_mut().add_arms(match_arms);
                }
                Entry::Vacant(vacant) => {
                    vacant
                        .insert(Regrouped {
                            match_arms: vec![],
                            first_meta: meta,
                            absent_arm: None,
                        })
                        .add_arms(match_arms);
                }
            }
        } else {
            other_nodes.push(TypedHirNode(kind, meta));
        }
    }

    let mut output = Vec::with_capacity(regroup_map.len() + other_nodes.len());

    for ((struct_var, prop_id), regrouped) in regroup_map {
        let mut match_arms = regrouped.match_arms;

        if let Some(absent_arm) = regrouped.absent_arm {
            match_arms.push(absent_arm);
        }

        output.push(TypedHirNode(
            ontol_hir::Kind::MatchProp(struct_var, prop_id, match_arms),
            regrouped.first_meta,
        ));
    }

    output.extend(other_nodes);

    output
}

struct Regrouped<'m> {
    match_arms: Vec<ontol_hir::MatchArm<'m, TypedHir>>,
    first_meta: Meta<'m>,
    absent_arm: Option<ontol_hir::MatchArm<'m, TypedHir>>,
}

impl<'m> Regrouped<'m> {
    fn add_arms(&mut self, match_arms: Vec<ontol_hir::MatchArm<'m, TypedHir>>) {
        for match_arm in match_arms {
            if matches!(match_arm.pattern, ontol_hir::PropPattern::Absent) {
                self.absent_arm = Some(match_arm);
            } else {
                self.match_arms.push(match_arm);
            }
        }
    }
}

fn needs_regroup(nodes: &[TypedHirNode]) -> bool {
    let mut variant_counter: FnvHashMap<(ontol_hir::Var, PropertyId), usize> = Default::default();

    for node in nodes {
        if let ontol_hir::Kind::MatchProp(struct_var, prop_id, _) = node.kind() {
            let count = variant_counter.entry((*struct_var, *prop_id)).or_default();
            *count += 1;
        }
    }

    variant_counter.into_iter().any(|(_, count)| count > 1)
}
