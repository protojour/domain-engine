use fnv::FnvHashMap;
use indexmap::{map::Entry, IndexMap};
use ontol_runtime::value::PropertyId;

use crate::typed_hir::{Meta, TypedHir, TypedHirData};

pub fn regroup_match_prop<'m>(
    nodes: ontol_hir::Nodes,
    arena: &mut ontol_hir::arena::Arena<'m, TypedHir>,
) -> ontol_hir::Nodes {
    if !needs_regroup(&nodes, arena) {
        return nodes;
    }

    let mut regroup_map: IndexMap<(ontol_hir::Var, PropertyId), Regrouped<'m>> = Default::default();
    let mut other_nodes: Vec<ontol_hir::Node> = vec![];

    for node in nodes {
        let data = &arena[node];
        let (kind, meta) = (data.hir(), data.meta());
        if let ontol_hir::Kind::MatchProp(struct_var, prop_id, match_arms) = kind {
            match regroup_map.entry((*struct_var, *prop_id)) {
                Entry::Occupied(mut regrouped) => {
                    regrouped.get_mut().add_arms(match_arms);
                }
                Entry::Vacant(vacant) => {
                    vacant
                        .insert(Regrouped {
                            match_arms: vec![],
                            first_meta: *meta,
                            absent_arm: None,
                        })
                        .add_arms(match_arms);
                }
            }
        } else {
            other_nodes.push(node);
        }
    }

    let mut output = ontol_hir::Nodes::default();

    for ((struct_var, prop_id), mut regrouped) in regroup_map {
        if let Some(absent_arm) = regrouped.absent_arm {
            regrouped.match_arms.push(absent_arm);
        }

        output.push(arena.add(TypedHirData(
            ontol_hir::Kind::MatchProp(struct_var, prop_id, regrouped.match_arms.into()),
            regrouped.first_meta,
        )));
    }

    output.extend(other_nodes);

    output
}

struct Regrouped<'m> {
    match_arms: Vec<(ontol_hir::PropPattern<'m, TypedHir>, ontol_hir::Nodes)>,
    first_meta: Meta<'m>,
    absent_arm: Option<(ontol_hir::PropPattern<'m, TypedHir>, ontol_hir::Nodes)>,
}

impl<'m> Regrouped<'m> {
    fn add_arms(
        &mut self,
        match_arms: &[(ontol_hir::PropPattern<'m, TypedHir>, ontol_hir::Nodes)],
    ) {
        for (pattern, body) in match_arms {
            if matches!(pattern, ontol_hir::PropPattern::Absent) {
                self.absent_arm = Some((pattern.clone(), body.clone()));
            } else {
                self.match_arms.push((pattern.clone(), body.clone()));
            }
        }
    }
}

fn needs_regroup(nodes: &[ontol_hir::Node], arena: &ontol_hir::arena::Arena<TypedHir>) -> bool {
    let mut variant_counter: FnvHashMap<(ontol_hir::Var, PropertyId), usize> = Default::default();

    for node in nodes {
        if let ontol_hir::Kind::MatchProp(struct_var, prop_id, _) = arena.kind(*node) {
            let count = variant_counter.entry((*struct_var, *prop_id)).or_default();
            *count += 1;
        }
    }

    variant_counter.into_iter().any(|(_, count)| count > 1)
}
