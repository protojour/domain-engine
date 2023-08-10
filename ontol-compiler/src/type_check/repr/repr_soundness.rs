use std::collections::BTreeSet;

use indexmap::IndexMap;
use ontol_runtime::{smart_format, DefId};

use crate::{error::CompileError, SpannedCompileError};

use super::{
    repr_check::{IsData, ReprCheck},
    repr_model::ReprKind,
};

impl<'c, 'm> ReprCheck<'c, 'm> {
    pub(super) fn check_soundness(
        &mut self,
        repr: &mut ReprKind,
        collected_mesh: &IndexMap<DefId, IsData>,
    ) {
        match repr {
            ReprKind::Intersection(members) => {
                let mut base_defs: BTreeSet<DefId> = Default::default();

                for (def_id, _) in members {
                    self.collect_base_defs(*def_id, &mut base_defs);
                }

                for mesh_def_id in collected_mesh.keys() {
                    self.collect_base_defs(*mesh_def_id, &mut base_defs);
                }

                self.validate_base_defs(base_defs);
            }
            ReprKind::Scalar(def_id, _) => {
                let mut base_defs: BTreeSet<DefId> = Default::default();

                self.collect_base_defs(*def_id, &mut base_defs);

                for mesh_def_id in collected_mesh.keys() {
                    self.collect_base_defs(*mesh_def_id, &mut base_defs);
                }

                self.validate_base_defs(base_defs);
            }
            _ => {}
        }
    }

    fn collect_base_defs(&self, def_id: DefId, output: &mut BTreeSet<DefId>) {
        let mut has_supertypes = false;

        if let Some(ontology_mesh) = self.relations.ontology_mesh.get(&def_id) {
            for (is, _) in ontology_mesh {
                if is.is_super() {
                    self.collect_base_defs(is.def_id, output);

                    has_supertypes = true;
                }
            }
        }

        if !has_supertypes {
            output.insert(def_id);
        }
    }

    fn validate_base_defs(&mut self, base_defs: BTreeSet<DefId>) {
        if base_defs.len() > 1 {
            let root_def = self.defs.table.get(&self.root_def_id).unwrap();

            self.errors.push(SpannedCompileError {
                error: CompileError::TODO(smart_format!("Invalid type intersection")),
                span: root_def.span,
                notes: vec![],
            });
        }
    }
}
