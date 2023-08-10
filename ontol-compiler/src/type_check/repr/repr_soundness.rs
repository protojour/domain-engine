use std::collections::BTreeSet;

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{smart_format, DefId};

use crate::{
    error::CompileError, relation::TypeRelation, types::FormatType, Note, SpannedCompileError,
    SpannedNote,
};

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

                self.validate_base_defs_intersection(base_defs, collected_mesh);
            }
            ReprKind::Scalar(def_id, _) => {
                let mut base_defs: BTreeSet<DefId> = Default::default();

                self.collect_base_defs(*def_id, &mut base_defs);

                for mesh_def_id in collected_mesh.keys() {
                    self.collect_base_defs(*mesh_def_id, &mut base_defs);
                }

                self.validate_base_defs_intersection(base_defs, collected_mesh);
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

    fn validate_base_defs_intersection(
        &mut self,
        base_defs: BTreeSet<DefId>,
        collected_mesh: &IndexMap<DefId, IsData>,
    ) {
        if base_defs.len() < 2 {
            return;
        }

        let root_def = self.defs.table.get(&self.root_def_id).unwrap();
        let mut notes = vec![];

        for base_def in base_defs {
            if let Some(level1_path) = self.level1_path_to_base(collected_mesh, base_def) {
                let base_ty = self.def_types.table.get(&base_def).unwrap();

                notes.push(SpannedNote {
                    note: Note::BaseTypeIs(smart_format!(
                        "{}",
                        FormatType(base_ty, self.defs, self.primitives)
                    )),
                    span: level1_path.rel_span,
                });
            }
        }

        self.errors.push(SpannedCompileError {
            error: CompileError::IntersectionOfDisjointTypes,
            span: root_def.span,
            notes,
        });
    }

    fn level1_path_to_base<'d>(
        &self,
        collected_mesh: &'d IndexMap<DefId, IsData>,
        base_def_id: DefId,
    ) -> Option<&'d IsData> {
        for (def_id, is_data) in collected_mesh {
            if is_data.level == 1
                && self.has_path_to_base(*def_id, base_def_id, &mut Default::default())
            {
                return Some(is_data);
            }
        }

        None
    }

    fn has_path_to_base(
        &self,
        sub_def_id: DefId,
        super_def_id: DefId,
        visited: &mut FnvHashSet<DefId>,
    ) -> bool {
        if super_def_id == sub_def_id {
            return true;
        }

        if !visited.insert(sub_def_id) {
            return false;
        }

        if let Some(mesh) = self.relations.ontology_mesh.get(&sub_def_id) {
            for (is, _) in mesh {
                if matches!(is.rel, TypeRelation::Super)
                    && self.has_path_to_base(is.def_id, super_def_id, visited)
                {
                    return true;
                }
            }
        }

        visited.remove(&sub_def_id);

        false
    }
}
