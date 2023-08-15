use std::collections::BTreeSet;

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{smart_format, DefId};

use crate::{
    def::{BuiltinRelationKind, DefKind},
    error::CompileError,
    relation::{RelObjectConstraint, TypeRelation},
    types::FormatType,
    Note, SpannedNote,
};

use super::{
    repr_check::{IsData, ReprCheck},
    repr_model::{Repr, ReprKind},
};

impl<'c, 'm> ReprCheck<'c, 'm> {
    pub(super) fn check_soundness(
        &mut self,
        repr: &mut Repr,
        collected_mesh: &IndexMap<DefId, IsData>,
    ) -> Result<(), ()> {
        match &mut repr.kind {
            ReprKind::Intersection(members) => {
                let mut base_defs: BTreeSet<DefId> = Default::default();

                for (def_id, _) in members {
                    self.collect_base_defs(*def_id, &mut base_defs);
                }

                for mesh_def_id in collected_mesh.keys() {
                    self.collect_base_defs(*mesh_def_id, &mut base_defs);
                }

                let base_def_id = self.check_valid_intersection(base_defs, collected_mesh)?;
                self.check_type_params(base_def_id, repr)?;
            }
            ReprKind::Scalar(def_id, _) => {
                let mut base_defs: BTreeSet<DefId> = Default::default();

                self.collect_base_defs(*def_id, &mut base_defs);

                for mesh_def_id in collected_mesh.keys() {
                    self.collect_base_defs(*mesh_def_id, &mut base_defs);
                }

                let base_def_id = self.check_valid_intersection(base_defs, collected_mesh)?;
                self.check_type_params(base_def_id, repr)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn check_type_params(&mut self, base_def_id: DefId, repr: &mut Repr) -> Result<(), ()> {
        let mut min = None;
        let mut max = None;

        for (relation_def_id, type_param) in &repr.type_params {
            if let Some(constraints) = self.relations.rel_type_constraints.get(relation_def_id) {
                if !constraints.subject_set.is_empty()
                    && !constraints.subject_set.contains(&base_def_id)
                {
                    self.errors.error(
                        CompileError::TODO(smart_format!("Subject type constraint not satisfied")),
                        &type_param.span,
                    );
                }

                for obj_constraint in &constraints.object {
                    match obj_constraint {
                        RelObjectConstraint::ConstantOfSubjectType => {
                            let _value_def_kind = self.defs.def_kind(type_param.object);
                        }
                        RelObjectConstraint::Generator => {}
                    }
                }
            }

            match self.defs.def_kind(*relation_def_id) {
                DefKind::BuiltinRelType(BuiltinRelationKind::Min) => {
                    min = Some(type_param.object);
                }
                DefKind::BuiltinRelType(BuiltinRelationKind::Max) => {
                    max = Some(type_param.object);
                }
                _ => {}
            }
        }

        if min.is_some() || max.is_some() {
            match &mut repr.kind {
                ReprKind::Scalar(def_id, _) => {
                    // User-defined number
                    *def_id = self.root_def_id;
                }
                _ => {
                    self.errors.error(
                        CompileError::TODO(smart_format!("Must be a scalar")),
                        &self.defs.def_span(self.root_def_id),
                    );
                }
            }
        }

        Ok(())
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

    fn check_valid_intersection(
        &mut self,
        base_defs: BTreeSet<DefId>,
        collected_mesh: &IndexMap<DefId, IsData>,
    ) -> Result<DefId, ()> {
        match base_defs.len() {
            0 => panic!("Empty intersection"),
            1 => return base_defs.into_iter().next().ok_or(()),
            _ => {}
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

        self.errors.error_with_notes(
            CompileError::IntersectionOfDisjointTypes,
            &root_def.span,
            notes,
        );

        Err(())
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
