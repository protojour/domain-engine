use std::collections::BTreeSet;

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{smart_format, DefId};
use ordered_float::NotNan;
use tracing::debug;

use crate::{
    def::{BuiltinRelationKind, DefKind},
    error::CompileError,
    relation::{RelObjectConstraint, TypeRelation},
    types::FormatType,
    Note, SpannedNote, NO_SPAN,
};

use super::{
    repr_check::{IsData, ReprCheck},
    repr_model::{NumberResolution, Repr, ReprBuilder, ReprKind},
};

impl<'c, 'm> ReprCheck<'c, 'm> {
    pub(super) fn check_soundness(
        &mut self,
        mut builder: ReprBuilder,
        collected_mesh: &IndexMap<DefId, IsData>,
    ) -> Option<Repr> {
        let number_resolution = self.check_number_resolution(&mut builder);

        let mut checked_type_params = CheckedTypeParams::default();

        match builder.kind {
            Some(kind) => {
                let mut repr = Repr {
                    kind,
                    type_params: builder.type_params,
                };

                match &mut repr.kind {
                    ReprKind::Intersection(members) => {
                        let mut base_defs: BTreeSet<DefId> = Default::default();

                        for (def_id, _) in members {
                            self.collect_base_defs(*def_id, &mut base_defs);
                        }

                        for mesh_def_id in collected_mesh.keys() {
                            self.collect_base_defs(*mesh_def_id, &mut base_defs);
                        }

                        let base_def_id =
                            self.check_valid_intersection(base_defs, collected_mesh)?;
                        self.check_type_params(base_def_id, &mut repr, &mut checked_type_params)?;
                        Some(repr)
                    }
                    ReprKind::Scalar(def_id, _) => {
                        let mut base_defs: BTreeSet<DefId> = Default::default();

                        self.collect_base_defs(*def_id, &mut base_defs);

                        for mesh_def_id in collected_mesh.keys() {
                            self.collect_base_defs(*mesh_def_id, &mut base_defs);
                        }

                        let base_def_id =
                            self.check_valid_intersection(base_defs, collected_mesh)?;
                        self.check_type_params(base_def_id, &mut repr, &mut checked_type_params)?;

                        if base_def_id == self.primitives.number {
                            match (
                                number_resolution,
                                checked_type_params.min,
                                checked_type_params.max,
                            ) {
                                (Some(NumberResolution::Integer), Some(min), Some(max)) => {
                                    debug!("Concrete number({:?}): {repr:?}", self.root_def_id);

                                    let min: Result<i64, _> = min.parse();
                                    let max: Result<i64, _> = max.parse();

                                    match (min, max) {
                                        (Ok(min), Ok(max)) => Some(Repr {
                                            kind: ReprKind::I64(
                                                self.root_def_id,
                                                min..max,
                                                NO_SPAN,
                                            ),
                                            type_params: Default::default(),
                                        }),
                                        _ => todo!("Report error"),
                                    }
                                }
                                (Some(NumberResolution::F64), Some(_min), Some(_max)) => {
                                    Some(Repr {
                                        kind: ReprKind::F64(
                                            self.root_def_id,
                                            NotNan::new(f64::MIN).unwrap()
                                                ..NotNan::new(f64::MAX).unwrap(),
                                            NO_SPAN,
                                        ),
                                        type_params: Default::default(),
                                    })
                                }
                                _ => {
                                    // FIXME: Report this?
                                    // self.state.abstract_notes.push(SpannedNote {
                                    //     note: Note::NumberTypeIsAbstract,
                                    //     span: NO_SPAN,
                                    // });

                                    None
                                }
                            }
                        } else {
                            Some(repr)
                        }
                    }
                    _ => Some(repr),
                }
            }
            None => None,
        }
    }

    fn check_type_params(
        &mut self,
        base_def_id: DefId,
        repr: &mut Repr,
        output: &mut CheckedTypeParams<'m>,
    ) -> Option<()> {
        for (relation_def_id, type_param) in &repr.type_params {
            if let Some(constraints) = self.relations.rel_type_constraints.get(relation_def_id) {
                if !constraints.subject_set.is_empty()
                    && !constraints.subject_set.contains(&base_def_id)
                {
                    // self.errors.error(
                    //     CompileError::TODO(smart_format!("Subject type constraint not satisfied")),
                    //     &type_param.span,
                    // );
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
                    output.min = Some(self.get_number_literal(type_param.object));
                }
                DefKind::BuiltinRelType(BuiltinRelationKind::Max) => {
                    output.max = Some(self.get_number_literal(type_param.object));
                }
                _ => {}
            }
        }

        if output.min.is_some() || output.max.is_some() {
            match &mut repr.kind {
                ReprKind::Scalar(_def_id, _) => {
                    // User-defined number
                    // *def_id = self.root_def_id;
                }
                _ => {
                    self.errors.error(
                        CompileError::TODO(smart_format!("Must be a scalar")),
                        &self.defs.def_span(self.root_def_id),
                    );
                }
            }
        }

        Some(())
    }

    fn get_number_literal(&self, def_id: DefId) -> &'m str {
        match self.defs.def_kind(def_id) {
            DefKind::NumberLiteral(lit) => lit,
            _ => panic!(),
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

    fn check_valid_intersection(
        &mut self,
        base_defs: BTreeSet<DefId>,
        collected_mesh: &IndexMap<DefId, IsData>,
    ) -> Option<DefId> {
        match base_defs.len() {
            0 => panic!("Empty intersection"),
            1 => return base_defs.into_iter().next(),
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

        None
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

    fn check_number_resolution(&mut self, builder: &mut ReprBuilder) -> Option<NumberResolution> {
        if builder.number_resolutions.is_empty() {
            return None;
        }

        // Remove parent classes
        let resolution_set: Vec<_> = builder.number_resolutions.keys().cloned().collect();
        for resolution in &resolution_set {
            if resolution_set
                .iter()
                .any(|res| res.is_sub_resolution_of(*resolution))
            {
                builder.number_resolutions.remove(resolution);
            }
        }

        if !builder.number_resolutions.is_empty() {
            debug!("resolutions: {:?}", builder.number_resolutions);
        }

        match builder.number_resolutions.len() {
            1 => builder.number_resolutions.keys().cloned().next(),
            _ => {
                let notes: Vec<_> = builder
                    .number_resolutions
                    .iter()
                    .flat_map(|(resolution, span)| {
                        let type_def_id = resolution.def_id(&self.primitives);
                        if let Some(identifier) = self.defs.def_kind(type_def_id).opt_identifier() {
                            Some(SpannedNote {
                                note: Note::BaseTypeIs(identifier.into()),
                                span: *span,
                            })
                        } else {
                            None
                        }
                    })
                    .collect();

                self.errors.error_with_notes(
                    CompileError::AmbiguousNumberResolution,
                    &self.defs.def_span(self.root_def_id),
                    notes,
                );

                None
            }
        }
    }
}

#[derive(Default)]
struct CheckedTypeParams<'m> {
    min: Option<&'m str>,
    max: Option<&'m str>,
}
