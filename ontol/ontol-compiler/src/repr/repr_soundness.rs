use std::collections::BTreeSet;

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{DefId, OntolDefTag, OntolDefTagExt};
use ordered_float::NotNan;

use crate::{
    Note,
    def::{BuiltinRelationKind, DefKind},
    error::CompileError,
    misc::RelObjectConstraint,
    primitive::PrimitiveKind,
    thesaurus::TypeRelation,
    types::FormatType,
};

use super::{
    repr_check::{IsData, ReprCheck},
    repr_model::{NumberResolution, Repr, ReprBuilder, ReprFormat, ReprKind, ReprScalarKind},
};

impl<'m> ReprCheck<'_, 'm> {
    pub(super) fn check_soundness(
        &mut self,
        mut builder: ReprBuilder,
        thesaurus_mesh: &IndexMap<DefId, IsData>,
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

                        for mesh_def_id in thesaurus_mesh.keys() {
                            self.collect_base_defs(*mesh_def_id, &mut base_defs);
                        }

                        let base_def_id =
                            self.check_valid_intersection(&base_defs, thesaurus_mesh)?;
                        self.check_type_params(base_def_id, &mut repr, &mut checked_type_params)?;
                        Some(repr)
                    }
                    ReprKind::Scalar(def_id, _scalar_kind, span) => {
                        let def_id = *def_id;
                        let span = *span;
                        let mut base_defs: BTreeSet<DefId> = Default::default();

                        self.collect_base_defs(def_id, &mut base_defs);

                        for mesh_def_id in thesaurus_mesh.keys() {
                            self.collect_base_defs(*mesh_def_id, &mut base_defs);
                        }

                        let base_def_id =
                            self.check_valid_intersection(&base_defs, thesaurus_mesh)?;
                        self.check_type_params(base_def_id, &mut repr, &mut checked_type_params)?;

                        if base_def_id == OntolDefTag::Number.def_id() {
                            match (
                                number_resolution,
                                checked_type_params.min,
                                checked_type_params.max,
                            ) {
                                (Some(NumberResolution::Integer), Some(min), Some(max)) => {
                                    let min: Result<i64, _> = min.parse();
                                    let max: Result<i64, _> = max.parse();

                                    match (min, max) {
                                        (Ok(min), Ok(max)) => Some(Repr {
                                            kind: ReprKind::Scalar(
                                                OntolDefTag::I64.def_id(),
                                                ReprScalarKind::I64(min..=max),
                                                span,
                                            ),
                                            type_params: Default::default(),
                                        }),
                                        _ => todo!("Report error"),
                                    }
                                }
                                (Some(NumberResolution::F64), Some(min), Some(max)) => {
                                    let min: Result<NotNan<f64>, _> = min.parse();
                                    let max: Result<NotNan<f64>, _> = max.parse();

                                    match (min, max) {
                                        (Ok(min), Ok(max)) => Some(Repr {
                                            kind: ReprKind::Scalar(
                                                OntolDefTag::I64.def_id(),
                                                ReprScalarKind::F64(min..=max),
                                                span,
                                            ),
                                            type_params: Default::default(),
                                        }),
                                        _ => todo!("Report error"),
                                    }
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
                        } else if base_def_id == OntolDefTag::Text.def_id() {
                            let mut repr_scalar_kind = ReprScalarKind::Text;

                            if matches!(self.defs.def_kind(def_id), DefKind::TextLiteral(_)) {
                                repr_scalar_kind = ReprScalarKind::TextConstant(def_id);
                            } else {
                                for def_id in &base_defs {
                                    if matches!(
                                        self.defs.def_kind(*def_id),
                                        DefKind::TextLiteral(_)
                                    ) && matches!(repr_scalar_kind, ReprScalarKind::Text)
                                    {
                                        repr_scalar_kind = ReprScalarKind::TextConstant(*def_id);
                                    }
                                }
                            }

                            Some(Repr {
                                kind: ReprKind::Scalar(def_id, repr_scalar_kind, span),
                                type_params: Default::default(),
                            })
                        } else if base_def_id == OntolDefTag::Octets.def_id() {
                            match &repr.kind {
                                ReprKind::Scalar(
                                    _,
                                    ReprScalarKind::Octets(ReprFormat::Unspecified),
                                    _,
                                ) => {
                                    if builder.formats.len() == 1 {
                                        let (repr_format, _) =
                                            builder.formats.iter().next().unwrap();
                                        Some(Repr {
                                            kind: ReprKind::Scalar(
                                                def_id,
                                                ReprScalarKind::Octets(*repr_format),
                                                span,
                                            ),
                                            type_params: Default::default(),
                                        })
                                    } else {
                                        // still abstract
                                        None
                                    }
                                }
                                // any other repr of octets (also with known format) is concrete
                                _ => Some(repr),
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
            if let Some(constraints) = self.misc_ctx.rel_type_constraints.get(relation_def_id) {
                if !constraints.subject_set.is_empty()
                    && !constraints.subject_set.contains(&base_def_id)
                {
                    // self.errors.error(
                    //     CompileError::TODO(format!("Subject type constraint not satisfied")),
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
                DefKind::BuiltinRelType(BuiltinRelationKind::Min, _) => {
                    output.min = Some(self.get_number_literal(type_param.object));
                }
                DefKind::BuiltinRelType(BuiltinRelationKind::Max, _) => {
                    output.max = Some(self.get_number_literal(type_param.object));
                }
                _ => {}
            }
        }

        if output.min.is_some() || output.max.is_some() {
            match &mut repr.kind {
                ReprKind::Scalar(_def_id, _, _) => {
                    // User-defined number
                    // *def_id = self.root_def_id;
                }
                _ => {
                    CompileError::TODO("Must be a scalar")
                        .span(self.defs.def_span(self.root_def_id))
                        .report(self);
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

        for (is, _) in self.thesaurus.entries(def_id, self.defs) {
            if is.is_super() {
                self.collect_base_defs(is.def_id, output);

                has_supertypes = true;
            }
        }

        if !has_supertypes {
            match self.defs.def_kind(def_id) {
                DefKind::Primitive(PrimitiveKind::Format, _) => {
                    // format is not a base def, can be used "anywhere"
                }
                _ => {
                    output.insert(def_id);
                }
            }
        }
    }

    fn check_valid_intersection(
        &mut self,
        base_defs: &BTreeSet<DefId>,
        collected_mesh: &IndexMap<DefId, IsData>,
    ) -> Option<DefId> {
        match base_defs.len() {
            0 => panic!("Empty intersection"),
            1 => return base_defs.iter().next().copied(),
            _ => {}
        }

        let root_def = self.defs.table.get(&self.root_def_id).unwrap();
        let mut notes = vec![];

        for base_def in base_defs {
            if let Some(level1_path) = self.level1_path_to_base(collected_mesh, *base_def) {
                let base_ty = self.def_types.def_table.get(base_def).unwrap();

                notes.push(
                    Note::BaseTypeIs(format!("{}", FormatType::new(base_ty, self.defs)))
                        .span(level1_path.rel_span),
                );
            }
        }

        CompileError::IntersectionOfDisjointTypes
            .span(root_def.span)
            .with_notes(notes)
            .report(self);

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

        for (is, _) in self.thesaurus.entries(sub_def_id, self.defs) {
            if matches!(is.rel, TypeRelation::Super | TypeRelation::ImplicitSuper)
                && self.has_path_to_base(is.def_id, super_def_id, visited)
            {
                return true;
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
                builder.number_resolutions.swap_remove(resolution);
            }
        }

        match builder.number_resolutions.len() {
            1 => builder.number_resolutions.keys().cloned().next(),
            _ => {
                let notes: Vec<_> = builder
                    .number_resolutions
                    .iter()
                    .flat_map(|(resolution, span)| {
                        let type_def_id = resolution.def_id();
                        self.defs
                            .def_kind(type_def_id)
                            .opt_identifier()
                            .map(|identifier| Note::BaseTypeIs(identifier.into()).span(*span))
                    })
                    .collect();

                CompileError::AmbiguousNumberResolution
                    .span(self.defs.def_span(self.root_def_id))
                    .with_notes(notes)
                    .report(self);

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
