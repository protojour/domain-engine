use ontol_runtime::{
    ontology::{PropertyCardinality, ValueCardinality},
    smart_format,
    var::Var,
    DefId,
};
use tracing::debug;

use crate::{
    def::{Def, DefKind, Defs, LookupRelationshipMeta, RelParams, Relationship},
    pattern::{CompoundPatternAttr, PatId, Pattern, PatternKind, Patterns, TypePath},
    relation::Relations,
    CompileError, CompileErrors, Compiler, SourceSpan, SpannedCompileError,
};

#[derive(Clone, Copy)]
pub struct MapArmInferenceInfo {
    pub target: (PatId, DefId),
    pub source: (PatId, DefId),
}

struct RelationshipInfo {
    val_def_id: DefId,
}

pub struct MapArmDefInferencer<'c, 'm> {
    map_def_id: DefId,
    new_defs: Vec<DefId>,
    patterns: &'c Patterns,
    relations: &'c mut Relations,
    defs: &'c mut Defs<'m>,
    errors: &'c mut CompileErrors,
}

impl<'c, 'm> MapArmDefInferencer<'c, 'm> {
    pub fn infer_map_arm_type(&mut self, info: MapArmInferenceInfo) -> Vec<DefId> {
        let target_pattern = self.patterns.table.get(&info.target.0).unwrap();

        // Force creation of properties and table:
        self.relations
            .properties_by_def_id_mut(info.target.1)
            .table_mut();

        self.traverse_pattern(target_pattern, info.target.1, info);
        std::mem::take(&mut self.new_defs)
    }

    fn traverse_pattern(
        &mut self,
        target_pat: &Pattern,
        target_def_id: DefId,
        info: MapArmInferenceInfo,
    ) {
        match &target_pat.kind {
            PatternKind::Call(_, _) => self.error(
                CompileError::TODO(smart_format!("Not inferrable")),
                &target_pat.span,
            ),
            PatternKind::Compound {
                is_unit_binding,
                attributes,
                ..
            } => {
                if *is_unit_binding {
                    self.error(
                        CompileError::TODO(smart_format!("Not inferrable")),
                        &target_pat.span,
                    );
                    return;
                }

                for pattern_attr in attributes.iter() {
                    self.infer_attr(pattern_attr, target_def_id, info);
                }
            }
            PatternKind::Seq(_, _) => todo!(),
            PatternKind::Variable(_) => todo!(),
            PatternKind::ConstI64(_) => todo!(),
            PatternKind::ConstText(_) => todo!(),
            PatternKind::Regex(_) => todo!(),
        }
    }

    fn infer_attr(
        &mut self,
        pattern_attr: &CompoundPatternAttr,
        parent_def_id: DefId,
        info: MapArmInferenceInfo,
    ) {
        match &pattern_attr.value.kind {
            PatternKind::Call(_, _) => self.error(
                CompileError::TODO(smart_format!("Not inferrable")),
                &pattern_attr.value.span,
            ),
            PatternKind::Variable(var) => {
                let Some(relation_info) = self.lookup_source_variable(*var, info) else {
                    self.error(
                        CompileError::TODO(smart_format!(
                            "Inference failed: Corresponding variable not found"
                        )),
                        &pattern_attr.value.span,
                    );
                    return;
                };
                let relationship = Relationship {
                    relation_def_id: pattern_attr.key.0,
                    subject: (parent_def_id, pattern_attr.value.span),
                    subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
                    object: (relation_info.val_def_id, pattern_attr.value.span),
                    object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
                    object_prop: None,
                    rel_params: RelParams::Unit,
                };

                let relationship_id = self.defs.alloc_def_id(self.map_def_id.package_id());
                self.defs.table.insert(
                    relationship_id,
                    Def {
                        id: relationship_id,
                        package: self.map_def_id.package_id(),
                        kind: DefKind::Relationship(relationship),
                        span: pattern_attr.value.span,
                    },
                );

                self.new_defs.push(relationship_id);
            }
            PatternKind::Compound { .. } => todo!(),
            PatternKind::Seq(..) => todo!(),
            PatternKind::ConstI64(_) => todo!(),
            PatternKind::Regex(_) => todo!(),
            PatternKind::ConstText(_) => todo!(),
        }
    }

    fn lookup_source_variable(
        &self,
        var: Var,
        info: MapArmInferenceInfo,
    ) -> Option<RelationshipInfo> {
        self.find_source_variable(self.get_pattern(info.source.0), var)
    }

    fn find_source_variable(&self, pattern: &Pattern, var: Var) -> Option<RelationshipInfo> {
        match &pattern.kind {
            PatternKind::Call(..) => None,
            PatternKind::Variable(_) => None,
            PatternKind::Compound {
                type_path,
                attributes,
                ..
            } => {
                let TypePath::Specified { def_id, .. } = type_path else {
                    return None;
                };
                debug!("HERE");
                for attr in attributes.iter() {
                    match &attr.value.kind {
                        PatternKind::Compound { .. } => {
                            if let Some(lol) = self.find_source_variable(&attr.value, var) {
                                return Some(lol);
                            }
                        }
                        PatternKind::Variable(pat_var) => {
                            debug!("A");
                            let properties = self.relations.properties_by_def_id(*def_id)?;
                            debug!("B");
                            let table = properties.table.as_ref()?;

                            debug!("C");

                            let (prop_id, found_relationship_meta) =
                                table.iter().find_map(|(prop_id, _property)| {
                                    let meta = self.defs.relationship_meta(prop_id.relationship_id);
                                    if meta.relationship.relation_def_id == attr.key.0 {
                                        Some((*prop_id, meta))
                                    } else {
                                        None
                                    }
                                })?;

                            debug!("D");

                            let (val_def_id, _cardinality, _) = found_relationship_meta
                                .relationship
                                .by(prop_id.role.opposite());

                            if pat_var == &var {
                                return Some(RelationshipInfo { val_def_id });
                            }
                        }
                        _ => {}
                    }
                }

                None
            }
            PatternKind::Seq(_, elements) => {
                for element in elements {
                    if let Some(found) = self.find_source_variable(&element.pattern, var) {
                        return Some(found);
                    }
                }
                None
            }
            PatternKind::ConstI64(_) => todo!(),
            PatternKind::Regex(_) => todo!(),
            PatternKind::ConstText(_) => todo!(),
        }
    }

    fn get_pattern(&self, id: PatId) -> &Pattern {
        self.patterns.table.get(&id).unwrap()
    }

    fn error(&mut self, error: CompileError, span: &SourceSpan) {
        self.errors.push(SpannedCompileError {
            error,
            span: *span,
            notes: vec![],
        })
    }
}

impl<'m> Compiler<'m> {
    pub fn check_map_arm_def_inference(
        &mut self,
        map_def_id: DefId,
    ) -> Option<MapArmInferenceInfo> {
        let DefKind::Mapping { arms, .. } = &self.defs.table.get(&map_def_id).unwrap().kind else {
            panic!();
        };

        let arms = *arms;

        enum InfStatus {
            Infer(DefId),
            Source(DefId),
            Invalid,
        }

        fn inf_status(pattern: &Pattern) -> InfStatus {
            match &pattern.kind {
                PatternKind::Compound { type_path, .. } => match type_path {
                    TypePath::Inferred { def_id } => InfStatus::Infer(*def_id),
                    TypePath::Specified { def_id, .. } => InfStatus::Source(*def_id),
                    _ => InfStatus::Invalid,
                },
                PatternKind::Seq(_, elements) => {
                    if elements.len() != 1 {
                        return InfStatus::Invalid;
                    }
                    inf_status(&elements.iter().next().unwrap().pattern)
                }
                _ => InfStatus::Invalid,
            }
        }

        let statuses = arms.map(|pat_id| inf_status(self.patterns.table.get(&pat_id).unwrap()));
        match statuses {
            [InfStatus::Infer(_), InfStatus::Infer(_)] => {
                self.errors.push(SpannedCompileError {
                    error: CompileError::TODO(smart_format!("Mutual inference")),
                    span: self.defs.def_span(map_def_id),
                    notes: vec![],
                });
                None
            }
            [InfStatus::Infer(target_def_id), InfStatus::Source(source_def_id)] => {
                Some(MapArmInferenceInfo {
                    target: (arms[0], target_def_id),
                    source: (arms[1], source_def_id),
                })
            }
            [InfStatus::Source(source_def_id), InfStatus::Infer(target_def_id)] => {
                Some(MapArmInferenceInfo {
                    target: (arms[0], target_def_id),
                    source: (arms[1], source_def_id),
                })
            }
            _ => None,
        }
    }

    pub fn map_arm_def_inferencer(&mut self, map_def_id: DefId) -> MapArmDefInferencer<'_, 'm> {
        MapArmDefInferencer {
            map_def_id,
            new_defs: vec![],
            patterns: &self.patterns,
            defs: &mut self.defs,
            relations: &mut self.relations,
            errors: &mut self.errors,
        }
    }
}
