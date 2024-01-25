use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    ontology::{PropertyCardinality, ValueCardinality},
    smart_format,
    value::PropertyId,
    var::Var,
    DefId,
};
use tracing::info;

use crate::{
    def::{Def, DefKind, Defs, LookupRelationshipMeta, RelParams, Relationship},
    pattern::{CompoundPatternAttrKind, PatId, Pattern, PatternKind, Patterns, TypePath},
    relation::{Property, Relations},
    CompileError, CompileErrors, Compiler, SourceSpan, SpannedCompileError,
};

#[derive(Clone, Copy)]
pub struct MapArmTypeInferred {
    /// The inferred type
    pub target: (PatId, DefId),
    /// The opposing arm that's not inferred
    pub source: (PatId, DefId),
}

#[derive(Debug)]
struct VarRelationship {
    val_def_id: DefId,
    flags: VarFlags,
}

#[derive(Default, Clone, Copy, PartialEq, Debug)]
struct VarFlags {
    is_option: bool,
    is_set: bool,
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
    pub fn infer_map_arm_type(&mut self, info: MapArmTypeInferred) -> Vec<DefId> {
        let target_pattern = self.patterns.table.get(&info.target.0).unwrap();

        // Force creation of properties and table:
        self.relations
            .properties_by_def_id_mut(info.target.1)
            .table_mut();

        let mut source_variables = Default::default();
        self.scan_source_variables(
            self.get_pattern(info.source.0),
            VarFlags::default(),
            &mut source_variables,
        );

        self.traverse_pattern(target_pattern, info.target.1, &source_variables);
        std::mem::take(&mut self.new_defs)
    }

    fn traverse_pattern(
        &mut self,
        target_pat: &Pattern,
        target_def_id: DefId,
        source_vars: &FnvHashMap<Var, Vec<VarRelationship>>,
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
                    match &pattern_attr.kind {
                        CompoundPatternAttrKind::Value { rel: _, val }
                        | CompoundPatternAttrKind::ContainsElement { rel: _, val } => {
                            self.infer_attr_sub_pat(
                                val,
                                VarFlags {
                                    is_option: pattern_attr.bind_option,
                                    is_set: false,
                                },
                                pattern_attr.key.0,
                                target_def_id,
                                source_vars,
                            );
                        }
                        CompoundPatternAttrKind::SetOperator { .. } => {
                            info!("TODO: SetOperator");
                        }
                    }
                }
            }
            PatternKind::Set { .. } => todo!(),
            PatternKind::Variable(_) => todo!(),
            PatternKind::ConstI64(_) => todo!(),
            PatternKind::ConstText(_) => todo!(),
            PatternKind::Regex(_) => todo!(),
        }
    }

    fn infer_attr_sub_pat(
        &mut self,
        pattern: &Pattern,
        flags: VarFlags,
        relation_def_id: DefId,
        parent_def_id: DefId,
        source_vars: &FnvHashMap<Var, Vec<VarRelationship>>,
    ) {
        match &pattern.kind {
            PatternKind::Call(_, _) => self.error(
                CompileError::TODO(smart_format!("Not inferrable")),
                &pattern.span,
            ),
            PatternKind::Variable(var) => {
                if let Some(var_relationship) =
                    self.find_source_var_relationship(source_vars, *var, &pattern.span)
                {
                    if flags != var_relationship.flags {
                        self.error(CompileError::InferenceCardinalityMismatch, &pattern.span);
                    }

                    let value_cardinality = if flags.is_set {
                        ValueCardinality::Many
                    } else {
                        ValueCardinality::One
                    };

                    let relationship = Relationship {
                        relation_def_id,
                        subject: (parent_def_id, pattern.span),
                        subject_cardinality: if flags.is_option {
                            (PropertyCardinality::Optional, value_cardinality)
                        } else {
                            (PropertyCardinality::Mandatory, value_cardinality)
                        },
                        object: (var_relationship.val_def_id, pattern.span),
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
                            span: pattern.span,
                        },
                    );

                    self.new_defs.push(relationship_id);
                }
            }
            PatternKind::Compound { .. } => {
                self.error(
                    CompileError::TODO(smart_format!("Recursive compounds not supported")),
                    &pattern.span,
                );
            }
            PatternKind::Set { elements, .. } => {
                for element in elements {
                    self.infer_attr_sub_pat(
                        &element.val,
                        VarFlags {
                            is_set: true,
                            ..flags
                        },
                        relation_def_id,
                        parent_def_id,
                        source_vars,
                    );
                }
            }
            PatternKind::ConstI64(_) => todo!(),
            PatternKind::Regex(_) => todo!(),
            PatternKind::ConstText(_) => todo!(),
        }
    }

    fn find_source_var_relationship<'r>(
        &mut self,
        source_vars: &'r FnvHashMap<Var, Vec<VarRelationship>>,
        var: Var,
        span: &SourceSpan,
    ) -> Option<&'r VarRelationship> {
        match source_vars.get(&var) {
            None => {
                self.error(
                    CompileError::TODO(smart_format!(
                        "Inference failed: Corresponding variable not found"
                    )),
                    span,
                );
                None
            }
            Some(relationships) => {
                if relationships.len() != 1 {
                    self.error(
                        CompileError::TODO(smart_format!(
                            "Inference failed: Variable is mentioned more than once in the opposing arm"
                        )),
                        span,
                    );
                    None
                } else {
                    relationships.iter().next()
                }
            }
        }
    }

    fn scan_source_variables(
        &self,
        pattern: &Pattern,
        flags: VarFlags,
        output: &mut FnvHashMap<Var, Vec<VarRelationship>>,
    ) {
        match &pattern.kind {
            PatternKind::Call(..) => {}
            PatternKind::Variable(_) => {}
            PatternKind::Compound {
                type_path,
                attributes,
                ..
            } => {
                let TypePath::Specified { def_id, .. } = type_path else {
                    return;
                };
                let Some(properties) = self.relations.properties_by_def_id(*def_id) else {
                    return;
                };
                let Some(table) = properties.table.as_ref() else {
                    return;
                };
                for attr in attributes.iter() {
                    match &attr.kind {
                        CompoundPatternAttrKind::Value { rel, val }
                        | CompoundPatternAttrKind::ContainsElement { rel, val } => {
                            if let Some(rel) = rel {
                                self.scan_source_variables(rel, VarFlags::default(), output);
                            }

                            self.scan_compound_attr_sub_pattern_source_variables(
                                val,
                                attr.key.0,
                                VarFlags {
                                    is_option: attr.bind_option,
                                    is_set: false,
                                },
                                table,
                                output,
                            );
                        }
                        CompoundPatternAttrKind::SetOperator {
                            operator: _,
                            elements,
                        } => {
                            let sub_flags = VarFlags {
                                is_option: flags.is_option || attr.bind_option,
                                is_set: true,
                            };
                            for element in elements.iter() {
                                if let Some(rel) = &element.rel {
                                    self.scan_source_variables(rel, sub_flags, output);
                                }
                                self.scan_compound_attr_sub_pattern_source_variables(
                                    &element.val,
                                    attr.key.0,
                                    sub_flags,
                                    table,
                                    output,
                                );
                            }
                        }
                    }
                }
            }
            PatternKind::Set { elements, .. } => {
                for element in elements {
                    if let Some(rel) = element.rel.as_ref() {
                        self.scan_source_variables(
                            rel,
                            VarFlags {
                                is_set: true,
                                ..flags
                            },
                            output,
                        );
                    }

                    self.scan_source_variables(
                        &element.val,
                        VarFlags {
                            is_set: true,
                            ..flags
                        },
                        output,
                    );
                }
            }
            PatternKind::ConstI64(_) => {}
            PatternKind::Regex(_) => {}
            PatternKind::ConstText(_) => {}
        }
    }

    fn scan_compound_attr_sub_pattern_source_variables(
        &self,
        pattern: &Pattern,
        attr_relation_id: DefId,
        flags: VarFlags,
        parent_table: &IndexMap<PropertyId, Property>,
        output: &mut FnvHashMap<Var, Vec<VarRelationship>>,
    ) {
        match &pattern.kind {
            PatternKind::Compound { .. } => {
                self.scan_source_variables(pattern, flags, output);
            }
            PatternKind::Variable(pat_var) => {
                let Some((prop_id, found_relationship_meta)) =
                    parent_table.iter().find_map(|(prop_id, _property)| {
                        let meta = self.defs.relationship_meta(prop_id.relationship_id);
                        if meta.relationship.relation_def_id == attr_relation_id {
                            Some((*prop_id, meta))
                        } else {
                            None
                        }
                    })
                else {
                    return;
                };

                let (val_def_id, _cardinality, _) = found_relationship_meta
                    .relationship
                    .by(prop_id.role.opposite());

                output
                    .entry(*pat_var)
                    .or_default()
                    .push(VarRelationship { val_def_id, flags });
            }
            other => {
                info!("Skipping a pattern: {other:?}");
            }
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
    pub fn check_map_arm_def_inference(&mut self, map_def_id: DefId) -> Option<MapArmTypeInferred> {
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
                PatternKind::Set { elements, .. } => {
                    if elements.len() != 1 {
                        return InfStatus::Invalid;
                    }
                    inf_status(&elements.iter().next().unwrap().val)
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
                Some(MapArmTypeInferred {
                    target: (arms[0], target_def_id),
                    source: (arms[1], source_def_id),
                })
            }
            [InfStatus::Source(source_def_id), InfStatus::Infer(target_def_id)] => {
                Some(MapArmTypeInferred {
                    target: (arms[1], target_def_id),
                    source: (arms[0], source_def_id),
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
