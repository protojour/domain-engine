use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    ontology::domain::EdgeCardinalProjection,
    property::{PropertyCardinality, ValueCardinality},
    tuple::CardinalIdx,
    var::Var,
    DefId, RelId,
};
use tracing::{debug, info};

use crate::{
    def::{DefKind, Defs},
    edge::EdgeCtx,
    entity::entity_ctx::EntityCtx,
    pattern::{CompoundPatternAttrKind, PatId, Pattern, PatternKind, Patterns, TypePath},
    primitive::Primitives,
    properties::{PropCtx, Property},
    relation::{rel_def_meta, RelCtx, RelParams, Relationship},
    CompileError, CompileErrors, Compiler, SourceSpan,
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
    is_iter: bool,
}

#[derive(Default)]
pub struct Outcome {
    pub new_defs: Vec<DefId>,
    pub new_rels: Vec<RelId>,
}

pub struct MapArmDefInferencer<'c, 'm> {
    map_def_id: DefId,
    outcome: Outcome,
    patterns: &'c Patterns,
    rel_ctx: &'c mut RelCtx,
    prop_ctx: &'c mut PropCtx,
    defs: &'c mut Defs<'m>,
    edge_ctx: &'c mut EdgeCtx,
    entity_ctx: &'c EntityCtx,
    primitives: &'c Primitives,
    errors: &'c mut CompileErrors,
}

impl<'c, 'm> MapArmDefInferencer<'c, 'm> {
    pub fn infer_map_arm_type(&mut self, info: MapArmTypeInferred) -> Outcome {
        let target_pattern = self.patterns.table.get(&info.target.0).unwrap();

        // Force creation of properties and table:
        self.prop_ctx
            .properties_by_def_id_mut(info.target.1)
            .table_mut();

        let mut source_variables = Default::default();
        self.scan_source_variables(
            self.get_pattern(info.source.0),
            VarFlags::default(),
            &mut source_variables,
        );

        self.traverse_pattern(target_pattern, info.target.1, &source_variables);
        std::mem::take(&mut self.outcome)
    }

    fn traverse_pattern(
        &mut self,
        target_pat: &Pattern,
        target_def_id: DefId,
        source_vars: &FnvHashMap<Var, Vec<VarRelationship>>,
    ) {
        match &target_pat.kind {
            PatternKind::Call(_, _) => {
                CompileError::TODO("Not inferrable")
                    .span(target_pat.span)
                    .report(self);
            }
            PatternKind::Compound {
                is_unit_binding,
                attributes,
                ..
            } => {
                if *is_unit_binding {
                    CompileError::TODO("Not inferrable")
                        .span(target_pat.span)
                        .report(&mut self.errors);
                    return;
                }

                for pattern_attr in attributes.iter() {
                    match &pattern_attr.kind {
                        CompoundPatternAttrKind::Value { rel: _, val } => {
                            self.infer_attr_sub_pat(
                                val,
                                VarFlags {
                                    is_option: pattern_attr.bind_option.is_some(),
                                    is_iter: false,
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
            PatternKind::ConstInt(_) => todo!(),
            PatternKind::ConstText(_) => todo!(),
            PatternKind::ConstBool(_) => todo!(),
            PatternKind::Regex(_) => todo!(),
            PatternKind::Error => {}
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
            PatternKind::Call(_, _) => {
                CompileError::TODO("Not inferrable")
                    .span(pattern.span)
                    .report(self);
            }
            PatternKind::Variable(var) => {
                if let Some(var_relationship) =
                    self.find_source_var_relationship(source_vars, *var, &pattern.span)
                {
                    if flags != var_relationship.flags {
                        debug!("flags: {flags:?} var_rel: {:?}", var_relationship.flags);
                        CompileError::InferenceCardinalityMismatch
                            .span(pattern.span)
                            .report(self);
                    }

                    let value_cardinality = if flags.is_iter {
                        ValueCardinality::IndexSet
                    } else {
                        ValueCardinality::Unit
                    };

                    let edge_id = self.edge_ctx.alloc_edge_id(self.map_def_id.package_id());

                    let relationship = Relationship {
                        relation_def_id,
                        projection: EdgeCardinalProjection {
                            id: edge_id,
                            object: CardinalIdx(0),
                            subject: CardinalIdx(0),
                            one_to_one: false,
                        },
                        relation_span: pattern.span,
                        subject: (parent_def_id, pattern.span),
                        subject_cardinality: if flags.is_option {
                            (PropertyCardinality::Optional, value_cardinality)
                        } else {
                            (PropertyCardinality::Mandatory, value_cardinality)
                        },
                        object: (var_relationship.val_def_id, pattern.span),
                        object_cardinality: (
                            PropertyCardinality::Mandatory,
                            ValueCardinality::Unit,
                        ),
                        rel_params: RelParams::Unit,
                        macro_source: None,
                    };

                    debug!("new rel for {parent_def_id:?}");

                    let rel_id = self.rel_ctx.alloc_rel_id(parent_def_id);
                    self.rel_ctx.commit_rel(rel_id, relationship, pattern.span);

                    self.outcome.new_rels.push(rel_id);
                }
            }
            PatternKind::Compound { .. } => {
                CompileError::TODO("Recursive compounds not supported")
                    .span(pattern.span)
                    .report(self);
            }
            PatternKind::Set { elements, .. } => {
                for element in elements.iter() {
                    self.infer_attr_sub_pat(
                        &element.val,
                        VarFlags {
                            is_iter: element.is_iter,
                            ..flags
                        },
                        relation_def_id,
                        parent_def_id,
                        source_vars,
                    );
                }
            }
            PatternKind::ConstInt(_) => todo!(),
            PatternKind::Regex(_) => todo!(),
            PatternKind::ConstText(_) => todo!(),
            PatternKind::ConstBool(_) => todo!(),
            PatternKind::Error => {}
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
                CompileError::TODO("Inference failed: Corresponding variable not found")
                    .span(*span)
                    .report(self);
                None
            }
            Some(relationships) => {
                if relationships.len() != 1 {
                    CompileError::TODO(
                            "Inference failed: Variable is mentioned more than once in the opposing arm"
                        ).span(*span)
                        .report(self);
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
                let Some(properties) = self.prop_ctx.properties_by_def_id(*def_id) else {
                    return;
                };
                let Some(table) = properties.table.as_ref() else {
                    return;
                };
                for attr in attributes.iter() {
                    match &attr.kind {
                        CompoundPatternAttrKind::Value { rel, val } => {
                            if let Some(rel) = rel {
                                self.scan_source_variables(rel, VarFlags::default(), output);
                            }

                            self.scan_compound_attr_sub_pattern_source_variables(
                                val,
                                attr.key.0,
                                VarFlags {
                                    is_option: attr.bind_option.is_some(),
                                    is_iter: false,
                                },
                                (*def_id, table),
                                output,
                            );
                        }
                        CompoundPatternAttrKind::SetOperator {
                            operator: _,
                            elements,
                        } => {
                            for element in elements.iter() {
                                let sub_flags = VarFlags {
                                    is_option: flags.is_option || attr.bind_option.is_some(),
                                    is_iter: element.is_iter,
                                };
                                if let Some(rel) = &element.rel {
                                    self.scan_source_variables(rel, sub_flags, output);
                                }
                                self.scan_compound_attr_sub_pattern_source_variables(
                                    &element.val,
                                    attr.key.0,
                                    sub_flags,
                                    (*def_id, table),
                                    output,
                                );
                            }
                        }
                    }
                }
            }
            PatternKind::Set { elements, .. } => {
                for element in elements.iter() {
                    if let Some(rel) = element.rel.as_ref() {
                        self.scan_source_variables(
                            rel,
                            VarFlags {
                                is_iter: true,
                                ..flags
                            },
                            output,
                        );
                    }

                    self.scan_source_variables(
                        &element.val,
                        VarFlags {
                            is_iter: true,
                            ..flags
                        },
                        output,
                    );
                }
            }
            PatternKind::ConstInt(_) => {}
            PatternKind::Regex(_) => {}
            PatternKind::ConstText(_) => {}
            PatternKind::ConstBool(_) => {}
            PatternKind::Error => {}
        }
    }

    fn scan_compound_attr_sub_pattern_source_variables(
        &self,
        pattern: &Pattern,
        attr_relation_id: DefId,
        flags: VarFlags,
        (parent_def_id, parent_table): (DefId, &IndexMap<RelId, Property>),
        output: &mut FnvHashMap<Var, Vec<VarRelationship>>,
    ) {
        if attr_relation_id == self.primitives.relations.order {
            let Some(info) = self.entity_ctx.entities.get(&parent_def_id) else {
                return;
            };

            let Some(order_union) = info.order_union else {
                return;
            };

            match &pattern.kind {
                PatternKind::Variable(pat_var) => {
                    output.entry(*pat_var).or_default().push(VarRelationship {
                        val_def_id: order_union,
                        flags,
                    });
                }
                PatternKind::Set { elements, .. } => {
                    for element in elements.iter() {
                        let mut flags = flags;

                        if element.is_iter {
                            flags.is_iter = true;
                        }

                        if let PatternKind::Variable(pat_var) = &element.val.kind {
                            output.entry(*pat_var).or_default().push(VarRelationship {
                                val_def_id: order_union,
                                flags,
                            });
                        }
                    }
                }
                _ => {}
            }
        } else if attr_relation_id == self.primitives.relations.direction {
            if let PatternKind::Variable(pat_var) = &pattern.kind {
                output.entry(*pat_var).or_default().push(VarRelationship {
                    val_def_id: self.primitives.direction_union,
                    flags,
                });
            }
        } else {
            match &pattern.kind {
                PatternKind::Compound { .. } => {
                    self.scan_source_variables(pattern, flags, output);
                }
                PatternKind::Variable(pat_var) => {
                    let found = parent_table.iter().find_map(|(rel_id, _property)| {
                        let meta = rel_def_meta(*rel_id, self.rel_ctx, self.defs);
                        if meta.relationship.relation_def_id == attr_relation_id {
                            Some((*rel_id, meta))
                        } else {
                            None
                        }
                    });

                    if let Some((_rel_id, found_relationship_meta)) = found {
                        let (val_def_id, _cardinality, _) =
                            found_relationship_meta.relationship.object();

                        output
                            .entry(*pat_var)
                            .or_default()
                            .push(VarRelationship { val_def_id, flags });
                    }
                }
                other => {
                    info!("Skipping a pattern: {other:?}");
                }
            }
        }
    }

    fn get_pattern(&self, id: PatId) -> &Pattern {
        self.patterns.table.get(&id).unwrap()
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
                CompileError::TODO("Mutual inference")
                    .span(self.defs.def_span(map_def_id))
                    .report(self);
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
            outcome: Outcome::default(),
            patterns: &self.patterns,
            defs: &mut self.defs,
            edge_ctx: &mut self.edge_ctx,
            rel_ctx: &mut self.rel_ctx,
            prop_ctx: &mut self.prop_ctx,
            entity_ctx: &self.entity_ctx,
            primitives: &self.primitives,
            errors: &mut self.errors,
        }
    }
}

impl<'c, 'm> AsMut<CompileErrors> for MapArmDefInferencer<'c, 'm> {
    fn as_mut(&mut self) -> &mut CompileErrors {
        self.errors
    }
}
