use std::vec;

use anyhow::anyhow;
use domain_engine_core::{
    entity_id_utils::{find_inherent_entity_id, try_generate_entity_id, GeneratedId},
    system::SystemAPI,
    DomainError, DomainResult,
};
use ontol_runtime::{
    attr::Attr,
    interface::serde::{
        operator::SerdePropertyFlags,
        processor::{ProcessorMode, SubProcessorContext},
    },
    ontology::{
        domain::{
            DataRelationshipInfo, DataRelationshipKind, DataRelationshipSource,
            DataRelationshipTarget,
        },
        Ontology,
    },
    property::ValueCardinality,
    query::select::Select,
    value::{Value, ValueDebug, ValueTag},
    DefId,
};
use serde_json::json;
use substring::Substring;
use tracing::debug;

use super::{
    aql::*,
    data_store::{serialize, EdgeWriteMode, WriteMode},
    AqlQuery, ArangoDatabase,
};

impl AqlQuery {
    /// Build write-oriented AqlQuery from an entity Value and a Select
    pub fn build_write(
        mode: WriteMode,
        mut entity: Value,
        select: &Select,
        ontology: &Ontology,
        database: &ArangoDatabase,
    ) -> DomainResult<Vec<AqlQuery>> {
        let def = ontology.def(entity.type_def_id());
        let def_name = def.name().expect("entity should have a name");
        debug!("AqlQuery::build_write {:?} {}", mode, &ontology[def_name]);

        if def.entity().is_some()
            && matches!(mode, WriteMode::Insert)
            && matches!(entity, Value::Struct(..))
        {
            generate_id(&mut entity, ontology, database.system.as_ref())?;
        }

        let mut meta =
            MetaQuery::from(Ident::new(&ontology[def_name]).to_var(), ontology, database);

        // debug!("entity {:#?}", entity);

        let collection = database
            .collections
            .get(&entity.type_def_id())
            .expect("collection should exist");

        meta.with.insert(collection.clone());

        match entity {
            Value::Struct(ref mut struct_map, _) => {
                for (rel_id, attr) in struct_map.clone().iter() {
                    let rel_info = def
                        .data_relationships
                        .get(rel_id)
                        .expect("property not found in type info");
                    if let DataRelationshipKind::Edge(_) = rel_info.kind {
                        meta.write_relation(mode, attr.clone(), rel_info, 0)?;
                        struct_map.remove(rel_id); // TODO: verify
                    }
                }
            }
            _ => match mode {
                WriteMode::Delete => {}
                _ => return Err(DomainError::EntityMustBeStruct),
            },
        }

        let operator_addr = def.operator_addr.expect("type should have an operator");
        let profile = database.profile();
        let processor = ontology
            .new_serde_processor(operator_addr, ProcessorMode::RawTreeOnly)
            .with_profile(&profile);
        let mut data = serialize(&entity, &processor);

        let operation = match mode {
            WriteMode::Insert => Operation::Insert(Insert {
                data: data.to_string(),
                collection: collection.clone(),
                ..Default::default()
            }),
            WriteMode::Update => Operation::Update(Update {
                data: data.to_string(),
                collection: collection.clone(),
                ..Default::default()
            }),
            WriteMode::Delete => {
                data.as_object_mut().unwrap().retain(|key, _| key == "_key");
                Operation::Remove(Remove {
                    key: data.to_string(),
                    collection: collection.clone(),
                    ..Default::default()
                })
            }
        };

        let mut return_var: Expr = match mode {
            WriteMode::Insert | WriteMode::Update => Expr::complex("NEW"),
            WriteMode::Delete => Expr::complex("OLD"),
        };

        if matches!(mode, WriteMode::Update)
            && matches!(
                meta.rels.iter().next(),
                Some((_, MetaQueryData { mode: Some(_), .. }))
            )
        {
            let key = data["_key"]
                .as_str()
                .expect("entity id should serialize to str")
                .to_owned();
            let id = collection.format_id(&key);

            meta.selection = Some(Selection::Document(meta.var.clone(), id));
        } else {
            meta.ops.push(Operation::Let(Let {
                var: meta.var.clone(),
                query: Query {
                    operations: Some(vec![operation.clone()]),
                    returns: Return {
                        var: return_var.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            }));
        }

        for query in [&mut meta.upserts, &mut meta.rels] {
            for (collection, metadata) in query {
                let data = match &metadata.data.len() {
                    1 => metadata.data[0].clone(),
                    _ => "item".to_string(),
                };

                debug!("mode {mode:?}, metadata.mode {:?}", metadata.mode);

                let operations = match metadata.mode {
                    Some(mode) => match mode {
                        EdgeWriteMode::Insert => vec![Operation::Insert(Insert {
                            data,
                            collection: collection.clone(),
                            options: metadata.options.clone(),
                        })],
                        EdgeWriteMode::UpsertSelfIdentifying => {
                            vec![Operation::Upsert(Upsert {
                                search: data.clone(),
                                insert: Insert {
                                    data: data.clone(),
                                    collection: collection.clone(),
                                    options: None,
                                },
                                update: Update {
                                    var: None,
                                    data: "{}".to_string(),
                                    collection: collection.clone(),
                                    options: None,
                                },
                                collection: collection.clone(),
                                options: Some(
                                    json!({ "ignoreErrors": true, "exclusive": true }).to_string(),
                                ),
                            })]
                        }
                        EdgeWriteMode::Overwrite | EdgeWriteMode::OverwriteInverse => {
                            metadata.selection = Some(Selection::Loop(For {
                                var: "obj".into(),
                                edge: Some("obj_edge".into()),
                                direction: metadata.direction.clone(),
                                object: match meta.docs.first() {
                                    Some(Operation::Selection(Selection::Document(var, _))) => {
                                        var.clone()
                                    }
                                    _ => meta.return_var.clone(),
                                },
                                edges: Some(vec![collection.clone()]),
                                ..Default::default()
                            }));
                            metadata.return_vars.entry("_edge".to_string()).or_default();
                            return_var = "obj".into();

                            vec![
                                // Operation::Filter(Filter {
                                //     var: "obj._key".to_string(),
                                //     comp: Some(Comparison::Eq),
                                //     val: metadata.filter_key.clone(),
                                // }),
                                Operation::Let(Let {
                                    var: "_edge".into(),
                                    query: Query {
                                        operations: Some(vec![Operation::Update(Update {
                                            var: Some("obj_edge".to_string()),
                                            data,
                                            collection: collection.clone(),
                                            ..Default::default()
                                        })]),
                                        returns: Return {
                                            var: "NEW".into(),
                                            ..Default::default()
                                        },
                                        ..Default::default()
                                    },
                                }),
                            ]
                        }
                        EdgeWriteMode::UpdateExisting => {
                            metadata.selection = Some(Selection::Loop(For {
                                var: "obj".into(),
                                edge: Some("obj_edge".into()),
                                direction: metadata.direction.clone(),
                                object: meta.return_var.clone(),
                                edges: Some(vec![collection.clone()]),
                                ..Default::default()
                            }));
                            metadata.return_vars.entry("_edge".to_string()).or_default();
                            return_var = "obj".into();

                            vec![
                                Operation::Filter(Filter {
                                    var: "obj._key".into(),
                                    comp: Some(Comparison::Eq),
                                    val: metadata.filter_key.clone(),
                                }),
                                Operation::Let(Let {
                                    var: "_edge".into(),
                                    query: Query {
                                        operations: Some(vec![Operation::Update(Update {
                                            var: Some("obj_edge".to_string()),
                                            data,
                                            collection: collection.clone(),
                                            ..Default::default()
                                        })]),
                                        returns: Return {
                                            var: "NEW".into(),
                                            ..Default::default()
                                        },
                                        ..Default::default()
                                    },
                                }),
                            ]
                        }
                        EdgeWriteMode::Delete => {
                            metadata.selection = Some(Selection::Loop(For {
                                var: "obj".into(),
                                edge: Some("obj_edge".into()),
                                direction: metadata.direction.clone(),
                                object: meta.return_var.clone(),
                                edges: Some(vec![collection.clone()]),
                                ..Default::default()
                            }));
                            return_var = "null".into();

                            vec![
                                Operation::Filter(Filter {
                                    var: "obj._key".into(),
                                    comp: Some(Comparison::Eq),
                                    val: metadata.filter_key.clone(),
                                }),
                                Operation::Let(Let {
                                    var: "_edge".into(),
                                    query: Query {
                                        operations: Some(vec![Operation::Remove(Remove {
                                            key: "obj_edge".to_string(),
                                            collection: collection.clone(),
                                            ..Default::default()
                                        })]),
                                        returns: Return {
                                            var: "null".into(),
                                            ..Default::default()
                                        },
                                        ..Default::default()
                                    },
                                }),
                            ]
                        }
                    },
                    None => match mode {
                        WriteMode::Insert => vec![Operation::Insert(Insert {
                            data,
                            collection: collection.clone(),
                            options: metadata.options.clone(),
                        })],
                        WriteMode::Update => vec![Operation::Update(Update {
                            data,
                            collection: collection.clone(),
                            options: metadata.options.clone(),
                            ..Default::default()
                        })],
                        WriteMode::Delete => vec![Operation::Remove(Remove {
                            key: data,
                            collection: collection.clone(),
                            ..Default::default()
                        })],
                    },
                };

                match metadata.data.len() {
                    0 | 1 => meta.ops.push(Operation::Let(Let {
                        var: metadata.var.clone(),
                        query: Query {
                            selection: metadata.selection.clone(),
                            operations: Some(operations),
                            returns: Return {
                                var: return_var.clone(),
                                merge: metadata.return_vars.to_string_maybe(),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    })),
                    _ => {
                        meta.ops.push(Operation::LetData(LetData {
                            var: metadata.var.as_var().unwrap().append("_data").to_var(),
                            data: metadata.data.to_vec(),
                        }));
                        meta.ops.push(Operation::Let(Let {
                            var: metadata.var.clone(),
                            query: Query {
                                selection: Some(Selection::Loop(For {
                                    var: "item".into(),
                                    object: metadata.var.as_var().unwrap().append("_data").to_var(),
                                    ..Default::default()
                                })),
                                operations: Some(operations),
                                returns: Return {
                                    var: return_var.clone(),
                                    merge: metadata.return_vars.to_string_maybe(),
                                    ..Default::default()
                                },
                                ..Default::default()
                            },
                        }))
                    }
                }
            }
        }

        let mut queries = meta.write_select(select.clone())?;

        let mut ops = vec![];
        ops.extend(meta.docs);
        ops.extend(meta.ops);

        let query = AqlQuery {
            query: Query {
                with: (!meta.with.is_empty()).then_some(meta.with.into_iter().collect()),
                selection: meta.selection,
                operations: Some(ops),
                returns: Return {
                    var: meta.return_var,
                    merge: meta.return_vars.to_string_maybe(),
                    ..Default::default()
                },
                ..Default::default()
            },
            bind_vars: meta.bind_vars,
            operator_addr,
        };

        queries.insert(0, query);

        Ok(queries)
    }
}

/// Generate and add id to an entity, if applicable
fn generate_id(
    mut entity: &mut Value,
    ontology: &Ontology,
    system: &dyn SystemAPI,
) -> DomainResult<()> {
    let def = ontology.def(entity.type_def_id());
    let entity_info = def.entity().expect("def should be an entity");

    let inherent_id = find_inherent_entity_id(entity, ontology)?;
    let id = match (&inherent_id, entity_info.id_value_generator) {
        (Some(_), _) => inherent_id.clone(),
        (None, Some(value_generator)) => {
            match try_generate_entity_id(
                entity_info.id_operator_addr,
                value_generator,
                ontology,
                system,
            )? {
                (GeneratedId::Generated(value), container) => Some(container.wrap(value)),
                // autogenerated by ArangoDB
                (GeneratedId::AutoIncrementSerial(_), _) => None,
            }
        }
        (None, None) => return Err(DomainError::TypeCannotBeUsedForIdGeneration),
    };

    match &mut entity {
        Value::Struct(ref mut struct_map, _) => {
            if let Some(id) = &id {
                struct_map.insert(entity_info.id_relationship_id, id.clone().into());
            }
        }
        _ => return Err(DomainError::EntityMustBeStruct),
    }

    Ok(())
}

impl<'a> MetaQuery<'a> {
    /// Add a relation to a write MetaQuery
    fn write_relation(
        &mut self,
        mode: WriteMode,
        attr: Attr,
        rel_info: &DataRelationshipInfo,
        index: usize,
    ) -> DomainResult<()> {
        let (mut val, rel_params) = match attr {
            Attr::Unit(val) => (val, Value::unit()),
            Attr::Tuple(tuple) => {
                let mut iter = tuple.elements.into_iter();
                let Some(val) = iter.next() else {
                    return Ok(());
                };
                let rel = iter.next().unwrap_or(Value::unit());
                (val, rel)
            }
            Attr::Matrix(matrix) => {
                // todo!("write matrix {matrix:#?}");

                match &rel_info.target {
                    // TODO: use subsequence data
                    DataRelationshipTarget::Unambiguous(_) => {
                        for row in matrix.into_rows() {
                            self.write_relation(mode, Attr::Tuple(Box::new(row)), rel_info, index)?;
                        }
                    }
                    DataRelationshipTarget::Union { .. } => {
                        // use a counter for index, but reset between variants
                        let mut last_def_id: Option<DefId> = None;
                        let mut counter = 0;

                        for row in matrix.into_rows() {
                            let def_id = row.elements[0].type_def_id();
                            if last_def_id.is_none() {
                                last_def_id = Some(def_id)
                            }
                            let index = match last_def_id == Some(def_id) {
                                true => counter,
                                false => {
                                    counter = 0;
                                    counter
                                }
                            };

                            let mut rel_info = rel_info.clone();
                            rel_info.target = DataRelationshipTarget::Unambiguous(def_id);
                            self.write_relation(
                                mode,
                                Attr::Tuple(Box::new(row)),
                                &rel_info,
                                index,
                            )?;

                            last_def_id = Some(def_id);
                            counter += 1;
                        }
                    }
                }

                return Ok(());
            }
        };

        let def = self.ontology.def(val.type_def_id());
        if def.entity().is_some()
            && matches!(mode, WriteMode::Insert)
            && matches!(val, Value::Struct(..))
        {
            generate_id(&mut val, self.ontology, self.database.system.as_ref())?;
        }

        let rel_name = rel_info.name;
        let var_name = Ident::new(format!(
            "{}_{}{}",
            self.var.raw_str(),
            &self.ontology[rel_name],
            match rel_info.target {
                DataRelationshipTarget::Unambiguous(def_id) => {
                    let def = self.ontology.def(def_id);
                    let def_name = def.name().expect("type should have a name");
                    format!("_{}", &self.ontology[def_name])
                }
                DataRelationshipTarget::Union { .. } => "".to_string(), // TODO: validate
            }
        ));

        debug!(
            "MetaQuery::write_relation {:?} {} {}",
            mode,
            var_name,
            ValueDebug(&val)
        );

        let return_many = match rel_info.cardinality {
            (_, ValueCardinality::Unit) => false,
            (_, ValueCardinality::IndexSet | ValueCardinality::List) => true,
        };

        match val {
            Value::Struct(ref mut struct_map, tag) => {
                let (def_id, def) = match &rel_info.target {
                    DataRelationshipTarget::Unambiguous(def_id) => (def_id, def),
                    DataRelationshipTarget::Union(union_def_id) => self
                        .ontology
                        .union_variants(*union_def_id)
                        .iter()
                        .find_map(|variant_def_id| {
                            let def = self.ontology.def(*variant_def_id);
                            let entity = def.entity().unwrap();
                            if *variant_def_id == tag.def_id() {
                                assert!(entity.is_self_identifying);
                                Some((variant_def_id, def))
                            } else if entity.id_value_def_id == tag.def_id() {
                                Some((variant_def_id, def))
                            } else {
                                None
                            }
                        })
                        .expect("union variant not identified"),
                };

                let collection = self
                    .database
                    .collections
                    .get(def_id)
                    .expect("collection should exist");

                let entity = self.ontology.def(*def_id).entity().unwrap();

                // handle simple id reference
                if *def_id != tag.def_id() {
                    let id_val = val.clone();
                    let def = self.ontology.def(*def_id);
                    let operator_addr = def.entity().unwrap().id_operator_addr;
                    let profile = self.database.profile();
                    let id_context = SubProcessorContext {
                        parent_property_flags: SerdePropertyFlags::ENTITY_ID,
                        ..Default::default()
                    };
                    let processor = self
                        .ontology
                        .new_serde_processor(operator_addr, ProcessorMode::RawTreeOnly)
                        .with_profile(&profile)
                        .with_known_context(id_context);

                    let key = serialize(&id_val, &processor)
                        .as_str()
                        .expect("entity id should serialize to str")
                        .to_owned();
                    let id = collection.format_id(&key);

                    self.with.insert(collection.clone());

                    return self.write_relation_edge(
                        mode,
                        id_val.tag(),
                        rel_params,
                        &var_name,
                        id,
                        rel_info,
                    );
                };

                let mut sub_meta =
                    MetaQuery::from(var_name.clone().to_var(), self.ontology, self.database);
                for (rel_id, attr) in struct_map.clone().iter() {
                    let rel_info = def
                        .data_relationships
                        .get(rel_id)
                        .expect("property not found in type info");
                    if let DataRelationshipKind::Edge(_) = rel_info.kind {
                        match &rel_info.target {
                            DataRelationshipTarget::Unambiguous(_) => {
                                sub_meta.write_relation(mode, attr.clone(), rel_info, 0)?;
                            }
                            DataRelationshipTarget::Union { .. } => {
                                let def_id = val.type_def_id();
                                let mut rel_info = rel_info.clone();
                                rel_info.target = DataRelationshipTarget::Unambiguous(def_id);
                                sub_meta.write_relation(mode, attr.clone(), &rel_info, 0)?;
                            }
                        }
                    }
                }

                // add data gathered into sub_meta
                self.with.extend(sub_meta.with);
                self.docs.extend(sub_meta.docs);

                for (collection, upsert_data) in sub_meta.upserts {
                    let entry = self.upserts.entry(collection).or_default();
                    entry.var = upsert_data.var;
                    entry.data.extend(upsert_data.data);
                    entry.return_vars = upsert_data.return_vars;
                }
                for (collection, rel_data) in sub_meta.rels {
                    let entry = self.rels.entry(collection).or_default();
                    entry.var = rel_data.var;
                    entry.data.extend(rel_data.data);
                }

                // add data for this struct
                let operator_addr = def.operator_addr.expect("type should have an operator");
                let profile = self.database.profile();
                let processor = self
                    .ontology
                    .new_serde_processor(operator_addr, ProcessorMode::RawTreeOnly)
                    .with_profile(&profile);
                let data = serialize(&val, &processor);

                self.with.insert(collection.clone());

                let entry = self.upserts.entry(collection.clone()).or_default();
                if entity.is_self_identifying {
                    entry.mode = Some(EdgeWriteMode::UpsertSelfIdentifying);
                }

                entry.var = var_name.clone().to_var();
                entry.data.push(data.to_string());
                entry.return_vars = sub_meta.return_vars;

                match &rel_info.target {
                    DataRelationshipTarget::Unambiguous(_) => {}
                    DataRelationshipTarget::Union(union_def_id) => {
                        for def_id in self.ontology.union_variants(*union_def_id) {
                            self.database
                                .collections
                                .get(def_id)
                                .expect("collection should exist");
                            self.with.insert(collection.clone());
                        }
                    }
                }

                let return_var = if return_many {
                    var_name.clone().to_var()
                } else {
                    Expr::complex(format!("{var_name}[0]"))
                };
                self.return_vars
                    .entry(self.ontology[rel_name].to_string())
                    .and_modify(|vars| {
                        if !vars.iter().any(|v| v.raw_str() == var_name.raw_str()) {
                            vars.push(return_var.clone());
                        }
                    })
                    .or_insert(vec![return_var.clone()]);

                let id = format!("{var_name}[{index}]._id");
                self.write_relation_edge(mode, val.tag(), rel_params, &var_name, id, rel_info)?;
            }
            Value::Sequence(..) => {
                // TODO: use subsequence data
                todo!("write sequence");
            }
            Value::Filter(ref filter, tag) => {
                self.add_filter(tag.def_id(), filter)?;
            }
            _ => {
                let def_id = match &rel_info.target {
                    DataRelationshipTarget::Unambiguous(def_id) => def_id,
                    DataRelationshipTarget::Union { .. } => {
                        panic!("target should be unambiguous at this point")
                    }
                };
                let collection = self
                    .database
                    .collections
                    .get(def_id)
                    .expect("collection should exist");

                let operator_addr = def.operator_addr.expect("type should have an operator");
                let profile = self.database.profile();
                let processor = self
                    .ontology
                    .new_serde_processor(operator_addr, ProcessorMode::RawTreeOnly)
                    .with_profile(&profile);
                let mut data = serialize(&val, &processor);

                self.with.insert(collection.clone());

                if matches!(mode, WriteMode::Delete) {
                    data.as_object_mut().unwrap().retain(|key, _| key == "_key");
                }

                let target_def = self.ontology.def(*def_id);
                if target_def
                    .entity()
                    .is_some_and(|entity_info| entity_info.is_self_identifying)
                {
                    let entry = self.upserts.entry(collection.clone()).or_default();
                    entry.var = var_name.clone().to_var();
                    entry.data.push(format!(r#"{{ "_key": {data} }}"#));
                    // cannot disambiguate lookup or insert for self identifying type
                    // INSERT with overwriteMode: "ignore" option is functionally equivalent
                    entry.options = Some(r#"{ overwriteMode: "ignore" }"#.to_string());

                    let return_var = if return_many {
                        var_name.clone().to_var()
                    } else {
                        Expr::complex(format!("{var_name}[0]"))
                    };
                    self.return_vars
                        .entry(self.ontology[rel_name].to_string())
                        .and_modify(|vars| vars.push(return_var.clone()))
                        .or_insert(vec![return_var]);

                    let id = format!("{var_name}[{index}]._id");
                    self.write_relation_edge(mode, val.tag(), rel_params, &var_name, id, rel_info)?;
                } else {
                    let key = data
                        .as_str()
                        .expect("entity id should serialize to str")
                        .to_owned();
                    let id = collection.format_id(&key);

                    // target is represented as a DOCUMENT
                    self.docs.push(Operation::Selection(Selection::Document(
                        var_name.clone().to_var(),
                        id,
                    )));

                    let return_var: Expr = if return_many {
                        Expr::complex(format!("[{var_name}]"))
                    } else {
                        var_name.clone().to_var()
                    };
                    self.return_vars
                        .entry(self.ontology[rel_name].to_string())
                        .and_modify(|vars| vars.push(return_var.clone()))
                        .or_insert(vec![return_var]);

                    let id = format!("{var_name}._id");
                    self.write_relation_edge(mode, val.tag(), rel_params, &var_name, id, rel_info)?;
                }
            }
        }

        Ok(())
    }

    /// Add a relation edge to a write MetaQuery
    fn write_relation_edge(
        &mut self,
        mode: WriteMode,
        val_tag: ValueTag,
        rel_params: Value,
        var_name: &Ident,
        id: String,
        rel_info: &DataRelationshipInfo,
    ) -> DomainResult<()> {
        let DataRelationshipKind::Edge(edge_projection) = rel_info.kind else {
            panic!("not an edge");
        };

        let edge_write_mode = match (
            mode,
            val_tag.is_delete(),
            val_tag.is_update(),
            self.docs.first(),
        ) {
            (WriteMode::Update, _, _, Some(_)) => Some(EdgeWriteMode::OverwriteInverse),
            (WriteMode::Update, _, _, None) => Some(EdgeWriteMode::Overwrite),
            (_, true, _, _) => Some(EdgeWriteMode::Delete),
            (_, _, true, _) => Some(EdgeWriteMode::UpdateExisting),
            _ => None,
        };

        debug!(
            "MetaQuery::write_relation_edge {} {:?}",
            match edge_write_mode {
                Some(mode) => format!("({mode:?})"),
                None => "".to_string(),
            },
            &rel_params
        );

        let edge_collection = self
            .database
            .edge_collections
            .get(&edge_projection.id)
            .expect("edge collection should exist");

        let entry = self.rels.entry(edge_collection.name.clone()).or_default();

        if !matches!(edge_write_mode, Some(EdgeWriteMode::Delete)) {
            let rel_data = if let Some(def_id) = edge_collection.rel_params {
                let edge_def = self.ontology.def(def_id);
                let profile = self.database.profile();
                let processor = self
                    .ontology
                    .new_serde_processor(
                        edge_def
                            .operator_addr
                            .expect("type should have an operator"),
                        ProcessorMode::RawTreeOnly,
                    )
                    .with_profile(&profile);
                serialize(&rel_params, &processor)
            } else {
                json!({})
            };

            let var_index = match mode {
                WriteMode::Insert => "[0]",
                _ => "",
            };

            // dirty...
            let raw_string = rel_data.to_string();
            let raw_props = raw_string.substring(1, raw_string.as_bytes().len() - 1);
            let raw_spacer = if raw_props.is_empty() { "" } else { ", " };

            let data = match (edge_write_mode, edge_projection.proj()) {
                (Some(EdgeWriteMode::UpdateExisting), _) => raw_string,
                (Some(EdgeWriteMode::Overwrite), (0, 1)) => {
                    format!(r#"{{ _to: {}{}{} }}"#, id, raw_spacer, raw_props)
                }
                (Some(EdgeWriteMode::Overwrite), (1, 0)) => {
                    format!(r#"{{ _from: {}{}{} }}"#, id, raw_spacer, raw_props)
                }
                (Some(EdgeWriteMode::OverwriteInverse), (0, 1)) => {
                    format!(
                        r#"{{ _from: {}{}._id{}{} }}"#,
                        self.var, var_index, raw_spacer, raw_props
                    )
                }
                (Some(EdgeWriteMode::OverwriteInverse), (1, 0)) => {
                    format!(
                        r#"{{ _to: {}{}._id{}{} }}"#,
                        self.var, var_index, raw_spacer, raw_props
                    )
                }
                (_, (0, 1)) => format!(
                    r#"{{ _from: {}{}._id, _to: {}{}{} }}"#,
                    self.var, var_index, id, raw_spacer, raw_props
                ),
                (_, (1, 0)) => format!(
                    r#"{{ _from: {}, _to: {}{}._id{}{} }}"#,
                    id, self.var, var_index, raw_spacer, raw_props
                ),
                _ => {
                    return Err(DomainError::DataStore(anyhow!(
                        "unsupported edge projection"
                    )));
                }
            };

            entry.data.push(data.to_string());
        }

        // TODO: there must be an easier way to find reverse variants of a union
        if matches!(edge_write_mode, Some(EdgeWriteMode::OverwriteInverse))
            && matches!(rel_info.source, DataRelationshipSource::ByUnionProxy)
        {
            let def_id = match rel_info.target {
                DataRelationshipTarget::Unambiguous(def_id) => def_id,
                DataRelationshipTarget::Union { .. } => panic!("shouldn't be possible"),
            };
            for (_, rel_info, _projection) in self.ontology.def(def_id).edge_relationships() {
                match &rel_info.target {
                    DataRelationshipTarget::Unambiguous(_) => {}
                    DataRelationshipTarget::Union(union_def_id) => {
                        // TODO: and check if this is the right union (not necessary, but cleaner)
                        for def_id in self.ontology.union_variants(*union_def_id) {
                            let collection = self
                                .database
                                .collections
                                .get(def_id)
                                .expect("collection should exist");
                            self.with.insert(collection.clone());
                        }
                    }
                }
            }
        }

        let dir = match (edge_write_mode, edge_projection.proj()) {
            (Some(EdgeWriteMode::OverwriteInverse), (0, 1)) => Some(Direction::Inbound),
            (Some(EdgeWriteMode::OverwriteInverse), (1, 0)) => Some(Direction::Outbound),
            (_, (0, 1)) => Some(Direction::Outbound),
            (_, (1, 0)) => Some(Direction::Inbound),
            _ => {
                return Err(DomainError::DataStore(anyhow!(
                    "unsupported edge projection"
                )))
            }
        };

        self.with.insert(edge_collection.name.clone());

        let var_name = var_name.append("_rel").to_var();
        entry.var.clone_from(&var_name);
        entry.mode = edge_write_mode;
        entry.direction = dir;
        entry.filter_key = id
            .substring(1, id.as_bytes().len() - 1)
            .split_once('/')
            .map(|parts| format!(r#""{}""#, parts.1));

        Ok(())
    }

    /// Add a Select to a write MetaQuery
    pub fn write_select(&mut self, select: Select) -> DomainResult<Vec<AqlQuery>> {
        match select {
            Select::EntityId => {
                self.return_var = match self.selection {
                    Some(_) => self.var.clone(),
                    None => Expr::complex(format!("{}[0]", self.var)),
                }
            }
            Select::Leaf => {
                self.return_var = Expr::complex(format!("{{ _key: {}._key }}", self.var));
            }
            Select::Struct(ref struct_select) => {
                let def = self.ontology.def(struct_select.def_id);
                let selection = struct_select.properties.clone();

                self.return_var = match self.selection {
                    Some(_) => self.var.clone(),
                    None => Expr::complex(format!("{}[0]", self.var)),
                };

                for (rel_id, sub_select) in &selection {
                    let rel_info = def
                        .data_relationships
                        .get(rel_id)
                        .expect("property not found in type info");
                    if let DataRelationshipKind::Edge(_) = rel_info.kind {
                        let rel_name = rel_info.name;
                        let var_name = Ident::new(format!(
                            "{}_{}{}",
                            self.var.raw_str(),
                            &self.ontology[rel_name].to_string(),
                            match rel_info.target {
                                DataRelationshipTarget::Unambiguous(def_id) => {
                                    let def = self.ontology.def(def_id);
                                    let def_name = def.name().expect("type should have a name");
                                    format!("_{}", &self.ontology[def_name])
                                }
                                DataRelationshipTarget::Union { .. } => "".to_string(),
                            }
                        ));

                        // find items covered by write operation
                        let mut let_match = false;
                        for ops in [&self.docs, &self.ops] {
                            for op in ops {
                                match op {
                                    Operation::Selection(Selection::Document(var, _)) => {
                                        if var.raw_str() == var_name.raw_str() {
                                            let_match = true;
                                        }
                                    }
                                    Operation::Let(Let { var, .. })
                                    | Operation::LetData(LetData { var, .. }) => {
                                        if var.raw_str() == var_name.raw_str() {
                                            let_match = true;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }

                        if let_match {
                            // all select items covered by write operation
                            // can be merged in RETURN statement
                            let return_many = match rel_info.cardinality {
                                (_, ValueCardinality::Unit) => false,
                                (_, ValueCardinality::IndexSet | ValueCardinality::List) => true,
                            };

                            let return_var: Expr = match (&self.selection, return_many) {
                                (Some(_), true) => Expr::Complex(format!("[{var_name}]")),
                                (None, true) => Expr::Complex(format!("{var_name}[0]")),
                                (_, false) => var_name.clone().to_var(),
                            };

                            self.return_vars
                                .entry(self.ontology[rel_name].to_string())
                                .and_modify(|vars| {
                                    if !vars.contains(&return_var) {
                                        vars.push(return_var.clone());
                                    }
                                })
                                .or_insert(vec![return_var.clone()]);

                            self.write_select(sub_select.clone())?;
                        } else {
                            // select contains items not covered by write operations
                            // generate a secondary read query
                            let query = AqlQuery::build_query(
                                select.clone(),
                                self.ontology,
                                self.database,
                            )?;
                            return Ok(vec![query]);
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(vec![])
    }
}
