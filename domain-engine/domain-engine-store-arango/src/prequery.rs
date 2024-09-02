use domain_engine_core::{domain_error::DomainErrorKind, DomainResult};

use ontol_runtime::{
    attr::AttrRef,
    interface::serde::{operator::SerdeOperatorAddr, processor::ProcessorMode},
    ontology::{
        domain::{DataRelationshipInfo, DataRelationshipKind, DataRelationshipTarget},
        Ontology,
    },
    value::Value,
};
use tracing::debug;

use super::{aql::*, data_store::serialize, AqlQuery, ArangoDatabase};

impl AqlQuery {
    /// Build prequery AqlQuery for inherent ids
    pub fn prequery_from_entity(
        entity: &Value,
        ontology: &Ontology,
        database: &ArangoDatabase,
    ) -> DomainResult<AqlQuery> {
        let def = ontology.def(entity.type_def_id());
        let def_name = def.ident().expect("entity should have an identifier");

        debug!("AqlQuery::prequery_from_entity {}", &ontology[def_name]);

        let mut meta =
            MetaQuery::from(Ident::new(&ontology[def_name]).to_var(), ontology, database);

        match &entity {
            Value::Struct(struct_map, _) => {
                for (prop_id, attr) in struct_map.iter() {
                    if let Some(rel_info) = def.data_relationships.get(prop_id) {
                        match rel_info.kind {
                            DataRelationshipKind::Tree | DataRelationshipKind::Id => {}
                            DataRelationshipKind::Edge(_) => {
                                meta.prequery_relation(attr.as_ref(), rel_info)?;
                            }
                        }
                    }
                }
            }
            _ => return Err(DomainErrorKind::EntityMustBeStruct.into_error()),
        };

        let query = AqlQuery {
            query: Query {
                with: (!meta.with.is_empty()).then_some(meta.with.into_iter().collect()),
                operations: (!meta.ops.is_empty()).then_some(meta.ops.clone()),
                returns: Return {
                    var: Expr::complex(format!("{{ {} }}", meta.return_vars)),
                    ..Default::default()
                },
                ..Default::default()
            },
            bind_vars: meta.bind_vars,
            operator_addr: SerdeOperatorAddr(0),
        };

        Ok(query)
    }
}

impl<'a> MetaQuery<'a> {
    /// Recursively find inherent ids in relations and add to MetaQuery
    pub fn prequery_relation(
        &mut self,
        attr: AttrRef,
        rel_info: &DataRelationshipInfo,
    ) -> DomainResult<()> {
        match attr {
            AttrRef::Unit(value) => {
                let target_def = {
                    let rel_target_def_id = match rel_info.target {
                        DataRelationshipTarget::Unambiguous(def_id) => def_id,
                        DataRelationshipTarget::Union(def_id) => def_id,
                    };

                    self.ontology.def(rel_target_def_id)
                };

                let runtime_def = self.ontology.def(value.type_def_id());

                if target_def
                    .entity()
                    .is_some_and(|entity_info| entity_info.is_self_identifying)
                {
                    return Ok(());
                }

                match value {
                    Value::Struct(struct_map, _tag) => {
                        for (prop_id, attr) in struct_map.iter() {
                            if let Some(rel_info) = runtime_def.data_relationships.get(prop_id) {
                                if let DataRelationshipKind::Edge(_) = rel_info.kind {
                                    self.prequery_relation(attr.as_ref(), rel_info)?;
                                }
                            }
                        }
                    }
                    Value::Sequence(sequence, _) => match &rel_info.target {
                        DataRelationshipTarget::Unambiguous(_) => {
                            for value in sequence.elements() {
                                self.prequery_relation(AttrRef::Unit(value), rel_info)?;
                            }
                        }
                        DataRelationshipTarget::Union { .. } => {
                            for value in sequence.elements() {
                                let def_id = value.type_def_id();
                                let mut rel_info = rel_info.clone();
                                rel_info.target = DataRelationshipTarget::Unambiguous(def_id);
                                self.prequery_relation(AttrRef::Unit(value), &rel_info)?;
                            }
                        }
                    },
                    Value::Filter(ref filter, tag) => {
                        self.add_filter(tag.def_id(), filter)?;
                    }
                    _ => {
                        let collection = match &rel_info.target {
                            DataRelationshipTarget::Unambiguous(def_id) => self
                                .database
                                .collections
                                .get(def_id)
                                .expect("collection should exist"),
                            DataRelationshipTarget::Union { .. } => {
                                panic!("target should be unambiguous at this point")
                            }
                        };
                        let operator_addr = runtime_def
                            .operator_addr
                            .expect("type should have an operator");
                        let profile = self.database.profile();
                        let processor = self
                            .ontology
                            .new_serde_processor(operator_addr, ProcessorMode::Raw)
                            .with_profile(&profile);

                        let data = serialize(value, &processor);

                        let key_quoted = data.to_string();
                        let key = data
                            .as_str()
                            .expect("entity id should serialize to str")
                            .to_owned();
                        let id = collection.format_id(&key);

                        self.with.insert(collection.clone());

                        // ignore if duplicate
                        for op in &self.ops {
                            if let Operation::Selection(Selection::Document(_, doc_id)) = op {
                                if doc_id == &id {
                                    return Ok(());
                                }
                            }
                        }

                        let rel_name = rel_info.name;
                        let var_name = Ident::new(format!(
                            "{}_{}",
                            self.var.raw_str(),
                            &self.ontology[rel_name]
                        ))
                        .to_var();

                        self.ops.push(Operation::Selection(Selection::Document(
                            var_name.clone(),
                            id,
                        )));

                        self.return_vars
                            .entry(key_quoted)
                            .and_modify(|vars| vars.push(var_name.clone()))
                            .or_insert(vec![var_name.clone()]);
                    }
                }
            }
            AttrRef::Matrix(matrix) => {
                let mut tuple = Default::default();
                let mut rows = matrix.rows();

                while rows.iter_next(&mut tuple) {
                    let value = tuple.elements[0];

                    match &rel_info.target {
                        DataRelationshipTarget::Unambiguous(_) => {
                            self.prequery_relation(AttrRef::Unit(value), rel_info)?;
                        }
                        DataRelationshipTarget::Union { .. } => {
                            let def_id = value.type_def_id();
                            let mut rel_info = rel_info.clone();
                            rel_info.target = DataRelationshipTarget::Unambiguous(def_id);
                            self.prequery_relation(AttrRef::Unit(value), &rel_info)?;
                        }
                    }
                }
            }
            attr_ref => todo!("{attr_ref:?}"),
        }

        Ok(())
    }
}
