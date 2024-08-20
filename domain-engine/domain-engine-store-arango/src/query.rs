use std::collections::HashMap;

use domain_engine_core::{domain_error::DomainErrorKind, DomainError, DomainResult};
use ontol_runtime::{
    attr::Attr,
    ontology::{
        domain::{DataRelationshipInfo, DataRelationshipKind, DataRelationshipTarget},
        Ontology,
    },
    property::ValueCardinality,
    query::select::{Select, StructOrUnionSelect},
    sequence::Sequence,
    value::Value,
    DefId,
};
use tracing::debug;

use super::{aql::*, data_store::Cursor, AqlQuery, ArangoDatabase};

impl AqlQuery {
    /// Build an AqlQuery from a Select
    pub fn build_query(
        select: &Select,
        ontology: &Ontology,
        database: &ArangoDatabase,
    ) -> DomainResult<AqlQuery> {
        let def = match &select {
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => ontology.def(struct_select.def_id),
                _ => {
                    return Err(DomainError::data_store_bad_request(
                        "Query entity union at root level not supported",
                    ))
                }
            },
            Select::Struct(struct_select) => ontology.def(struct_select.def_id),
            _ => return Err(DomainErrorKind::EntityMustBeStruct.into_error()),
        };
        let def_name = def.name().expect("entity should have a name");
        debug!("AqlQuery::build_query for {}", &ontology[def_name]);

        let mut meta = MetaQuery::from("obj".into(), ontology, database);
        meta.query_select(select)?;

        let query = AqlQuery {
            query: Query {
                with: (!meta.with.is_empty()).then_some(meta.with.into_iter().collect()),
                selection: meta.selection,
                operations: (!meta.ops.is_empty()).then_some(meta.ops),
                returns: Return {
                    var: meta.return_var,
                    merge: meta.return_vars.to_string_maybe(),
                    ..Default::default()
                },
                ..Default::default()
            },
            bind_vars: HashMap::new(),
            operator_addr: def.operator_addr.expect("type should have an operator"),
        };

        Ok(query)
    }
}

impl<'a> MetaQuery<'a> {
    /// Add a Select to MetaQuery
    pub fn query_select(&mut self, select: &Select) -> DomainResult<()> {
        match select {
            Select::Unit => {}
            Select::EntityId => {
                self.return_var = Expr::complex(format!("{}[0]", self.var));
            }
            Select::Leaf => {
                self.return_var = Expr::complex(format!("{{ _key: {}._key }}", self.var));
            }
            Select::Struct(struct_select) => {
                let def = self.ontology.def(struct_select.def_id);
                let collection = self
                    .database
                    .collections
                    .get(&struct_select.def_id)
                    .expect("collection should exist");

                self.with.insert(collection.clone());
                self.selection = Some(Selection::Loop(For {
                    var: self.var.clone(),
                    object: collection.clone().to_var(),
                    ..Default::default()
                }));

                for (prop_id, select) in &struct_select.properties {
                    if let Some(rel_info) = def.data_relationships.get(prop_id) {
                        if let DataRelationshipKind::Edge(_) = rel_info.kind {
                            self.query_relation(select, rel_info, &struct_select.def_id)?;
                        }
                    }
                }
            }
            Select::Entity(entity_select) => {
                match &entity_select.source {
                    StructOrUnionSelect::Struct(struct_select) => {
                        self.query_select(&Select::Struct(struct_select.clone()))?;
                    }
                    StructOrUnionSelect::Union(_, variants) => {
                        for struct_select in variants {
                            let def = self.ontology.def(struct_select.def_id);
                            for (prop_id, select) in &struct_select.properties {
                                if let Some(rel_info) = def.data_relationships.get(prop_id) {
                                    if let DataRelationshipKind::Edge(_) = rel_info.kind {
                                        self.query_relation(
                                            select,
                                            rel_info,
                                            &struct_select.def_id,
                                        )?;
                                    }
                                }
                            }
                        }
                    }
                };

                self.add_filter(entity_select.source.def_id(), &entity_select.filter)?;

                let cursor: Option<Cursor> = entity_select
                    .after_cursor
                    .as_deref()
                    .map(bincode::deserialize)
                    .transpose()
                    .map_err(|_| DomainError::data_store("invalid cursor format"))?;

                self.ops.push(Operation::Limit(Limit {
                    skip: match cursor {
                        Some(cursor) => cursor.offset,
                        None => 0,
                    },
                    limit: entity_select.limit,
                }));
            }
            Select::StructUnion(_, selects) => {
                for struct_select in selects {
                    let def = self.ontology.def(struct_select.def_id);
                    for (prop_id, select) in &struct_select.properties {
                        if let Some(rel_info) = def.data_relationships.get(prop_id) {
                            if let DataRelationshipKind::Edge(_) = rel_info.kind {
                                self.query_relation(select, rel_info, &struct_select.def_id)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Add query for relation through edge to MetaQuery
    pub fn query_relation(
        &mut self,
        select: &Select,
        rel_info: &DataRelationshipInfo,
        parent_id: &DefId,
    ) -> DomainResult<()> {
        let DataRelationshipKind::Edge(edge_projection) = rel_info.kind else {
            panic!();
        };

        let sub_var = Ident::new(format!("sub_{}", self.var.raw_str())).to_var();
        let sub_var_edge = Ident::new(format!("{}_edge", sub_var.raw_str())).to_var();
        let mut sub_meta = MetaQuery::from(sub_var.clone(), self.ontology, self.database);
        sub_meta.query_select(select)?;

        let rel_name = rel_info.name;
        let var_name = Ident::new(format!(
            "{}_{}",
            self.var.raw_str(),
            &self.ontology[rel_name]
        ));

        let return_many = match rel_info.cardinality {
            (_, ValueCardinality::Unit) => false,
            (_, ValueCardinality::IndexSet | ValueCardinality::List) => true,
        };

        let return_var = if return_many {
            var_name.clone().to_var()
        } else {
            Expr::complex(format!("{var_name}[0]"))
        };
        self.return_vars
            .entry(self.ontology[rel_name].to_string())
            .and_modify(|vars| vars.push(return_var.clone()))
            .or_insert(vec![return_var.clone()]);

        let def_id = match &rel_info.target {
            DataRelationshipTarget::Unambiguous(def_id) => def_id,
            DataRelationshipTarget::Union { .. } => parent_id,
        };

        let target_def = self.ontology.def(*def_id);

        let mut rel_props = vec![];
        for rel_info in target_def.data_relationships.values() {
            if let DataRelationshipKind::Edge(projection) = rel_info.kind {
                let edge = self.database.edge_collections.get(&projection.id).unwrap();

                if let Some(def_id) = edge.rel_params {
                    let rel_def = self.ontology.def(def_id);
                    for rel_info in rel_def.data_relationships.values() {
                        rel_props.push(self.ontology[rel_info.name].to_string());
                    }
                }
            }
        }
        let rel_props_map = Expr::complex(format!(
            r#"{{ {} }}"#,
            rel_props
                .iter()
                .map(|prop| format!("{prop}: {sub_var_edge}.{prop}"))
                .collect::<Vec<String>>()
                .join(", ")
        ));
        if !rel_props.is_empty() {
            sub_meta
                .return_vars
                .entry("_edge".to_string())
                .and_modify(|vars| vars.push(rel_props_map.clone()))
                .or_insert(vec![rel_props_map.clone()]);
        }

        let collection = self
            .database
            .collections
            .get(def_id)
            .expect("collection should exist");
        let edge_collection = self
            .database
            .edge_collections
            .get(&edge_projection.id)
            .expect("collection should exist");

        self.with.extend(sub_meta.with);
        self.with.insert(edge_collection.name.clone());
        self.with.insert(collection.clone());

        match &rel_info.target {
            DataRelationshipTarget::Unambiguous(_) => {}
            DataRelationshipTarget::Union(union_def_id) => {
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

        self.ops.push(Operation::Let(Let {
            var: var_name.to_var(),
            query: Query {
                selection: Some(Selection::Loop(For {
                    var: Expr::complex(format!("{sub_var}, {sub_var_edge}")),
                    direction: match edge_projection.proj() {
                        (0, 1) => Some(Direction::Outbound),
                        (1, 0) => Some(Direction::Inbound),
                        proj => {
                            return Err(DomainError::data_store(format!(
                                "unsupported edge projection {proj:?}"
                            )))
                        }
                    },
                    object: self.var.clone(),
                    edges: Some(vec![edge_collection.name.clone()]),
                    ..Default::default()
                })),
                operations: Some(sub_meta.ops.clone()),
                returns: Return {
                    var: sub_meta.return_var,
                    merge: sub_meta.return_vars.to_string_maybe(),
                    pre_merge: match sub_meta.ops.first() {
                        Some(Operation::Let(Let { var, .. })) => Some(format!("({var} > []) ?")),
                        _ => None,
                    },
                    post_merge: match sub_meta.ops.first() {
                        Some(Operation::Let(Let { .. })) => Some(": {}".to_string()),
                        _ => None,
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
        }));
        Ok(())
    }
}

#[derive(Debug)]
pub enum AttrMut<'a> {
    Unit(&'a mut Value),
    #[allow(unused)]
    Tuple(&'a mut [Value]),
    Matrix(&'a mut [Sequence<Value>]),
}

impl<'a> AttrMut<'a> {
    pub fn from_attr(attr: &'a mut Attr) -> Self {
        match attr {
            Attr::Unit(value) => Self::Unit(value),
            Attr::Tuple(tuple) => Self::Tuple(&mut tuple.elements),
            Attr::Matrix(matrix) => Self::Matrix(&mut matrix.columns),
        }
    }
}

/// Apply Select to deserialized data from Arango
/// a false return value means the attribute should be deleted
pub fn apply_select(attr: AttrMut, select: &Select, ontology: &Ontology) -> DomainResult<bool> {
    // debug!("attr {attr:#?} select {select:#?}");

    match (attr, select) {
        (AttrMut::Unit(Value::Sequence(seq, _)), select) => {
            for value in seq.elements_mut() {
                if !apply_select(AttrMut::Unit(value), select, ontology)? {
                    *value = Value::Void(DefId::unit().into());
                }
            }
        }
        (AttrMut::Unit(val), Select::EntityId | Select::Leaf) => {
            let def_id = val.type_def_id();
            if let Value::Struct(ref mut attr_map, _) = val {
                let def = ontology.def(def_id);
                let entity_info = def
                    .entity()
                    .ok_or(DomainErrorKind::NotAnEntity(def_id).into_error())?;
                let id_prop_id = entity_info.id_prop;
                *val = attr_map
                    .get(&id_prop_id)
                    .expect("entity should have an id")
                    .clone()
                    .into_unit()
                    .unwrap();
            }
        }
        (AttrMut::Unit(Value::Struct(attr_map, _)), Select::Struct(struct_select)) => {
            let def = ontology.def(struct_select.def_id);

            for (prop_id, select) in &struct_select.properties {
                let rel_info = def
                    .data_relationships
                    .get(prop_id)
                    .expect("property not found in type info");
                if let DataRelationshipKind::Edge(_) = rel_info.kind {
                    let def_id = match &rel_info.target {
                        DataRelationshipTarget::Unambiguous(def_id) => *def_id,
                        DataRelationshipTarget::Union { .. } => match attr_map.get(prop_id) {
                            Some(Attr::Unit(val)) => val.type_def_id(),
                            Some(Attr::Tuple(tuple)) => tuple.elements[0].type_def_id(),
                            Some(Attr::Matrix(matrix)) => matrix
                                .get_ref(0, 0)
                                .map(|val| val.as_unit().unwrap().type_def_id())
                                .unwrap_or(DefId::unit()),
                            None => DefId::unit(),
                        },
                    };
                    match &select {
                        Select::StructUnion(_, ref selects) => {
                            if selects.iter().any(|s| s.def_id == def_id) {
                                if let Some(attr) = attr_map.get_mut(prop_id) {
                                    if !apply_select(AttrMut::from_attr(attr), select, ontology)? {
                                        attr_map.remove(prop_id);
                                    }
                                }
                            } else {
                                attr_map.remove(prop_id);
                            }
                        }
                        _ => {
                            if let Some(attr) = attr_map.get_mut(prop_id) {
                                if !apply_select(AttrMut::from_attr(attr), select, ontology)? {
                                    attr_map.remove(prop_id);
                                }
                            }
                        }
                    }
                }
            }
        }
        (AttrMut::Unit(val), Select::Entity(entity_select)) => match &entity_select.source {
            StructOrUnionSelect::Struct(struct_select) => {
                apply_select(
                    AttrMut::Unit(val),
                    &Select::Struct(struct_select.clone()),
                    ontology,
                )?;
            }
            StructOrUnionSelect::Union(_, selects) => {
                if let Some(struct_select) = selects
                    .iter()
                    .find(|struct_select| struct_select.def_id == val.type_def_id())
                {
                    apply_select(
                        AttrMut::Unit(val),
                        &Select::Struct(struct_select.clone()),
                        ontology,
                    )?;
                } else {
                    return Ok(false);
                }
            }
        },
        (AttrMut::Unit(value), Select::StructUnion(_, selects)) => {
            if selects.iter().any(|sel| sel.def_id == value.type_def_id()) {
                for struct_select in selects.iter() {
                    apply_select(
                        AttrMut::Unit(value),
                        &Select::Struct(struct_select.clone()),
                        ontology,
                    )?;
                }
            } else {
                return Ok(false);
            }
        }
        (AttrMut::Matrix(columns), select) => {
            if !columns.is_empty() {
                for value in columns[0].elements_mut() {
                    apply_select(AttrMut::Unit(value), select, ontology)?;
                }
            }
        }
        (a, b) => todo!("{a:?} {b:?}"),
    }

    Ok(true)
}
