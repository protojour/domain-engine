use std::collections::HashMap;

use anyhow::anyhow;
use domain_engine_core::{DomainError, DomainResult};
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
        select: Select,
        ontology: &Ontology,
        database: &ArangoDatabase,
    ) -> DomainResult<AqlQuery> {
        let def = match &select {
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => ontology.def(struct_select.def_id),
                _ => {
                    return Err(DomainError::DataStoreBadRequest(anyhow!(
                        "Query entity union at root level not supported"
                    )))
                }
            },
            Select::Struct(struct_select) => ontology.def(struct_select.def_id),
            _ => return Err(DomainError::EntityMustBeStruct),
        };
        let def_name = def.name().expect("entity should have a name");
        debug!("AqlQuery::build_query for {}", &ontology[def_name]);

        let mut meta = MetaQuery::from("obj", ontology, database);
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
    pub fn query_select(&mut self, select: Select) -> DomainResult<()> {
        match select {
            Select::EntityId => {
                self.return_var = format!("{}[0]", self.var);
            }
            Select::Leaf => {
                self.return_var = format!("{{ _key: {}._key }}", self.var);
            }
            Select::Struct(struct_select) => {
                let def = self.ontology.def(struct_select.def_id);
                let collection = self
                    .database
                    .collections
                    .get(&struct_select.def_id)
                    .expect("collection should exist");

                self.with.insert(collection.to_string());
                self.selection = Some(Selection::Loop(For {
                    var: self.var.to_string(),
                    object: collection.to_string(),
                    ..Default::default()
                }));

                for (rel_id, select) in struct_select.properties {
                    if let Some(rel_info) = def.data_relationships.get(&rel_id) {
                        if let DataRelationshipKind::Edge(_) = rel_info.kind {
                            self.query_relation(select, rel_info, &struct_select.def_id)?;
                        }
                    }
                }
            }
            Select::Entity(entity_select) => {
                match &entity_select.source {
                    StructOrUnionSelect::Struct(struct_select) => {
                        self.query_select(Select::Struct(struct_select.clone()))?;
                    }
                    StructOrUnionSelect::Union(_, variants) => {
                        for struct_select in variants {
                            let def = self.ontology.def(struct_select.def_id);
                            for (rel_id, select) in &struct_select.properties {
                                if let Some(rel_info) = def.data_relationships.get(rel_id) {
                                    if let DataRelationshipKind::Edge(_) = rel_info.kind {
                                        self.query_relation(
                                            select.clone(),
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
                    .map_err(|_| DomainError::DataStore(anyhow!("Invalid cursor format")))?;

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
                    for (rel_id, select) in struct_select.properties {
                        if let Some(rel_info) = def.data_relationships.get(&rel_id) {
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
        select: Select,
        rel_info: &DataRelationshipInfo,
        parent_id: &DefId,
    ) -> DomainResult<()> {
        let DataRelationshipKind::Edge(edge_projection) = rel_info.kind else {
            panic!();
        };

        let sub_var = format!("sub_{}", self.var);
        let sub_var_edge = format!("{sub_var}_edge");
        let mut sub_meta = MetaQuery::from(&sub_var, self.ontology, self.database);
        sub_meta.query_select(select)?;

        let rel_name = rel_info.name;
        let var_name = format!("{}_{}", self.var, &self.ontology[rel_name]);

        let return_many = match rel_info.cardinality {
            (_, ValueCardinality::Unit) => false,
            (_, ValueCardinality::IndexSet | ValueCardinality::List) => true,
        };

        let return_var = format!("{var_name}{}", if return_many { "" } else { "[0]" });
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
        let rel_props_map = format!(
            r#"{{ {} }}"#,
            rel_props
                .iter()
                .map(|prop| format!("{prop}: {sub_var_edge}.{prop}"))
                .collect::<Vec<String>>()
                .join(", ")
        );
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
        self.with.insert(edge_collection.name.to_string());
        self.with.insert(collection.to_string());

        match &rel_info.target {
            DataRelationshipTarget::Unambiguous(_) => {}
            DataRelationshipTarget::Union(union_def_id) => {
                for def_id in self.ontology.union_variants(*union_def_id) {
                    let collection = self
                        .database
                        .collections
                        .get(def_id)
                        .expect("collection should exist");
                    self.with.insert(collection.to_string());
                }
            }
        }

        self.ops.push(Operation::Let(Let {
            var: var_name.clone(),
            query: Query {
                selection: Some(Selection::Loop(For {
                    var: format!("{sub_var}, {sub_var_edge}"),
                    direction: match edge_projection.proj() {
                        (0, 1) => Some(Direction::Outbound),
                        (1, 0) => Some(Direction::Inbound),
                        proj => {
                            return Err(DomainError::DataStore(anyhow!(
                                "unsupported edge projection {proj:?}"
                            )))
                        }
                    },
                    object: self.var.to_string(),
                    edges: Some(vec![edge_collection.name.to_string()]),
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
pub fn apply_select(attr: AttrMut, select: &mut Select, ontology: &Ontology) -> DomainResult<()> {
    // debug!("attr {attr:#?} select {select:#?}");

    match (attr, select) {
        (AttrMut::Unit(Value::Sequence(seq, _)), select) => {
            for value in seq.elements_mut() {
                apply_select(AttrMut::Unit(value), select, ontology)?;
            }
        }
        (AttrMut::Unit(val), Select::EntityId | Select::Leaf) => {
            let def_id = val.type_def_id();
            if let Value::Struct(ref mut attr_map, _) = val {
                let def = ontology.def(def_id);
                let entity_info = def.entity().ok_or(DomainError::NotAnEntity(def_id))?;
                let id_rel_id = entity_info.id_relationship_id;
                *val = attr_map
                    .get(&id_rel_id)
                    .expect("entity should have an id")
                    .clone()
                    .into_unit()
                    .unwrap();
            }
        }
        (AttrMut::Unit(Value::Struct(attr_map, _)), Select::Struct(struct_select)) => {
            let def = ontology.def(struct_select.def_id);
            let selection = struct_select.properties.clone();

            for (rel_id, select) in selection {
                let rel_info = def
                    .data_relationships
                    .get(&rel_id)
                    .expect("property not found in type info");
                if let DataRelationshipKind::Edge(_) = rel_info.kind {
                    let def_id = match &rel_info.target {
                        DataRelationshipTarget::Unambiguous(def_id) => *def_id,
                        DataRelationshipTarget::Union { .. } => match attr_map.get(&rel_id) {
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
                            match selects.iter().any(|s| s.def_id == def_id) {
                                true => {
                                    if let Some(attr) = attr_map.get_mut(&rel_id) {
                                        apply_select(
                                            AttrMut::from_attr(attr),
                                            &mut select.clone(),
                                            ontology,
                                        )?;
                                    }
                                }
                                false => {
                                    if let Some(attr) = attr_map.get_mut(&rel_id) {
                                        if let Some(def) = ontology
                                            .find_domain(def_id.package_id())
                                            .unwrap()
                                            .defs()
                                            .find(|def| {
                                                if let Some(entity) = def.entity() {
                                                    entity.id_value_def_id == def_id
                                                } else {
                                                    false
                                                }
                                            })
                                        {
                                            match attr {
                                                Attr::Unit(val) => {
                                                    *val = Value::new_struct(
                                                        HashMap::new(),
                                                        def.id.into(),
                                                    );
                                                }
                                                _ => todo!("other"),
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            if let Some(attr) = attr_map.get_mut(&rel_id) {
                                apply_select(
                                    AttrMut::from_attr(attr),
                                    &mut select.clone(),
                                    ontology,
                                )?;
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
                    &mut Select::Struct(struct_select.clone()),
                    ontology,
                )?;
            }
            StructOrUnionSelect::Union(_, selects) => {
                let struct_select = selects
                    .iter()
                    .find(|struct_select| struct_select.def_id == val.type_def_id())
                    .expect("union variant not found");
                apply_select(
                    AttrMut::Unit(val),
                    &mut Select::Struct(struct_select.clone()),
                    ontology,
                )?;
            }
        },
        (AttrMut::Unit(value), Select::StructUnion(_, selects)) => {
            for struct_select in selects.iter() {
                apply_select(
                    AttrMut::Unit(value),
                    &mut Select::Struct(struct_select.clone()),
                    ontology,
                )?;
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

    Ok(())
}
