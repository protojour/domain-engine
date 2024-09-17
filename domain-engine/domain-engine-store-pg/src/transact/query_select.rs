use std::collections::{BTreeMap, BTreeSet};

use domain_engine_core::DomainResult;
use ontol_runtime::{
    ontology::domain::{DataRelationshipKind, Def},
    query::select::{Select, StructOrUnionSelect, StructSelect},
    tuple::CardinalIdx,
    DefId, PropId,
};
use smallvec::{smallvec, SmallVec};

use crate::pg_model::{EdgeId, PgEdgeCardinalKind, PgTable, PgTableKey};

use super::TransactCtx;

#[derive(Debug)]
pub enum QuerySelect {
    Unit,
    Vertex(VertexSelect),
    VertexUnion(Vec<VertexSelect>),
    VertexAddress,
    Field(PropId),
}

impl QuerySelect {
    pub fn as_ref(&self) -> QuerySelectRef<'_> {
        match self {
            Self::Unit => QuerySelectRef::Unit,
            Self::Vertex(vertex_select) => {
                QuerySelectRef::VertexUnion(std::slice::from_ref(vertex_select))
            }
            Self::VertexUnion(vertex_selects) => QuerySelectRef::VertexUnion(vertex_selects),
            Self::VertexAddress => QuerySelectRef::VertexAddress,
            Self::Field(prop_id) => QuerySelectRef::Field(*prop_id),
        }
    }
}

#[derive(Clone, Copy)]
pub enum QuerySelectRef<'a> {
    Unit,
    VertexUnion(&'a [VertexSelect]),
    VertexAddress,
    Field(PropId),
}

#[derive(Debug)]
pub struct VertexSelect {
    pub def_id: DefId,
    pub inherent_set: BTreeSet<PropId>,
    pub abstract_set: BTreeMap<PropId, VertexSelect>,
    pub edge_set: BTreeMap<PropId, SmallVec<CardinalSelect, 2>>,
}

#[derive(Debug)]
pub struct CardinalSelect {
    pub cardinal_idx: CardinalIdx,
    pub select: QuerySelect,
}

impl<'a> TransactCtx<'a> {
    fn analyze_query_select(&self, select: &Select) -> DomainResult<QuerySelect> {
        match select {
            Select::Struct(struct_select) => Ok(QuerySelect::Vertex(
                self.analyze_struct_select(struct_select)?,
            )),
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => Ok(QuerySelect::Vertex(
                    self.analyze_struct_select(struct_select)?,
                )),
                StructOrUnionSelect::Union(_, struct_selects) => {
                    let mut vertex_selects = Vec::with_capacity(struct_selects.len());

                    for struct_select in struct_selects {
                        let vertex_select = self.analyze_struct_select(struct_select)?;
                        vertex_selects.push(vertex_select);
                    }

                    Ok(QuerySelect::VertexUnion(vertex_selects))
                }
            },
            Select::StructUnion(_, struct_selects) => {
                let mut vertex_selects = Vec::with_capacity(struct_selects.len());

                for struct_select in struct_selects {
                    let vertex_select = self.analyze_struct_select(struct_select)?;
                    vertex_selects.push(vertex_select);
                }

                Ok(QuerySelect::VertexUnion(vertex_selects))
            }
            Select::Unit => Ok(QuerySelect::Unit),
            Select::VertexAddress => Ok(QuerySelect::VertexAddress),
            sel => todo!("other: {sel:?}"),
        }
    }

    fn analyze_struct_select(&self, struct_select: &StructSelect) -> DomainResult<VertexSelect> {
        let domain_index = struct_select.def_id.domain_index();
        let def = self.ontology.def(struct_select.def_id);
        let pg_datatable = self
            .pg_model
            .datatable(domain_index, struct_select.def_id)?;

        let vertex_select =
            self.analyze_vertex_select_properties(def, pg_datatable, &struct_select.properties)?;

        Ok(vertex_select)
    }

    pub fn analyze_vertex_select(
        &self,
        def: &Def,
        pg_datatable: &'a PgTable,
        select: &Select,
    ) -> DomainResult<VertexSelect> {
        match select {
            Select::Struct(struct_select) => {
                self.analyze_vertex_select_properties(def, pg_datatable, &struct_select.properties)
            }
            Select::Unit | Select::EntityId => {
                let mut output = VertexSelect {
                    def_id: def.id,
                    inherent_set: Default::default(),
                    abstract_set: Default::default(),
                    edge_set: Default::default(),
                };

                let Some(entity) = def.entity() else {
                    return Ok(output);
                };

                output.inherent_set.insert(entity.id_prop);

                Ok(output)
            }
            sel => todo!("other: {sel:?}"),
        }
    }

    pub fn analyze_vertex_select_properties(
        &self,
        def: &Def,
        pg_datatable: &'a PgTable,
        properties: &BTreeMap<PropId, Select>,
    ) -> DomainResult<VertexSelect> {
        let mut output = VertexSelect {
            def_id: def.id,
            inherent_set: Default::default(),
            abstract_set: Default::default(),
            edge_set: Default::default(),
        };

        for (prop_id, sub_sel) in properties {
            if let Some(rel_info) = def.data_relationships.get(prop_id) {
                match &rel_info.kind {
                    DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                        if let Some(reg_key) = pg_datatable.find_abstract_property(prop_id) {
                            let PgTableKey::Data {
                                domain_index,
                                def_id,
                            } = self.pg_model.reg_key_to_table_key.get(&reg_key).unwrap();
                            let pg = self.pg_model.datatable(*domain_index, *def_id)?;
                            let def = self.ontology.def(*def_id);

                            output
                                .abstract_set
                                .insert(*prop_id, self.analyze_vertex_select(def, pg, sub_sel)?);
                        } else {
                            output.inherent_set.insert(*prop_id);
                        }
                    }
                    DataRelationshipKind::Edge(proj) => {
                        let mut cardinal_selects = smallvec![];
                        let edge_info = self.ontology.find_edge(proj.edge_id).unwrap();
                        let pg_edge = self.pg_model.pg_domain_edgetable(&EdgeId(proj.edge_id))?;

                        let mut has_vertex_cardinal = false;

                        for (cardinal_idx, _cardinal) in edge_info.cardinals.iter().enumerate() {
                            let Ok(cardinal_idx) = cardinal_idx.try_into() else {
                                continue;
                            };
                            let cardinal_idx = CardinalIdx(cardinal_idx);

                            let pg_cardinal = pg_edge.table.edge_cardinal(cardinal_idx)?;

                            match &pg_cardinal.kind {
                                PgEdgeCardinalKind::Dynamic { .. }
                                | PgEdgeCardinalKind::PinnedDef { .. } => {
                                    if cardinal_idx == proj.subject || cardinal_idx != proj.object {
                                        continue;
                                    }

                                    if !has_vertex_cardinal {
                                        cardinal_selects.push(CardinalSelect {
                                            cardinal_idx,
                                            select: self.analyze_query_select(sub_sel)?,
                                        });
                                        has_vertex_cardinal = true;
                                    }
                                }
                                PgEdgeCardinalKind::Parameters(def_id) => {
                                    let def = self.ontology.def(*def_id);
                                    let mut inherent_set = BTreeSet::new();

                                    // "infer" parameter selection:
                                    for prop_id in def.data_relationships.keys() {
                                        inherent_set.insert(*prop_id);
                                    }

                                    cardinal_selects.push(CardinalSelect {
                                        cardinal_idx,
                                        select: QuerySelect::Vertex(VertexSelect {
                                            def_id: *def_id,
                                            inherent_set,
                                            abstract_set: Default::default(),
                                            edge_set: Default::default(),
                                        }),
                                    });
                                }
                            }
                        }

                        output.edge_set.insert(*prop_id, cardinal_selects);
                    }
                }
            }
        }

        Ok(output)
    }
}
