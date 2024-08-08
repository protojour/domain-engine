use std::collections::BTreeMap;

use domain_engine_core::{domain_error::DomainErrorKind, DomainError, DomainResult};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipInfo, DataRelationshipKind, Def},
    tuple::CardinalIdx,
    value::Value,
    EdgeId, RelId,
};
use tracing::debug;

use crate::{ds_err, pg_model::InDomain};

use super::{
    data::{Data, ScalarAttrs},
    TransactCtx,
};

pub struct AnalyzedStruct<'m, 'b> {
    pub root_attrs: ScalarAttrs<'m, 'b>,
    pub edge_projections: BTreeMap<EdgeId, EdgeProjection>,
}

pub struct EdgeProjection {
    #[allow(unused)]
    pub subject: CardinalIdx,
    pub tuples: Vec<EdgeEndoTuple>,
}

pub struct EdgeEndoTuple {
    /// The length of the vector is the edge cardinality - 1
    elements: Vec<(CardinalIdx, Value)>,
}

impl EdgeEndoTuple {
    pub fn into_elements(self) -> impl Iterator<Item = Value> {
        self.elements.into_iter().map(|(_, value)| value)
    }

    fn from_tuple(it: impl Iterator<Item = Value>) -> Self {
        Self {
            elements: it
                .enumerate()
                .map(|(idx, value)| (CardinalIdx(idx as u8), value))
                .collect(),
        }
    }

    /// build the tuple one-by-one
    fn insert_element(&mut self, idx: CardinalIdx, value: Value) -> DomainResult<()> {
        match self.elements.binary_search_by_key(&idx, |(idx, _)| *idx) {
            Ok(pos) => Err(ds_err(format!("duplicate edge cardinal at {pos}"))),
            Err(pos) => {
                self.elements.insert(pos, (idx, value));
                Ok(())
            }
        }
    }
}

impl<'a> TransactCtx<'a> {
    pub(super) fn analyze_struct(
        &self,
        value: InDomain<Value>,
        def: &Def,
    ) -> DomainResult<AnalyzedStruct> {
        let datatable = self.pg_model.datatable(value.pkg_id, value.type_def_id())?;

        let Value::Struct(attrs, _struct_tag) = value.value else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        let mut root_attrs = ScalarAttrs {
            map: Default::default(),
            datatable,
        };

        let mut edge_projections: BTreeMap<EdgeId, EdgeProjection> = Default::default();

        for (rel_id, attr) in *attrs {
            let data_relationship = find_data_relationship(def, &rel_id)?;

            match (
                data_relationship.kind,
                attr,
                data_relationship.cardinality.1,
            ) {
                (DataRelationshipKind::Id | DataRelationshipKind::Tree, Attr::Unit(value), _) => {
                    match self.data_from_value(value)? {
                        Data::Sql(scalar) => {
                            root_attrs.map.insert(rel_id, scalar);
                        }
                        Data::Compound(comp) => {
                            todo!("compound: {comp:?}");
                        }
                    }
                }
                (DataRelationshipKind::Edge(proj), attr, _) => {
                    let projection =
                        edge_projections
                            .entry(proj.id)
                            .or_insert_with(|| EdgeProjection {
                                subject: proj.subject,
                                tuples: vec![],
                            });

                    match attr {
                        Attr::Unit(value) => {
                            if projection.tuples.is_empty() {
                                projection.tuples.push(EdgeEndoTuple { elements: vec![] });
                            }
                            projection.tuples[0].insert_element(proj.object, value)?;
                        }
                        Attr::Tuple(tuple) => {
                            projection
                                .tuples
                                .push(EdgeEndoTuple::from_tuple(tuple.elements.into_iter()));
                        }
                        Attr::Matrix(matrix) => projection.tuples.extend(
                            matrix
                                .into_rows()
                                .map(|tuple| EdgeEndoTuple::from_tuple(tuple.elements.into_iter())),
                        ),
                    }
                }
                _ => {
                    debug!("edge ignored");
                }
            }
        }

        Ok(AnalyzedStruct {
            root_attrs,
            edge_projections,
        })
    }
}

fn find_data_relationship<'d>(
    def: &'d Def,
    rel_id: &RelId,
) -> DomainResult<&'d DataRelationshipInfo> {
    def.data_relationships.get(rel_id).ok_or_else(|| {
        DomainError::data_store_bad_request(format!(
            "data relationship {def_id:?} -> {rel_id} does not exist",
            def_id = def.id
        ))
    })
}
