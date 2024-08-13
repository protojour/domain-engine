use domain_engine_core::{domain_error::DomainErrorKind, DomainError, DomainResult};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipInfo, DataRelationshipKind, Def},
    value::Value,
    RelId,
};
use tracing::debug;

use crate::pg_model::InDomain;

use super::{
    data::{Data, ScalarAttrs},
    edge_patch::{EdgeEndoTuplePatch, EdgePatches},
    MutationMode, TransactCtx,
};

pub struct AnalyzedStruct<'m, 'b> {
    pub root_attrs: ScalarAttrs<'m, 'b>,
    pub edges: EdgePatches,
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

        let mut edge_patches = EdgePatches::default();

        for (rel_id, attr) in *attrs {
            let rel_info = find_data_relationship(def, &rel_id)?;

            match (rel_info.kind, attr) {
                (DataRelationshipKind::Id | DataRelationshipKind::Tree, Attr::Unit(value)) => {
                    match self.data_from_value(value)? {
                        Data::Sql(scalar) => {
                            root_attrs.map.insert(rel_id, scalar);
                        }
                        Data::Compound(comp) => {
                            todo!("compound: {comp:?}");
                        }
                    }
                }
                (DataRelationshipKind::Edge(proj), attr) => {
                    let patch = edge_patches.patch(proj.id, proj.subject);

                    match attr {
                        Attr::Unit(value) => {
                            if patch.tuples.is_empty() {
                                patch.tuples.push(EdgeEndoTuplePatch { elements: vec![] });
                            }
                            patch.tuples[0].insert_element(
                                proj.object,
                                value,
                                MutationMode::insert(),
                            )?;
                        }
                        Attr::Tuple(tuple) => {
                            patch.tuples.push(EdgeEndoTuplePatch::from_tuple(
                                tuple
                                    .elements
                                    .into_iter()
                                    .map(|val| (val, MutationMode::insert())),
                            ));
                        }
                        Attr::Matrix(matrix) => {
                            patch.tuples.extend(matrix.into_rows().map(|tuple| {
                                EdgeEndoTuplePatch::from_tuple(
                                    tuple
                                        .elements
                                        .into_iter()
                                        .map(|val| (val, MutationMode::insert())),
                                )
                            }))
                        }
                    }
                }
                _ => {
                    debug!("edge ignored");
                }
            }
        }

        Ok(AnalyzedStruct {
            root_attrs,
            edges: edge_patches,
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
