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
    TransactCtx,
};

pub struct AnalyzedStruct<'d> {
    pub root_attrs: ScalarAttrs<'d>,
}

impl<'d, 't> TransactCtx<'d, 't> {
    pub(super) fn analyze_struct(
        &self,
        value: InDomain<Value>,
        def: &Def,
    ) -> DomainResult<AnalyzedStruct> {
        let datatable = self
            .pg_model
            .find_datatable(value.pkg_id, value.type_def_id())?;

        let Value::Struct(attrs, _struct_tag) = value.value else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        let mut root_attrs = ScalarAttrs {
            map: Default::default(),
            datatable,
        };

        for (rel_id, attr) in *attrs {
            let data_relationship = find_data_relationship(def, &rel_id)?;

            match (
                data_relationship.kind,
                attr,
                data_relationship.cardinality.1,
            ) {
                (DataRelationshipKind::Id | DataRelationshipKind::Tree, Attr::Unit(value), _) => {
                    match self.data_from_value(value)? {
                        Data::Scalar(scalar) => {
                            root_attrs.map.insert(rel_id, scalar);
                        }
                        Data::Compound(comp) => {
                            todo!("compound: {comp:?}");
                        }
                    }
                }
                _ => {
                    debug!("edge ignored");
                }
            }
        }

        Ok(AnalyzedStruct { root_attrs })
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
