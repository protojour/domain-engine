use domain_engine_core::{DomainResult, VertexAddr};
use ontol_runtime::{crdt::Automerge, PropId};
use thin_vec::ThinVec;
use tracing::debug;

use crate::{
    address::deserialize_address,
    pg_error::PgError,
    pg_model::{PgDataKey, PgDomain, PgRegKey},
    sql::{self, Expr},
};

use super::{data::ParentProp, mut_ctx::PgMutCtx, TransactCtx};

impl<'a> TransactCtx<'a> {
    pub async fn insert_initial_automerge_crdt(
        &self,
        parent: ParentProp,
        automerge: Automerge,
        _mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<()> {
        let domain_index = parent.prop_id.0.domain_index();
        let pg_domain = self.pg_model.pg_domain(domain_index)?;

        let parent_prop_key = self
            .pg_model
            .datatable(domain_index, parent.prop_id.0)?
            .abstract_property(&parent.prop_id)?;

        self.save_impl(
            pg_domain,
            parent_prop_key,
            parent.key,
            "snapshot",
            automerge.save().as_slice(),
        )
        .await?;

        Ok(())
    }

    pub async fn save_crdt_incremental(
        &self,
        vertex_addr: VertexAddr,
        prop_id: PropId,
        payload: Vec<u8>,
    ) -> DomainResult<()> {
        let (pg_domain, prop_key, data_key) = self.crdt_meta(vertex_addr, prop_id)?;

        self.save_impl(pg_domain, prop_key, data_key, "incremental", &payload)
            .await?;

        Ok(())
    }

    pub async fn crdt_get(
        &self,
        vertex_addr: VertexAddr,
        prop_id: PropId,
    ) -> DomainResult<Option<ThinVec<u8>>> {
        let (pg_domain, prop_key, data_key) = self.crdt_meta(vertex_addr, prop_id)?;

        let select = sql::Select {
            with: None,
            expressions: sql::Expressions {
                items: vec![sql::Expr::StringAgg(
                    Box::new(sql::Expr::path1("chunk")),
                    Box::new(sql::Expr::LiteralBytea(&[])),
                )],
                multiline: false,
            },
            from: vec![sql::FromItem::Select(Box::new(sql::Select {
                with: None,
                expressions: vec![sql::Expr::path1("chunk")].into(),
                from: vec![sql::TableName(&pg_domain.schema_name, "crdt").into()],
                where_: Some(sql::Expr::eq(
                    sql::Expr::Tuple(vec![sql::Expr::path1("_fprop"), sql::Expr::path1("_fkey")]),
                    sql::Expr::Tuple(vec![sql::Expr::LiteralInt(prop_key), sql::Expr::param(0)]),
                )),
                order_by: sql::OrderBy {
                    expressions: vec![sql::Expr::path1("chunk_id").into()],
                },
                ..Default::default()
            }))],

            ..Default::default()
        };

        let sql = select.to_string();
        debug!("{sql}");

        let row = self
            .client()
            .query_opt(&sql, &[&data_key])
            .await
            .map_err(PgError::SelectQuery)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let bytes: &[u8] = row.get(0);

        Ok(Some(ThinVec::from_iter(bytes.iter().copied())))
    }

    fn crdt_meta(
        &self,
        vertex_addr: VertexAddr,
        prop_id: PropId,
    ) -> DomainResult<(&PgDomain, PgRegKey, PgDataKey)> {
        let (reg_key, data_key) = deserialize_address(&vertex_addr)?;
        let (domain_index, def_id) = self.pg_model.datatable_key_by_def_key(reg_key)?;
        let pg_vertex_table = self.pg_model.pg_domain_datatable(domain_index, def_id)?;
        let pg_domain = pg_vertex_table.domain;

        let prop_key = self
            .pg_model
            .datatable(domain_index, prop_id.0)?
            .abstract_property(&prop_id)?;

        Ok((pg_domain, prop_key, data_key))
    }

    /// Save and return the chunk ID
    async fn save_impl(
        &self,
        pg_domain: &PgDomain,
        prop_key: PgRegKey,
        data_key: PgDataKey,
        chunk_type: &str,
        payload: &[u8],
    ) -> DomainResult<i64> {
        let insert = sql::Insert {
            with: None,
            into: sql::TableName(&pg_domain.schema_name, "crdt"),
            as_: None,
            column_names: vec!["_fprop", "_fkey", "chunk_type", "chunk"],
            values: vec![
                sql::Expr::param(0),
                sql::Expr::param(1),
                sql::Expr::LiteralStr(chunk_type),
                sql::Expr::param(2),
            ],
            on_conflict: None,
            returning: vec![Expr::path1("chunk_id")],
        };

        let sql = insert.to_string();

        debug!("{sql}");

        let row = self
            .client()
            .query_one(&sql, &[&prop_key, &data_key, &payload])
            .await
            .map_err(PgError::InsertQuery)?;

        Ok(row.get::<_, i64>(0))
    }
}
