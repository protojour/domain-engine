use anyhow::{anyhow, Context};
use indoc::indoc;
use itertools::Itertools;
use ontol_runtime::{
    debug::OntolDebug,
    interface::serde::{operator::SerdeOperator, processor::ProcessorMode},
    ontology::{
        domain::{DataRelationshipKind, DataRelationshipTarget, Def, Domain, Entity},
        Ontology,
    },
    DefId, DefRelTag,
};
use tokio_postgres::Transaction;
use tracing::info;

use crate::{
    migrate::{DefUid, MigrationStep, PgDomain, PgSerial},
    pg_model::{PgDataField, PgDataTable, PgType},
    sql::Unpack,
};

use super::{DomainUid, MigrationCtx};

pub async fn migrate_domain_steps<'t>(
    domain: &Domain,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
    txn: &Transaction<'t>,
) -> anyhow::Result<()> {
    let domain_uid = domain.domain_id().ulid;
    let unique_name = &ontology[domain.unique_name()];
    let schema = format!("m6m_d_{unique_name}").into_boxed_str();

    let row = txn
        .query_opt(
            "SELECT key, schema_name FROM m6m_reg.domain WHERE uid = $1",
            &[&domain_uid],
        )
        .await?;

    if let Some(row) = row {
        info!("domain already deployed");

        let (domain_key, schema_name) = row.unpack();

        let pg_datatables = txn
            .query(
                indoc! {"
                    SELECT key, def_domain_key, def_tag, table_name
                    FROM m6m_reg.datatable
                    WHERE domain_key = $1
                "},
                &[&domain_key],
            )
            .await?
            .into_iter()
            .map(|row| -> anyhow::Result<_> {
                let key: PgSerial = row.get(0);
                let def_domain_key: PgSerial = row.get(1);
                let def_tag: u16 = row.get::<_, i32>(2).try_into()?;
                let table_name: Box<str> = row.get(3);

                // for now
                assert_eq!(def_domain_key, domain_key);

                Ok((
                    DefUid(domain_uid, def_tag),
                    PgDataTable {
                        key,
                        table_name,
                        data_fields: Default::default(),
                    },
                ))
            })
            .try_collect()?;

        let pg_domain = PgDomain {
            key: Some(domain_key),
            schema_name,
            datatables: pg_datatables,
        };
        ctx.domains.insert(domain_uid, pg_domain.clone());

        if pg_domain.schema_name != schema {
            ctx.steps.push((
                domain_uid,
                MigrationStep::RenameDomainSchema {
                    old: pg_domain.schema_name.clone(),
                    new: schema,
                },
            ));
        }
    } else {
        ctx.domains.insert(
            domain_uid,
            PgDomain {
                key: None,
                schema_name: schema.clone(),
                datatables: Default::default(),
            },
        );

        ctx.steps.push((
            domain_uid,
            MigrationStep::DeployDomain {
                name: ontology[domain.unique_name()].into(),
                schema_name: schema,
            },
        ));
    };

    for def in domain.defs() {
        let Some(entity) = def.entity() else {
            continue;
        };

        migrate_vertex_steps(domain_uid, def.id, def, entity, ontology, txn, ctx).await?;
    }

    Ok(())
}

async fn migrate_vertex_steps<'t>(
    domain_uid: DomainUid,
    vertex_def_id: DefId,
    def: &Def,
    entity: &Entity,
    ontology: &Ontology,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let name = &ontology[entity.name];
    let table_name = format!("v_{}", name).into_boxed_str();
    let pg_domain = ctx.domains.get_mut(&domain_uid).unwrap();
    let def_uid = DefUid(domain_uid, vertex_def_id.1);

    if let Some(datatable) = pg_domain.datatables.get_mut(&def_uid) {
        let pg_fields = txn
            .query(
                indoc! {"
                    SELECT key, rel_tag, column_name
                    FROM m6m_reg.datafield
                    WHERE datatable_key = $1
                "},
                &[&datatable.key],
            )
            .await
            .context("read datatables")?
            .into_iter()
            .map(|row| -> anyhow::Result<_> {
                let _key: PgSerial = row.get(0);
                let rel_tag = DefRelTag(row.get::<_, i32>(1).try_into()?);
                let column_name: Box<str> = row.get(2);

                Ok((
                    rel_tag,
                    PgDataField {
                        column_name,
                        pg_type: PgType::Text,
                    },
                ))
            })
            .try_collect()?;

        if datatable.table_name != table_name {
            ctx.steps.push((
                domain_uid,
                MigrationStep::RenameDataTable {
                    def_uid,
                    old: datatable.table_name.clone(),
                    new: table_name.clone(),
                },
            ));
        }

        datatable.data_fields = pg_fields;
    } else {
        ctx.steps.push((
            domain_uid,
            MigrationStep::DeployVertex {
                vertex_def_id,
                table_name: table_name.clone(),
            },
        ));
    }

    let mut tree_relationships: Vec<_> = def
        .data_relationships
        .iter()
        .filter_map(|(rel_id, rel)| match &rel.kind {
            DataRelationshipKind::Id | DataRelationshipKind::Tree => Some((rel_id.tag(), rel)),
            DataRelationshipKind::Edge(_) => None,
        })
        .collect_vec();

    tree_relationships.sort_by_key(|(rel_tag, _)| rel_tag.0);

    for (rel_tag, rel) in tree_relationships {
        let pg_data_field = pg_domain
            .datatables
            .get(&def_uid)
            .and_then(|datatable| datatable.data_fields.get(&rel_tag));

        let column_name = format!("r_{}", &ontology[rel.name]).into_boxed_str();

        let pg_type = match rel.target {
            DataRelationshipTarget::Unambiguous(def_id) => {
                let def = ontology.get_def(def_id).unwrap();
                let operator_addr = def
                    .operator_addr
                    .ok_or_else(|| anyhow!("no operator addr available for PgType detection"))?;

                // BUG: it's not right to look at the external interface for the type
                let operator = ontology
                    .new_serde_processor(operator_addr, ProcessorMode::Raw)
                    .value_operator;
                match operator {
                    SerdeOperator::Unit => continue,
                    SerdeOperator::True(_)
                    | SerdeOperator::False(_)
                    | SerdeOperator::Boolean(_) => PgType::Boolean,
                    SerdeOperator::TextPattern(_) => PgType::Text,
                    SerdeOperator::CapturingTextPattern(_) => PgType::Text,

                    other @ (SerdeOperator::AnyPlaceholder
                    | SerdeOperator::I32(_, _)
                    | SerdeOperator::I64(_, _)
                    | SerdeOperator::F64(_, _)
                    | SerdeOperator::Serial(_)
                    | SerdeOperator::String(_)
                    | SerdeOperator::StringConstant(_, _)
                    | SerdeOperator::DynamicSequence
                    | SerdeOperator::RelationList(_)
                    | SerdeOperator::RelationIndexSet(_)
                    | SerdeOperator::ConstructorSequence(_)
                    | SerdeOperator::Alias(_)
                    | SerdeOperator::Union(_)
                    | SerdeOperator::Struct(_)
                    | SerdeOperator::IdSingletonStruct(_, _, _)) => {
                        return Err(anyhow!("cannot use operator {:?}", other.debug(ontology)))
                    }
                }
            }
            DataRelationshipTarget::Union(_) => {
                return Err(anyhow!("FIXME: union target for {rel_tag:?}"));
            }
        };

        if let Some(pg_data_field) = pg_data_field {
            assert_eq!(
                pg_data_field.column_name, column_name,
                "TODO: rename column"
            );
            assert_eq!(
                pg_data_field.pg_type, pg_type,
                "TODO: change data field pg_type",
            );
        } else {
            ctx.steps.push((
                domain_uid,
                MigrationStep::DeployDataField {
                    datatable_def_id: vertex_def_id,
                    rel_tag,
                    pg_type,
                    column_name,
                },
            ));
        }
    }

    Ok(())
}
