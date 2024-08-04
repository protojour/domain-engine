use anyhow::{anyhow, Context};
use indoc::indoc;
use itertools::Itertools;
use ontol_runtime::{
    ontology::{
        domain::{
            DataRelationshipKind, DataRelationshipTarget, Def, DefKind, DefRepr, Domain, Entity,
        },
        Ontology,
    },
    DefId, DefRelTag, PackageId,
};
use tokio_postgres::Transaction;
use tracing::info;

use crate::{
    migrate::{MigrationStep, PgDomain, PgSerial},
    pg_model::{PgDataField, PgDataTable, PgType},
    sql::Unpack,
};

use super::{MigrationCtx, PgDomainIds};

pub async fn migrate_domain_steps<'t>(
    pkg_id: PackageId,
    domain: &Domain,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
    txn: &Transaction<'t>,
) -> anyhow::Result<()> {
    let domain_uid = domain.domain_id().ulid;
    let domain_ids = PgDomainIds {
        pkg_id,
        uid: domain_uid,
    };
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
                    DefId(pkg_id, def_tag),
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
        ctx.domains.insert(pkg_id, pg_domain.clone());

        if pg_domain.schema_name != schema {
            ctx.steps.push((
                domain_ids,
                MigrationStep::RenameDomainSchema {
                    old: pg_domain.schema_name.clone(),
                    new: schema,
                },
            ));
        }
    } else {
        ctx.domains.insert(
            pkg_id,
            PgDomain {
                key: None,
                schema_name: schema.clone(),
                datatables: Default::default(),
            },
        );

        ctx.steps.push((
            domain_ids,
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

        migrate_vertex_steps(domain_ids, def.id, def, entity, ontology, txn, ctx).await?;
    }

    Ok(())
}

async fn migrate_vertex_steps<'t>(
    domain_ids: PgDomainIds,
    vertex_def_id: DefId,
    def: &Def,
    entity: &Entity,
    ontology: &Ontology,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let name = &ontology[entity.name];
    let table_name = format!("v_{}", name).into_boxed_str();
    let pg_domain = ctx.domains.get_mut(&domain_ids.pkg_id).unwrap();

    if let Some(datatable) = pg_domain.datatables.get_mut(&vertex_def_id) {
        let pg_fields = txn
            .query(
                indoc! {"
                    SELECT key, rel_tag, pg_type, column_name
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
                let pg_type = row.get(2);
                let column_name: Box<str> = row.get(3);

                Ok((
                    rel_tag,
                    PgDataField {
                        column_name,
                        pg_type,
                    },
                ))
            })
            .try_collect()?;

        if datatable.table_name != table_name {
            ctx.steps.push((
                domain_ids,
                MigrationStep::RenameDataTable {
                    def_id: vertex_def_id,
                    old: datatable.table_name.clone(),
                    new: table_name.clone(),
                },
            ));
        }

        datatable.data_fields = pg_fields;
    } else {
        ctx.steps.push((
            domain_ids,
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
            .get(&vertex_def_id)
            .and_then(|datatable| datatable.data_fields.get(&rel_tag));

        let column_name = format!("{}", &ontology[rel.name]).into_boxed_str();

        let pg_type = match rel.target {
            DataRelationshipTarget::Unambiguous(def_id) => {
                let def = ontology.get_def(def_id).unwrap();
                let def_repr = match &def.kind {
                    DefKind::Data(basic_def) => &basic_def.repr,
                    _ => &DefRepr::Unknown,
                };

                match def_repr {
                    DefRepr::Unit => return Err(anyhow!("TODO: ignore unit column")),
                    DefRepr::I64 => PgType::BigInt,
                    DefRepr::F64 => PgType::DoublePrecision,
                    DefRepr::Serial => PgType::Bigserial,
                    DefRepr::Boolean => PgType::Boolean,
                    DefRepr::Text => PgType::Text,
                    DefRepr::Octets => PgType::Bytea,
                    DefRepr::DateTime => PgType::Timestamp,
                    DefRepr::Seq => todo!("seq"),
                    DefRepr::Struct => todo!("struct"),
                    DefRepr::Intersection(_) => todo!("intersection"),
                    DefRepr::StructUnion(_) => todo!("struct union"),
                    DefRepr::Union(_) => todo!("union"),
                    DefRepr::Unknown => return Err(anyhow!("unknown repr: {def_id:?}")),
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
                domain_ids,
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
