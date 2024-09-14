use anyhow::Context;
use fnv::FnvHashMap;
use indoc::indoc;
use ontol_runtime::{ontology::Ontology, tuple::CardinalIdx, DefId, DefPropTag, DomainIndex};
use tokio_postgres::Transaction;
use tracing::info;
use ulid::Ulid;

use crate::pg_model::{
    EdgeId, PgColumn, PgDomain, PgDomainTableType, PgEdgeCardinal, PgEdgeCardinalKind, PgIndexData,
    PgIndexType, PgProperty, PgRegKey, PgTable, PgType,
};

use super::MigrationCtx;

enum TableRef {
    Vertex(DomainIndex, DefId),
    Edge(EdgeId),
}

/// Read the entire database registry into PgModel
pub async fn read_registry<'t>(
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
    txn: &Transaction<'t>,
) -> anyhow::Result<()> {
    let domain_index_by_ulid: FnvHashMap<Ulid, DomainIndex> = ontology
        .domains()
        .map(|(domain_index, domain)| (domain.domain_id().ulid, domain_index))
        .collect();
    let mut domain_index_by_key: FnvHashMap<PgRegKey, DomainIndex> = Default::default();
    let mut table_by_key: FnvHashMap<PgRegKey, TableRef> = Default::default();
    let mut next_schema_disambiguator: i32 = 0;

    // domains
    for row in txn
        .query("SELECT key, uid, schema_name FROM m6mreg.domain", &[])
        .await
        .context("read domains")?
    {
        let key: PgRegKey = row.get(0);
        let uid: Ulid = row.get(1);
        let schema_name = row.get(2);

        next_schema_disambiguator = i32::max(next_schema_disambiguator, key);

        if let Some(domain_index) = domain_index_by_ulid.get(&uid) {
            let pg_domain = PgDomain {
                key: Some(key),
                schema_name,
                datatables: Default::default(),
                edgetables: Default::default(),
            };
            ctx.domains.insert(*domain_index, pg_domain.clone());
            domain_index_by_key.insert(key, *domain_index);
        } else {
            info!("domain id={uid} is persisted but no longer in the ontology, ignoring");
        }
    }

    ctx.next_schema_disambiguator = next_schema_disambiguator + 1;

    // domaintables
    for row in txn
        .query(
            indoc! {"
                SELECT
                    key,
                    domain_key,
                    table_type,
                    def_domain_key,
                    def_tag,
                    table_name,
                    fprop_column,
                    fkey_column
                FROM m6mreg.domaintable"
            },
            &[],
        )
        .await
        .context("read domaintables")?
    {
        let key: PgRegKey = row.get(0);
        let domain_key: PgRegKey = row.get(1);
        let table_type: PgDomainTableType = row.get(2);

        let Some(owner_domain_index) = domain_index_by_key.get(&domain_key).copied() else {
            continue;
        };

        let def_domain_key: PgRegKey = row.get(3);
        let def_tag: u16 = row.get::<_, i32>(4).try_into()?;
        let table_name: Box<str> = row.get(5);
        let fprop_column: Option<Box<str>> = row.get(6);
        let fkey_column: Option<Box<str>> = row.get(7);

        let pg_table = PgTable {
            key,
            table_name,
            has_fkey: fprop_column.is_some() && fkey_column.is_some(),
            properties: Default::default(),
            edge_cardinals: Default::default(),
            property_indexes: Default::default(),
        };

        let Some(owner_pg_domain) = ctx.domains.get_mut(&owner_domain_index) else {
            continue;
        };

        match table_type {
            PgDomainTableType::Vertex => {
                let def_domain_index = domain_index_by_key.get(&def_domain_key).unwrap();
                let def_id = DefId(*def_domain_index, def_tag);
                owner_pg_domain.datatables.insert(def_id, pg_table);
                table_by_key.insert(key, TableRef::Vertex(owner_domain_index, def_id));
            }
            PgDomainTableType::Edge => {
                owner_pg_domain.edgetables.insert(def_tag, pg_table);
                table_by_key.insert(
                    key,
                    TableRef::Edge(EdgeId(DefId(owner_domain_index, def_tag))),
                );
            }
        }
    }

    // properties
    for row in txn
        .query(
            "SELECT key, domaintable_key, prop_tag, column_name, pg_type FROM m6mreg.property",
            &[],
        )
        .await
        .context("read properties")?
    {
        let key: PgRegKey = row.get(0);
        let domaintable_key: PgRegKey = row.get(1);
        let prop_tag = DefPropTag(row.get::<_, i32>(2).try_into()?);
        let col_name: Option<Box<str>> = row.get(3);
        let pg_type: Option<PgType> = row.get(4);

        let pg_property = match (col_name, pg_type) {
            (Some(col_name), Some(pg_type)) => PgProperty::Column(PgColumn {
                key,
                col_name,
                pg_type,
            }),
            (None, None) => PgProperty::Abstract(key),
            _ => unreachable!(),
        };

        match table_by_key.get(&domaintable_key) {
            Some(TableRef::Vertex(owner_domain_index, def_id)) => {
                let pg_domain = ctx.domains.get_mut(owner_domain_index).unwrap();

                let pg_table = pg_domain
                    .datatables
                    .get_mut(def_id)
                    .unwrap_or_else(|| panic!("no datatable for {def_id:?}"));

                pg_table.properties.insert(prop_tag, pg_property);
            }
            Some(TableRef::Edge(edge_id)) => {
                let pg_domain = ctx.domains.get_mut(&edge_id.domain_index()).unwrap();
                let pg_table = pg_domain.edgetables.get_mut(&edge_id.def_id().1).unwrap();

                pg_table.properties.insert(prop_tag, pg_property);
            }
            None => {}
        }
    }

    // property indexes
    for row in txn
        .query(
            "SELECT domaintable_key, def_domain_key, def_tag, index_type, property_keys FROM m6mreg.domaintable_index",
            &[],
        )
        .await
        .context("read property indexes")?
    {
        let domaintable_key: PgRegKey = row.get(0);
        let def_domain_key: PgRegKey = row.get(1);
        let def_tag: u16 = row.get::<_, i32>(2).try_into()?;
        let index_type: PgIndexType = row.get(3);
        let property_keys: Vec<PgRegKey> = row.get(4);

        let Some(domain_index) = domain_index_by_key.get(&def_domain_key).copied() else {
            continue;
        };
        let index_def_id = DefId(domain_index, def_tag);

        match table_by_key.get(&domaintable_key) {
            Some(TableRef::Vertex(owner_domain_index, def_id)) => {
                let pg_domain = ctx.domains.get_mut(owner_domain_index).unwrap();
                let pg_table = pg_domain.datatables.get_mut(def_id).unwrap();

                pg_table.property_indexes.insert((index_def_id, index_type), PgIndexData {
                    property_keys
                });
            }
            Some(TableRef::Edge(_edge_id)) => {
                todo!("edge custom index");
            }
            None => unreachable!(),
        }
    }

    // edge cardinals
    for row in txn
        .query(
            indoc! {"
                SELECT
                    key,
                    domaintable_key,
                    ordinal,
                    ident,
                    def_column_name,
                    pinned_domaintable_key,
                    key_column_name,
                    index_type
                FROM m6mreg.edgecardinal
                ORDER BY ordinal
            "},
            &[],
        )
        .await
        .context("read edge cardinals")?
    {
        let key: PgRegKey = row.get(0);
        let domaintable_key: PgRegKey = row.get(1);
        let ordinal: i32 = row.get(2);
        let ident = row.get(3);
        let def_col_name: Option<Box<str>> = row.get(4);
        let pinned_domaintable_key: Option<PgRegKey> = row.get(5);
        let key_col_name: Option<Box<str>> = row.get(6);
        let index_type: Option<PgIndexType> = row.get(7);

        let pinned_domaintable_def_id = pinned_domaintable_key.map(|key| {
            let Some(TableRef::Vertex(_, def_id)) = table_by_key.get(&key) else {
                panic!()
            };
            *def_id
        });

        let Some(TableRef::Edge(edge_id)) = table_by_key.get(&domaintable_key) else {
            continue;
        };

        let pg_domain = ctx.domains.get_mut(&edge_id.domain_index()).unwrap();
        let pg_table = pg_domain.edgetables.get_mut(&edge_id.def_id().1).unwrap();

        pg_table.edge_cardinals.insert(
            CardinalIdx(ordinal.try_into()?),
            PgEdgeCardinal {
                key,
                ident,
                kind: match (key_col_name, pinned_domaintable_def_id, def_col_name) {
                    (Some(key_col_name), Some(pinned_domaintable_def_id), None) => {
                        PgEdgeCardinalKind::PinnedDef {
                            pinned_def_id: pinned_domaintable_def_id,
                            key_col_name,
                        }
                    }
                    (Some(key_col_name), None, Some(def_col_name)) => PgEdgeCardinalKind::Dynamic {
                        def_col_name,
                        key_col_name,
                    },
                    (None, None, None) => {
                        // updated below
                        PgEdgeCardinalKind::Parameters(DefId::unit())
                    }
                    _ => {
                        unreachable!()
                    }
                },
                index_type,
            },
        );
    }

    Ok(())
}
