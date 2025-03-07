use async_trait::async_trait;
use deadpool_postgres::Transaction;
use refinery::Migration;
use refinery_core::{
    AsyncMigrate,
    traits::r#async::{AsyncQuery, AsyncTransaction},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub struct TxnWrapper {
    pub conn: deadpool_postgres::Object,
}

#[async_trait]
impl AsyncTransaction for TxnWrapper {
    type Error = tokio_postgres::Error;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let transaction = self.conn.build_transaction().start().await?;
        let mut count = 0;

        for query in queries {
            transaction.batch_execute(query).await?;
            count += 1;
        }

        transaction.commit().await?;

        Ok(count as usize)
    }
}

#[async_trait]

impl AsyncQuery<Vec<Migration>> for TxnWrapper {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let transaction = self.conn.build_transaction().start().await?;
        let applied = query_applied_migrations(&transaction, query).await?;
        transaction.commit().await?;
        Ok(applied)
    }
}

impl AsyncMigrate for TxnWrapper {}

async fn query_applied_migrations(
    transaction: &Transaction<'_>,
    query: &str,
) -> Result<Vec<Migration>, tokio_postgres::Error> {
    let rows = transaction.query(query, &[]).await?;

    let mut applied = Vec::new();

    for row in rows.into_iter() {
        let version = row.get(0);
        let applied_on: String = row.get(2);

        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();

        let checksum: String = row.get(3);

        applied.push(Migration::applied(
            version,
            row.get(1),
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }

    Ok(applied)
}
