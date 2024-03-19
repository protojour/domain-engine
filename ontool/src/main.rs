#![forbid(unsafe_code)]

use ontool::OntoolError;

#[tokio::main]
async fn main() -> Result<(), OntoolError> {
    ontool::run().await
}
