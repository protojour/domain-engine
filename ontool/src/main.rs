#![forbid(unsafe_code)]

use ontool::OntoolError;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), OntoolError> {
    ontool::run().await
}
