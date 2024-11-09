use ontol_lsp::Backend;
use tower_lsp::{LspService, Server};

#[tokio::main(worker_threads = 2)]
async fn main() -> tokio::io::Result<()> {
    tracing_subscriber::fmt()
        .without_time()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(Backend::new);
    Server::new(stdin, stdout, socket)
        .concurrency_level(2)
        .serve(service)
        .await;

    Ok(())
}
