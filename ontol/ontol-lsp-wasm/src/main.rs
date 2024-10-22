use ontol_lsp::Backend;
use tower_lsp::{LspService, Server};

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(Backend::new);
    Server::new(stdin, stdout, socket).serve(service).await;

    Ok(())
}
