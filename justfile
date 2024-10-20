default:
    @just --list

docs:
    cargo install mdbook
    mdbook build docs

pdf:
    #!/usr/bin/env bash
    cargo install mdbook-pdf
    python -m venv docs/pdf/.venv
    source docs/pdf/.venv/bin/activate
    pip install mdbook-pdf-outline
    mdbook build docs/pdf

ontool:
    cargo install --path ontool --debug --force

lsp:
    #!/usr/bin/env bash
    export RUSTFLAGS=" \
        --cfg tokio_unstable \
        -Clink-arg=--initial-memory=1966080 \
        -Clink-arg=--max-memory=7864320"
    cargo build --package ontol-lsp-wasm \
                --target wasm32-wasip1-threads \
                --release
    cp target/wasm32-wasip1-threads/release/ontol-lsp-wasm.wasm \
       ontol/ontol-language/ontol-vscode/wasm/ontol-lsp.wasm
    cd ontol/ontol-language
    npx js-yaml ontol.tmLanguage.yaml > ontol.tmLanguage.json
    cp ontol.tmLanguage.json ontol.tmSnippet.json \
       ontol-vscode/
    cd ontol-vscode
    npm run build && \
    npm run package
    cd ../../..
