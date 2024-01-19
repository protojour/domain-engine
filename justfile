default:
    @just --list

ontool:
    cargo install --path ontool-bin --debug --force

lsp:
    #!/usr/bin/env bash
    cargo build --manifest-path ontool-bin/Cargo.toml --release
    cd ontol-language
    for dest in ontol-sublime/bin/ ontol-vscode/bin/; do
        mkdir -p "$dest"  && cp ../target/release/ontool "$dest"; done
    npx js-yaml ontol.tmLanguage.yaml > ontol.tmLanguage.json
    cp ontol.tmLanguage.yaml ontol-sublime/
    cp ontol.tmLanguage.json ontol.tmSnippet.json ontol-vscode/
    cd ontol-vscode
    npm run build && \
    npm run package
    cd ../..
