default:
    @just --list

ontool:
    cargo install --path ontool

lsp: ontool
    #!/usr/bin/env bash
    cd ontol-language
    for dest in ontol-sublime/bin/ ontol-vscode/bin/; do
        mkdir -p "$dest"  && cp ~/.cargo/bin/ontool "$dest"; done
    npx js-yaml ontol.tmLanguage.yaml > ontol.tmLanguage.json
    cp ontol.tmLanguage.yaml ontol-sublime/
    cp ontol.tmLanguage.json ontol.tmSnippet.json ontol-vscode/
    cd ontol-vscode
    npm run build && \
    npm run package
    cd ../..
