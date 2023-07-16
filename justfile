default:
    @just --list

ontool:
    cargo install --path ontool

lsp: ontool
    #!/usr/bin/env bash
    cd ontol-language
    for dest in ontol-sublime/bin/ ontol-vscode/bin/; do
        mkdir -p "$dest"  && cp ~/.cargo/bin/ontool "$dest"; done
    npx js-yaml ONTOL.tmLanguage.yaml > ONTOL.tmLanguage.json
    cp ONTOL.tmLanguage.yaml ontol-sublime/
    cp ONTOL.tmLanguage.json ontol-vscode/syntaxes/
    cd ontol-vscode
    npm run build && \
    npm run package
    cd ../..

wasm:
    wasm-pack build --target web domain-engine-wasm
    cp -r domain-engine-wasm/pkg ../ontol-domain-editor/
