# ONTOL Language Support

Language server and grammar for [ONTOL](https://software.situ.net/memoriam/docs/domain-engine/introduction.html) (ONTOlogy Language), a strongly typed declarative domain model and mapping language for the [Memoriam](https://software.situ.net/memoriam/docs/) domain engine.

## Features

- Syntax highlighting
- Snippets for common patterns
- Language server, including:
  - Compiler error diagnostics
  - Goto definition
  - Hover documentation on keywords, symbols and structures
  - Code completion

## Dependencies

The language server is compiled to WASM with a [WASI](https://github.com/WebAssembly/WASI) target, and requires the Microsoft [WASM WASI Core Extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode.wasm-wasi-core), which will be installed automatically.

## License

ONTOL is licensed under GNU Affero General Public License, version 3 (AGPL-3.0).
