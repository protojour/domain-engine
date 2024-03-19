# ONTOL Language Resources

This folder contains a basic TextMate grammar for ONTOL, which is combined with the Language Server from `ontool` and `ontol-lsp`, into a modern developer experience for VS Code and Sublime Text.


## Working with TextMate grammar

There is some divergence in syntax grammar definitions (Sublime Text uses a new format, VS Code uses `tmLanguage` as JSON, see below for Monaco), but for now TextMate grammars have fairly wide compatibility.

`ONTOL.tmLanguage.yaml` should be considered to be the source file, since it's easier to write. This gets converted to `ONTOL.tmLanguage.json` by the Sublime Text [PackageDev](https://packagecontrol.io/packages/PackageDev) build system, or by running:

```bash
npx js-yaml ONTOL.tmLanguage.yaml > ONTOL.tmLanguage.json
```

Tooling here mostly refers to Sublime Text, but VS Code has similar tooling with [TextMate Languages](https://marketplace.visualstudio.com/items?itemName=pedro-w.tmlanguage)

Rules are defined as follows:

```yaml
- comment: use declaration
  name: keyword.import.ontol
  match: \b(use)\b
```

`comment` is just a description, `name` is a categorization which associates the token(s) with some role, and consequently, theme colors. A good overview on `name` conventions can be found [here](https://www.sublimetext.com/docs/scope_naming.html). `match` is an [Oniguruma regular expression](https://raw.githubusercontent.com/kkos/oniguruma/v6.9.1/doc/RE), but should be familiar enough. 

There's a lot more to it, see [here](https://docs.sublimetext.io/reference/syntaxdefs_legacy.html) and [here](https://sublime-text-unofficial-documentation.readthedocs.io/en/sublime-text-2/extensibility/syntaxdefs.html) to learn more.


### Testing

`syntax_test.on` should (eventually) cover the entire syntax, including edge cases. It follows the Sublime Text [syntax test format](https://www.sublimetext.com/docs/syntax.html#testing), and tests can be ran with the Sublime Text build system for syntax tests.


## Sublime Text package

There's a Sublime Text language definition package in the `ontol-sublime` folder. It will evolve alongside ONTOL, and won't be published until ONTOL is stable. 

For now, just copy or symlink the `ontol-sublime` folder to `~/.config/sublime-text/Packages/` (or your platform equivalent).


## VS Code extension

There's a VS Code language definition extension in the `ontol-vscode` folder. It will evolve alongside ONTOL, and won't be published until ONTOL is stable. 

A `.vsix` package can be found in the folder, and can be installed via the extensions submenu in VS Code, or by running:

```bash
code --install-extension ontol-vscode/ontol.vsix
```

Restart VS Code 


## Monaco integration

[Monaco](https://microsoft.github.io/monaco-editor/) is the actual code editor engine in VS Code. Because TextMate grammars depend on Oniguruma, which is implemented in C, Monaco doesn't support `tmLanguage`, and instead uses [Monarch](https://microsoft.github.io/monaco-editor/monarch.html) as its default tokenizer. However, [monaco-editor-textmate](https://www.npmjs.com/package/monaco-editor-textmate) provides `tmLanguage` support with Oniguruma as a WASM module (onigasm).

There's some work done on this in [ontol-domain-editor](https://gitlab.com/protojour/memoriam/ontol-domain-editor).
