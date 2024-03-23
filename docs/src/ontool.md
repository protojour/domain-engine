# `ontool`

***`ontool`*** is the ONTOlogy Language tool, a command line application for ONTOL:

```
ontool – ONTOlogy Language tool

Usage: ontool [COMMAND]

Commands:
  check     Check ONTOL (.on) files for errors
  compile   Compile ONTOL (.on) files
  generate  Generate schemas from ONTOL (.on) files
  serve     Run ontool in development server mode
  lsp       Run ontool in language server mode
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

`ontool` can [check](#ontool-check) ONTOL (.on) files, [compile](#ontool-compile) an _ontology_, [generate](#ontool-generate) schemas for testing, run as a GraphQL [development server](#ontool-serve) or as an ONTOL [language server](#ontool-lsp).


## `ontool check`

```
Check ONTOL (.on) files for errors

Usage: ontool check [OPTIONS] [FILES]...

Arguments:
  [FILES]...  ONTOL (.on) files to check for errors

Options:
  -w, --dir <DIR>  Search directory for ONTOL files [default: .]
```

`ontool check` checks the given file(s) for errors.

Note that this command, and all others, will try to follow [`use`](use.md) statements and resolve imports in the given search directory (`-w`, `--dir`). As such, it is not necessary to specify every single ONTOL file in your project.

Errors are output to `stderr` with a nonzero exit code. A successful run exits with zero.


## `ontool compile`

```
Compile ONTOL (.on) files

Usage: ontool compile [OPTIONS] [FILES]...

Arguments:
  [FILES]...  ONTOL (.on) files to compile

Options:
  -w, --dir <DIR>                Search directory for ONTOL files [default: .]
  -d, --data-store <DATA_STORE>  Specify a domain to be backed by a data store
  -b, --backend <BACKEND>        Specify a data store backend [inmemory, arangodb, ..]
  -o, --output <OUTPUT>          Ontology output file [default: ontology]
```

`ontool compile` compiles the given file(s) into an ontology. This is a binary file which can be loaded and used in the domain engine. It is not necessary to `check` files before a `compile`.

The data store (`-d`, `--data-store`) option is used to specify which domain, if any, is to be backed by the data store. Use the filename of the domain without extension.

The backend (`-b`, `--backend`) option is used to specify which data store backend the ontology uses. The default is `inmemory`, an in-memory store which is only suitable for testing. If compiling for a full development or production setting, use the `arangodb` backend.

The output (`-o`, `--output`) option specifies the file path for the compiled ontology.


## `ontool generate`

```
Generate JSON schema from ONTOL (.on) files

Usage: ontool generate [OPTIONS] [FILES]...

Arguments:
  [FILES]...  ONTOL (.on) files to generate schemas from

Options:
  -w, --dir <DIR>        Search directory for ONTOL files [default: .]
  -f, --format <FORMAT>  Output format for JSON schema [default: json] [possible values: json, yaml]
```

`ontool generate` outputs a JSON Schema representation of the (non-`@private`) [`def`](def.md)s in the given domain(s) to `stdout`. This can be useful for testing or for generating documentation.

The format (`-f`, `--format`) option specifies the output format, either `json` or `yaml`.


## `ontool serve`

```
Run ontool in development server mode

Usage: ontool serve [OPTIONS] [FILES]...

Arguments:
  [FILES]...  Root file(s) of the ontology

Options:
  -w, --dir <DIR>                Search directory for ONTOL files [default: .]
  -d, --data-store <DATA_STORE>  Specify a domain to be backed by a data store
  -p, --port <PORT>              Specify a port for the server [default: 5000]
```

`ontool serve` compiles the given ONTOL file(s) and starts a hot-reloading GraphQL development server using an `inmemory` data store.

Changes to any root file or indirectly imported file will cause the server to recompile the ontology, and (on successful compilation) reload the server and GraphQL schema. Errors are output to `stderr`.

See [`ontool compile`](#ontool-compile) above for information on the data store option.


## `ontool lsp`

`ontool lsp` starts the ONTOL language server. It communicates via `stdin` and `stdout`, and can be set up to work with any editor that supports the Language Server Protocol (LSP).

An ONTOL language support extension that includes the language server, syntax highlighting and snippets, is [available on the VSCode marketplace](https://marketplace.visualstudio.com/items?itemName=Protojour.ontol).

