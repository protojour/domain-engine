import { ExtensionContext, Uri, window, workspace } from "vscode"
import { LanguageClient, LanguageClientOptions, ServerOptions } from "vscode-languageclient/node"
import { Wasm, WasmProcess, ProcessOptions } from "@vscode/wasm-wasi/v1"
import { createStdioOptions, createUriConverters, startServer } from "@vscode/wasm-wasi-lsp"

let client: LanguageClient
let process: WasmProcess

export async function activate(context: ExtensionContext) {
  const channel = window.createOutputChannel("ONTOL language server", { log: true })

  const serverOptions: ServerOptions = async () => {
    const wasm = await Wasm.load()
    const file = Uri.joinPath(context.extensionUri, "wasm", "ontol-lsp.wasm")
    const bits = await workspace.fs.readFile(file)
    const module = await WebAssembly.compile(bits)

    const options: ProcessOptions = {
      stdio: createStdioOptions(),
      mountPoints: [{ kind: "workspaceFolder" }],
    }

    process = await wasm.createProcess(
      "ontol-lsp",
      module,
      { initial: 30, maximum: 120, shared: true },
      options,
    )

    const decoder = new TextDecoder("utf-8")
    process.stderr!.onData((data) => {
      channel.append(decoder.decode(data) + "\n\n")
    })

    return startServer(process)
  }

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "ontol" }],
    outputChannel: channel,
    uriConverters: createUriConverters(),
  }

  client = new LanguageClient(
    "ontol-language-client",
    "ONTOL language client",
    serverOptions,
    clientOptions,
    true,
  )

  try {
    await client.start()
  } catch (error) {
    client.error("Language service failed", error, "force")
  }
}

export async function deactivate() {
  if (!client) return
  await process.terminate()
  await client.stop()
}
