import { ExtensionContext, Uri, window, workspace, commands } from "vscode"
import { BaseLanguageClient, type LanguageClientOptions, type ServerOptions } from "vscode-languageclient"
import { Wasm, WasmProcess, ProcessOptions } from "@vscode/wasm-wasi/v1"
import { createStdioOptions, createUriConverters, startServer } from "@vscode/wasm-wasi-lsp"

let client: BaseLanguageClient
let server: WasmProcess

export async function activate(context: ExtensionContext) {

  const channel = window.createOutputChannel("ONTOL language server", { log: true })
  context.subscriptions.push(channel)

  const decoder = new TextDecoder("utf-8")

  // stderr is used all logs
  const stderrLogger = (data: Uint8Array) => {
    let msg = decoder.decode(data).trim()
    if (msg.startsWith("DEBUG"))
      channel.debug(
        msg.replace("DEBUG", "").trim()
      )
    else if (msg.startsWith("WARN"))
      channel.warn(
        msg.replace("WARN", "").trim()
      )
    else if (msg.startsWith("ERROR"))
      channel.error(
        msg.replace("ERROR", "").trim()
      )
    else
      channel.info(
        msg.replace("INFO", "").trim()
      )
  }

  // stdout is LSP out only
  const stdoutLogger = (data: Uint8Array) => {
    let msg = decoder.decode(data).trim()
    if (msg.startsWith("Content")) return
    channel.debug(msg)
  }

  const serverOptions: ServerOptions = async () => {
    const wasm = await Wasm.load()
    const file = Uri.joinPath(context.extensionUri, "wasm", "ontol-lsp.wasm")
    const data = await workspace.fs.readFile(file)
    const module = await WebAssembly.compile(data)

    const options: ProcessOptions = {
      stdio: createStdioOptions(),
      mountPoints: [{ kind: "workspaceFolder" }],
    }

    server = await wasm.createProcess(
      "ontol-lsp",
      module,
      { initial: 30, maximum: 120, shared: true },
      options,
    )
    context.subscriptions.push({ dispose: server.terminate })

    const stderrSub = server.stderr!.onData(stderrLogger, null)
    const stdoutSub = server.stdout!.onData(stdoutLogger, null)

    context.subscriptions.push(stderrSub)
    context.subscriptions.push(stdoutSub)

    return startServer(server)
  }

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "ontol" }],
    outputChannel: channel,
    uriConverters: createUriConverters(),
  }

  if (process.env.PLATFORM === "node") {
    let { LanguageClient } = await import("vscode-languageclient/node")
    client = new LanguageClient(
      "ontol-language-client",
      "ONTOL language client",
      serverOptions,
      clientOptions,
    )
  } else {
    let { LanguageClient } = await import("vscode-languageclient/browser")
    client = new LanguageClient(
      "ontol-language-client",
      "ONTOL language client",
      serverOptions,
      clientOptions,
    )
  }

  commands.registerCommand("ontol.restartLanguageServer", async () => {
    await client.stop(3000)
    await client.start()
  })

  try {
    await client.start()
  } catch (error) {
    client.error(`Language service failed: ${error}`, error, "force")
  }
}

export async function deactivate() {
  if (!client) return
  await server.terminate()
  await client.stop()
}
