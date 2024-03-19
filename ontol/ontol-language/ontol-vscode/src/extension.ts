import * as path from "path"
import { ExtensionContext } from "vscode"
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
} from "vscode-languageclient/node"

let client: LanguageClient

export function activate(context: ExtensionContext) {
  let serverOptions: ServerOptions = {
    command: path.join(context.extensionPath, "bin", "ontool"),
    args: ["lsp"],
    options: {
      detached: true,
      shell: true,
    }
  }

  let clientOptions: LanguageClientOptions = {
    documentSelector: [
      {
        scheme: "file",
        language: "ontol",
      },
    ],
  }

  client = new LanguageClient(
    "ontol-language-server",
    "ONTOL language server",
    serverOptions,
    clientOptions,
  )

  client.start()
}

export function deactivate(): Thenable<void> | undefined {
  if (!client) return undefined
  return client.stop()
}
