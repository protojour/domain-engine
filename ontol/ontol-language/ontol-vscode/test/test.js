const crlf = "\r\n"
const init = [
  '{"jsonrpc": "2.0", "method": "initialize", "id": 1, "params": {"capabilities": {}}}',
  '{"jsonrpc": "2.0", "method": "initialized", "params": {}}',
  '{"jsonrpc": "2.0", "method": "textDocument/definition", "id": 2, "params": {"textDocument": {"uri": "file://temp"}, "position": {"line": 1, "character": 1}}}',
]

for (let msg of init) {
  setTimeout(() => {
    const buffer = Buffer.from("Content-Length: " + msg.length + crlf + crlf + msg, "utf8")
    process.stderr.write("\n\nCLIENT says ------- \n\n" + buffer + "\n\nSERVER says ------- \n\n")
    process.stdout.write(buffer)
  }, (init.indexOf(msg) + 1) * 100)
}
