const crlf = "\r\n"
const init = [
  '{"jsonrpc": "2.0", "method": "initialize", "id": 1, "params": {"capabilities": {}}}',
  '{"jsonrpc": "2.0", "method": "initialized", "params": {}}',
  '{"jsonrpc": "2.0", "method": "textDocument/definition", "id": 2, "params": {"textDocument": {"uri": "file://temp"}, "position": {"line": 1, "character": 1}}}'
]

process.stdin.on("data", (data) => {
  process.stderr.write(data)
})

setTimeout(() => {
  for (let msg of init) {
    const buffer = Buffer.from(
      "Content-Length: " + (msg.length + 4) +
      crlf + crlf + msg + crlf + crlf,
      "utf8"
    )
    process.stderr.write(buffer)
    process.stdout.write(buffer)
  }
}, 200)

