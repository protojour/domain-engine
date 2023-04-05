# domain-engine-wasm
WebAssembly distribution/API of domain engine

## development
Run tests in js environment (from this directory):

```
wasm-pack test --node
```

There is (currently) a bug in rust-analyzer that it reloads the entire project on each file save.
There is a [workaround](https://github.com/rust-lang/rust-analyzer/issues/6007#issuecomment-1379342831) for this bug.
