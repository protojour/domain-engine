# Comments

ONTOL files may include comments, indicated by `//`. Everything from the start of the comment to the next line break is completely discarded by the compiler.

```ontol
// this is a comment

def some_def () // this is also a comment
```

## Doc comments

Some ONTOL statements (`def`, `rel`, `fmt`, `map`) may also include doc comments, indicated by `///`. Multi-line comments are joined automatically, and may include Markdown syntax.

Doc comments are not discarded, but kept as documentation of the statement immediately following it, and is used in API documentation and the ONTOL Language Server.


```ontol
/// This is a doc comment for `some_def`.
/// It may span multiple lines and include **Markdown**.
def some_def (
    /// This is documentation for `some_field`
    rel .'some_field': text
    // ...
)
```
