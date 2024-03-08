# Comments

ONTOL files may include comments, indicated by `//`. Everything from the start of the comment to the next line break is completely discarded by the compiler.

```ontol
// this is a comment

def some_def () // this is also a comment
```

# Doc comments

Certain ONTOL structures (`def`, `rel`, `fmt`, `map`) may also include doc comments, indicated by `///`. Doc comments are not discarded, but kept as documentation of the statement immediately following it.

Multi-line comments are joined automatically, may include Markdown syntax, and are used in API documentation and the ONTOL Language Server.

```ontol
/// This is a doc comment on `some_def`.
/// It may span multiple lines.
def some_def (
    /// This is documentation for `some_field`
    rel .'some_field': text
    // ...
)
```
