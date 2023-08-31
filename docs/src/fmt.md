# fmt

...

## from an empty string

Starting from an empty string `''`, the `fmt` statement builds a string matching pattern. The pattern below describes a string consisting of the string `'prefix/'` plus a `uuid`.

```ontol
fmt '' => 'prefix/' => uuid
```

## from an empty sequence

Starting from an empty sequence `[]`, the `fmt` statement builds a sequence matching pattern. The pattern below describes a sequence of three consecutive strings.

```ontol
fmt [] => string => string => string
```

## usage

`fmt` statements are not very useful with context, it should refer to something. We do this by adding a final arrow `=>` operator and an identifier. A common pattern would be using the `fmt` statement as part of a [`def`](def.md):

```ontol
def some_def {
    fmt '' => 'prefix/' => uuid => .
}
```

We use the `.` operator to refer to the scope's context, similar to "self" or "this". Now `some_def` is a string consisting of the string `'prefix/'` plus a `uuid`.
