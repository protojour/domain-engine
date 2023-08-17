# fmt

## from an empty string

Starting from an empty string `''`, the `fmt` statement builds a string matching pattern. The pattern below describes a string consisting of the string `'prefix/'` plus an `uuid`.

```ontol
fmt '' => 'prefix/' => uuid => .
```

## from an empty sequence

Starting from an empty sequence `[]`, the `fmt` statement builds a sequence matching pattern. The pattern below describes a sequence of three consecutive strings.

```ontol
fmt [] => string => string => string => .
```