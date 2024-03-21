# `fmt`

The `fmt` statement is used for building pattern matching expressions. These are similar to (and may even include) regular expressions. The main difference is that `fmt` can include ONTOL types, and have uses beyond text patterns (TBD).

Starting from an empty text literal `''`, the `fmt` statement builds a text matching pattern, with segments linked by the arrow operator `=>`. The final segment binds the pattern to a type.

```ontol
fmt '' => 'prefix/' => uuid => prefixed_uuid
```

This describes a pattern consisting of the text literal `'prefix/'` plus any [`uuid`](primitives.md#uuid), and binds it to the type `prefixed_uuid`.

We may also use the `.` operator, and bind to the enclosing scope:

```ontol
def prefixed_uuid (
    fmt '' => 'prefix/' => uuid => .
)
```

Or, as an [anonymous `def`](def.md#anonymous-defs):

```ontol
(fmt '' => 'prefix/' => uuid => .)
```

`fmt` expressions using `serial` or `uuid` as the variable part may also be used with used with [`gen: auto`](generator_types.md#auto), which ensures only the variable part gets generated:

```ontol
rel some_def '_id'[rel .gen: auto]|id: (fmt '' => 'prefix/' => uuid => .)
```


## Regular expressions

Regular expressions can be used in a similar way to `fmt` expressions, but are limited to text, and their value cannot be generated. Used as a field value, they have a base type of `text`, but impose constraints on its contents.

```ontol
def prefixed_text (
    rel .is: /prefix--.*/
)
```

Regular expressions can be used in [`map`](map.md) expressions, which are detailed in the next chapter.
