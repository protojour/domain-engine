# The `ontol` domain

The ONTOL language has a built-in domain called ***`ontol`***, which defines some essential building blocks on which to build other concepts.

Everything in the `ontol` namespace is automatically imported and available in other domains, but the `ontol` prefix may be used to disambiguate types when name collisions occur:

```ontol
def text (
    rel* is: ontol.text
)
```

The following pages detail the contents of the `ontol` domain:

- [Primitives](./primitives.md)
- [Relation types](./relation_types.md)
- [Generator types](./generator_types.md)
