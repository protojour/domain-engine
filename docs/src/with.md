# with

The `with` keyword allows you to open a new block, continuing the definition of some predefined type in the same domain.

```ontol
// type must be predefined
type some_type

with some_type {
    // ...
}
```

Imported types cannot be extended in this way. For this, see `is` relations.