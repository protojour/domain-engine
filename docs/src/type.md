# type

The `type` keyword defines a type.

```ontol
type some_type
```

# pub type

Prefixing a `type` by `pub` defines the type as public. It will be exposed if the domain is imported in a `use` statement, or if the domain is exposed as an API.

```ontol
pub type public_type
```

`pub` is transitive.

```ontol
type some_type

pub type public_type {
    rel . 'some_type': some_type
}
```

Including `some_type` in `public_type` will make `some_type` public, but it cannot be referred to directly except through `public_type`.