# use

The `use` keyword is used to import another domain.

```ontol
use 'domain'
```

Public (`pub`) types from `'domain'` are now available under the `domain` namespace:

```ontol
type some_type {
    rel . 'domain_type_a': domain.type_a
}
```

# use as

The `as` keyword allows an alias to be used for the imported namespace:

```ontol
use 'domain' as alias

type some_type {
    rel . 'alias_type_a': alias.type_a
}
```
