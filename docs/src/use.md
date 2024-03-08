# `use`

The `use` keyword is used to ***import*** another [domain](domains.md). A local namespace alias for the imported domain is required.

```ontol
use 'domain' as dom
```

Public (non-`private`) [`def`](def.md)s from the domain `'domain'` are now available under the `dom` namespace:

```ontol
def some_def (
    rel .'domain_def_a': dom.def_a
)
```

Imported definitions cannot be extended in the same way as `def`s in the current domain.

For extending foreign `def`s, use [`is`](relationship_types.md#is) relationships:

```ontol
use 'foreign_domain' as foreign

def some_def (
    rel .is: foreign.thing
)
```

`some_def` now takes on all properties of `foreign.thing`, and can be extended (or refined).
