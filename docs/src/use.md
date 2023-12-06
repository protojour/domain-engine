# use

The `use` keyword is used to import another domain. A local name is required.

```ontol
use 'domain' as dom
```

Public (`pub`) [`def`](def.md)s from `'domain'` are now available under the `dom` namespace:

```ontol
def some_def (
    rel . 'domain_def_a': dom.def_a
)
```
