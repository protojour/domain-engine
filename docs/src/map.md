# map

A map statement describes how one struct type maps to another (and vice versa). The map statement has two arms, each referring to a struct type.

Variable names can be anything, you can make them up as you go along.

All variables mentioned must be present in both arms, and all properties must be accounted for.

```ontol
map {
    type_a {      // first map arm, name of a struct type
        'foo': f  // name of a property in `type_a` and a variable name
        'bar': b  // name of a property in `type_a` and a variable name
    }
    type_b {      // second map arm, name of a struct type
        'Foo': f  // name of a property in `type_b` and a variable name
        'Bar': b  // name of a property in `type_b` and a variable name
    }
}
```

# one-way map

Following the `map` statement with an arrow operator `=>` indicates the mapping is one-way. `type_a` can be mapped to `type_b`, but not vice versa.

```ontol
map => {
    type_a {
        // ...
    }
    type_b {
        // ...
    }
}
```
