# map

A `map` statement describes how one struct [`def`](def.md) maps to another (and vice versa). The `map` statement has two arms, each containing a `def` identifer.

For referring to properties between one arm and another, we use variables. Variable names can be anything, you can make them up as you go along.

All variables mentioned must be present in both arms, and all properties must be accounted for.

```ontol
map {
    def_a {       // first map arm, name of a struct def
        'foo': f  // name of a property in `def_a` and a variable name
        'bar': b  // name of a property in `def_a` and a variable name
    }
    def_b {       // second map arm, name of a struct def
        'Foo': f  // name of a property in `def_b` and a variable name
        'Bar': b  // name of a property in `def_b` and a variable name
    }
}
```

# match map

If one map arm is followed by the `match` keyword, the semantics change. `def_a` can be mapped to `def_b`, but not vice versa, and `def_b` does not need to have all properties accounted for.

```ontol
map {
    def_a {
        // ...
    }
    def_b match {
        // ...
    }
}
```
