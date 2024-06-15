# `sym`

Symbols are text constants with domain-specific meaning.
Symbols are declared within a domain using the `sym` statement, which declares a _symbol group_.

```ontol
sym { symbolic }
```

This statement creates one symbol called `symbolic`, and it's technically equivalent to

```ontol
def symbolic (rel .is: 'symbolic')
```

which also declares a domain-specific subtype of that text literal.

The `sym` statement can declare any number of symbols within the same group:

```ontol
sym {
    /// The first symbol in this group
    first,

    /// The second symbol in this group
    second,
}
```

## Symbols in ONTOL
The same word can mean different things in different contexts, and this makes symbols very useful within domain thinking.

That a symbol belongs to a domain, forces external domains to specify the _namespace_ of the symbol to disambiguate its meaning.

Symbols are currently used for [_ordering_](interfaces.md#ordering).


## Symbol groups for edge declaration (experimental)
TBD
