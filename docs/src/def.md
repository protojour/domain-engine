# def

The `def` keyword *defines* a concept or data structure. It requires a body, which may be empty.

```ontol
def some_def {}
```

A definition may be extended freely further along in the same domain.

```ontol
def some_def {}

// ...

def some_def {
    rel .'some_prop': text
}
```

[Imported](use.md) definitions cannot be extended in this way. For extending foreign `def`s, use [`is`](special_relations.md) relations:

```ontol
use 'foreign_domain' as foreign

def some_def {
    rel .is: foreign.thing
}
```

`some_def` now takes on all properties of `foreign.thing`, but may be extended (or refined) further.


# def modifiers
`def` modifiers are contextual keywords appearing within parentheses right after the keyword: `def(modifiers)`.

## private

`def(private)` makes the definition *private*. It will not be accessible to other domains if the domain is imported in a `use` statement.

```ontol
def(private) public_def
```

```ontol
def(private) some_def {}

def public_def {
    rel . 'some_def': some_def
}
```

Including `some_def` in `public_def` will make `some_def` public, but it cannot be referred to directly except through `public_def`.

## open

`def(open)` makes the definition's immediate tree-like relations _open_, so that any arbitrary data can be associated with it.
