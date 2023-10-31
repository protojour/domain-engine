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

## pub

`def(pub)` makes the definition *public*. It will be accessible to other domains if the domain is imported in a `use` statement, or if the domain is exposed as an API.

```ontol
def(pub) public_def
```

`pub` is transitive.

```ontol
def some_def {}

def(pub) public_def {
    rel . 'some_def': some_def
}
```

Including `some_def` in `public_def` will make `some_def` public, but it cannot be referred to directly except through `public_def`.
