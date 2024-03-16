# `def`

The `def` keyword ***defines*** a concept, type or data structure. The name may include alphanumeric characters (upper and lower case), dashes and underscores, but must start with a letter. It requires a body, enclosed by `()` parentheses, which may be empty.

```ontol
def some_def ()
```

Expressions within the body of a statement are usually related to that statement. A definition may be extended freely further along in the same domain.

```ontol
def some_def ()

// ...

def some_def (
    rel . 'some_prop': text
)
```


## Anonymous `def`s

It is possible to use _anonymous_ `def`s. They usually appear inline in other expressions:

```ontol
def some_def (
    rel . 'some_prop': (rel .is: text)
)
```

`(rel .is: text)` is an anonymous `def` "with the properties of" [`text`](primitives.md#text). More on this in the next chapter.


## `def` modifiers

`def` modifiers are contextual keywords appearing right after the keyword: `def @modifier`. Multiple modifiers are possible.


### `@private`

`def @private` makes the definition *private*. It will not be accessible to other domains if the domain is imported in a `use` statement.

```ontol
def @private private_def ()

def public_def (
    rel . 'exposed': private_def
)
```

Including `private_def` in `public_def` will indirectly expose `private_def`, but it cannot be referred to except through `public_def`.


### `@open`

`def @open` makes the definition's immediate tree-like relationships _open_, so that any arbitrary data can be associated with it.

```ontol
def @open all_yours ()
```


### `@symbol`

`def @symbol` creates a _symbol_, an otherwise empty definition representing the definition name itself.

```ontol
def @symbol symbolic ()
```

This is technically equivalent to:

```ontol
def symbolic (rel .is: 'symbolic')
```


### `@extern` (unstable)

_The details of `def @extern` are subject to change._

`def @extern` defines an _external_ hook, and must include an `'url'` property and a [`map`](map.md) statement.

```ontol
def input (
    rel . 'prop': text
)

def output (
    rel . 'prop': text
    rel . 'additional_prop': text
)

def @extern hook (
    rel . 'url': 'http://localhost:8080/listener'
    map(input(), output())
)
```

Here, the listener, a server endpoint at the url defined in `hook` can expect to get a structure on the form of `input`, and is expected to respond with a structure corresponding to `output` – in other words, it is expected to supply the text property `'additional_prop'`.
