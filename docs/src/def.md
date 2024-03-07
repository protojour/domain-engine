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
    rel .'some_prop': text
)
```


## Anonymous `def`s

It is possible to use _anonymous_ `def`s. They usually appear inline in other expressions:

```ontol
def some_def (
    rel .'some_prop': (rel .is: text)
)
```

`(rel .is: text)` is an anonymous `def`, separate from, but with the properties of [`text`](primitives.md#text).


## `def` modifiers

`def` modifiers are contextual keywords appearing within parentheses right after the keyword: `def(modifiers)`.

Multiple modifiers are separated by a `|` character: `def(private|open)`


### `private`

`def(private)` makes the definition *private*. It will not be accessible to other domains if the domain is imported in a `use` statement.

```ontol
def(private) private_def ()

def public_def (
    rel .'exposed': private_def
)
```

Including `private_def` in `public_def` will indirectly expose `private_def`, but it cannot be referred to except through `public_def`.


### `open`

`def(open)` makes the definition's immediate tree-like relationships _open_, so that any arbitrary data can be associated with it.

```ontol
def(open) all_yours ()
```


### `extern` (unstable)

_The details of `def(extern)` are subject to change._

`def(extern)` defines an external hook, and must include an `'url'` property and a [`map`](map.md) statement.

```ontol
def input (
    rel .'prop': text
)

def output (
    rel .'prop': text
    rel .'additional_prop': text
)

def(extern) hook (
    rel .'url': 'http://localhost:8080/listener'
    map(input(), output())
)
```

Here, the listener, a server endpoint at the url defined in `hook` can expect to get a structure on the form of `input`, and is expected to respond with a structure corresponding to `output` – in other words, it is expected to supply the property `'additional_prop'`.


## Unions

Unions are also defined using `def`, using conditional [`is`](relationship_types.md#is) relationships. Union variants must have a discriminator for the domain engine to tell them apart; either their primary key structure is different or some inherent property is, such as a constant:

```ontol
def type_a (
    rel .'type': 'a'
)

def type_b (
    rel .'type': 'b'
)

def union_ab (
    rel .is?: type_a
    rel .is?: type_b
)
```

The union `union_ab` may now be used as any other type.

```ontol
def some_def (
    rel .'a_or_b': union_ab
)
```
