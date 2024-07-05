# `def`

The `def` keyword ***defines*** a concept, type or data structure.

The first parameter is the _name_ of the defintion. The name may include alphanumeric characters (upper and lower case), dashes and underscores, but must start with a letter.

It requires a body, enclosed by `()` parentheses, which may be empty.

```ontol
def some_def ()
```

Expressions within the body of a statement are usually related to that statement. A definition may be also be extended freely further along in the same domain with additional def statements.

```ontol
def some_def ()

// ...

def some_def (
    rel* 'some_prop': text
)
```


## Anonymous `def`s

It is possible to create _anonymous_ definitions. They are often used inline with other expressions.

```ontol
def some_def (
    rel* 'some_prop': (rel* is: text)
)
```

`(rel* is: text)` is an anonymous `def` "with the properties of" [`text`](primitives.md#text) — you can read more on what this means in the next chapter.

Anonymous `def`s usually cannot be accessed unless they are bound to something, and may have names generated for them if necessary. They have their uses, but it is usually better to name them.


## `def` modifiers

`def` modifiers are contextual keywords appearing right after the keyword: `def @modifier`. Multiple modifiers may be combined, in any order.


### `@private`

`def @private` makes a definition _private_. This means it is intended for internal use, and it will not be accessible to other domains if the domain is imported in a `use` statement.

```ontol
def @private private_def ()

def public_def (
    rel* 'exposed': private_def
)
```

Note that including `private_def` in `public_def` will indirectly expose `private_def`, but it cannot be referred to except through `public_def`.

Private defs are also excluded from [data stores](data_stores.md) and [interfaces](interfaces.md), unless they are indirectly exposed this way.


### `@open`

`def @open` makes the definition's immediate tree-like relationships _open_, which means arbitrary data can be associated with it.

An open def may have conventional properties added like any other, but it will also accept and store any addtional data you pass it.

```ontol
def @open all_yours (
    rel* 'predefined_property': text
)
```


### `@extern` (advanced, _unstable_)

***Note:*** _The details of `def @extern` are subject to change._

`def @extern` defines an _external_ HTTP hook.

The definition must include an `'url'` property and a [`map`](map.md) statement. When the domain engine routes data from input to output, the HTTP hook is called with a POST request.

```ontol
def input (
    rel* 'prop': text
)

def output (
    rel* 'prop': text
    rel* 'additional_prop': text
)

def @extern hook (
    rel* 'url': 'http://localhost:8080/listener'
    map(input(), output())
)
```

Here, the listener, a server endpoint at the url defined in `hook` can expect to get a JSON structure on the form of `input`, and is expected to respond with a JSON structure corresponding to `output` — in this simple case, it is expected to supply the text property `'additional_prop'`.
