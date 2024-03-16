# Entities

A simple `def` just defines a type or data structure:

```ontol
def book (
    rel .'title': text
)
```

There is no way to adress a specific instance of a `book`. In reality, several different books may have the same title, or we could have several copies of the same book. To adress a specific `book`, we have to give it an identity:

```ontol
def book (
    rel .'id'|id: (rel .is: serial)
    rel .'title': text
)
```

An instance of `book` is now an _entity_. The value of the [`'id'|id`](relationship_types.md#id) property is required to be unique, and _identifies_ a unique `book`.

Notice how we do not say `rel .'id'|id: serial`. [`serial`](primitives.md#serial) is used for all sorts of ids, not just book-identifiers. Thus, `serial` itself cannot identify a book, but an anonymous type that `is: serial` can.

Relationships between types and entities are different:

```ontol
def book (
    rel .'title': text
    rel .'author': author
)

def author (
    rel .'name': text
)
```

Here, the structure of `author` becomes part of the tree-structure `book`, which is fine in some cases. But authors are (usually) unique too, and they sometimes even write more than one book. It is inefficient to include the data associated with `author` as part of `book`.

```ontol
def book (
    rel .'id'|id: (rel .is: serial)
    rel .'title': text
    rel {.}'author': author
)

def author (
    rel .'id'|id: (rel .is: serial)
    rel .'name': text
)
```

Expressing both as entities make them part of a graph. Here, a `book` has only one `author`, but an `author` may have written many `book`s.

Even further, we can express this relation as:

```ontol
rel {book} 'authors'::'books_written' {author}
```

Now, a `book` may have several `'authors'`, and an `author` may have several `'books_written'`.

Entities are an important concept in [data stores](data_stores.md) and [Interfaces](interfaces.md).
