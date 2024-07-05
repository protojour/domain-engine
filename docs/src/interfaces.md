# Interfaces

If one [domain](domains.md) in your ontology is backed by a [data store](data_stores.md), one or more domains may serve as ***interfaces***, through which users and services may interact with its entities.

Entities can be exposed through _queries_, which directly or indirectly map to entities in your data store-backed domain, and allow read operations. These maps are only required to be _one-way_, since data flows only one way.

All entities which directly or indirectly map to entities in your data store-backed domain will have _mutations_ generated, which allow create, update and delete operations. Mutations involve both reading and writing, so these maps must account for all properties of the datastore entity. This usually implies _two-way_ mapping.

The details on how entities are exposed may be subject to change.


## Queries

In order to expose entities in an interface, we create named [`map`](map.md) statements called queries:

```ontol
def book_id (rel* is: serial)

def Book (
    rel. 'id': book_id
    rel* 'title': text
)

/// Single `Book` query
map book (
    book_id(id),
    @match Book('id': id)
)

/// Multiple `Book` query
map books (
    ('title'?: title),
    { ..@match Book('title'?: title) }
)
```

- The `map` statement gets a name, which identifies the query
- The first map arm is a type representing query parameters
- The second arm should `@match` an entity or a set of entities
- The order of the map arms doesn't actually matter, direction does. Data always flows _from_ a `@match`.

The `book` query matches a `Book`, and uses the `book_id` type to address a specific book. The query will have a parameter called `book_id`, whose value should correspond to a known `Book` id.

The `books` query matches a set of `Book`s (the `..` operator indicates looping over a set). The query will potentially return all `Book`s however the query will have cursor pagination parameters `first` (the number of results to return, e.g. "return the first 100" Books) and `after` (a cursor id).

`maps` arms may also be wrapped using local defs. This is what we already see with `book`, using `book_id`.

```ontol
map book (
    book_id(id),
    Book@match Book('id': id)
)

map books (
    ('title'?: title),
    { ..@match Book('title'?: title) }
)
```


### Set operators

#### `@in`

The given value must be in the set that follows.

```ontol
map books (
    (
        'name': {..name}
    ),
    {..@match Book(
        'name': @in {..name}
    )}
)
```

#### `@all_in`

The given values must all be in the set that follows.

```ontol
map books (
    (
        'name': {..name}
    ),
    {..@match Book(
        'name': @all_in {..name}
    )}
)
```

#### `@contains_all`

The set must contain all the values that follow.

```ontol
map books (
    (
        'name': {..name}
    ),
    {..@match Book(
        'name': @contains_all {..name}
    )}
)
```

#### `@intersects`

The set must intersect with the set that follows.

```ontol
map books (
    (
        'name': {..name}
    ),
    {..@match Book(
        'name': @intersects {..name}
    )}
)
```

#### `@equals`

The values (or sets) must be equal.

```ontol
map books (
    (
        'name': {..name}
    ),
    {..@match Book(
        'name': @equals {..name}
    )}
)
```


### Ordering

An _ordering_ is a relationship where the subject is an entity, the relation is the [`order`](relation_types.md#order) relation type, and the object is a [symbol](def.md#symbol).

```ontol
def entity ()
sym { symbol }
rel {entity} order[...]: symbol
```

The relation should have tuple relationships referencing one or more field names on the subject, and may have a [`direction`](relation_types.md#direction) relationship to either [`ascending`](primitives.md#ascending) (default) or [`descending`](primitives.md#descending).

```ontol
def book_id (rel* is: serial)
def Book (
    rel. 'id': book_id
    rel* 'title': text
)

sym { by_title }
rel Book order[
    rel* 0: 'title'
    rel* direction: ascending  // default, redundant
]: by_title
```

This will associate an ordering with `Book` entities, and makes the order accessible by the symbol `by_title`.

The ordering and symbol may then be used in queries, to return `Book` entities ordered by `title`:

```ontol
map books_by_title (
    (),
    {..@match Book(
        order: by_title()
    )}
)
```

Choosing an available ordering at the API level is also possible:

```ontol
map books_ordered (
    (
        'order': order
    ),
    {..@match Book(
        order: order
    )}
)
```


## GraphQL interface

Memoriam and [`ontool`](ontool.md) both expose a GraphQL interface for every domain. Let's see what is generated from a simple domain like this:

```ontol
def book_id (rel* is: serial)
def Book (
    rel. 'id'[rel* gen: auto]: book_id
    rel* 'title': text
    rel* 'author'?: Author
)

def author_id (rel* is: serial)
def Author (
    rel. 'id'[rel* gen: auto]|id: author_id
    rel* 'name': text
    rel* 'books_written'?: {Book}
)

map book (
    book_id(id),
    @match Book(
        'id'?: id
    )
)

map books (
    ('title'?: title),
    {..@match Book(
        'title'?: title,
    )},
)
```


### Queries

Queries are available through the root `Query`:

```graphql
type Query {
    book(book_id: String!): Book
    books(
        input: booksInput! = {}
        first: Int
        after: String
    ): BookConnection
}

input booksInput {
    title: String
}

type Book {
    id: ID!
    title: String!
    author: Author
}

type Author {
    id: ID!
    name: String!
    books_written(
        first: Int
        after: String
    ): BookConnection!
}

type BookConnection {
    nodes: [Book!]
    edges: [BookEdge!]
    pageInfo: PageInfo!
    totalCount: Int!
}

type BookEdge {
    node: Book!
}

type PageInfo {
    endCursor: String
    hasNextPage: Boolean!
}
```

`book` takes a `book_id` as input.

`books` takes an `input` of type `booksInput` and pagination inputs.

A `Book` has a single `author`.

An `Author` may have several `books_written`, which is another query with pagination inputs.

`BookConnection` is a utility type allowing you to access `nodes`, `edges` , `pageInfo`, and a `totalCount`.

`BookEdge` is a representation of the edge between a `Book` and `Author`, any edge properties would appear here.

`PageInfo` has an `endCursor` id, it can be used with the `after` parameter of `books` to get the next page if it `hasNextPage`.


### Mutations

Each data store-backed entity will have a mutation on the root `Mutation`:

```graphql
type Mutation {
    Book(
        create: [BookInput!] = []
        update: [BookPartialInput!] = []
        delete: [String!] = []
    ): [BookMutation!]!
    Author(
        create: [AuthorInput!] = []
        update: [AuthorPartialInput!] = []
        delete: [String!] = []
    ): [AuthorMutation!]!
}

input BookInput {
    title: String!
    author: AuthorEdgeInput
}

input BookPartialInput {
    title: String
    author: AuthorEdgeInput
}

input AuthorEdgeInput {
    id: ID
    name: String
    books_written: BookPatchEdgesInput!
}

input BookPatchEdgesInput {
    add: [BookInput!]
    update: [BookPartialInput!]
    remove: [BookPartialInput!]
}

type BookMutation {
    node: Book!
    deleted: Boolean!
}
```

We see that the `Book` mutation allows multiple `create`, `update` and `delete` operations, the default on each is an empty list.

For `create` operations, required fields are required in `BookInput`.

For `update` operations, every field is optional in `BookPartialInput`.

For `delete` operations, inputs are `book_id`s.

The result of each is a `BookMutation` with either the upated `node`, or a `deleted` result and the old `node`.

`AuthorEdgeInput` represents an edge from a `Book` to an `Author`.

If you give it an author `id`, it will create an edge to an existing `Author`.

If you give it a `name`, it will create a new `Author`.

If you give it `books_written`, you can chain additional `add`, `update` and `remove` operations

The `Author` mutation uses inputs and types following a similar pattern, they are omitted here.
