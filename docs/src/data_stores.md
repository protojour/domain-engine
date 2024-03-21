# Data stores

Typically, one [domain](domains.md) in your ontology is backed by a ***data store***, through which the domain engine may create, read from, update and delete entities and entity relationships in a database.

Other domains may [`map`](map.md) to the data domain through any number of layers. The simplest setups will only have one domain, which serves both as the data store-backed domain and an [interface](interfaces.md). But databases tend to get messy and technical, and you might not want to present its raw face to the world.

All types you wish to store in the database have to be [entities](entities.md). In fact, all the entities in the data domain become collections in the database, and entity relationships become edge collections.

In other words, the data domain serves (more or less) as a _database schema_. Depending on the use case it can either be seen as the "working copy" of the database schema, or an existing database schema, modelled in ONTOL.

If you've started with ONTOL, the database will follow the structure the data domain describes.

If you already had a database and schema, or if you have other specific requirements, ensure the structure and relationships of these entities conform to your needs as closely as possible.

Ontologies also store what data store backend they are meant for. The choices in Memoriam are, for the time being, ["inmemory"](#in-memory-store) and ["arangodb"](#arangodb).


## In-memory store

The domain-engine in-memory store is a simple data structure that makes no attempt to permanently store your data or keep it safe in any way. It serves mainly as a test tool and a reference implementation for data stores.

Having such a store makes it easy to interact with your ontology during development without needing to set up an actual database. There may also be cases where the in-memory store can be used creatively, as a "zero-cost" temporary database for certain operations that you don't wish to persist.

The in-memory store uses native domain engine concepts internally, and doesn't have particular requirements when it comes to data modelling.


## ArangoDB

The ArangoDB data store is a fully-managed integration with the [ArangoDB](https://arangodb.com/) multi-model graph database.

Here's what you might want to know about how it works:


### `_id`, `_key`, and other ArangoDB-specific fields

ArangoDB has several reserved fields with special meaning:

- `_key`: document key, the primary key of an entity
- `_id`: document identifier, composed of `"collection_name/" + _key`
- `_rev`: document revision
- `_from`: document id for the source of an edge
- `_to`: document id for the target of an edge

These fields are either _ignored_ or _handled_ by the ArangoDB integration, and not made available to the user.

`_key` is the only relevant field exposed by the integration, but it gets the name and value of entity's id property. So in this simple case:

```ontol
def Book (
    rel .'id'|id: (rel .is: serial)
    rel .'title': text
)
```

`_key` gets renamed `id` and has a `serial` value (an integer).

Technically, the ArangoDB `_key` will get the value of any _variable part_ of the type set up for the entity id.

You can simulate the `_id` fields in ArangoDB like this:

```ontol
def Book (
    rel .'_id'[rel .gen: auto]|id: ( fmt '' => 'book/' => serial => . )
    rel .'title': text
)
```

Here, the value of `_id` would be formatted e.g. `"book/123"`. The `123` part is still the `_key` internally, and the prefix gets added by the domain engine.


### Collection names

The ArangoDB integration uses the names of entities in the data domain (verbatim, case sensitive) as _collection names_. These get resolved first, and cannot collide, as `def` names are required to be unique within a domain.

_Edge collections_ are named according to the subject's name for an entity relationship. Collisions may occur here, and are solved by prefixing the relation name with its subject.

This domain will generate or require three collections:

```ontol
def Book (
    rel .'id'|id: (rel .is: serial)
    rel .'title': text
    rel {.}'author': author
)

def Author (
    rel .'id'|id: (rel .is: serial)
    rel .'name': text
)
```

- `Book`
- `Author`
- `author` (edge collection)

If we used lowercase `def` names:

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

- `book`
- `author`
- `bookauthor` (edge collection, prefixed due to collision with `author`)


### Indexing

...


## Migrations

...
