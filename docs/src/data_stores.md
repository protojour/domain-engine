# Data stores

Typically, one domain in your ontology is backed by a data store, through which the domain engine may create, read from, update and delete its entities and entity relationships in a database.

In other words, the data store-backed domain serves as an indirect database schema.

If you have specific requirements, ensure the structure and relationships of these datamodels conform to your needs as closely as possible.

The ONTOL compiler is deterministic enough to allow custom tools to generate database schemas based on its output.


## Data store API

The domain engine will typically give the data store a single (read) query, or a list of one or more write requests. The database integration layer does its best to analyze and rearrange these into one or more queries and/or transactions, optimizing for speed and correctness.


## In-memory store

The domain-engine In-memory store is a simple data structure that makes no attempt to permanently store your data or keep it safe in any way. It serves mainly as a test tool and reference implementation of the data store API.

Having such a store makes it easy to interact with your ontology during development without needing to set up an actual database. There may also be cases where the in-memory store can be used creatively, as a "zero-cost" temporary database for certain operations that you don't wish to persist.


## ArangoDB

The ArangoDB data store is a fully-managed integration with the ArangoDB multi-model graph database

The following features are employed (or are planned):

- Managed authentication and authorization
- Database, collection and index provisioning
- Solid schema- and data migration system based on domain knowledge
- Highly optimized queries
- Analytics that leverage built-in AQL aggregation and analysis functionality


## Search engine
