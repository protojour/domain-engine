CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TYPE m6m_pg_index_type AS ENUM (
    'unique',
    'btree'
);

CREATE TYPE m6m_pg_type AS ENUM (
    'boolean',
    'integer',
    'bigint',
    'double precision',
    'text',
    'bytea',
    'timestamptz',
    'bigserial'
);

CREATE SCHEMA m6mreg;

-- The state of the migrated domain schemas outside m6mreg.
CREATE TABLE m6mreg.domain_migration
(
    version integer
);

-- Initialize the version to 1, representing _this migration file_.
INSERT INTO m6mreg.domain_migration (version) VALUES (1);

-- The set of persisted domains
CREATE TABLE m6mreg.domain
(
    key serial PRIMARY KEY,
    uid uuid UNIQUE NOT NULL,
    name text NOT NULL,
    schema_name text NOT NULL
);

-- A dynamic table created for a domain
CREATE TABLE m6mreg.domaintable
(
    key serial PRIMARY KEY,
    -- the domain which owns this data
    domain_key integer NOT NULL REFERENCES m6mreg.domain(key),
    -- the def domain (can be different than the owning domain)
    def_domain_key integer REFERENCES m6mreg.domain(key),
    -- the def tag within the def domain
    def_tag integer,
    edge_tag integer,
    -- a path of relationships in the case of child tables.
    -- the root is always 'root'
    relpath ltree NOT NULL DEFAULT 'root',
    -- the name of the table within the owning domain's schema
    table_name text NOT NULL,
    -- the name of the key column, if specified
    key_column text,

    -- the domaintable is either for an edge or a def
    CHECK ((edge_tag IS NULL) != (def_tag IS NULL)),
    -- def_domain_key and def_tag must be set at the same time
    CHECK ((def_domain_key IS NULL) = (def_tag IS NULL)),
    -- def tables are keyed, edges are not
    CHECK ((def_domain_key IS NULL) = (key_column IS NULL))
);

-- Represents a postgres unique constraint over a field tuple
CREATE TABLE m6mreg.domaintable_index
(
    -- the domaintable implied by this unique group
    domaintable_key integer NOT NULL REFERENCES m6mreg.domaintable(key),
    -- the domain of the definition that identifies this index
    def_domain_key integer NOT NULL REFERENCES m6mreg.domain(key),
    -- the tag of the definition that identifies this index
    def_tag integer NOT NULL,
    -- the type of the index
    index_type m6m_pg_index_type NOT NULL,
    -- datafield participating in the index. Must be fields of the domaintable key
    datafield_keys integer[] NOT NULL,

    UNIQUE (domaintable_key, def_domain_key, def_tag, index_type)
);

-- The set of keys per domaintable
CREATE TABLE m6mreg.datafield
(
    key serial PRIMARY KEY,
    domaintable_key integer NOT NULL REFERENCES m6mreg.domaintable(key),
    rel_tag integer NOT NULL,
    pg_type m6m_pg_type NOT NULL,
    column_name text NOT NULL
);

-- Registry of edge cardinals
CREATE TABLE m6mreg.edgecardinal
(
    key serial PRIMARY KEY,
    domaintable_key integer NOT NULL REFERENCES m6mreg.domaintable(key),
    ordinal integer NOT NULL,
    ident text NOT NULL,
    def_column_name text,
    pinned_domaintable_key integer REFERENCES m6mreg.domaintable(key),
    key_column_name text,
    index_type m6m_pg_index_type,

    UNIQUE (domaintable_key, ordinal),
    CHECK ((key_column_name IS NULL) = ((def_column_name IS NULL) AND (pinned_domaintable_key IS NULL))),
    CHECK (pinned_domaintable_key != domaintable_key)
);
