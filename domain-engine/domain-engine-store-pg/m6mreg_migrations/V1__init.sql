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

CREATE TYPE m6m_pg_domaintable_type AS ENUM (
    'vertex',
    'edge'
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
    -- the fundamental type of the table
    table_type m6m_pg_domaintable_type NOT NULL,
    -- the def domain (can be different than the owning domain)
    def_domain_key integer NOT NULL REFERENCES m6mreg.domain(key),
    -- the def tag within the def domain
    def_tag integer NOT NULL,
    -- a path of properties in the case of child tables.
    -- the root is always 'root'
    proppath ltree NOT NULL DEFAULT 'root',
    -- the name of the table within the owning domain's schema
    table_name text NOT NULL,
    -- the name of the key column, if specified
    key_column text,
    -- the name of the foreign property column, if specified
    fprop_column text,
    -- the name of the foreign key column, if specified
    fkey_column text,
    -- the name of the created timestamp column, if specified
    created_at_column text,
    -- the name of the updated timestamp column, if specified
    updated_at_column text,

    UNIQUE(def_domain_key, def_tag),

    -- def tables are keyed, edges are not
    CHECK ((table_type = 'edge') = (key_column IS NULL)),
    -- foreign key consists of fprop AND fkey
    CHECK ((fprop_column IS NULL) = (fkey_column IS NULL))
);

-- Represents a postgres index over a set of column properties
CREATE TABLE m6mreg.domaintable_index
(
    -- the domaintable implied by this DB index
    domaintable_key integer NOT NULL REFERENCES m6mreg.domaintable(key),
    -- the domain of the definition that identifies this index
    def_domain_key integer NOT NULL REFERENCES m6mreg.domain(key),
    -- the tag of the definition that identifies this index
    def_tag integer NOT NULL,
    -- the type of the index
    index_type m6m_pg_index_type NOT NULL,
    -- properties participating in the index. Must be properties of the domaintable key
    property_keys integer[] NOT NULL,

    UNIQUE (domaintable_key, def_domain_key, def_tag, index_type, property_keys)
);

-- The set of properties per domaintable
CREATE TABLE m6mreg.property
(
    key serial PRIMARY KEY,
    domaintable_key integer NOT NULL REFERENCES m6mreg.domaintable(key),
    prop_tag integer NOT NULL,

    -- if these are null, the property data is found in another table
    column_name text,
    pg_type m6m_pg_type,

    CHECK ((pg_type IS NULL) = (column_name IS NULL)),
    UNIQUE (domaintable_key, prop_tag)
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
