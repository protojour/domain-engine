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

CREATE SCHEMA m6m_reg;

-- The state of the migrated domain schemas outside m6m_reg.
CREATE TABLE m6m_reg.domain_migration
(
    version integer
);

-- Initialize the version to 1, representing _this migration file_.
INSERT INTO m6m_reg.domain_migration (version) VALUES (1);

-- The set of persisted domains
CREATE TABLE m6m_reg.domain
(
    key serial PRIMARY KEY,
    uid uuid UNIQUE NOT NULL,
    name text NOT NULL,
    schema_name text NOT NULL
);

-- A data table with actual data
CREATE TABLE m6m_reg.datatable
(
    key serial PRIMARY KEY,
    -- the domain which owns this data
    domain_key integer NOT NULL REFERENCES m6m_reg.domain(key),
    -- the def domain (can be different than the owning domain)
    def_domain_key integer NOT NULL REFERENCES m6m_reg.domain(key),
    -- the def tag within the def domain
    def_tag integer NOT NULL,
    -- a path of relationships in the case of child tables.
    -- the root is always 'vtx'
    relpath ltree NOT NULL DEFAULT 'vtx',
    -- the name of the table within the owning domain's schema
    table_name text NOT NULL,
    -- the name of the key column
    key_column text NOT NULL
);

-- Represents a postgres unique constraint over a field tuple
CREATE TABLE m6m_reg.datatable_index
(
    -- the datatable implied by this unique group
    datatable_key integer NOT NULL REFERENCES m6m_reg.datatable(key),
    -- the domain of the definition that identifies this index
    def_domain_key integer NOT NULL REFERENCES m6m_reg.domain(key),
    -- the tag of the definition that identifies this index
    def_tag integer NOT NULL,
    -- the type of the index
    index_type m6m_pg_index_type NOT NULL,
    -- datafield participating in the index. Must be fields of the datatable key
    datafield_keys integer[] NOT NULL,

    UNIQUE (datatable_key, def_domain_key, def_tag, index_type)
);

-- The set of keys per datatable
CREATE TABLE m6m_reg.datafield
(
    key serial PRIMARY KEY,
    datatable_key integer NOT NULL REFERENCES m6m_reg.datatable(key),
    rel_tag integer NOT NULL,
    pg_type m6m_pg_type NOT NULL,
    column_name text NOT NULL
);

-- Registry of edge tables
CREATE TABLE m6m_reg.edgetable
(
    key serial PRIMARY KEY,
    domain_key integer NOT NULL REFERENCES m6m_reg.domain(key),
    edge_tag integer NOT NULL,
    table_name text NOT NULL
);

-- Registry of edge cardinals
CREATE TABLE m6m_reg.edgecardinal
(
    key serial PRIMARY KEY,
    edge_key integer NOT NULL REFERENCES m6m_reg.edgetable(key),
    ordinal integer NOT NULL,
    ident text NOT NULL,
    def_column_name text NOT NULL,
    key_column_name text NOT NULL,

    UNIQUE (edge_key, ordinal)
);
