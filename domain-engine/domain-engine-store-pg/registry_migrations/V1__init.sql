CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS ltree;

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

-- The set of keys per datatable
CREATE TABLE m6m_reg.datafield
(
    key serial PRIMARY KEY,
    datatable_key integer NOT NULL REFERENCES m6m_reg.datatable(key),
    rel_tag integer NOT NULL,
    pg_type text NOT NULL,
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
