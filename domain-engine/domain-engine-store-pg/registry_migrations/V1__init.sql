CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

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
    domain_id uuid UNIQUE NOT NULL,
    name text NOT NULL,
    schema text NOT NULL
);

-- The set of persisted vertices per domain
CREATE TABLE m6m_reg.vertex
(
    domain_id uuid REFERENCES m6m_reg.domain(domain_id),
    def_tag integer NOT NULL,
    "table" text NOT NULL,

    PRIMARY KEY (domain_id, def_tag)
);

-- The set of keys per persisted vertice
CREATE TABLE m6m_reg.vertex_key
(
    domain_id uuid REFERENCES m6m_reg.domain(domain_id),
    vertex_def_tag integer NOT NULL,
    key_def_tag integer NOT NULL,
    "column" text NOT NULL,

    PRIMARY KEY (domain_id, vertex_def_tag, key_def_tag)
);
