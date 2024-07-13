CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA m6m_reg;

CREATE TABLE m6m_reg.persistent_domain
(
    domain_id uuid UNIQUE NOT NULL,
    m6m_id serial UNIQUE,
    unique_name text NOT NULL,
    schema_name text NOT NULL
);

CREATE TABLE m6m_reg.vertice
(
    id serial UNIQUE NOT NULL,
    m6m_domain integer REFERENCES m6m_reg.persistent_domain(m6m_id)
);
