CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA m6m_reg;

CREATE TABLE m6m_reg.domain
(
    domain_id uuid UNIQUE NOT NULL,
    name text NOT NULL,
    schema text NOT NULL
);

CREATE TABLE m6m_reg.vertex
(
    domain_id uuid REFERENCES m6m_reg.domain(domain_id),
    def_tag integer NOT NULL,
    "table" text NOT NULL,

    PRIMARY KEY (domain_id, def_tag)
);
