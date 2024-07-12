CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA m6m_reg;

CREATE TABLE m6m_reg.domain
(
    unique_name text PRIMARY KEY,
    schema_name text NOT NULL
);
