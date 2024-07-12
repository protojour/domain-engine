CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA m6m_reg;

CREATE TABLE m6m_reg.domain
(
    id uuid PRIMARY KEY,
    unique_name text NOT NULL
);
