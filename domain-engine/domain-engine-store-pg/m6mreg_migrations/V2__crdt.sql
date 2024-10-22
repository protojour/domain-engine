CREATE TYPE m6m_crdt_chunk_type AS ENUM (
    'snapshot',
    'incremental'
);

ALTER TABLE m6mreg.domain ADD COLUMN has_crdt boolean DEFAULT false;
