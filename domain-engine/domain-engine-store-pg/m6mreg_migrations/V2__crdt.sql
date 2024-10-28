CREATE TYPE m6m_property_type AS ENUM (
    'column',
    'abstract_struct',
    'abstract_crdt'
);

CREATE TYPE m6m_crdt_chunk_type AS ENUM (
    'snapshot',
    'incremental'
);

ALTER TABLE m6mreg.domain ADD COLUMN has_crdt boolean DEFAULT false;

ALTER TABLE m6mreg.property ADD COLUMN property_type m6m_property_type;

UPDATE m6mreg.property SET property_type = CASE WHEN (column_name IS NULL) THEN 'column'::m6m_property_type ELSE 'abstract_struct'::m6m_property_type END;

ALTER TABLE m6mreg.property ALTER COLUMN property_type SET NOT NULL;
