-- Schema
CREATE SCHEMA IF NOT EXISTS ingestion;

-- 1. Dedup state
CREATE TABLE IF NOT EXISTS ingestion.dedup_state (
    source          TEXT NOT NULL,
    identity        TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    bronze_uri      TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, identity)
);
CREATE INDEX IF NOT EXISTS idx_dedup_content
    ON ingestion.dedup_state (source, content_hash);

-- 2. Raw documents (connector → extractor handoff)
CREATE TABLE IF NOT EXISTS ingestion.raw_documents (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source            TEXT NOT NULL,
    identity          TEXT NOT NULL,
    bronze_uri        TEXT NOT NULL,
    content_hash      TEXT NOT NULL,
    content_type      TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    source_uri        TEXT NOT NULL,
    connector_version TEXT NOT NULL,
    retrieved_at      TIMESTAMPTZ NOT NULL,
    file_size_bytes   BIGINT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, identity)
);

-- 3. Extractor registry (config table)
CREATE TABLE IF NOT EXISTS ingestion.extractor_registry (
    source          TEXT NOT NULL,
    extractor_name  TEXT NOT NULL,
    PRIMARY KEY (source, extractor_name)
);

-- Seed data for prototype
INSERT INTO ingestion.extractor_registry (source, extractor_name) VALUES
    ('nih_idsr',            'nih_idsr_disease_tables'),
    ('pitb_dss',            'pitb_dss_disease_tables'),
    ('ajk_idsrs',           'ajk_idsrs_disease_tables'),
    ('dhis_punjab_weekly',  'dhis_punjab_disease_tables')
ON CONFLICT DO NOTHING;

-- 4. Extractor status (per document x per extractor)
CREATE TABLE IF NOT EXISTS ingestion.extractor_status (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_document_id   BIGINT NOT NULL REFERENCES ingestion.raw_documents(id),
    extractor_name    TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',
    records_produced  INT,
    error_message     TEXT,
    error_at          TIMESTAMPTZ,
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (raw_document_id, extractor_name)
);
CREATE INDEX IF NOT EXISTS idx_extractor_status_pending
    ON ingestion.extractor_status (extractor_name, status)
    WHERE status = 'pending';

-- 5. Connector runs (operational audit)
CREATE TABLE IF NOT EXISTS ingestion.connector_runs (
    run_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source            TEXT NOT NULL,
    connector_version TEXT NOT NULL,
    discovered        INT NOT NULL DEFAULT 0,
    fetched           INT NOT NULL DEFAULT 0,
    archived          INT NOT NULL DEFAULT 0,
    skipped_identity  INT NOT NULL DEFAULT 0,
    skipped_content   INT NOT NULL DEFAULT 0,
    errors            INT NOT NULL DEFAULT 0,
    duration_ms       INT NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
