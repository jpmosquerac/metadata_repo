-- ============================================================
-- Repositorio de Metadatos
-- Universidad Javeriana — Modelos y Persistencia de Datos 2026
-- ============================================================

-- ─── CAPA 1: Metadata Técnica ────────────────────────────────

CREATE TABLE IF NOT EXISTS data_source (
    source_id   SERIAL       PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    dbms_type   VARCHAR(50)  NOT NULL,
    host        VARCHAR(200),
    port        INTEGER,
    schema_name VARCHAR(100),
    description TEXT,
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS db_table (
    table_id   SERIAL       PRIMARY KEY,
    source_id  INTEGER      NOT NULL REFERENCES data_source(source_id),
    table_name VARCHAR(100) NOT NULL,
    row_count  INTEGER,
    description TEXT,
    UNIQUE (source_id, table_name)
);

CREATE TABLE IF NOT EXISTS db_column (
    column_id        SERIAL       PRIMARY KEY,
    table_id         INTEGER      NOT NULL REFERENCES db_table(table_id),
    column_name      VARCHAR(100) NOT NULL,
    data_type        VARCHAR(100) NOT NULL,
    max_length       INTEGER,
    is_nullable      BOOLEAN,
    is_primary_key   BOOLEAN      DEFAULT FALSE,
    is_foreign_key   BOOLEAN      DEFAULT FALSE,
    fk_table         VARCHAR(100),
    fk_column        VARCHAR(100),
    ordinal_position INTEGER,
    UNIQUE (table_id, column_name)
);

-- ─── CAPA 2: Metadata de Negocio ─────────────────────────────

CREATE TABLE IF NOT EXISTS data_domain (
    domain_id   SERIAL       PRIMARY KEY,
    domain_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS business_entity (
    entity_id         SERIAL       PRIMARY KEY,
    domain_id         INTEGER      NOT NULL REFERENCES data_domain(domain_id),
    entity_name       VARCHAR(100) NOT NULL,
    entity_definition TEXT,
    UNIQUE (domain_id, entity_name)
);

CREATE TABLE IF NOT EXISTS business_attribute (
    attribute_id         SERIAL       PRIMARY KEY,
    entity_id            INTEGER      NOT NULL REFERENCES business_entity(entity_id),
    attribute_name       VARCHAR(200) NOT NULL,
    attribute_definition TEXT,
    data_type            VARCHAR(100),
    UNIQUE (entity_id, attribute_name)
);

-- ─── CAPA 3: Linaje Semántico ─────────────────────────────────

CREATE TABLE IF NOT EXISTS column_lineage (
    lineage_id   SERIAL  PRIMARY KEY,
    column_id    INTEGER NOT NULL REFERENCES db_column(column_id),
    attribute_id INTEGER NOT NULL REFERENCES business_attribute(attribute_id),
    UNIQUE (column_id, attribute_id)
);
