-- ============================================================
-- Extensión del Repositorio de Metadatos — Entrega 2
-- Nuevas entidades para describir el almacén de datos
-- Se aplica sobre la base de datos del repositorio de metadatos
-- (misma instancia que schema.sql)
-- ============================================================

-- ─── CAPA 4: Almacén de Datos ─────────────────────────────────

-- Describe las capas lógicas del almacén
CREATE TABLE IF NOT EXISTS dw_layer (
    layer_id    SERIAL       PRIMARY KEY,
    layer_name  VARCHAR(50)  NOT NULL UNIQUE,  -- 'extraccion', 'staging', 'presentacion'
    description TEXT
);

-- Registra cada dimensión del DW con su tipo y granularidad
CREATE TABLE IF NOT EXISTS dw_dimension (
    dimension_id    SERIAL       PRIMARY KEY,
    dimension_name  VARCHAR(100) NOT NULL UNIQUE,  -- 'dim_tiempo', 'dim_cliente', ...
    dim_type        VARCHAR(20)  NOT NULL,           -- 'compartida' | 'exclusiva_ventas' | 'exclusiva_servicio'
    grain_desc      TEXT,                            -- descripción de la granularidad
    source_id       INTEGER      REFERENCES data_source(source_id),  -- fuente DW target
    expected_rows   INTEGER
);

-- Registra cada tabla de hechos del DW
CREATE TABLE IF NOT EXISTS dw_fact_table (
    fact_id         SERIAL       PRIMARY KEY,
    fact_name       VARCHAR(100) NOT NULL UNIQUE,  -- 'fact_ventas' | 'fact_servicio'
    grain_desc      TEXT,
    source_id       INTEGER      REFERENCES data_source(source_id),
    layer_id        INTEGER      REFERENCES dw_layer(layer_id),
    expected_rows   INTEGER
);

-- Relación N:M entre tablas de hechos y dimensiones
CREATE TABLE IF NOT EXISTS dw_fact_dimension (
    fact_id         INTEGER NOT NULL REFERENCES dw_fact_table(fact_id),
    dimension_id    INTEGER NOT NULL REFERENCES dw_dimension(dimension_id),
    fk_column       VARCHAR(100),                   -- nombre de la FK en la tabla de hechos
    PRIMARY KEY (fact_id, dimension_id)
);

-- ─── CAPA 5: Procesos ETL ─────────────────────────────────────

-- Registra cada proceso ETL como artefacto catalogado
CREATE TABLE IF NOT EXISTS etl_process (
    process_id      SERIAL       PRIMARY KEY,
    process_name    VARCHAR(100) NOT NULL UNIQUE,  -- 'etl_dw.py'
    process_type    VARCHAR(50),                    -- 'full_load' | 'incremental'
    script_path     VARCHAR(500),
    description     TEXT,
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- Log de cada ejecución del ETL
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id              SERIAL      PRIMARY KEY,
    process_id          INTEGER     NOT NULL REFERENCES etl_process(process_id),
    started_at          TIMESTAMP   NOT NULL,
    finished_at         TIMESTAMP,
    status              VARCHAR(20),    -- 'success' | 'error' | 'partial'
    rows_loaded         INTEGER,
    rows_quarantined    INTEGER,
    error_message       TEXT
);
