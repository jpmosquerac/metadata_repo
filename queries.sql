-- ============================================================
-- Consultas sobre el Repositorio de Metadatos
-- Universidad Javeriana — Modelos y Persistencia de Datos 2026
-- ============================================================


-- Q1: ¿Qué tablas existen en cada fuente de datos?
-- ─────────────────────────────────────────────────
SELECT
    ds.source_name    AS fuente,
    ds.dbms_type      AS motor,
    dt.table_name     AS tabla
FROM db_table    dt
JOIN data_source ds ON dt.source_id = ds.source_id
ORDER BY ds.source_name, dt.table_name;


-- Q2: ¿Qué columnas tiene una tabla específica?
--     (nombre, tipo, longitud, PK, FK, nulabilidad)
--     Cambiar los filtros WHERE para consultar otra tabla/fuente.
-- ─────────────────────────────────────────────────
SELECT
    dc.ordinal_position AS posicion,
    dc.column_name      AS columna,
    dc.data_type        AS tipo,
    dc.max_length       AS longitud,
    CASE WHEN dc.is_primary_key THEN 'Sí' ELSE 'No' END AS clave_primaria,
    CASE WHEN dc.is_foreign_key THEN dc.fk_table || '.' || dc.fk_column ELSE '' END AS referencia_fk,
    CASE WHEN dc.is_nullable   THEN 'Sí' ELSE 'No' END AS permite_nulo
FROM db_column   dc
JOIN db_table    dt ON dc.table_id  = dt.table_id
JOIN data_source ds ON dt.source_id = ds.source_id
WHERE ds.source_name = 'classicmodels'   -- ← cambiar fuente aquí
  AND dt.table_name  = 'orders'          -- ← cambiar tabla aquí
ORDER BY dc.ordinal_position;


-- Q3: Glosario de negocio — entidades y sus dominios
-- ─────────────────────────────────────────────────
SELECT
    dd.domain_name       AS dominio,
    be.entity_name       AS entidad,
    be.entity_definition AS definicion
FROM business_entity be
JOIN data_domain     dd ON be.domain_id = dd.domain_id
ORDER BY dd.domain_name, be.entity_name;


-- Q4: ¿Qué atributos tiene una entidad de negocio específica?
--     Cambiar el filtro WHERE para consultar otra entidad.
-- ─────────────────────────────────────────────────
SELECT
    be.entity_name          AS entidad,
    ba.attribute_name       AS atributo,
    ba.data_type            AS tipo,
    ba.attribute_definition AS definicion
FROM business_attribute ba
JOIN business_entity    be ON ba.entity_id = be.entity_id
WHERE be.entity_name = 'Cliente'   -- ← cambiar entidad aquí
ORDER BY ba.attribute_id;


-- Q5: Localización de entidades
--     ¿En qué fuente de datos y tabla está almacenada cada entidad de negocio?
-- ─────────────────────────────────────────────────
SELECT
    be.entity_name AS entidad,
    ds.source_name AS fuente,
    ds.dbms_type   AS motor,
    dt.table_name  AS tabla
FROM column_lineage     cl
JOIN db_column          dc ON cl.column_id    = dc.column_id
JOIN db_table           dt ON dc.table_id     = dt.table_id
JOIN data_source        ds ON dt.source_id    = ds.source_id
JOIN business_attribute ba ON cl.attribute_id = ba.attribute_id
JOIN business_entity    be ON ba.entity_id    = be.entity_id
GROUP BY be.entity_name, ds.source_name, ds.dbms_type, dt.table_name
ORDER BY be.entity_name, ds.source_name;


-- Q6: Linaje semántico de la tabla cs_customers
--     Para cada columna: nombre técnico, tipo de dato,
--     atributo de negocio, definición del atributo y entidad de negocio.
-- ─────────────────────────────────────────────────
SELECT
    dc.column_name          AS columna_tecnica,
    dc.data_type            AS tipo_dato,
    ba.attribute_name       AS atributo_negocio,
    ba.attribute_definition AS definicion_atributo,
    be.entity_name          AS entidad_negocio
FROM column_lineage     cl
JOIN db_column          dc ON cl.column_id    = dc.column_id
JOIN db_table           dt ON dc.table_id     = dt.table_id
JOIN data_source        ds ON dt.source_id    = ds.source_id
JOIN business_attribute ba ON cl.attribute_id = ba.attribute_id
JOIN business_entity    be ON ba.entity_id    = be.entity_id
WHERE ds.source_name = 'customerservice'
  AND dt.table_name  = 'cs_customers'
ORDER BY dc.ordinal_position;
