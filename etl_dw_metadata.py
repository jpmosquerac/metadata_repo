#!/usr/bin/env python3
"""
ETL — Extensión del Repositorio de Metadatos para el Almacén de Datos
Universidad Javeriana — Modelos y Persistencia de Datos 2026

Fases:
  1. Metadata técnica del DW  — parsea datawarehouse.sql y carga data_source,
                                db_table, db_column para el schema dw
  2. Estructura del DW        — puebla dw_layer, dw_dimension, dw_fact_table,
                                dw_fact_dimension
  3. Procesos ETL             — registra etl_dw.py en etl_process

Prerequisito: schema_dw_extension.sql debe haberse ejecutado primero.

Ejecución:
  python etl_dw_metadata.py
  docker compose run --rm etl python etl_dw_metadata.py
"""

import re
import os
import sys
import logging
import psycopg2
from datetime import datetime

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Configuración ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("REPO_HOST",     "localhost"),
    "port":     int(os.getenv("REPO_PORT", "5433")),
    "dbname":   os.getenv("REPO_DB",       "metadata_repository"),
    "user":     os.getenv("REPO_USER",     "admin"),
    "password": os.getenv("REPO_PASSWORD", "admin123"),
}

DW_SCHEMA_FILE = os.getenv(
    "DW_SCHEMA_FILE",
    os.path.join(os.path.dirname(__file__), "..", "datawarehouse", "schema", "datawarehouse.sql"),
)
DW_ETL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "datawarehouse", "etl", "etl_dw.py"
)


# ══════════════════════════════════════════════════════════════
# FASE 1 — Parser DDL del almacén (schema dw)
# ══════════════════════════════════════════════════════════════

def parse_dw_ddl(sql: str) -> dict:
    """
    Extrae tablas y columnas del DDL de datawarehouse.sql.
    Las tablas están en el schema dw: CREATE TABLE dw.<name> (...)
    """
    tables = {}

    blocks = re.findall(
        r"CREATE TABLE\s+dw\.(\w+)\s*\((.*?)\);",
        sql, flags=re.DOTALL | re.IGNORECASE,
    )

    for table_name, body in blocks:
        primary_keys  = set()
        foreign_keys  = {}
        columns       = []

        for raw in body.split("\n"):
            line = raw.strip().rstrip(",")
            if not line:
                continue

            # PRIMARY KEY inline o constraint
            if re.match(r"PRIMARY KEY", line, re.IGNORECASE):
                for col in re.findall(r"\b(\w+)\b", line.split("(")[-1]):
                    primary_keys.add(col)
                continue

            # REFERENCES inline — capturado más abajo por la columna
            # Constraint lines
            if re.match(r"(CONSTRAINT|UNIQUE|CHECK|INDEX)", line, re.IGNORECASE):
                continue

            # Definición de columna
            col_m = re.match(r"(\w+)\s+(.+)", line)
            if not col_m:
                continue

            col_name = col_m.group(1)
            if col_name.upper() in ("PRIMARY", "FOREIGN", "UNIQUE", "CHECK",
                                     "CONSTRAINT", "CREATE", "SET", "--"):
                continue

            rest      = col_m.group(2)
            type_raw  = re.match(r"([\w ]+?(?:\(\s*[\d,]+\s*\))?)\s*(?:NOT NULL|DEFAULT|REFERENCES|PRIMARY|$)", rest, re.IGNORECASE)
            data_type = type_raw.group(1).strip() if type_raw else rest.split()[0]

            length_m  = re.search(r"\((\d+)", data_type)
            max_length = int(length_m.group(1)) if length_m else None

            dtype = re.sub(r"\s*\([^)]*\)", "", data_type).strip().upper()
            dtype = re.sub(r"CHARACTER VARYING", "VARCHAR",   dtype)
            dtype = re.sub(r"TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP", dtype)

            # FK inline: col ... REFERENCES dw.table(col)
            fk_m = re.search(r"REFERENCES\s+dw\.(\w+)\s*\((\w+)\)", rest, re.IGNORECASE)
            if fk_m:
                foreign_keys[col_name] = (fk_m.group(1), fk_m.group(2))

            # PK inline
            is_pk = bool(re.search(r"PRIMARY KEY", rest, re.IGNORECASE))
            if is_pk:
                primary_keys.add(col_name)

            # SERIAL implica PK en nuestro esquema
            if "SERIAL" in dtype:
                primary_keys.add(col_name)

            columns.append({
                "column_name":    col_name,
                "data_type":      dtype,
                "max_length":     max_length,
                "is_nullable":    "NOT NULL" not in rest.upper() and not is_pk,
                "is_primary_key": False,
                "is_foreign_key": col_name in foreign_keys,
                "fk_table":       None,
                "fk_column":      None,
            })

        # Aplicar PKs
        for c in columns:
            if c["column_name"] in primary_keys:
                c["is_primary_key"] = True
            if c["column_name"] in foreign_keys:
                c["is_foreign_key"] = True
                c["fk_table"], c["fk_column"] = foreign_keys[c["column_name"]]

        tables[table_name] = columns

    return tables


# ══════════════════════════════════════════════════════════════
# FASE 1 — Carga de metadata técnica del DW
# ══════════════════════════════════════════════════════════════

def load_dw_technical(cur, tables: dict) -> int:
    """Registra el DW como data_source y carga sus tablas y columnas."""
    log.info("  Registrando data_source: datawarehouse")
    cur.execute("""
        INSERT INTO data_source (source_name, dbms_type, schema_name, description)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_name) DO UPDATE SET
            dbms_type   = EXCLUDED.dbms_type,
            schema_name = EXCLUDED.schema_name,
            description = EXCLUDED.description
        RETURNING source_id
    """, (
        "datawarehouse",
        "PostgreSQL",
        "dw",
        "Almacén de datos analítico con esquema de constelación. "
        "Contiene 2 tablas de hechos (fact_ventas, fact_servicio) "
        "y 7 dimensiones compartidas y exclusivas.",
    ))
    source_id = cur.fetchone()[0]

    for table_name, columns in tables.items():
        cur.execute("""
            INSERT INTO db_table (source_id, table_name)
            VALUES (%s, %s)
            ON CONFLICT (source_id, table_name) DO UPDATE SET table_name = EXCLUDED.table_name
            RETURNING table_id
        """, (source_id, table_name))
        table_id = cur.fetchone()[0]

        for pos, col in enumerate(columns, 1):
            cur.execute("""
                INSERT INTO db_column (
                    table_id, column_name, data_type, max_length,
                    is_nullable, is_primary_key, is_foreign_key,
                    fk_table, fk_column, ordinal_position
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (table_id, column_name) DO UPDATE SET
                    data_type        = EXCLUDED.data_type,
                    max_length       = EXCLUDED.max_length,
                    is_nullable      = EXCLUDED.is_nullable,
                    is_primary_key   = EXCLUDED.is_primary_key,
                    is_foreign_key   = EXCLUDED.is_foreign_key,
                    fk_table         = EXCLUDED.fk_table,
                    fk_column        = EXCLUDED.fk_column,
                    ordinal_position = EXCLUDED.ordinal_position
            """, (
                table_id,
                col["column_name"], col["data_type"], col["max_length"],
                col["is_nullable"], col["is_primary_key"], col["is_foreign_key"],
                col["fk_table"], col["fk_column"], pos,
            ))

        log.info(f"    ✓ dw.{table_name} ({len(columns)} columnas)")

    return source_id


# ══════════════════════════════════════════════════════════════
# FASE 2 — Estructura del almacén (capas, dimensiones, hechos)
# ══════════════════════════════════════════════════════════════

DW_LAYERS = [
    ("extraccion",   "Lee los archivos .sql fuente, parsea INSERTs y COPYs, valida encoding y cuarentena registros con caracteres no-ASCII."),
    ("staging",      "Área transitoria para registros excluidos del DW. Almacenada como Parquet en datawarehouse/stg/ para revisión manual."),
    ("presentacion", "Tablas del schema dw (dimensiones y hechos) disponibles para consultas analíticas y reportes."),
]

DW_DIMENSIONS = [
    # (name, type, grain_desc, expected_rows)
    ("dim_tiempo",             "compartida",          "Un registro por día calendario (clave YYYYMMDD como entero).",                      1100),
    ("dim_cliente",            "compartida",          "Un registro por cliente (customer_number). Merge classicmodels + customerservice.", 122),
    ("dim_producto",           "compartida",          "Un registro por producto (product_code). Merge con 36 productos solo en cs.",       110),
    ("dim_empleado_ventas",    "exclusiva_ventas",    "Un registro por representante de ventas (classicmodels.employees).",                23),
    ("dim_oficina",            "exclusiva_ventas",    "Un registro por oficina comercial (7 sedes en 4 territorios).",                     7),
    ("dim_linea_producto",     "exclusiva_ventas",    "Un registro por línea/categoría de producto (7 categorías).",                      7),
    ("dim_empleado_servicio",  "exclusiva_servicio",  "Un registro por agente del call center (customerservice.cs_employees).",            15),
]

DW_FACTS = [
    # (name, grain_desc, layer_name, expected_rows)
    (
        "fact_ventas",
        "Una fila por línea de orden (orderdetails). Incluye medidas: cantidad, precio unitario, monto, margen y MSRP.",
        "presentacion",
        2996,
    ),
    (
        "fact_servicio",
        "Una fila por llamada de servicio al cliente (cs_customer_calls). Medidas: conteo de llamadas y presencia de notas.",
        "presentacion",
        None,
    ),
]

# (fact_name, dimension_name, fk_column_in_fact)
DW_FACT_DIMENSIONS = [
    # fact_ventas
    ("fact_ventas", "dim_tiempo",            "tiempo_key"),
    ("fact_ventas", "dim_cliente",           "cliente_key"),
    ("fact_ventas", "dim_producto",          "producto_key"),
    ("fact_ventas", "dim_empleado_ventas",   "empleado_ventas_key"),
    ("fact_ventas", "dim_oficina",           "oficina_key"),
    ("fact_ventas", "dim_linea_producto",    "linea_producto_key"),
    # fact_servicio
    ("fact_servicio", "dim_tiempo",            "tiempo_key"),
    ("fact_servicio", "dim_cliente",           "cliente_key"),
    ("fact_servicio", "dim_producto",          "producto_key"),
    ("fact_servicio", "dim_empleado_servicio", "empleado_servicio_key"),
]


def load_dw_structure(cur, dw_source_id: int):
    log.info("  Cargando capas del DW (dw_layer)...")
    layer_ids = {}
    for layer_name, description in DW_LAYERS:
        cur.execute("""
            INSERT INTO dw_layer (layer_name, description)
            VALUES (%s, %s)
            ON CONFLICT (layer_name) DO UPDATE SET description = EXCLUDED.description
            RETURNING layer_id
        """, (layer_name, description))
        layer_ids[layer_name] = cur.fetchone()[0]
        log.info(f"    ✓ {layer_name}")

    log.info("  Cargando dimensiones (dw_dimension)...")
    dim_ids = {}
    for dim_name, dim_type, grain_desc, expected_rows in DW_DIMENSIONS:
        cur.execute("""
            INSERT INTO dw_dimension (dimension_name, dim_type, grain_desc, source_id, expected_rows)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dimension_name) DO UPDATE SET
                dim_type      = EXCLUDED.dim_type,
                grain_desc    = EXCLUDED.grain_desc,
                source_id     = EXCLUDED.source_id,
                expected_rows = EXCLUDED.expected_rows
            RETURNING dimension_id
        """, (dim_name, dim_type, grain_desc, dw_source_id, expected_rows))
        dim_ids[dim_name] = cur.fetchone()[0]
        log.info(f"    ✓ {dim_name} ({dim_type})")

    log.info("  Cargando tablas de hechos (dw_fact_table)...")
    fact_ids = {}
    for fact_name, grain_desc, layer_name, expected_rows in DW_FACTS:
        cur.execute("""
            INSERT INTO dw_fact_table (fact_name, grain_desc, source_id, layer_id, expected_rows)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (fact_name) DO UPDATE SET
                grain_desc    = EXCLUDED.grain_desc,
                source_id     = EXCLUDED.source_id,
                layer_id      = EXCLUDED.layer_id,
                expected_rows = EXCLUDED.expected_rows
            RETURNING fact_id
        """, (fact_name, grain_desc, dw_source_id, layer_ids[layer_name], expected_rows))
        fact_ids[fact_name] = cur.fetchone()[0]
        log.info(f"    ✓ {fact_name}")

    log.info("  Registrando relaciones hecho-dimensión (dw_fact_dimension)...")
    for fact_name, dim_name, fk_column in DW_FACT_DIMENSIONS:
        cur.execute("""
            INSERT INTO dw_fact_dimension (fact_id, dimension_id, fk_column)
            VALUES (%s, %s, %s)
            ON CONFLICT (fact_id, dimension_id) DO UPDATE SET fk_column = EXCLUDED.fk_column
        """, (fact_ids[fact_name], dim_ids[dim_name], fk_column))
        log.info(f"    ✓ {fact_name} → {dim_name}")


# ══════════════════════════════════════════════════════════════
# FASE 3 — Procesos ETL
# ══════════════════════════════════════════════════════════════

def load_etl_process(cur):
    log.info("  Registrando proceso ETL (etl_process)...")
    script_path = os.path.normpath(DW_ETL_PATH)
    cur.execute("""
        INSERT INTO etl_process (process_name, process_type, script_path, description)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (process_name) DO UPDATE SET
            process_type = EXCLUDED.process_type,
            script_path  = EXCLUDED.script_path,
            description  = EXCLUDED.description
        RETURNING process_id
    """, (
        "etl_dw.py",
        "full_load",
        script_path,
        "ETL del almacén de datos. Lee directamente los dumps .sql (sin conexión "
        "a las BD originales), parsea MySQL INSERT y PostgreSQL COPY, cuarentena "
        "registros con caracteres no-ASCII a Parquet, y carga dimensiones y hechos "
        "con ON CONFLICT DO UPDATE para garantizar idempotencia.",
    ))
    process_id = cur.fetchone()[0]
    log.info(f"    ✓ etl_dw.py  (process_id={process_id})")
    log.info("    ℹ  etl_run_log se puebla en tiempo de ejecución por etl_dw.py")


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("  ETL — Extensión Repositorio de Metadatos (DW)")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    if not os.path.exists(DW_SCHEMA_FILE):
        log.error(f"Archivo no encontrado: {DW_SCHEMA_FILE}")
        sys.exit(1)

    log.info(f"Leyendo {DW_SCHEMA_FILE} ...")
    with open(DW_SCHEMA_FILE, "r", encoding="utf-8") as f:
        dw_sql = f.read()

    log.info("Parseando DDL del almacén (schema dw)...")
    dw_tables = parse_dw_ddl(dw_sql)
    log.info(f"  Tablas detectadas: {list(dw_tables.keys())}")

    log.info(f"Conectando a {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            log.info("── Fase 1: Metadata técnica del DW ──────────────────")
            dw_source_id = load_dw_technical(cur, dw_tables)

            log.info("── Fase 2: Estructura del almacén ────────────────────")
            load_dw_structure(cur, dw_source_id)

            log.info("── Fase 3: Procesos ETL ──────────────────────────────")
            load_etl_process(cur)

        conn.commit()
        log.info("=" * 60)
        log.info("  ✅ ETL de metadatos DW completado exitosamente")
        log.info("=" * 60)

    except Exception as exc:
        conn.rollback()
        log.error(f"Error — rollback ejecutado: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
