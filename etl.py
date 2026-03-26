#!/usr/bin/env python3
"""
ETL — Repositorio de Metadatos
Universidad Javeriana — Modelos y Persistencia de Datos 2026

Fases:
  1. Metadata técnica  — parsea DDL de los .sql y carga data_source, db_table, db_column
  2. Metadata de negocio — carga data_domain, business_entity, business_attribute
  3. Linaje semántico  — mapea db_column ↔ business_attribute en column_lineage

Ejecución:
  python etl.py                         # usa variables de entorno o defaults
  docker compose run --rm etl           # dentro del stack Docker
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

# ─── Configuración (sobreescribible por variables de entorno) ──
DB_CONFIG = {
    "host":     os.getenv("REPO_HOST",     "localhost"),
    "port":     int(os.getenv("REPO_PORT", "5433")),
    "dbname":   os.getenv("REPO_DB",       "metadata_repository"),
    "user":     os.getenv("REPO_USER",     "admin"),
    "password": os.getenv("REPO_PASSWORD", "admin123"),
}

DATA_DIR   = os.getenv("DATA_SOURCES_DIR", "./data_sources")
MYSQL_FILE = os.path.join(DATA_DIR, "mysqlsampledatabase.sql")
PG_FILE    = os.path.join(DATA_DIR, "customerservice.sql")


# ══════════════════════════════════════════════════════════════
# FASE 1 — Parsers de DDL
# ══════════════════════════════════════════════════════════════

def parse_mysql_ddl(sql: str) -> dict:
    """Extrae tablas y columnas del dump MySQL (classicmodels)."""
    tables = {}

    blocks = re.findall(
        r"CREATE TABLE\s+`?(\w+)`?\s*\((.*?)\)\s*ENGINE",
        sql, flags=re.DOTALL | re.IGNORECASE,
    )

    for table_name, body in blocks:
        primary_keys = set()
        foreign_keys = {}
        columns = []

        for raw in body.split("\n"):
            line = raw.strip().rstrip(",")
            if not line:
                continue

            # PRIMARY KEY (simple o compuesto)
            if re.match(r"PRIMARY KEY", line, re.IGNORECASE):
                for col in re.findall(r"`(\w+)`", line):
                    primary_keys.add(col)
                continue

            # FOREIGN KEY
            fk = re.match(
                r"(?:CONSTRAINT\s+`\w+`\s+)?FOREIGN KEY\s*\(`?(\w+)`?\)\s*"
                r"REFERENCES\s*`?(\w+)`?\s*\(`?(\w+)`?\)",
                line, re.IGNORECASE,
            )
            if fk:
                foreign_keys[fk.group(1)] = (fk.group(2), fk.group(3))
                continue

            # Otras restricciones / índices
            if re.match(r"(KEY|UNIQUE|INDEX|CONSTRAINT)", line, re.IGNORECASE):
                continue

            # Definición de columna
            col_m = re.match(r"`?(\w+)`?\s+(.+)", line)
            if not col_m:
                continue

            col_name = col_m.group(1)
            rest     = col_m.group(2)

            type_raw = re.match(r"(\w+(?:\s*\(\s*[\d,]+\s*\))?)", rest)
            data_type = type_raw.group(1) if type_raw else rest.split()[0]

            length_m = re.search(r"\((\d+)", data_type)
            max_length = int(length_m.group(1)) if length_m else None

            dtype = re.sub(r"\s*\([^)]*\)", "", data_type).strip().upper()

            columns.append({
                "column_name":    col_name,
                "data_type":      dtype,
                "max_length":     max_length,
                "is_nullable":    "NOT NULL" not in rest.upper(),
                "is_primary_key": False,
                "is_foreign_key": False,
                "fk_table":       None,
                "fk_column":      None,
            })

        # Aplicar PKs y FKs
        for c in columns:
            if c["column_name"] in primary_keys:
                c["is_primary_key"] = True
            if c["column_name"] in foreign_keys:
                c["is_foreign_key"] = True
                c["fk_table"], c["fk_column"] = foreign_keys[c["column_name"]]

        tables[table_name] = columns

    return tables


def parse_postgres_ddl(sql: str) -> dict:
    """Extrae tablas y columnas del dump PostgreSQL (customerservice)."""
    tables = {}

    blocks = re.findall(
        r"CREATE TABLE\s+(?:public\.)?(\w+)\s*\((.*?)\);",
        sql, flags=re.DOTALL | re.IGNORECASE,
    )

    for table_name, body in blocks:
        columns = []

        for raw in body.split("\n"):
            line = raw.strip().rstrip(",")
            if not line:
                continue
            if re.match(r"(CONSTRAINT|PRIMARY KEY|FOREIGN KEY|UNIQUE|CHECK)", line, re.IGNORECASE):
                continue

            col_m = re.match(r"(\w+)\s+(.+)", line)
            if not col_m:
                continue

            col_name = col_m.group(1)
            if col_name.upper() in ("PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT", "NOT"):
                continue

            rest     = col_m.group(2)
            type_raw = re.match(r"([\w ]+?(?:\(\s*[\d,]+\s*\))?)\s*(?:NOT NULL|DEFAULT|$)", rest, re.IGNORECASE)
            data_type = type_raw.group(1).strip() if type_raw else rest.split()[0]

            length_m = re.search(r"\((\d+)", data_type)
            max_length = int(length_m.group(1)) if length_m else None

            dtype = re.sub(r"\s*\([^)]*\)", "", data_type).strip().upper()
            dtype = re.sub(r"CHARACTER VARYING", "VARCHAR",   dtype)
            dtype = re.sub(r"TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP", dtype)
            dtype = re.sub(r"\bCHARACTER\b", "CHAR", dtype)

            columns.append({
                "column_name":    col_name,
                "data_type":      dtype,
                "max_length":     max_length,
                "is_nullable":    "NOT NULL" not in rest.upper(),
                "is_primary_key": False,
                "is_foreign_key": False,
                "fk_table":       None,
                "fk_column":      None,
            })

        tables[table_name] = columns

    # PKs desde ALTER TABLE
    for tbl, pk_cols in re.findall(
        r"ALTER TABLE\s+(?:ONLY\s+)?(?:public\.)?(\w+)\s+ADD CONSTRAINT\s+\w+\s+PRIMARY KEY\s*\(([^)]+)\)",
        sql, re.IGNORECASE,
    ):
        if tbl in tables:
            pks = {c.strip() for c in pk_cols.split(",")}
            for col in tables[tbl]:
                if col["column_name"] in pks:
                    col["is_primary_key"] = True

    # FKs desde ALTER TABLE
    for tbl, fk_col, ref_tbl, ref_col in re.findall(
        r"ALTER TABLE\s+(?:ONLY\s+)?(?:public\.)?(\w+)\s+ADD CONSTRAINT\s+\w+\s+"
        r"FOREIGN KEY\s*\(([^)]+)\)\s*REFERENCES\s+(?:public\.)?(\w+)\s*\(([^)]+)\)",
        sql, re.IGNORECASE,
    ):
        if tbl in tables:
            fk_list  = [c.strip() for c in fk_col.split(",")]
            ref_list = [c.strip() for c in ref_col.split(",")]
            for col in tables[tbl]:
                if col["column_name"] in fk_list:
                    idx = fk_list.index(col["column_name"])
                    col["is_foreign_key"] = True
                    col["fk_table"]  = ref_tbl
                    col["fk_column"] = ref_list[idx] if idx < len(ref_list) else ref_list[0]

    return tables


# ══════════════════════════════════════════════════════════════
# FASE 1 — Carga de metadata técnica
# ══════════════════════════════════════════════════════════════

def load_technical(cur, source_name, dbms_type, schema_name, description, tables):
    log.info(f"  Fuente: {source_name} ({dbms_type})")

    cur.execute("""
        INSERT INTO data_source (source_name, dbms_type, schema_name, description)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_name) DO UPDATE SET
            dbms_type   = EXCLUDED.dbms_type,
            schema_name = EXCLUDED.schema_name,
            description = EXCLUDED.description
        RETURNING source_id
    """, (source_name, dbms_type, schema_name, description))
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
                table_id, col["column_name"], col["data_type"], col["max_length"],
                col["is_nullable"], col["is_primary_key"], col["is_foreign_key"],
                col["fk_table"], col["fk_column"], pos,
            ))

        log.info(f"    ✓ {table_name} ({len(columns)} columnas)")


# ══════════════════════════════════════════════════════════════
# FASE 2 — Metadata de negocio
# ══════════════════════════════════════════════════════════════

BUSINESS_METADATA = [
    {
        "domain_name": "Ventas",
        "description": "Agrupa los procesos comerciales: clientes, pedidos, pagos y empleados de ventas.",
        "entities": [
            {
                "entity_name": "Cliente",
                "entity_definition": "Persona o empresa que realiza compras de productos a la organización.",
                "attributes": [
                    ("Número de Cliente",      "Identificador único del cliente.",                          "INTEGER"),
                    ("Nombre del Cliente",      "Razón social o nombre del cliente.",                       "VARCHAR"),
                    ("Apellido de Contacto",    "Apellido de la persona de contacto del cliente.",          "VARCHAR"),
                    ("Nombre de Contacto",      "Nombre de la persona de contacto del cliente.",            "VARCHAR"),
                    ("Teléfono",                "Número telefónico del cliente.",                           "VARCHAR"),
                    ("Dirección Línea 1",       "Primera línea de la dirección del cliente.",               "VARCHAR"),
                    ("Dirección Línea 2",       "Segunda línea de la dirección del cliente (opcional).",    "VARCHAR"),
                    ("Ciudad",                  "Ciudad donde se ubica el cliente.",                        "VARCHAR"),
                    ("Estado / Región",         "Estado o región donde se ubica el cliente.",               "VARCHAR"),
                    ("Código Postal",           "Código postal de la dirección del cliente.",               "VARCHAR"),
                    ("País",                    "País donde se ubica el cliente.",                          "VARCHAR"),
                    ("Representante de Ventas", "Número de empleado asignado como representante de ventas.","INTEGER"),
                    ("Límite de Crédito",       "Monto máximo de crédito autorizado al cliente.",           "DECIMAL"),
                ],
            },
            {
                "entity_name": "Empleado de Ventas",
                "entity_definition": "Persona que trabaja en la organización gestionando relaciones con clientes y ventas.",
                "attributes": [
                    ("Número de Empleado", "Identificador único del empleado.",                      "INTEGER"),
                    ("Apellido",           "Apellido del empleado.",                                 "VARCHAR"),
                    ("Nombre",             "Nombre del empleado.",                                   "VARCHAR"),
                    ("Extensión",          "Extensión telefónica interna del empleado.",             "VARCHAR"),
                    ("Correo Electrónico", "Dirección de correo electrónico del empleado.",          "VARCHAR"),
                    ("Código de Oficina",  "Oficina a la que pertenece el empleado.",                "VARCHAR"),
                    ("Reporta A",          "Número de empleado del supervisor directo.",             "INTEGER"),
                    ("Cargo Laboral",      "Título o cargo del empleado dentro de la organización.", "VARCHAR"),
                ],
            },
            {
                "entity_name": "Oficina",
                "entity_definition": "Sede física de la organización desde donde operan los empleados de ventas.",
                "attributes": [
                    ("Código de Oficina", "Identificador único de la oficina.",           "VARCHAR"),
                    ("Ciudad",            "Ciudad donde se ubica la oficina.",             "VARCHAR"),
                    ("Teléfono",          "Número telefónico de la oficina.",              "VARCHAR"),
                    ("Dirección Línea 1", "Primera línea de la dirección de la oficina.", "VARCHAR"),
                    ("Dirección Línea 2", "Segunda línea de la dirección (opcional).",    "VARCHAR"),
                    ("Estado / Región",   "Estado o región de la oficina.",               "VARCHAR"),
                    ("País",              "País donde se ubica la oficina.",               "VARCHAR"),
                    ("Código Postal",     "Código postal de la oficina.",                  "VARCHAR"),
                    ("Territorio",        "Región comercial a la que pertenece la oficina.","VARCHAR"),
                ],
            },
            {
                "entity_name": "Pedido",
                "entity_definition": "Solicitud formal de compra realizada por un cliente.",
                "attributes": [
                    ("Número de Pedido",  "Identificador único del pedido.",                     "INTEGER"),
                    ("Fecha de Pedido",   "Fecha en que se realizó el pedido.",                  "DATE"),
                    ("Fecha Requerida",   "Fecha en que el cliente requiere el pedido.",          "DATE"),
                    ("Fecha de Envío",    "Fecha en que el pedido fue despachado.",              "DATE"),
                    ("Estado del Pedido", "Estado actual del pedido (Shipped, Pending, etc.).",  "VARCHAR"),
                    ("Comentarios",       "Observaciones adicionales sobre el pedido.",          "TEXT"),
                    ("Número de Cliente", "Cliente que realizó el pedido.",                     "INTEGER"),
                ],
            },
            {
                "entity_name": "Detalle de Pedido",
                "entity_definition": "Línea de producto incluida en un pedido específico.",
                "attributes": [
                    ("Número de Pedido",   "Pedido al que pertenece este detalle.",            "INTEGER"),
                    ("Código de Producto", "Producto incluido en el detalle del pedido.",      "VARCHAR"),
                    ("Cantidad Ordenada",  "Cantidad de unidades del producto ordenadas.",     "INTEGER"),
                    ("Precio Unitario",    "Precio de venta por unidad en este pedido.",       "DECIMAL"),
                    ("Número de Línea",    "Posición del producto dentro del pedido.",         "INTEGER"),
                ],
            },
            {
                "entity_name": "Pago",
                "entity_definition": "Registro de un pago realizado por un cliente.",
                "attributes": [
                    ("Número de Cliente", "Cliente que realizó el pago.",          "INTEGER"),
                    ("Número de Cheque",  "Identificador del cheque o transacción.","VARCHAR"),
                    ("Fecha de Pago",     "Fecha en que se realizó el pago.",       "DATE"),
                    ("Monto",             "Valor total del pago realizado.",         "DECIMAL"),
                ],
            },
        ],
    },
    {
        "domain_name": "Inventario",
        "description": "Gestiona el catálogo de productos y sus líneas de clasificación.",
        "entities": [
            {
                "entity_name": "Producto",
                "entity_definition": "Artículo físico que la organización comercializa a sus clientes.",
                "attributes": [
                    ("Código de Producto",      "Identificador único del producto.",                       "VARCHAR"),
                    ("Nombre del Producto",      "Nombre comercial del producto.",                          "VARCHAR"),
                    ("Línea de Producto",        "Categoría a la que pertenece el producto.",               "VARCHAR"),
                    ("Escala del Producto",      "Escala de representación del modelo (ej. 1:18).",         "VARCHAR"),
                    ("Proveedor",                "Empresa fabricante o proveedora del producto.",           "VARCHAR"),
                    ("Descripción del Producto", "Descripción detallada de las características.",          "TEXT"),
                    ("Cantidad en Stock",        "Número de unidades disponibles en inventario.",           "INTEGER"),
                    ("Precio de Compra",         "Costo de adquisición del producto.",                      "DECIMAL"),
                    ("Precio Sugerido (MSRP)",   "Precio de venta sugerido al público.",                    "DECIMAL"),
                ],
            },
            {
                "entity_name": "Línea de Producto",
                "entity_definition": "Categoría que agrupa productos con características o temáticas similares.",
                "attributes": [
                    ("Línea de Producto", "Nombre identificador de la línea de productos.",  "VARCHAR"),
                    ("Descripción Texto", "Descripción textual de la línea de productos.",   "TEXT"),
                    ("Descripción HTML",  "Descripción en formato HTML de la línea.",        "TEXT"),
                    ("Imagen",            "Imagen representativa de la línea de productos.", "BLOB"),
                ],
            },
        ],
    },
    {
        "domain_name": "Servicio al Cliente",
        "description": "Gestiona la atención y soporte a clientes mediante llamadas y seguimiento.",
        "entities": [
            {
                "entity_name": "Empleado de Servicio",
                "entity_definition": "Agente del call center encargado de atender las llamadas de los clientes.",
                "attributes": [
                    ("Número de Empleado", "Identificador único del empleado de servicio.", "INTEGER"),
                    ("Apellido",           "Apellido del empleado de servicio.",             "VARCHAR"),
                    ("Nombre",             "Nombre del empleado de servicio.",               "VARCHAR"),
                    ("Correo Electrónico", "Correo electrónico del empleado de servicio.",   "VARCHAR"),
                ],
            },
            {
                "entity_name": "Llamada de Servicio",
                "entity_definition": "Registro de una llamada de servicio al cliente gestionada por un agente.",
                "attributes": [
                    ("Número de Empleado", "Agente que atendió la llamada.",                     "INTEGER"),
                    ("Número de Cliente",  "Cliente que realizó la llamada.",                    "INTEGER"),
                    ("Código de Producto", "Producto sobre el cual versó la llamada.",           "VARCHAR"),
                    ("Texto de Llamada",   "Transcripción o resumen del contenido de la llamada.","TEXT"),
                    ("Fecha de Llamada",   "Fecha en que se realizó la llamada.",                "DATE"),
                ],
            },
            {
                "entity_name": "Producto de Cliente",
                "entity_definition": "Relación entre un cliente y los productos que ha adquirido o consultado.",
                "attributes": [
                    ("Número de Cliente",  "Cliente asociado al producto.", "INTEGER"),
                    ("Código de Producto", "Producto asociado al cliente.", "VARCHAR"),
                ],
            },
        ],
    },
]


def load_business(cur) -> dict:
    """Carga dominios, entidades y atributos. Retorna dict (entity, attr) → attribute_id."""
    log.info("  Cargando dominios, entidades y atributos...")
    attribute_ids = {}

    for domain in BUSINESS_METADATA:
        cur.execute("""
            INSERT INTO data_domain (domain_name, description)
            VALUES (%s, %s)
            ON CONFLICT (domain_name) DO UPDATE SET description = EXCLUDED.description
            RETURNING domain_id
        """, (domain["domain_name"], domain["description"]))
        domain_id = cur.fetchone()[0]

        for entity in domain["entities"]:
            cur.execute("""
                INSERT INTO business_entity (domain_id, entity_name, entity_definition)
                VALUES (%s, %s, %s)
                ON CONFLICT (domain_id, entity_name) DO UPDATE SET entity_definition = EXCLUDED.entity_definition
                RETURNING entity_id
            """, (domain_id, entity["entity_name"], entity["entity_definition"]))
            entity_id = cur.fetchone()[0]

            for attr_name, attr_def, attr_type in entity["attributes"]:
                cur.execute("""
                    INSERT INTO business_attribute (entity_id, attribute_name, attribute_definition, data_type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (entity_id, attribute_name) DO UPDATE SET
                        attribute_definition = EXCLUDED.attribute_definition,
                        data_type            = EXCLUDED.data_type
                    RETURNING attribute_id
                """, (entity_id, attr_name, attr_def, attr_type))
                attribute_ids[(entity["entity_name"], attr_name)] = cur.fetchone()[0]

        log.info(f"    ✓ Dominio: {domain['domain_name']} ({len(domain['entities'])} entidades)")

    return attribute_ids


# ══════════════════════════════════════════════════════════════
# FASE 3 — Linaje semántico
# (source_name, table_name, column_name) → (entity_name, attribute_name)
# ══════════════════════════════════════════════════════════════

LINEAGE_MAP = [
    # ── classicmodels.customers → Cliente ──────────────────────
    ("classicmodels", "customers", "customerNumber",         "Cliente", "Número de Cliente"),
    ("classicmodels", "customers", "customerName",           "Cliente", "Nombre del Cliente"),
    ("classicmodels", "customers", "contactLastName",        "Cliente", "Apellido de Contacto"),
    ("classicmodels", "customers", "contactFirstName",       "Cliente", "Nombre de Contacto"),
    ("classicmodels", "customers", "phone",                  "Cliente", "Teléfono"),
    ("classicmodels", "customers", "addressLine1",           "Cliente", "Dirección Línea 1"),
    ("classicmodels", "customers", "addressLine2",           "Cliente", "Dirección Línea 2"),
    ("classicmodels", "customers", "city",                   "Cliente", "Ciudad"),
    ("classicmodels", "customers", "state",                  "Cliente", "Estado / Región"),
    ("classicmodels", "customers", "postalCode",             "Cliente", "Código Postal"),
    ("classicmodels", "customers", "country",                "Cliente", "País"),
    ("classicmodels", "customers", "salesRepEmployeeNumber", "Cliente", "Representante de Ventas"),
    ("classicmodels", "customers", "creditLimit",            "Cliente", "Límite de Crédito"),

    # ── classicmodels.employees → Empleado de Ventas ───────────
    ("classicmodels", "employees", "employeeNumber", "Empleado de Ventas", "Número de Empleado"),
    ("classicmodels", "employees", "lastName",       "Empleado de Ventas", "Apellido"),
    ("classicmodels", "employees", "firstName",      "Empleado de Ventas", "Nombre"),
    ("classicmodels", "employees", "extension",      "Empleado de Ventas", "Extensión"),
    ("classicmodels", "employees", "email",          "Empleado de Ventas", "Correo Electrónico"),
    ("classicmodels", "employees", "officeCode",     "Empleado de Ventas", "Código de Oficina"),
    ("classicmodels", "employees", "reportsTo",      "Empleado de Ventas", "Reporta A"),
    ("classicmodels", "employees", "jobTitle",       "Empleado de Ventas", "Cargo Laboral"),

    # ── classicmodels.offices → Oficina ────────────────────────
    ("classicmodels", "offices", "officeCode",   "Oficina", "Código de Oficina"),
    ("classicmodels", "offices", "city",         "Oficina", "Ciudad"),
    ("classicmodels", "offices", "phone",        "Oficina", "Teléfono"),
    ("classicmodels", "offices", "addressLine1", "Oficina", "Dirección Línea 1"),
    ("classicmodels", "offices", "addressLine2", "Oficina", "Dirección Línea 2"),
    ("classicmodels", "offices", "state",        "Oficina", "Estado / Región"),
    ("classicmodels", "offices", "country",      "Oficina", "País"),
    ("classicmodels", "offices", "postalCode",   "Oficina", "Código Postal"),
    ("classicmodels", "offices", "territory",    "Oficina", "Territorio"),

    # ── classicmodels.orders → Pedido ──────────────────────────
    ("classicmodels", "orders", "orderNumber",    "Pedido", "Número de Pedido"),
    ("classicmodels", "orders", "orderDate",      "Pedido", "Fecha de Pedido"),
    ("classicmodels", "orders", "requiredDate",   "Pedido", "Fecha Requerida"),
    ("classicmodels", "orders", "shippedDate",    "Pedido", "Fecha de Envío"),
    ("classicmodels", "orders", "status",         "Pedido", "Estado del Pedido"),
    ("classicmodels", "orders", "comments",       "Pedido", "Comentarios"),
    ("classicmodels", "orders", "customerNumber", "Pedido", "Número de Cliente"),

    # ── classicmodels.orderdetails → Detalle de Pedido ─────────
    ("classicmodels", "orderdetails", "orderNumber",     "Detalle de Pedido", "Número de Pedido"),
    ("classicmodels", "orderdetails", "productCode",     "Detalle de Pedido", "Código de Producto"),
    ("classicmodels", "orderdetails", "quantityOrdered", "Detalle de Pedido", "Cantidad Ordenada"),
    ("classicmodels", "orderdetails", "priceEach",       "Detalle de Pedido", "Precio Unitario"),
    ("classicmodels", "orderdetails", "orderLineNumber", "Detalle de Pedido", "Número de Línea"),

    # ── classicmodels.payments → Pago ──────────────────────────
    ("classicmodels", "payments", "customerNumber", "Pago", "Número de Cliente"),
    ("classicmodels", "payments", "checkNumber",    "Pago", "Número de Cheque"),
    ("classicmodels", "payments", "paymentDate",    "Pago", "Fecha de Pago"),
    ("classicmodels", "payments", "amount",         "Pago", "Monto"),

    # ── classicmodels.products → Producto ──────────────────────
    ("classicmodels", "products", "productCode",        "Producto", "Código de Producto"),
    ("classicmodels", "products", "productName",        "Producto", "Nombre del Producto"),
    ("classicmodels", "products", "productLine",        "Producto", "Línea de Producto"),
    ("classicmodels", "products", "productScale",       "Producto", "Escala del Producto"),
    ("classicmodels", "products", "productVendor",      "Producto", "Proveedor"),
    ("classicmodels", "products", "productDescription", "Producto", "Descripción del Producto"),
    ("classicmodels", "products", "quantityInStock",    "Producto", "Cantidad en Stock"),
    ("classicmodels", "products", "buyPrice",           "Producto", "Precio de Compra"),
    ("classicmodels", "products", "MSRP",               "Producto", "Precio Sugerido (MSRP)"),

    # ── classicmodels.productlines → Línea de Producto ─────────
    ("classicmodels", "productlines", "productLine",     "Línea de Producto", "Línea de Producto"),
    ("classicmodels", "productlines", "textDescription", "Línea de Producto", "Descripción Texto"),
    ("classicmodels", "productlines", "htmlDescription", "Línea de Producto", "Descripción HTML"),
    ("classicmodels", "productlines", "image",           "Línea de Producto", "Imagen"),

    # ── customerservice.cs_customers → Cliente ─────────────────
    ("customerservice", "cs_customers", "customernumber",   "Cliente", "Número de Cliente"),
    ("customerservice", "cs_customers", "contactlastname",  "Cliente", "Apellido de Contacto"),
    ("customerservice", "cs_customers", "contactfirstname", "Cliente", "Nombre de Contacto"),
    ("customerservice", "cs_customers", "phone",            "Cliente", "Teléfono"),
    ("customerservice", "cs_customers", "addressline1",     "Cliente", "Dirección Línea 1"),
    ("customerservice", "cs_customers", "addressline2",     "Cliente", "Dirección Línea 2"),
    ("customerservice", "cs_customers", "city",             "Cliente", "Ciudad"),
    ("customerservice", "cs_customers", "state",            "Cliente", "Estado / Región"),
    ("customerservice", "cs_customers", "postalcode",       "Cliente", "Código Postal"),
    ("customerservice", "cs_customers", "country",          "Cliente", "País"),

    # ── customerservice.cs_employees → Empleado de Servicio ────
    ("customerservice", "cs_employees", "employeenumber", "Empleado de Servicio", "Número de Empleado"),
    ("customerservice", "cs_employees", "lastname",       "Empleado de Servicio", "Apellido"),
    ("customerservice", "cs_employees", "firstname",      "Empleado de Servicio", "Nombre"),
    ("customerservice", "cs_employees", "email",          "Empleado de Servicio", "Correo Electrónico"),

    # ── customerservice.cs_customer_calls → Llamada de Servicio ─
    ("customerservice", "cs_customer_calls", "employeenumber", "Llamada de Servicio", "Número de Empleado"),
    ("customerservice", "cs_customer_calls", "customernumber", "Llamada de Servicio", "Número de Cliente"),
    ("customerservice", "cs_customer_calls", "productcode",    "Llamada de Servicio", "Código de Producto"),
    ("customerservice", "cs_customer_calls", "text",           "Llamada de Servicio", "Texto de Llamada"),
    ("customerservice", "cs_customer_calls", "date",           "Llamada de Servicio", "Fecha de Llamada"),

    # ── customerservice.cs_customer_products → Producto de Cliente
    ("customerservice", "cs_customer_products", "customernumber", "Producto de Cliente", "Número de Cliente"),
    ("customerservice", "cs_customer_products", "productcode",    "Producto de Cliente", "Código de Producto"),

    # ── customerservice.cs_products → Producto ─────────────────
    ("customerservice", "cs_products", "productcode",        "Producto", "Código de Producto"),
    ("customerservice", "cs_products", "productname",        "Producto", "Nombre del Producto"),
    ("customerservice", "cs_products", "productscale",       "Producto", "Escala del Producto"),
    ("customerservice", "cs_products", "productvendor",      "Producto", "Proveedor"),
    ("customerservice", "cs_products", "productdescription", "Producto", "Descripción del Producto"),
]


def load_lineage(cur, attribute_ids: dict):
    log.info("  Registrando linaje semántico...")
    ok = skipped = 0

    for source_name, table_name, column_name, entity_name, attr_name in LINEAGE_MAP:
        cur.execute("""
            SELECT dc.column_id
            FROM db_column dc
            JOIN db_table  dt ON dc.table_id  = dt.table_id
            JOIN data_source ds ON dt.source_id = ds.source_id
            WHERE ds.source_name = %s AND dt.table_name = %s AND dc.column_name = %s
        """, (source_name, table_name, column_name))
        row = cur.fetchone()

        if not row:
            log.warning(f"    ⚠ Columna no encontrada: {source_name}.{table_name}.{column_name}")
            skipped += 1
            continue

        key = (entity_name, attr_name)
        if key not in attribute_ids:
            log.warning(f"    ⚠ Atributo no encontrado: {entity_name} → {attr_name}")
            skipped += 1
            continue

        cur.execute("""
            INSERT INTO column_lineage (column_id, attribute_id)
            VALUES (%s, %s)
            ON CONFLICT (column_id, attribute_id) DO NOTHING
        """, (row[0], attribute_ids[key]))
        ok += 1

    log.info(f"    ✓ {ok} relaciones registradas  |  {skipped} omitidas")


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

def main():
    log.info("=" * 55)
    log.info("  ETL — Repositorio de Metadatos")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    # Leer archivos SQL
    for path in (MYSQL_FILE, PG_FILE):
        if not os.path.exists(path):
            log.error(f"Archivo no encontrado: {path}")
            sys.exit(1)

    log.info(f"Leyendo {MYSQL_FILE} ...")
    with open(MYSQL_FILE, "r", encoding="utf-8") as f:
        mysql_sql = f.read()

    log.info(f"Leyendo {PG_FILE} ...")
    with open(PG_FILE, "r", encoding="utf-8") as f:
        pg_sql = f.read()

    # Parsear DDL
    log.info("Parseando DDL MySQL (classicmodels)...")
    mysql_tables = parse_mysql_ddl(mysql_sql)
    log.info(f"  Tablas: {list(mysql_tables.keys())}")

    log.info("Parseando DDL PostgreSQL (customerservice)...")
    pg_tables = parse_postgres_ddl(pg_sql)
    log.info(f"  Tablas: {list(pg_tables.keys())}")

    # Conectar al repositorio
    log.info(f"Conectando a {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # ── Fase 1 ───────────────────────────────────────────
            log.info("── Fase 1: Metadata técnica ──────────────────────")
            load_technical(cur, "classicmodels",  "MySQL",      "classicmodels",
                           "Base de datos de ventas con pedidos, clientes, empleados, productos y pagos.",
                           mysql_tables)
            load_technical(cur, "customerservice", "PostgreSQL", "public",
                           "Base de datos de servicio al cliente con registros de llamadas y productos consultados.",
                           pg_tables)

            # ── Fase 2 ───────────────────────────────────────────
            log.info("── Fase 2: Metadata de negocio ───────────────────")
            attribute_ids = load_business(cur)

            # ── Fase 3 ───────────────────────────────────────────
            log.info("── Fase 3: Linaje semántico ──────────────────────")
            load_lineage(cur, attribute_ids)

        conn.commit()
        log.info("=" * 55)
        log.info("  ✅ ETL completado exitosamente")
        log.info("=" * 55)

    except Exception as exc:
        conn.rollback()
        log.error(f"❌ Error — rollback ejecutado: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
