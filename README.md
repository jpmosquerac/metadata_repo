# Repositorio de Metadatos
**Universidad Javeriana — Modelos y Persistencia de Datos 2026**

Repositorio de metadatos técnicos y de negocio integrado desde dos fuentes:
- **classicmodels** (MySQL) — ventas, pedidos, clientes, productos
- **customerservice** (PostgreSQL) — llamadas de servicio al cliente

---

## Arquitectura

El repositorio está implementado en **PostgreSQL 16** y organiza la metadata en cinco capas:

| Capa | Tablas | Descripción |
|------|--------|-------------|
| 1 — Técnica | `data_source`, `db_table`, `db_column` | Estructura física de las fuentes de datos |
| 2 — Negocio | `data_domain`, `business_entity`, `business_attribute` | Glosario conceptual de la organización |
| 3 — Linaje | `column_lineage` | Mapeo entre columnas técnicas y atributos de negocio |
| 4 — Almacén | `dw_layer`, `dw_dimension`, `dw_fact_table`, `dw_fact_dimension` | Estructura del almacén de datos |
| 5 — ETL | `etl_process`, `etl_run_log` | Catálogo de procesos y log de ejecuciones |

**Totales cargados:**
- Fuentes originales: 2 fuentes · 13 tablas · 85 columnas · 3 dominios · 11 entidades · 70 atributos · 85 relaciones de linaje
- Almacén: 9 tablas DW · 7 dimensiones · 2 tablas de hechos · 10 relaciones hecho-dimensión

---

## Requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- Los archivos fuente en `data_sources/` (incluidos en este repositorio):
  - `mysqlsampledatabase.sql`
  - `customerservice.sql`

---

## Estructura

```
metadata_repo/
├── data_sources/                        # Archivos SQL de las fuentes de datos
│   ├── mysqlsampledatabase.sql          #   Dump MySQL — classicmodels
│   └── customerservice.sql             #   Dump PostgreSQL — customerservice
│
├── discovery/                           # Descubrimiento de metadata (Entrega 1)
│   ├── classicmodels_metadata.xlsx      #   Metadata técnica classicmodels (Pentaho)
│   ├── customservice_metadata.xlsx      #   Metadata técnica customerservice (Pentaho)
│   ├── Transformation_Classicmodel.ktr  #   ETL Pentaho — extracción classicmodels
│   └── Transformation_Cusomservice.ktr  #   ETL Pentaho — extracción customerservice
│
├── profiling/                           # Perfilamiento de datos (Entrega 1)
│   └── PerfilamientoDatosPersistencia.ipynb
│
├── schema.sql               # DDL capas 1–3 (7 tablas)
├── schema_dw_extension.sql  # DDL capas 4–5 (6 tablas nuevas) — Entrega 2
├── etl.py                   # ETL de descubrimiento — puebla capas 1–3
├── etl_dw_metadata.py       # ETL de extensión — puebla capas 4–5 — Entrega 2
├── docker-compose.yml       # Stack standalone (solo repositorio, Entrega 1)
├── Dockerfile               # Imagen ETL (python:3.12-slim + psycopg2)
├── requirements.txt         # psycopg2-binary
├── queries.sql              # 6 consultas sobre el repositorio
├── queries.md               # Documentación de consultas con resultados esperados
└── README.md
```

> **Entrega 2:** el stack completo (repositorio + almacén + los 3 ETLs) se orquesta desde `../datawarehouse/docker-compose.yml`. El `docker-compose.yml` de esta carpeta es el stack standalone de la Entrega 1.

---

## Despliegue

### Opción A — Stack completo (Entrega 2, recomendado)

Usa el compose del almacén de datos, que levanta ambas bases de datos y expone los tres ETLs:

```bash
cd ../datawarehouse
docker compose up -d postgres
docker compose run --rm etl_metadata      # ETL capas 1–3
docker compose run --rm etl_dw            # carga el almacén
docker compose run --rm etl_metadata_ext  # ETL capas 4–5
```

Consulta [../datawarehouse/README.md](../datawarehouse/README.md) para la guía completa.

### Opción B — Solo repositorio de metadatos (Entrega 1)

```bash
cd metadata_repo
docker compose up -d metadata_repo
docker compose run --rm etl
```

---

## ETLs disponibles

### `etl.py` — Descubrimiento de metadatos (capas 1–3)

| Fase | Descripción |
|------|-------------|
| 1 — Metadata técnica | Parsea los `.sql` de `data_sources/` → `data_source`, `db_table`, `db_column` |
| 2 — Metadata de negocio | Carga dominios, entidades y atributos → `data_domain`, `business_entity`, `business_attribute` |
| 3 — Linaje semántico | Mapea columnas técnicas a atributos de negocio → `column_lineage` |

### `etl_dw_metadata.py` — Extensión del repositorio (capas 4–5)

Prerequisito: `etl_dw.py` (del almacén) debe haberse ejecutado primero.

| Fase | Descripción |
|------|-------------|
| 1 — Metadata técnica del DW | Parsea `datawarehouse.sql` → registra el DW como `data_source` con sus 9 tablas y columnas |
| 2 — Estructura del almacén | Puebla `dw_layer`, `dw_dimension`, `dw_fact_table`, `dw_fact_dimension` |
| 3 — Procesos ETL | Registra `etl_dw.py` en `etl_process` |

Ambos ETLs son **idempotentes**: pueden ejecutarse múltiples veces sin duplicar datos.

---

## Conexión al repositorio

| Parámetro | Valor |
|-----------|-------|
| Host      | `localhost` |
| Puerto    | `5433` |
| Base de datos | `metadata_repository` |
| Usuario   | `admin` |
| Contraseña | `admin123` |

```bash
psql -h localhost -p 5433 -U admin -d metadata_repository
```

---

## Consultas disponibles

Las 6 consultas requeridas están en `queries.sql` y documentadas con resultados en `queries.md`:

| Query | Pregunta |
|-------|----------|
| Q1 | ¿Qué tablas existen en cada fuente de datos? |
| Q2 | ¿Qué columnas tiene una tabla? (nombre, tipo, PK, FK, nulabilidad) |
| Q3 | ¿Qué entidades de negocio existen y a qué dominio pertenecen? |
| Q4 | ¿Qué atributos tiene una entidad de negocio específica? |
| Q5 | ¿En qué fuente de datos y tabla está almacenada cada entidad? |
| Q6 | Linaje semántico completo de la tabla `cs_customers` |

---

## Generar backup

Con el contenedor corriendo:

```bash
docker exec modelado_postgres pg_dump \
  -U admin -d metadata_repository \
  --format=custom --compress=9 \
  > backup_metadata_repository_$(date +%Y%m%d).dump
```

---

## Detener y limpiar

```bash
# Solo detener (conserva los datos en el volumen)
docker compose down

# Detener y eliminar los datos
docker compose down -v
```

---

## Variables de entorno

### `etl.py`

| Variable | Default | Descripción |
|----------|---------|-------------|
| `REPO_HOST` | `localhost` | Host del repositorio |
| `REPO_PORT` | `5433` | Puerto |
| `REPO_DB` | `metadata_repository` | Base de datos |
| `REPO_USER` | `admin` | Usuario |
| `REPO_PASSWORD` | `admin123` | Contraseña |
| `DATA_SOURCES_DIR` | `./data_sources` | Carpeta con los `.sql` fuente |

### `etl_dw_metadata.py`

Hereda las mismas variables de conexión que `etl.py`, más:

| Variable | Default | Descripción |
|----------|---------|-------------|
| `DW_SCHEMA_FILE` | `../datawarehouse/schema/datawarehouse.sql` | Ruta al DDL del almacén |

Ejecución local sin Docker (requiere `pip install psycopg2-binary`):

```bash
REPO_PORT=5433 DATA_SOURCES_DIR=./data_sources python etl.py
REPO_PORT=5433 DW_SCHEMA_FILE=../datawarehouse/schema/datawarehouse.sql python etl_dw_metadata.py
```
# metadata_repo
