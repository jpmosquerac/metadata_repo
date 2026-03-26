# Descubrimiento de Metadata — Entrega 1

Artefactos generados durante la fase de descubrimiento de metadata técnica usando **Pentaho Data Integration (PDI)**.

| Archivo | Tipo | Descripción |
|---------|------|-------------|
| `Transformation_Classicmodel.ktr` | Pentaho KTR | Transformación ETL que extrae metadata de `classicmodels` desde `INFORMATION_SCHEMA` de MySQL |
| `Transformation_Cusomservice.ktr` | Pentaho KTR | Transformación ETL que extrae metadata de `customerservice` desde `INFORMATION_SCHEMA` de PostgreSQL |
| `classicmodels_metadata .xlsx` | Excel | Resultado del KTR: metadata técnica de classicmodels (62 filas × 27 columnas) |
| `customservice_metadata.xlsx` | Excel | Resultado del KTR: metadata técnica de customerservice (29 filas × 14 columnas) |

---

## Contenido de los Excel

Cada archivo Excel contiene dos hojas:

| Hoja | Descripción |
|------|-------------|
| `metadatos` | Datos extraídos de `INFORMATION_SCHEMA`: tabla, columna, tipo de dato, longitud, nulabilidad, PK, FK, etc. |
| `clasificacion` | Clasificación de los campos de metadata según su categoría (técnica, semántica, de calidad, etc.) |

---

## Cómo ejecutar los KTR

1. Abrir **Pentaho Data Integration (Spoon)**
2. Abrir el archivo `.ktr` correspondiente
3. Configurar la conexión a la base de datos de origen (MySQL o PostgreSQL)
4. Ejecutar la transformación — genera el `.xlsx` de salida

> Los `.xlsx` incluidos en este directorio son el resultado de la última ejecución con las fuentes de datos del proyecto.
