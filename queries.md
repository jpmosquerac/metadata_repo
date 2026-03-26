# Consultas sobre el Repositorio de Metadatos
**Universidad Javeriana — Modelos y Persistencia de Datos 2026**

---

## Q1 — Tablas por fuente de datos

**Pregunta:** ¿Qué tablas existen en cada fuente de datos?

```sql
SELECT
    ds.source_name    AS fuente,
    ds.dbms_type      AS motor,
    dt.table_name     AS tabla
FROM db_table    dt
JOIN data_source ds ON dt.source_id = ds.source_id
ORDER BY ds.source_name, dt.table_name;
```

**Resultado:**

| fuente | motor | tabla |
|--------|-------|-------|
| classicmodels | MySQL | customers |
| classicmodels | MySQL | employees |
| classicmodels | MySQL | offices |
| classicmodels | MySQL | orderdetails |
| classicmodels | MySQL | orders |
| classicmodels | MySQL | payments |
| classicmodels | MySQL | productlines |
| classicmodels | MySQL | products |
| customerservice | PostgreSQL | cs_customer_calls |
| customerservice | PostgreSQL | cs_customer_products |
| customerservice | PostgreSQL | cs_customers |
| customerservice | PostgreSQL | cs_employees |
| customerservice | PostgreSQL | cs_products |

**Descripción:** La consulta une `db_table` con `data_source` para obtener todas las tablas registradas, agrupadas por fuente. Se identifican 8 tablas en la fuente MySQL `classicmodels` y 5 tablas en la fuente PostgreSQL `customerservice`, para un total de 13 tablas.

---

## Q2 — Columnas de una tabla específica

**Pregunta:** ¿Qué columnas tiene una tabla? (nombre, tipo, longitud, clave primaria, clave foránea, nulabilidad)

> El ejemplo muestra la tabla `orders` de `classicmodels`. Los filtros `WHERE` pueden ajustarse para consultar cualquier otra tabla o fuente.

```sql
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
WHERE ds.source_name = 'classicmodels'
  AND dt.table_name  = 'orders'
ORDER BY dc.ordinal_position;
```

**Resultado:**

| posicion | columna | tipo | longitud | clave_primaria | referencia_fk | permite_nulo |
|----------|---------|------|----------|----------------|---------------|--------------|
| 1 | orderNumber | INT | 11 | Sí | | No |
| 2 | orderDate | DATE | | No | | No |
| 3 | requiredDate | DATE | | No | | No |
| 4 | shippedDate | DATE | | No | | Sí |
| 5 | status | VARCHAR | 15 | No | | No |
| 6 | comments | TEXT | | No | | Sí |
| 7 | customerNumber | INT | 11 | No | customers.customerNumber | No |

**Descripción:** La consulta une `db_column` con `db_table` y `data_source` para obtener la estructura completa de una tabla. Muestra la posición ordinal de cada columna, su tipo de dato, longitud máxima (cuando aplica), si es clave primaria o foránea (indicando la tabla y columna referenciada), y si admite valores nulos.

---

## Q3 — Glosario de negocio

**Pregunta:** ¿Qué entidades de negocio existen en la organización y a qué dominio pertenecen?

```sql
SELECT
    dd.domain_name       AS dominio,
    be.entity_name       AS entidad,
    be.entity_definition AS definicion
FROM business_entity be
JOIN data_domain     dd ON be.domain_id = dd.domain_id
ORDER BY dd.domain_name, be.entity_name;
```

**Resultado:**

| dominio | entidad | definicion |
|---------|---------|------------|
| Inventario | Línea de Producto | Categoría que agrupa productos con características o temáticas similares. |
| Inventario | Producto | Artículo físico que la organización comercializa a sus clientes. |
| Servicio al Cliente | Empleado de Servicio | Agente del call center encargado de atender las llamadas de los clientes. |
| Servicio al Cliente | Llamada de Servicio | Registro de una llamada de servicio al cliente gestionada por un agente. |
| Servicio al Cliente | Producto de Cliente | Relación entre un cliente y los productos que ha adquirido o consultado. |
| Ventas | Cliente | Persona o empresa que realiza compras de productos a la organización. |
| Ventas | Detalle de Pedido | Línea de producto incluida en un pedido específico. |
| Ventas | Empleado de Ventas | Persona que trabaja en la organización gestionando relaciones con clientes y ventas. |
| Ventas | Oficina | Sede física de la organización desde donde operan los empleados de ventas. |
| Ventas | Pago | Registro de un pago realizado por un cliente. |
| Ventas | Pedido | Solicitud formal de compra realizada por un cliente. |

**Descripción:** La consulta une `business_entity` con `data_domain` para construir el glosario completo de negocio. Se identifican 11 entidades distribuidas en 3 dominios: **Ventas** (6 entidades), **Servicio al Cliente** (3 entidades) e **Inventario** (2 entidades).

---

## Q4 — Atributos de una entidad de negocio

**Pregunta:** ¿Qué atributos tiene una entidad de negocio específica?

> El ejemplo muestra la entidad `Cliente`. El filtro `WHERE` puede ajustarse para consultar cualquier otra entidad.

```sql
SELECT
    be.entity_name          AS entidad,
    ba.attribute_name       AS atributo,
    ba.data_type            AS tipo,
    ba.attribute_definition AS definicion
FROM business_attribute ba
JOIN business_entity    be ON ba.entity_id = be.entity_id
WHERE be.entity_name = 'Cliente'
ORDER BY ba.attribute_id;
```

**Resultado:**

| entidad | atributo | tipo | definicion |
|---------|----------|------|------------|
| Cliente | Número de Cliente | INTEGER | Identificador único del cliente. |
| Cliente | Nombre del Cliente | VARCHAR | Razón social o nombre del cliente. |
| Cliente | Apellido de Contacto | VARCHAR | Apellido de la persona de contacto del cliente. |
| Cliente | Nombre de Contacto | VARCHAR | Nombre de la persona de contacto del cliente. |
| Cliente | Teléfono | VARCHAR | Número telefónico del cliente. |
| Cliente | Dirección Línea 1 | VARCHAR | Primera línea de la dirección del cliente. |
| Cliente | Dirección Línea 2 | VARCHAR | Segunda línea de la dirección del cliente (opcional). |
| Cliente | Ciudad | VARCHAR | Ciudad donde se ubica el cliente. |
| Cliente | Estado / Región | VARCHAR | Estado o región donde se ubica el cliente. |
| Cliente | Código Postal | VARCHAR | Código postal de la dirección del cliente. |
| Cliente | País | VARCHAR | País donde se ubica el cliente. |
| Cliente | Representante de Ventas | INTEGER | Número de empleado asignado como representante de ventas. |
| Cliente | Límite de Crédito | DECIMAL | Monto máximo de crédito autorizado al cliente. |

**Descripción:** La consulta une `business_attribute` con `business_entity` para listar todos los atributos de una entidad, incluyendo su tipo de dato conceptual y definición de negocio. La entidad `Cliente` cuenta con 13 atributos que describen su información de contacto, ubicación y condiciones comerciales.

---

## Q5 — Localización de entidades

**Pregunta:** ¿En qué fuente de datos y tabla está almacenada cada entidad de negocio?

```sql
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
```

**Resultado:**

| entidad | fuente | motor | tabla |
|---------|--------|-------|-------|
| Cliente | classicmodels | MySQL | customers |
| Cliente | customerservice | PostgreSQL | cs_customers |
| Detalle de Pedido | classicmodels | MySQL | orderdetails |
| Empleado de Servicio | customerservice | PostgreSQL | cs_employees |
| Empleado de Ventas | classicmodels | MySQL | employees |
| Llamada de Servicio | customerservice | PostgreSQL | cs_customer_calls |
| Línea de Producto | classicmodels | MySQL | productlines |
| Oficina | classicmodels | MySQL | offices |
| Pago | classicmodels | MySQL | payments |
| Pedido | classicmodels | MySQL | orders |
| Producto | classicmodels | MySQL | products |
| Producto | customerservice | PostgreSQL | cs_products |
| Producto de Cliente | customerservice | PostgreSQL | cs_customer_products |

**Descripción:** La consulta recorre el linaje semántico completo desde `column_lineage` hasta `data_source`, agrupando por entidad y tabla para obtener una vista de localización única por combinación. Se destacan dos entidades presentes en **ambas fuentes**: `Cliente` (en `customers` y `cs_customers`) y `Producto` (en `products` y `cs_products`), evidenciando las estructuras comunes entre los dos sistemas.

---

## Q6 — Linaje semántico de `cs_customers`

**Pregunta:** Para cada columna de la tabla `cs_customers`, ¿cuál es su nombre técnico, tipo de dato, atributo de negocio correspondiente, definición del atributo y entidad de negocio a la que pertenece?

```sql
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
```

**Resultado:**

| columna_tecnica | tipo_dato | atributo_negocio | definicion_atributo | entidad_negocio |
|-----------------|-----------|------------------|---------------------|-----------------|
| customernumber | INTEGER | Número de Cliente | Identificador único del cliente. | Cliente |
| contactlastname | VARCHAR | Apellido de Contacto | Apellido de la persona de contacto del cliente. | Cliente |
| contactfirstname | VARCHAR | Nombre de Contacto | Nombre de la persona de contacto del cliente. | Cliente |
| phone | VARCHAR | Teléfono | Número telefónico del cliente. | Cliente |
| addressline1 | VARCHAR | Dirección Línea 1 | Primera línea de la dirección del cliente. | Cliente |
| addressline2 | VARCHAR | Dirección Línea 2 | Segunda línea de la dirección del cliente (opcional). | Cliente |
| city | VARCHAR | Ciudad | Ciudad donde se ubica el cliente. | Cliente |
| state | VARCHAR | Estado / Región | Estado o región donde se ubica el cliente. | Cliente |
| postalcode | VARCHAR | Código Postal | Código postal de la dirección del cliente. | Cliente |
| country | VARCHAR | País | País donde se ubica el cliente. | Cliente |

**Descripción:** Esta consulta representa el linaje semántico completo de la tabla `cs_customers`. Atraviesa las tres capas del repositorio — técnica, de negocio y de linaje — para mostrar cómo cada columna física en PostgreSQL se corresponde con un atributo del modelo conceptual de negocio. Las 10 columnas de `cs_customers` mapean en su totalidad a atributos de la entidad `Cliente` del dominio **Ventas**, confirmando que esta tabla es una representación del cliente desde la perspectiva del servicio al cliente.
