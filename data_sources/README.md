# Fuentes de Datos

Dumps SQL de las dos bases de datos de origen utilizadas en el proyecto.

| Archivo | Motor | Base de datos | Descripción |
|---------|-------|---------------|-------------|
| `mysqlsampledatabase.sql` | MySQL | `classicmodels` | Sistema de ventas: clientes, pedidos, productos, empleados y oficinas |
| `customerservice.sql` | PostgreSQL | `customerservice` | Sistema de servicio al cliente: llamadas, agentes y productos asociados |

---

## Uso

Estos archivos son consumidos directamente por el ETL (`etl.py`) mediante **parseo de DDL** — no se requiere una instancia activa de MySQL ni PostgreSQL para cargar la metadata técnica en el repositorio.

El ETL extrae de cada archivo:
- Nombres de tablas y columnas
- Tipos de dato y longitudes
- Claves primarias y foráneas
- Nulabilidad y posición ordinal de cada columna

Para ejecutar el ETL:

```bash
# Desde metadata_repo/
docker compose run --rm etl
```

---

## Tablas por fuente

**classicmodels** (8 tablas): `customers`, `employees`, `offices`, `orderdetails`, `orders`, `payments`, `productlines`, `products`

**customerservice** (5 tablas): `cs_customer_calls`, `cs_customer_products`, `cs_customers`, `cs_employees`, `cs_products`
