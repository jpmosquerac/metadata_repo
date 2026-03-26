# Perfilamiento de Datos — Entrega 1

| Archivo | Descripción |
|---------|-------------|
| `PerfilamientoDatosPersistencia.ipynb` | Notebook de análisis y perfilamiento de las dos fuentes de datos |

---

## Descripción del notebook

El notebook realiza un análisis exploratorio completo de las dos fuentes de datos leyendo los archivos `.sql` directamente (sin necesidad de conexión a una base de datos activa), cargándolos en **SQLite en memoria**.

### Secciones principales

| Sección | Contenido |
|---------|-----------|
| Carga de datos | Parseo de `mysqlsampledatabase.sql` y `customerservice.sql` en SQLite |
| Perfilamiento por tabla | Dimensiones, tipos de dato, nulos, duplicados y estadísticas descriptivas para cada tabla |
| Análisis cruzado | Comparación de tablas comunes entre fuentes: `customers`/`cs_customers`, `employees`/`cs_employees`, `products`/`cs_products` |
| Conclusiones | Hallazgos sobre solapamiento, calidad de datos y diferencias entre fuentes |

### Hallazgos principales

- **Clientes** (`customers` vs `cs_customers`): 122 registros idénticos en ambas fuentes — los IDs coinciden en su totalidad.
- **Empleados** (`employees` vs `cs_employees`): poblaciones completamente distintas — ventas vs. call center, sin solapamiento de correos.
- **Productos** (`products` vs `cs_products`): `classicmodels` tiene 74 productos, subconjunto de los 110 de `customerservice` (+36 adicionales).
- **Calidad general**: sin duplicados en claves primarias; campos opcionales con nulos esperados (`shippedDate`, `addressLine2`); `htmlDescription` e `image` con 100% de nulos en `productlines`.

---

## Ejecución

El notebook está diseñado para ejecutarse en **Google Colab** o cualquier entorno Jupyter con las siguientes dependencias:

```
pandas
sqlite3  (incluido en Python estándar)
matplotlib
seaborn
```

Los archivos `.sql` de `data_sources/` deben estar accesibles desde el entorno de ejecución. En Colab, subirlos manualmente o montando Google Drive.
