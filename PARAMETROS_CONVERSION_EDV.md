# Guía de Conversión de Scripts a Versión EDV

Este documento detalla los parámetros y cambios de código necesarios para convertir un script de procesamiento de datos del ambiente estándar (DDV) a su versión para el Enterprise Data Vault (EDV).

## 1. Resumen del Patrón de Conversión

El objetivo es modificar el script para que lea desde el ambiente de origen (DDV) y escriba todos los resultados (tablas finales y temporales) en el ambiente de destino (EDV). La lógica de negocio principal no se altera.

## 2. Cambios en Constantes

Se debe modificar la siguiente constante para apuntar al contenedor de almacenamiento de EDV.

| Constante             | Valor Original         | Valor Nuevo (EDV)              |
| --------------------- | ---------------------- | ------------------------------ |
| `CONS_CONTAINER_NAME` | `"abfss://lhcldata@"` | `"abfss://bcp-edv-trdata-012@"` |

## 3. Adición de Nuevos Parámetros (Widgets)

Añada los siguientes widgets de Databricks para definir el entorno de escritura de EDV.

```python
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')
```

## 4. Modificación de Parámetros Existentes

Actualice los valores por defecto de los siguientes parámetros para que apunten a los recursos correctos en el ambiente de producción/EDV.

| Parámetro                   | Valor de Ejemplo (Original)         | Valor de Ejemplo (EDV)              |
| --------------------------- | ----------------------------------- | ----------------------------------- |
| `PRM_STORAGE_ACCOUNT_DDV`   | `'adlscu1lhclbackd03'`              | `'adlscu1lhclbackp05'`              |
| `PRM_ESQUEMA_TABLA_DDV`     | `'bcp_ddv_matrizvariables'`         | `'bcp_ddv_matrizvariables_v'`       |
| `PRM_CATALOG_NAME`          | `'catalog_lhcl_desa_bcp'`           | `'catalog_lhcl_prod_bcp'`           |
| `PRM_TABLE_NAME`            | `'NOMBRE_TABLA'`                    | `'NOMBRE_TABLA_EDV'` (o similar)    |
| `PRM_FECHA_RUTINA`          | (Fecha de desarrollo)               | (Fecha de ejecución, ej: `'2025-09-01'`) |

*Nota: Los nombres de las tablas y carpetas temporales (`PRM_TABLA_SEGUNDATRANSPUESTA_TMP`, etc.) también suelen modificarse para evitar colisiones, añadiendo un sufijo como `_edv` o `_ruben`.*

## 5. Lógica de Construcción de Esquemas y Rutas

El cambio más importante es la separación de los esquemas de lectura y escritura.

### Lógica Original:
```python
# El esquema de lectura y escritura es el mismo
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME + "." + PRM_ESQUEMA_TABLA_DDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA + "." + PRM_TABLE_NAME
```

### Lógica Nueva (EDV):
```python
# Esquema de LECTURA (apunta a DDV)
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME + "." + PRM_ESQUEMA_TABLA_DDV

# Esquema de ESCRITURA (apunta a EDV)
PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME
```

## 6. Actualización de Tablas Temporales

Todas las operaciones que crean tablas temporales (`.saveAsTable()`) deben ser modificadas para usar el nuevo esquema de escritura (`PRM_ESQUEMA_TABLA_ESCRITURA`).

### Lógica Original:
```python
# Ejemplo de creación de tabla temporal
tmp_table = f'{PRM_ESQUEMA_TABLA}.{nombre_tabla_tmp}'.lower()
df.write.saveAsTable(tmp_table, ...)
```

### Lógica Nueva (EDV):
```python
# La tabla temporal ahora se crea en el esquema de escritura de EDV
tmp_table = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.{nombre_tabla_tmp}'.lower()
df.write.saveAsTable(tmp_table, ...)
```

Este es el patrón general. Al seguir estos pasos, cualquier script puede ser adaptado para ejecutarse en el entorno EDV, garantizando la separación de datos y la correcta canalización de los resultados.
