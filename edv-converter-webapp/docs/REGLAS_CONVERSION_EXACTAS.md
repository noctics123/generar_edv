# Reglas Exactas de Conversión DDV → EDV

## 📋 Cambios Obligatorios

### 0. AMBIENTE - Actualizar constantes y parámetros de ambiente (CRÍTICO)

**UBICACIÓN:** Sección de constantes y widgets

**CAMBIOS OBLIGATORIOS:**

**0.1 CONS_CONTAINER_NAME** - Cambiar de desarrollo (lhcldata) a producción (bcp-edv-trdata-012):
```python
# DDV (desarrollo):
CONS_CONTAINER_NAME = "abfss://lhcldata@"

# EDV (producción):
CONS_CONTAINER_NAME = "abfss://bcp-edv-trdata-012@"
```

**0.2 PRM_STORAGE_ACCOUNT_DDV** - Cambiar de desarrollo (d03) a producción (p05):
```python
# DDV (desarrollo):
dbutils.widgets.text(name="PRM_STORAGE_ACCOUNT_DDV", defaultValue='adlscu1lhclbackd03')

# EDV (producción):
dbutils.widgets.text(name="PRM_STORAGE_ACCOUNT_DDV", defaultValue='adlscu1lhclbackp05')
```

**0.3 PRM_CATALOG_NAME** - Cambiar de desarrollo (desa) a producción (prod):
```python
# DDV (desarrollo):
dbutils.widgets.text(name="PRM_CATALOG_NAME", defaultValue='catalog_lhcl_desa_bcp')

# EDV (producción):
dbutils.widgets.text(name="PRM_CATALOG_NAME", defaultValue='catalog_lhcl_prod_bcp')
```

**VALIDACIÓN:**
- ✅ CONS_CONTAINER_NAME debe ser: `abfss://bcp-edv-trdata-012@`
- ✅ PRM_STORAGE_ACCOUNT_DDV debe ser: `adlscu1lhclbackp05`
- ✅ PRM_CATALOG_NAME debe ser: `catalog_lhcl_prod_bcp`

---

### 1. WIDGETS - Agregar 2 nuevos widgets EDV

**UBICACIÓN:** Después del último widget existente (después de `PRM_TABLA_PARAM_GRUPO`)

**AGREGAR:**
```python
# Parámetros específicos para EDV
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')
```

**VALIDACIÓN:**
- ✅ Debe existir línea con `PRM_CATALOG_NAME_EDV`
- ✅ Debe existir línea con `PRM_ESQUEMA_TABLA_EDV`
- ✅ defaultValue correcto: `catalog_lhcl_prod_bcp_expl` y `bcp_edv_trdata_012`

---

### 2. GETS - Agregar 2 nuevas variables

**UBICACIÓN:** Después del último get existente (después de `PRM_TABLA_PARAM_GRUPO = dbutils.widgets.get(...)`)

**AGREGAR:**
```python
# Configuración específica para EDV
PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")
```

**VALIDACIÓN:**
- ✅ Debe existir: `PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")`
- ✅ Debe existir: `PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")`

---

### 3. ESQUEMA DE ESCRITURA - Agregar nueva variable

**UBICACIÓN:** Después de `PRM_ESQUEMA_TABLA = ...` y ANTES de `VAL_DESTINO_NAME`

**DDV (original):**
```python
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME+"."+PRM_ESQUEMA_TABLA_DDV
PRM_CARPETA_RAIZ_DE_PROYECTO = CONS_CONTAINER_NAME+PRM_STORAGE_ACCOUNT_DDV+CONS_DFS_NAME+PRM_RUTA_ADLS_TABLES
VAL_DESTINO_NAME= PRM_ESQUEMA_TABLA+"."+PRM_TABLE_NAME
```

**EDV (convertido):**
```python
PRM_CARPETA_RAIZ_DE_PROYECTO = CONS_CONTAINER_NAME+PRM_STORAGE_ACCOUNT_DDV+CONS_DFS_NAME+PRM_RUTA_ADLS_TABLES
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME+"."+PRM_ESQUEMA_TABLA_DDV

# IMPORTANTE: Configuración para escritura en esquema EDV
PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME
```

**VALIDACIÓN:**
- ✅ Debe existir: `PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV`
- ✅ `VAL_DESTINO_NAME` DEBE usar `PRM_ESQUEMA_TABLA_ESCRITURA` (NO `PRM_ESQUEMA_TABLA`)

---

### 4. SCHEMA DDV - Actualizar para usar VIEWS

**DDV (original):**
```python
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables')
```

**EDV (convertido):**
```python
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables_v')
```

**VALIDACIÓN:**
- ✅ `PRM_ESQUEMA_TABLA_DDV` defaultValue DEBE terminar en `_v`
- ✅ Ejemplos válidos: `bcp_ddv_matrizvariables_v`, `bcp_ddv_matriz_v`

---

### 5. TABLAS TEMPORALES - Convertir a Managed Tables

**DDV (con path - INCORRECTO para EDV):**
```python
ruta_salida = f"{PRM_CARPETA_RAIZ_DE_PROYECTO}/temp/{carpeta}_tmp"
tmp_table = f"{PRM_ESQUEMA_TABLA}.{carpeta}_tmp".lower()

df.write.format("delta").mode("overwrite").partitionBy("CODMES") \
    .saveAsTable(tmp_table, path=ruta_salida)

df = spark.read.format("delta").load(ruta_salida)
```

**EDV (managed - CORRECTO):**
```python
tmp_table = f"{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpeta}_tmp".lower()

df.write.format("delta").mode("overwrite").partitionBy("CODMES") \
    .saveAsTable(tmp_table)  # SIN path

df = spark.table(tmp_table)  # Leer con spark.table
```

**VALIDACIÓN:**
- ❌ NO debe haber: `.saveAsTable(tabla, path=...)`
- ✅ Debe usar: `.saveAsTable(tabla)` sin path
- ✅ Temporales deben usar: `f"{PRM_ESQUEMA_TABLA_ESCRITURA}..."`
- ✅ Lecturas deben usar: `spark.table(tmp_table)` (no `spark.read...load()`)

---

### 6. TABLA FINAL - Managed Table

**DDV (puede tener path):**
```python
df.write.format("delta").saveAsTable(VAL_DESTINO_NAME, path=ruta_final)
```

**EDV (managed sin path):**
```python
df.write.format("delta").saveAsTable(VAL_DESTINO_NAME)
```

**VALIDACIÓN:**
- ❌ NO debe haber: `saveAsTable(VAL_DESTINO_NAME, path=...)`
- ✅ Debe ser: `saveAsTable(VAL_DESTINO_NAME)` sin path

---

### 7. LIMPIEZA - DROP TABLE en vez de cleanPaths

**DDV:**
```python
funciones.cleanPaths(spark, [ruta_temp1, ruta_temp2])
```

**EDV:**
```python
# Limpiar tablas temporales
spark.sql(f"DROP TABLE IF EXISTS {PRM_ESQUEMA_TABLA_ESCRITURA}.carpeta1_tmp")
spark.sql(f"DROP TABLE IF EXISTS {PRM_ESQUEMA_TABLA_ESCRITURA}.carpeta2_tmp")
```

**VALIDACIÓN:**
- ⚠️ Si existe `cleanPaths`, debe reemplazarse por `DROP TABLE IF EXISTS`
- ✅ Cada temporal debe tener su DROP TABLE correspondiente

---

## 📊 Checklist de Validación Rigurosa

### Nivel 0: Ambiente EDV (CRÍTICO - Sin puntos pero OBLIGATORIO)

| # | Check | Pattern | Obligatorio |
|---|-------|---------|-------------|
| 0.1 | Container Name EDV | `CONS_CONTAINER_NAME = "abfss://bcp-edv-trdata-012@"` | ✅ SÍ |
| 0.2 | Storage Account Producción | `defaultValue='adlscu1lhclbackp05'` en PRM_STORAGE_ACCOUNT_DDV | ✅ SÍ |
| 0.3 | Catalog Producción | `defaultValue='catalog_lhcl_prod_bcp'` en PRM_CATALOG_NAME | ✅ SÍ |

**Penalización:** Si cualquiera de estos checks falla, se restan -20 puntos cada uno. Son CRÍTICOS porque definen el ambiente correcto de producción.

### Nivel 1: Parámetros EDV (CRÍTICO)

| # | Check | Pattern | Obligatorio |
|---|-------|---------|-------------|
| 1.1 | Widget PRM_CATALOG_NAME_EDV | `dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV"` | ✅ SÍ |
| 1.2 | Widget PRM_ESQUEMA_TABLA_EDV | `dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV"` | ✅ SÍ |
| 1.3 | Get PRM_CATALOG_NAME_EDV | `PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")` | ✅ SÍ |
| 1.4 | Get PRM_ESQUEMA_TABLA_EDV | `PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")` | ✅ SÍ |
| 1.5 | Variable PRM_ESQUEMA_TABLA_ESCRITURA | `PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV` | ✅ SÍ |
| 1.6 | VAL_DESTINO_NAME usa EDV | `VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + ` | ✅ SÍ |

### Nivel 2: Schema DDV Views (CRÍTICO)

| # | Check | Pattern | Obligatorio |
|---|-------|---------|-------------|
| 2.1 | Schema DDV termina en _v | `defaultValue='...._v'` en PRM_ESQUEMA_TABLA_DDV | ✅ SÍ |

### Nivel 3: Tablas Managed (CRÍTICO)

| # | Check | Validación | Obligatorio |
|---|-------|------------|-------------|
| 3.1 | Sin path en saveAsTable | NO existe `saveAsTable(..., path=` | ✅ SÍ |
| 3.2 | Temporales en esquema EDV | tmp_table usa `PRM_ESQUEMA_TABLA_ESCRITURA` | ✅ SÍ |
| 3.3 | Lecturas con spark.table | Existe `spark.table(` para temporales | ✅ SÍ |
| 3.4 | Sin lecturas de path | NO existe `spark.read...load(ruta_temp)` | ✅ SÍ |

### Nivel 4: Optimizaciones Spark (RECOMENDADO)

| # | Check | Config | Obligatorio |
|---|-------|--------|-------------|
| 4.1 | AQE enabled | `spark.conf.set("spark.sql.adaptive.enabled", "true")` | ⚠️ Recomendado |
| 4.2 | Coalesce partitions | `spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")` | ⚠️ Recomendado |
| 4.3 | Skew join | `spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")` | ⚠️ Recomendado |
| 4.4 | Shuffle partitions | `spark.conf.set("spark.sql.shuffle.partitions", "200")` | ⚠️ Recomendado |
| 4.5 | Broadcast threshold | `spark.conf.set("spark.sql.autoBroadcastJoinThreshold"` | ⚠️ Recomendado |
| 4.6 | Overwrite dinámico | `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` | ⚠️ Recomendado |
| 4.7 | Delta optimizeWrite | `spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")` | ⚠️ Recomendado |
| 4.8 | Delta autoCompact | `spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")` | ⚠️ Recomendado |

### Nivel 5: Limpieza (RECOMENDADO)

| # | Check | Validación | Obligatorio |
|---|-------|------------|-------------|
| 5.1 | Sin cleanPaths | NO existe `cleanPaths(` | ⚠️ Recomendado |
| 5.2 | Con DROP TABLE | Existe `DROP TABLE IF EXISTS` | ⚠️ Recomendado |

### Nivel 6: Parámetros Opcionales (ADVERTENCIAS)

| # | Check | Validación | Obligatorio |
|---|-------|------------|-------------|
| 6.1 | Sufijo en PRM_TABLE_NAME | Tabla tiene sufijo `_EDV`, `_RUBEN`, etc. | ⚠️ Recomendado |
| 6.2 | PRM_FECHA_RUTINA actualizada | Fecha es reciente (2024+) | ⚠️ Recomendado |
| 6.3 | Carpeta personalizada | PRM_CARPETA_OUTPUT es de usuario/proyecto | ⚠️ Recomendado |

**Nota:** Estos parámetros son opcionales y dependen del entorno/usuario. El converter NO los cambia automáticamente, pero el validator los detecta y da advertencias.

**Ejemplos de sufijos válidos:**
- `HM_MATRIZTRANSACCIONCAJERO_EDV`
- `HM_MATRIZTRANSACCIONCAJERO_RUBEN`
- `HM_MATRIZTRANSACCIONPOSMACROGIRO_RUBEN_2`
- `HM_MATRIZTRANSACCIONAGENTE_TEST`

**Razón:** Evitar colisiones con tablas existentes en EDV.

---

## 🎯 Score de Compliance

**Cálculo:**
- Nivel 1 (Parámetros): 6 checks × 15 puntos = **90 puntos** (CRÍTICO)
- Nivel 2 (Schema Views): 1 check × 10 puntos = **10 puntos** (CRÍTICO)
- Nivel 3 (Managed Tables): 4 checks × 0 puntos = **0 puntos** (incluido en Nivel 1)
- Nivel 4 (Optimizaciones): Bonus +10 si todas presentes
- Nivel 5 (Limpieza): Bonus +5 si correcto

**Total máximo: 100 puntos**

**Aprobación:**
- ✅ PASS: ≥ 90 puntos (todos los críticos)
- ⚠️ WARNING: 70-89 puntos (falta algún crítico)
- ❌ FAIL: < 70 puntos (faltan múltiples críticos)

---

## 🚨 Errores Comunes

### ❌ Error 1: VAL_DESTINO_NAME sigue usando PRM_ESQUEMA_TABLA
```python
# INCORRECTO
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA + "." + PRM_TABLE_NAME

# CORRECTO
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME
```

### ❌ Error 2: Temporales sin PRM_ESQUEMA_TABLA_ESCRITURA
```python
# INCORRECTO
tmp_table = f"{PRM_ESQUEMA_TABLA}.tabla_tmp"

# CORRECTO
tmp_table = f"{PRM_ESQUEMA_TABLA_ESCRITURA}.tabla_tmp"
```

### ❌ Error 3: Schema DDV sin _v
```python
# INCORRECTO
defaultValue='bcp_ddv_matrizvariables'

# CORRECTO
defaultValue='bcp_ddv_matrizvariables_v'
```

### ❌ Error 4: saveAsTable con path
```python
# INCORRECTO
.saveAsTable(tabla, path=ruta)

# CORRECTO
.saveAsTable(tabla)
```

---

## ✅ Template de Conversión

```python
# ========== SECCIÓN 1: WIDGETS ==========
# ... widgets existentes ...
dbutils.widgets.text(name="PRM_TABLA_PARAM_GRUPO", defaultValue='mm_parametrogrupoconcepto')

# Parámetros específicos para EDV
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')

# ========== SECCIÓN 2: GETS ==========
# ... gets existentes ...
PRM_TABLA_PARAM_GRUPO = dbutils.widgets.get("PRM_TABLA_PARAM_GRUPO")

# Configuración específica para EDV
PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")

# ========== SECCIÓN 3: VARIABLES ==========
PRM_CARPETA_RAIZ_DE_PROYECTO = CONS_CONTAINER_NAME+PRM_STORAGE_ACCOUNT_DDV+CONS_DFS_NAME+PRM_RUTA_ADLS_TABLES
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME+"."+PRM_ESQUEMA_TABLA_DDV

# IMPORTANTE: Configuración para escritura en esquema EDV
PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME
```

---

**Fecha:** 2025-10-04
**Versión:** 2.0 - Reglas Exactas
**Autor:** BCP Analytics Team
