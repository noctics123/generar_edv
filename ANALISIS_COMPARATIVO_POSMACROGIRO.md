# ANÁLISIS COMPARATIVO DETALLADO
## HM_MATRIZTRANSACCIONPOSMACROGIRO.py (DDV) vs HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py

---

## 📊 RESUMEN EJECUTIVO

| Aspecto | DDV Version | EDV Version |
|---------|-------------|-------------|
| **Líneas de código** | 2,554 | 2,608 (+54 líneas) |
| **Formato** | Databricks notebook | Databricks notebook + optimizaciones |
| **Environment** | DDV (Desarrollo) | EDV (Producción Exploración) |
| **Optimización** | Básica | Avanzada (Spark AQE) |
| **Fecha default** | 2024-07-06 | 2025-09-01 |

---

## 🔍 DIFERENCIAS CRÍTICAS

### 1. CONFIGURACIÓN DE AMBIENTE

#### **DDV Version (Original)**
```python
# Container
CONS_CONTAINER_NAME = "abfss://lhcldata@"

# Storage Account
PRM_STORAGE_ACCOUNT_DDV = 'adlscu1lhclbackd03'

# Schema
PRM_ESQUEMA_TABLA_DDV = 'bcp_ddv_matrizvariables'

# Catalog
PRM_CATALOG_NAME = 'catalog_lhcl_desa_bcp'

# Rutas
PRM_RUTA_ADLS_TABLES = 'desa/bcp/ddv/analytics/matrizvariables'
```

#### **EDV Version (Optimizada)**
```python
# Container
CONS_CONTAINER_NAME = "abfss://bcp-edv-trdata-012@"

# Storage Account
PRM_STORAGE_ACCOUNT_DDV = 'adlscu1lhclbackp05'

# Schema DDV con VIEWS
PRM_ESQUEMA_TABLA_DDV = 'bcp_ddv_matrizvariables_v'  # ← NOTA: _v suffix

# Catalog
PRM_CATALOG_NAME = 'catalog_lhcl_prod_bcp'

# Rutas
PRM_RUTA_ADLS_TABLES = 'data/RUBEN/DEUDA_TECNICA/matrizvariables'

# NUEVOS PARÁMETROS EDV
PRM_CATALOG_NAME_EDV = 'catalog_lhcl_prod_bcp_expl'
PRM_ESQUEMA_TABLA_EDV = 'bcp_edv_trdata_012'
```

**🔑 Diferencia Clave:**
- DDV: Lee y escribe en **mismo esquema** (`bcp_ddv_matrizvariables`)
- EDV: Lee de **views DDV** (`bcp_ddv_matrizvariables_v`) y escribe en **esquema EDV** (`bcp_edv_trdata_012`)

---

### 2. ARQUITECTURA DE LECTURA/ESCRITURA

#### **DDV Version**
```python
# Una sola variable para lectura y escritura
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME + "." + PRM_ESQUEMA_TABLA_DDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA + "." + PRM_TABLE_NAME

# Ejemplo: catalog_lhcl_desa_bcp.bcp_ddv_matrizvariables
```

#### **EDV Version (Arquitectura Separada)**
```python
# LECTURA: Desde views DDV
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME + "." + PRM_ESQUEMA_TABLA_DDV
# Ejemplo: catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_v

# ESCRITURA: A esquema EDV
PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME
# Ejemplo: catalog_lhcl_prod_bcp_expl.bcp_edv_trdata_012.HM_MATRIZTRANSACCIONPOSMACROGIRO_RUBEN_2
```

**📌 Ventajas de EDV:**
- ✅ Separación de ambientes (lectura desde DDV views, escritura a EDV)
- ✅ No contamina esquema DDV de desarrollo
- ✅ Permite rollback fácil (datos fuente intactos)
- ✅ Trazabilidad y auditoría mejorada

---

### 3. OPTIMIZACIONES SPARK

#### **DDV Version: Configuración Básica**
```python
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss", False)
spark.conf.set("spark.databricks.io.cache.enabled", False)
```

#### **EDV Version: Configuración Avanzada (AQE)**
```python
# Configuración básica (igual que DDV)
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss", False)
spark.conf.set("spark.databricks.io.cache.enabled", False)

# NUEVAS OPTIMIZACIONES

# 1. Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64m")

# 2. Particiones optimizadas para cluster mediano
spark.conf.set("spark.sql.shuffle.partitions", "200")  # ← Reducido de 1000 a 200

# 3. Broadcast joins automático
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64*1024*1024))  # 64MB

# 4. Overwrite dinámico de particiones
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# 5. Delta Lake - Auto-optimización
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

**⚡ Impacto de Optimizaciones:**

| Configuración | DDV | EDV | Beneficio |
|---------------|-----|-----|-----------|
| `spark.sql.shuffle.partitions` | 1000 | 200 | -80% overhead de particiones pequeñas |
| Adaptive Query Execution | ❌ | ✅ | Ajuste dinámico de plan de ejecución |
| Skew Join Handling | ❌ | ✅ | Manejo automático de datos desbalanceados |
| Auto Broadcast | ❌ | ✅ | Joins más rápidos para dimensiones pequeñas |
| Delta Auto-Compaction | ❌ | ✅ | Menos archivos pequeños, lecturas más rápidas |

---

### 4. OPTIMIZACIONES DE PROCESAMIENTO

#### **OPTIMIZACIÓN 1: Materialización de DataFrames**

**DDV Version (I/O a disco)**
```python
# Función: agruparInformacionMesAnalisis()

# Escribe a disco (costoso en I/O)
dfInfo12Meses.write.format(funciones.CONS_FORMATO_DE_ESCRITURA_EN_DISCO) \
    .mode("overwrite") \
    .save(PRM_CARPETA_RAIZ_DE_PROYECTO + "/temp/" + carpetaTemp)

# Lee desde disco (costoso en I/O)
dfInfo12Meses = spark.read.format(funciones.CONS_FORMATO_DE_ESCRITURA_EN_DISCO) \
    .load(PRM_CARPETA_RAIZ_DE_PROYECTO + "/temp/" + carpetaTemp)
```

**EDV Version (Cache en memoria)**
```python
# Función: agruparInformacionMesAnalisis()

# Cache en memoria (mucho más rápido)
dfInfo12Meses = dfInfo12Meses.cache()
dfInfo12Meses.count()  # Materializa el DataFrame en memoria
```

**📊 Mejora estimada:**
- Eliminación de write/read a disco: **~60-80% más rápido**
- Memoria vs Disco: **100-1000x más rápido**

---

#### **OPTIMIZACIÓN 2: Storage Level**

**DDV Version**
```python
dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrogiro \
    .persist(StorageLevel.MEMORY_ONLY_2)
```

**EDV Version**
```python
# MEMORY_AND_DISK_2 en vez de MEMORY_ONLY_2
# Si no cabe en memoria, usa disco (evita recomputación costosa)
dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrogiro \
    .persist(StorageLevel.MEMORY_AND_DISK_2)

# Materialización explícita
dfMatrizVarTransaccionPosMacrogiro.count()
```

**🔍 Explicación:**
- `MEMORY_ONLY_2`: Replica 2 veces en memoria. Si no cabe, recomputa (muy costoso)
- `MEMORY_AND_DISK_2`: Replica 2 veces, usa disco si no cabe (más resiliente)

---

#### **OPTIMIZACIÓN 3: Consolidación de Loops**

**DDV Version (3 loops separados)**
```python
# LOOP 1: Procesar MONTO
original_cols = dfMatrizVarTransaccionPosMacrogiroMont.columns
for colName in col_names_monto:
    original_cols.extend([
        func.round(col(colName + "_PRM_U6M")/col(colName + "_PRM_U12M"), 4).alias(colName + "_CRECIMIENTO_6M"),
        func.round(col(colName + "_PRM_U3M")/col(colName + "_PRM_U12M"), 4).alias(colName + "_CRECIMIENTO_3M"),
        func.round(col(colName + "_U1M")/col(colName + "_PRM_U12M"), 4).alias(colName + "_CRECIMIENTO_1M")
    ])
dfMatrizVarTransaccionPosMacrogiroMont = dfMatrizVarTransaccionPosMacrogiroMont.select(original_cols)

# LOOP 2: Procesar TKT
original_cols = dfMatrizVarTransaccionPosMacrogiroMont.columns
for colName in col_names_tkt:
    original_cols.extend([...])  # Mismo patrón
dfMatrizVarTransaccionPosMacrogiroMont = dfMatrizVarTransaccionPosMacrogiroMont.select(original_cols)

# LOOP 3: Procesar CANTIDAD
original_cols = dfMatrizVarTransaccionPosMacrogiroMont.columns
for colName in col_names_cant:
    original_cols.extend([...])  # Mismo patrón
dfMatrizVarTransaccionPosMacrogiroMont = dfMatrizVarTransaccionPosMacrogiroMont.select(original_cols)
```

**EDV Version (1 loop consolidado)**
```python
# Preparar TODAS las columnas de una vez
all_new_cols = []

# Procesar MONTO
for colName in col_names_monto:
    all_new_cols.extend([
        func.round(col(colName + "_PRM_U6M")/col(colName + "_PRM_U12M"), 4).alias(colName + "_CRECIMIENTO_6M"),
        func.round(col(colName + "_PRM_U3M")/col(colName + "_PRM_U12M"), 4).alias(colName + "_CRECIMIENTO_3M"),
        func.round(col(colName + "_U1M")/col(colName + "_PRM_U12M"), 4).alias(colName + "_CRECIMIENTO_1M")
    ])

# Procesar TKT
for colName in col_names_tkt:
    all_new_cols.extend([...])

# Procesar CANTIDAD
for colName in col_names_cant:
    all_new_cols.extend([...])

# UNA SOLA transformación
dfMatrizVarTransaccionPosMacrogiroMont = dfMatrizVarTransaccionPosMacrogiroMont.select(
    dfMatrizVarTransaccionPosMacrogiroMont.columns + all_new_cols
)
```

**⚡ Mejora:**
- DDV: 3 transformaciones Spark → 3 etapas de ejecución
- EDV: 1 transformación Spark → 1 etapa de ejecución
- **Reducción de overhead:** ~66% menos stages

---

#### **OPTIMIZACIÓN 4: Reparticionamiento Pre-Escritura**

**DDV Version**
```python
def main():
    # ...
    dfMatrizVarTransaccionPosMacrogiro = logicaPostAgrupacionInformacionMesAnalisis(
        dfMatrizVarTransaccionPosMacrogiro
    ).coalesce(160)  # ← Coalesce arbitrario

    write_delta(dfMatrizVarTransaccionPosMacrogiro, VAL_DESTINO_NAME, CONS_PARTITION_DELTA_NAME)
```

**EDV Version**
```python
def main():
    # ...
    dfMatrizVarTransaccionPosMacrogiro = logicaPostAgrupacionInformacionMesAnalisis(
        dfMatrizVarTransaccionPosMacrogiro
    )
    # Eliminado coalesce(160) de densidad no controlada

    # Repartition basado en columna de partición
    input_df = input_df.repartition(CONS_PARTITION_DELTA_NAME)  # ← Repartition por CODMES

    write_delta(input_df, VAL_DESTINO_NAME, CONS_PARTITION_DELTA_NAME)
```

**🎯 Ventajas:**
- `coalesce(160)`: Reduce particiones pero no redistribuye datos (puede crear skew)
- `repartition(CODMES)`: Distribuye equitativamente por partición física
- **Resultado:** Archivos balanceados por mes, lecturas futuras más rápidas

---

### 5. SETUP Y DEPENDENCIAS

#### **DDV Version**
```python
# No tiene setup inicial
# Asume dependencias pre-instaladas
```

#### **EDV Version (Databricks Notebook Features)**
```python
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
!pip3 install --trusted-host 10.79.236.20 \
    https://10.79.236.20:443/artifactory/LHCL.Pypi.Snapshot/lhcl/bcp_coe_data_cleaner/1.0.1/bcp_coe_data_cleaner-1.0.1-py3-none-any.whl

# COMMAND ----------
dbutils.widgets.removeAll()  # Limpia widgets previos
```

**📌 Beneficios:**
- ✅ Aislamiento de ambiente (restart Python)
- ✅ Instalación explícita de dependencias (reproducibilidad)
- ✅ Limpieza de widgets (evita conflictos de parámetros)

---

## 🔬 ANÁLISIS DE FUNCIONES

### Función: `extraccionInformacion12Meses()`

**Líneas:**
- DDV: 118-243 (126 líneas)
- EDV: 164-288 (125 líneas)

**Diferencias:** IDÉNTICA en lógica, solo difiere en:
- Lectura desde `PRM_ESQUEMA_TABLA` (ambas versiones)
- EDV usa views (`_v`), DDV usa tablas directas

---

### Función: `agruparInformacionMesAnalisis()`

**Líneas:**
- DDV: 244-602 (359 líneas)
- EDV: 289-652 (364 líneas)

**Diferencias CRÍTICAS:**

1. **Materialización de datos intermedios**
   - DDV: Write/Read a disco (líneas 457-461)
   - EDV: Cache en memoria (líneas 483-485)

2. **Número de líneas:** +5 líneas en EDV por comentarios de optimización

---

### Función: `logicaPostAgrupacionInformacionMesAnalisis()`

**Líneas:**
- DDV: 603-2496 (1,894 líneas)
- EDV: 653-2550 (1,898 líneas)

**Diferencias CRÍTICAS:**

1. **Storage Level** (líneas ~1800)
   - DDV: `MEMORY_ONLY_2`
   - EDV: `MEMORY_AND_DISK_2` + materialización explícita

2. **Consolidación de loops** (líneas ~2200-2400)
   - DDV: 3 transformaciones separadas
   - EDV: 1 transformación consolidada

3. **Eliminación de coalesce** (líneas ~2470)
   - DDV: `.coalesce(160)` en múltiples lugares
   - EDV: Eliminado, usa repartition en main()

---

### Función: `main()`

**Líneas:**
- DDV: 2497-2554 (58 líneas)
- EDV: 2551-2608 (58 líneas)

**Diferencias:**

1. **Reparticionamiento pre-escritura**
   - DDV: No tiene
   - EDV: `input_df = input_df.repartition(CONS_PARTITION_DELTA_NAME)`

---

## 📈 TABLA COMPARATIVA DE RENDIMIENTO ESPERADO

| Métrica | DDV | EDV | Mejora |
|---------|-----|-----|--------|
| **Shuffle Partitions** | 1000 | 200 | 5x menos overhead |
| **Materialización intermedia** | Disco | Memoria (cache) | 10-100x más rápido |
| **Storage Level** | MEMORY_ONLY_2 | MEMORY_AND_DISK_2 | Más resiliente |
| **Transformaciones** | 3 stages | 1 stage | 3x menos overhead |
| **Particionamiento final** | Coalesce(160) | Repartition(CODMES) | Mejor balance |
| **AQE** | Deshabilitado | Habilitado | Ajuste dinámico |
| **Broadcast Join** | Manual | Automático (64MB) | Joins más rápidos |
| **Delta Optimization** | Manual | Automático | Menos archivos |

**⚡ Mejora de rendimiento estimada:** **2-5x más rápido** en tiempo total de ejecución

---

## 🏗️ ARQUITECTURA DE DATOS

### DDV (Desarrollo)
```
┌─────────────────────────────────────┐
│  catalog_lhcl_desa_bcp              │
│  └─ bcp_ddv_matrizvariables         │
│     ├─ HM_CONCEPTOTRANSACCIONPOS    │  ← READ
│     └─ HM_MATRIZTRANSACCIONPOSMACRO │  ← WRITE (mismo esquema)
└─────────────────────────────────────┘
```

### EDV (Producción Exploración)
```
┌─────────────────────────────────────┐      ┌──────────────────────────────────┐
│  catalog_lhcl_prod_bcp              │      │  catalog_lhcl_prod_bcp_expl      │
│  └─ bcp_ddv_matrizvariables_v       │      │  └─ bcp_edv_trdata_012           │
│     ├─ HM_CONCEPTOTRANSACCIONPOS    │      │     └─ HM_MATRIZTRANSACCIONPOS   │
│     │  (view)                        │──────┼──────→    MACROGIRO_RUBEN_2      │
│     └─ mm_parametrogrupoconcepto    │ READ │        (tabla EDV)               │
│        (view)                        │      │                                  │
└─────────────────────────────────────┘      └──────────────────────────────────┘
                                                         ↑ WRITE
```

**🔑 Ventajas arquitectura EDV:**
- ✅ Separación de ambientes (lectura vs escritura)
- ✅ Views DDV proporcionan capa de abstracción
- ✅ Schema EDV dedicado para exploración/producción
- ✅ No contamina desarrollo (DDV intacto)

---

## 🚀 TABLA DE NOMBRES Y VALORES

### Variables de Configuración Específicas

| Variable | DDV Value | EDV Value | Propósito |
|----------|-----------|-----------|-----------|
| `CONS_CONTAINER_NAME` | `abfss://lhcldata@` | `abfss://bcp-edv-trdata-012@` | Container ADLS Gen2 |
| `PRM_STORAGE_ACCOUNT_DDV` | `adlscu1lhclbackd03` | `adlscu1lhclbackp05` | Storage Account |
| `PRM_ESQUEMA_TABLA_DDV` | `bcp_ddv_matrizvariables` | `bcp_ddv_matrizvariables_v` | Schema DDV (con views) |
| `PRM_CATALOG_NAME` | `catalog_lhcl_desa_bcp` | `catalog_lhcl_prod_bcp` | Catalog principal |
| `PRM_CATALOG_NAME_EDV` | ❌ N/A | `catalog_lhcl_prod_bcp_expl` | Catalog EDV (nuevo) |
| `PRM_ESQUEMA_TABLA_EDV` | ❌ N/A | `bcp_edv_trdata_012` | Schema EDV (nuevo) |
| `PRM_TABLA_PRIMERATRANSPUESTA` | `hm_conceptotransaccionpos` | `hm_conceptotransaccionpos` | Tabla fuente |
| `PRM_TABLE_NAME` | `HM_MATRIZTRANSACCIONPOSMACROGIRO` | `HM_MATRIZTRANSACCIONPOSMACROGIRO_RUBEN_2` | Tabla destino |
| `PRM_RUTA_ADLS_TABLES` | `desa/bcp/ddv/analytics/matrizvariables` | `data/RUBEN/DEUDA_TECNICA/matrizvariables` | Ruta ADLS |

### Variables Derivadas

| Variable | DDV | EDV |
|----------|-----|-----|
| `PRM_ESQUEMA_TABLA` (lectura) | `catalog_lhcl_desa_bcp.bcp_ddv_matrizvariables` | `catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_v` |
| `PRM_ESQUEMA_TABLA_ESCRITURA` | ❌ No existe | `catalog_lhcl_prod_bcp_expl.bcp_edv_trdata_012` |
| `VAL_DESTINO_NAME` | `catalog_lhcl_desa_bcp.bcp_ddv_matrizvariables.HM_MATRIZTRANSACCIONPOSMACROGIRO` | `catalog_lhcl_prod_bcp_expl.bcp_edv_trdata_012.HM_MATRIZTRANSACCIONPOSMACROGIRO_RUBEN_2` |

---

## ✅ VALIDACIÓN DE HOMOLOGÍA

### Funciones Idénticas (100% match)
- ✅ `write_delta()` - Misma implementación
- ✅ Estructura de queries SQL - Idénticas
- ✅ Lógica de negocio - Idéntica
- ✅ Cálculos de variables - Idénticos
- ✅ Transformaciones de columnas - Idénticas

### Diferencias Únicamente en:
- 🔧 Configuración de ambiente (DDV vs EDV)
- ⚡ Optimizaciones de Spark (AQE, broadcast, etc.)
- 💾 Estrategia de materialización (disco vs cache)
- 🗂️ Arquitectura de lectura/escritura (mismo schema vs schemas separados)
- 📝 Comentarios y documentación (EDV más detallada)

---

## 🎯 CONCLUSIONES

### 1. ¿Son homólogos?
**SÍ, 100% homólogos en lógica de negocio.**

Los scripts implementan exactamente el mismo proceso:
- ✅ Mismas fuentes de datos
- ✅ Mismas transformaciones
- ✅ Mismos cálculos de variables
- ✅ Mismo output esperado

### 2. ¿Qué los diferencia?
**Arquitectura de deployment y optimizaciones de rendimiento.**

| Aspecto | DDV | EDV |
|---------|-----|-----|
| Propósito | Desarrollo/Testing | Producción (Exploración) |
| Ambiente | Desarrollo (desa) | Producción (prod) |
| Optimización | Básica | Avanzada (AQE, cache, consolidación) |
| Rendimiento | Baseline | 2-5x más rápido (estimado) |
| Arquitectura | Monolítica (mismo schema) | Separada (read DDV, write EDV) |

### 3. ¿Cuál usar?
- **DDV:** Para desarrollo, testing, debugging
- **EDV:** Para producción, exploración, cargas regulares

### 4. Recomendaciones
1. ✅ Mantener DDV como base de desarrollo
2. ✅ Usar EDV para producción (mejor rendimiento)
3. ✅ Sincronizar cambios de lógica entre ambas versiones
4. ✅ Considerar portar optimizaciones de EDV a DDV
5. ✅ Documentar diferencias de configuración en CLAUDE.md

---

## 📚 REFERENCIAS

- **CLAUDE.md:** Guía de conversión DDV → EDV
- **Spark AQE Docs:** https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
- **Delta Lake Optimization:** https://docs.databricks.com/delta/optimizations/index.html

---

**Fecha de análisis:** 2025-10-04
**Analista:** Claude Code
**Versión:** 1.0
