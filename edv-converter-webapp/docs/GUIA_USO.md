# 📚 Guía de Uso - EDV Converter

## 🎯 Introducción

EDV Converter es una aplicación web que automatiza la conversión de scripts PySpark del ambiente DDV (Desarrollo) al ambiente EDV (Exploración/Producción) para BCP Analytics.

## 🚀 Inicio Rápido

### Opción 1: Ejecutar Localmente

1. **Clonar el repositorio:**
```bash
git clone https://github.com/noctics123/generar_edv.git
cd generar_edv/edv-converter-webapp
```

2. **Instalar dependencias (opcional):**
```bash
npm install
```

3. **Iniciar servidor local:**
```bash
npm start
```

4. **Abrir en navegador:**
```
http://localhost:8080
```

### Opción 2: Usar Versión Online

Visita: `https://noctics123.github.io/generar_edv/`

## 📖 Cómo Usar

### Paso 1: Cargar Script DDV

Tienes 3 opciones para cargar tu script:

#### A. Cargar Archivo
1. Haz clic en "Cargar archivo .py"
2. Selecciona tu script DDV
3. El archivo se cargará automáticamente en el editor

#### B. Pegar Código
1. Copia tu script DDV completo
2. Pega en el editor de texto
3. El contador de líneas se actualizará automáticamente

#### C. Usar Ejemplo
1. Haz clic en uno de los botones de ejemplo:
   - **Agente**: Script de matriz de transacciones para agentes
   - **Cajero**: Script de matriz de transacciones para cajeros
   - **Macrogiro**: Script de matriz de transacciones POS Macrogiro
2. El ejemplo se cargará pre-llenado

### Paso 2: Convertir

1. **Verifica tu script:**
   - Revisa que el código esté completo
   - Verifica el contador de líneas (debe ser > 100 para scripts reales)

2. **Haz clic en "⚡ Convertir a EDV"**
   - El proceso toma 1-2 segundos
   - Verás un mensaje "⏳ Convirtiendo..."

3. **Espera los resultados:**
   - La sección de resultados aparecerá automáticamente
   - Se mostrará un scroll suave hacia los resultados

### Paso 3: Revisar Resultados

#### Tarjetas de Estadísticas

Al completar la conversión verás 4 métricas:

1. **Cambios Aplicados**: Número de transformaciones realizadas
2. **Advertencias**: Alertas que requieren revisión manual
3. **Compliance Score**: Porcentaje de cumplimiento (0-100%)
4. **Estado**: PASS (≥80%) o FAIL (<80%)

#### Pestañas de Resultados

**📊 Diff**
- Vista lado a lado (ANTES | DESPUÉS)
- Código original DDV vs código EDV generado
- Líneas modificadas resaltadas en verde
- Scroll sincronizado

**📄 Script EDV**
- Código completo generado
- Editor de solo lectura
- Botones: 📋 Copiar | 💾 Descargar

**✅ Checklist**
- Lista de validaciones con ✅ o ❌
- 11 checks de compliance:
  1. Widgets EDV
  2. Variables EDV
  3. Esquema de escritura EDV
  4. Destino EDV
  5. Tablas managed
  6. Optimizaciones Spark (8 configs)
  7. Repartition pre-escritura
  8. Columna de partición
  9. Sin rutas hardcodeadas
  10. Schema DDV usa views
  11. Limpieza con DROP TABLE

**📝 Log**
- Cambios aplicados detallados
- Advertencias (requieren atención)
- Errores (deben corregirse)

### Paso 4: Descargar Script EDV

1. **Opción A: Copiar al Portapapeles**
   - Ve a la pestaña "📄 Script EDV"
   - Haz clic en "📋 Copiar"
   - Verás confirmación "✅ Copiado"

2. **Opción B: Descargar Archivo**
   - Haz clic en "💾 Descargar"
   - El archivo se descargará con nombre automático:
     - `MATRIZVARIABLES_HM_MATRIZTRANSACCIONAGENTE_EDV.py`
     - `MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO_EDV.py`
     - `HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py`

## 🔍 Entendiendo los Cambios

### Cambios Automáticos Aplicados

#### 1. Widgets EDV (Agregados)
```python
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')
```

#### 2. Variables EDV (Agregadas)
```python
PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")
PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
```

#### 3. Destino EDV (Modificado)
```python
# ANTES
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA + "." + PRM_TABLE_NAME

# DESPUÉS
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLA_SEGUNDATRANSPUESTA
```

#### 4. Tablas Managed (Convertidas)
```python
# ANTES
df.write.format("delta").saveAsTable(tmp_table, path=ruta_salida)
df = spark.read.format("delta").load(ruta_salida)

# DESPUÉS
df.write.format("delta").saveAsTable(tmp_table)  # Sin path
df = spark.table(tmp_table)
```

#### 5. Optimizaciones Spark (Agregadas)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64*1024*1024))
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

#### 6. Repartition Pre-Escritura (Agregado)
```python
input_df = input_df.repartition(CONS_PARTITION_DELTA_NAME)
write_delta(input_df, VAL_DESTINO_NAME, CONS_PARTITION_DELTA_NAME)
```

#### 7. Schema DDV con Views (Modificado)
```python
# ANTES
PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables'

# DESPUÉS
PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables_v'  # Agregado _v
```

#### 8. Limpieza de Temporales (Sugerido)
```python
# Se sugiere reemplazar
funciones.cleanPaths(...)

# Por
spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")
```

## ⚠️ Advertencias Comunes

### 1. "cleanPaths detectado"
**Problema:** El script usa `funciones.cleanPaths()`

**Solución:** Reemplazar manualmente con:
```python
spark.sql(f"DROP TABLE IF EXISTS {PRM_ESQUEMA_TABLA_ESCRITURA}.tabla_tmp")
```

### 2. "Verificar lecturas con spark.table()"
**Problema:** Las lecturas de temporales pueden estar incorrectas

**Solución:** Asegurar que todas las lecturas de temporales usen:
```python
df = spark.table(f"{PRM_ESQUEMA_TABLA_ESCRITURA}.tabla_tmp")
```

### 3. "No se encontró sección de widgets"
**Problema:** El script no tiene estructura estándar de widgets

**Solución:** Agregar manualmente los widgets EDV al inicio del script

## ✅ Checklist Pre-Deployment

Antes de ejecutar el script EDV en producción:

- [ ] Compliance Score ≥ 80%
- [ ] Todas las advertencias revisadas
- [ ] Tablas temporales sin `path=`
- [ ] Lecturas usan `spark.table()`
- [ ] `DROP TABLE IF EXISTS` reemplaza `cleanPaths`
- [ ] Schema DDV termina en `_v` (views)
- [ ] Optimizaciones Spark presentes
- [ ] Repartition antes de write_delta
- [ ] Sin rutas hardcodeadas
- [ ] Prueba en ambiente de desarrollo primero

## ⌨️ Atajos de Teclado

- **Ctrl + Enter**: Convertir script
- **Ctrl + S**: Descargar script EDV

## 🐛 Solución de Problemas

### El script no se carga
- Verifica que el archivo sea `.py`
- Asegúrate de que no esté corrupto
- Intenta copiar/pegar en vez de cargar archivo

### La conversión falla
- Revisa la consola del navegador (F12)
- Verifica que el script DDV sea válido
- Reporta el issue en GitHub

### El diff no se muestra
- Refresca la página (F5)
- Limpia cache del navegador
- Intenta en otro navegador

### Score muy bajo (<80%)
- Revisa las advertencias en la pestaña Log
- Corrige manualmente los items marcados en rojo
- Vuelve a convertir después de correcciones

## 📞 Soporte

- **Issues**: https://github.com/noctics123/generar_edv/issues
- **Documentación**: [CLAUDE.md](../../CLAUDE.md)
- **Reglas de Conversión**: [REGLAS_CONVERSION.md](REGLAS_CONVERSION.md)

## 🔄 Actualizaciones

Para ver la última versión:
```bash
git pull origin main
```

## 📝 Contribuir

1. Fork el repo
2. Crea tu feature branch
3. Commit tus cambios
4. Push al branch
5. Abre un Pull Request

---

**Última actualización:** 2025-10-04
**Versión:** 1.0.0
