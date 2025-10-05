# EDV Converter - Web Application

Aplicación web para convertir automáticamente scripts de PySpark DDV a versión EDV para BCP Analytics.

## 🎯 Descripción

Esta aplicación automatiza la conversión de scripts de matriz de transacciones desde el ambiente DDV (Desarrollo) al ambiente EDV (Exploración/Producción), aplicando:

- ✅ Separación de esquemas (lectura DDV, escritura EDV)
- ✅ Conversión de tablas temporales a managed tables
- ✅ Optimizaciones de rendimiento (AQE, cache, repartition)
- ✅ Validación automática de compliance
- ✅ Generación de diff visual

## 📁 Estructura del Proyecto

```
edv-converter-webapp/
├── public/                      # Archivos estáticos
│   ├── index.html              # Página principal
│   ├── favicon.ico
│   └── examples/               # Scripts de ejemplo
│       ├── agente_ddv.py
│       ├── cajero_ddv.py
│       └── macrogiro_ddv.py
├── src/
│   ├── components/             # Componentes UI
│   │   ├── FileUploader.js    # Carga de archivos
│   │   ├── DiffViewer.js      # Visualización de diferencias
│   │   ├── ComplianceChecklist.js  # Checklist de validación
│   │   └── DownloadButton.js  # Descarga de resultado
│   ├── utils/                  # Lógica de negocio
│   │   ├── edvConverter.js    # Motor de conversión DDV->EDV
│   │   ├── validator.js       # Validador de compliance
│   │   ├── diffGenerator.js   # Generador de diff
│   │   └── patterns.js        # Patrones regex de transformación
│   ├── styles/                 # Estilos CSS
│   │   ├── main.css
│   │   └── diff.css
│   └── app.js                  # Aplicación principal
├── docs/                       # Documentación
│   ├── GUIA_USO.md            # Guía de uso
│   ├── REGLAS_CONVERSION.md   # Reglas de conversión
│   └── API.md                 # API de componentes
├── tests/                      # Tests
│   ├── converter.test.js
│   └── validator.test.js
├── examples/                   # Ejemplos completos
│   ├── agente_antes_despues/
│   ├── cajero_antes_despues/
│   └── macrogiro_antes_despues/
├── package.json
├── .gitignore
└── README.md
```

## 🚀 Características

### 1. Conversión Automática

Transforma scripts DDV a EDV aplicando:

- **Widgets EDV**: Agrega `PRM_CATALOG_NAME_EDV` y `PRM_ESQUEMA_TABLA_EDV`
- **Esquemas separados**: Lee de DDV (`bcp_ddv_matrizvariables_v`), escribe a EDV (`bcp_edv_trdata_012`)
- **Managed tables**: Convierte temporales de path a `saveAsTable()` sin path
- **Optimizaciones**: AQE, broadcast, repartition, Delta optimizeWrite
- **Limpieza**: Reemplaza `cleanPaths` por `DROP TABLE IF EXISTS`

### 2. Validación de Compliance

Verifica automáticamente:

- ✅ Widgets EDV presentes
- ✅ `PRM_ESQUEMA_TABLA_ESCRITURA` correctamente definido
- ✅ `VAL_DESTINO_NAME` apunta a EDV
- ✅ Temporales sin `path=`
- ✅ Lecturas con `spark.table()`
- ✅ Configuraciones Spark AQE
- ✅ Repartition antes de write_delta
- ✅ Partición correcta (CODMES vs codmes)

### 3. Diff Interactivo

- Vista lado a lado (antes/después)
- Resaltado de cambios
- Navegación por secciones modificadas
- Exportación de diff en Markdown

### 4. Ejemplos Incluidos

Scripts de referencia:
- Agente (DDV → EDV)
- Cajero (DDV → EDV)
- Macrogiro (DDV → EDV)

## 🛠️ Tecnologías

- **Frontend**: HTML5, CSS3, JavaScript (Vanilla)
- **Diff Engine**: diff-match-patch
- **Syntax Highlighting**: Prism.js
- **Deployment**: GitHub Pages (estático)

## 📖 Uso

### Opción 1: Cargar Archivo

1. Hacer clic en "Cargar Script DDV"
2. Seleccionar archivo `.py`
3. Hacer clic en "Convertir a EDV"
4. Revisar diff y checklist
5. Descargar script EDV

### Opción 2: Pegar Código

1. Pegar código en el editor
2. Hacer clic en "Convertir a EDV"
3. Revisar cambios
4. Copiar o descargar resultado

### Opción 3: Usar Ejemplo

1. Seleccionar ejemplo (Agente/Cajero/Macrogiro)
2. Ver conversión pre-calculada
3. Estudiar patrones aplicados

## 🔍 Reglas de Transformación

### 1. Widgets EDV

```python
# AGREGADO
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')

PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")
```

### 2. Esquema de Escritura

```python
# AGREGADO
PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV

# MODIFICADO
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLA_SEGUNDATRANSPUESTA
```

### 3. Temporales Managed

```python
# ANTES
df.write.format("delta").saveAsTable(tmp_table, path=ruta_salida)
df = spark.read.format("delta").load(ruta_salida)

# DESPUÉS
df.write.format("delta").saveAsTable(tmp_table)  # Sin path
df = spark.table(tmp_table)
```

### 4. Optimizaciones Spark

```python
# AGREGADO al inicio
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64*1024*1024))
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

### 5. Repartition Pre-Escritura

```python
# AGREGADO antes de write_delta
input_df = input_df.repartition(CONS_PARTITION_DELTA_NAME)
write_delta(input_df, VAL_DESTINO_NAME, CONS_PARTITION_DELTA_NAME)
```

## 🧪 Testing

```bash
# Ejecutar tests
npm test

# Coverage
npm run coverage
```

## 📦 Deployment

### GitHub Pages

```bash
# Build
npm run build

# Deploy
npm run deploy
```

La app estará disponible en: `https://noctics123.github.io/generar_edv/`

## 🤝 Contribuir

1. Fork el repositorio
2. Crear branch de feature (`git checkout -b feature/nueva-regla`)
3. Commit cambios (`git commit -m 'Agregar nueva regla de conversión'`)
4. Push al branch (`git push origin feature/nueva-regla`)
5. Abrir Pull Request

## 📝 Checklist de Conversión EDV

- [ ] Widgets EDV agregados
- [ ] `PRM_ESQUEMA_TABLA_ESCRITURA` definido
- [ ] `VAL_DESTINO_NAME` apunta a EDV
- [ ] Temporales son managed tables (sin `path=`)
- [ ] Lecturas con `spark.table()`
- [ ] Configuraciones Spark AQE aplicadas
- [ ] Repartition antes de write
- [ ] Partición correcta
- [ ] `DROP TABLE IF EXISTS` para temporales
- [ ] Sin rutas hardcodeadas

## 📚 Documentación Adicional

- [Guía de Conversión Completa](docs/GUIA_USO.md)
- [Reglas de Transformación](docs/REGLAS_CONVERSION.md)
- [API de Componentes](docs/API.md)
- [Ejemplos Paso a Paso](examples/)

## 🐛 Problemas Conocidos

- Los scripts muy grandes (>10MB) pueden tardar en procesarse
- Algunos patrones complejos de SQL embebido requieren revisión manual

## 📄 Licencia

MIT

## 👥 Autores

- BCP Analytics Team
- Generado con Claude Code

## 🔗 Enlaces

- [Repositorio](https://github.com/noctics123/generar_edv)
- [Issues](https://github.com/noctics123/generar_edv/issues)
- [Documentación BCP](CLAUDE.md)

---

**Última actualización:** 2025-10-04
