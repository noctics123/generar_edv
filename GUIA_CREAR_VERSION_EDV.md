# Guía de Conversión a versión EDV (detallada)

- Alcance: convertir los procesos de HM_MATRIZTRANSACCION para Cajero, Agente y Macrogiro desde versión original (DDV) a versión EDV.
- Objetivo: estandarizar parámetros, mover temporales a tablas administradas (managed), unificar optimizaciones Spark/Delta y definir validación automatizada (incluida IA si aplica).

## Principios

- Lectura DDV, escritura EDV: las tablas fuente se leen del esquema DDV; la tabla final y temporales se crean en el esquema EDV.
- Temporales managed: sin rutas físicas (`path=`). Se crean como tablas administradas en EDV y se consultan con `spark.table`.
- Optimización consistente: AQE, broadcast, overwrite dinámico, Delta optimizeWrite/autoCompact, repartición por columna de partición antes de escribir.
- Idempotencia: usar `overwrite` con `partitionOverwriteMode=dynamic` y particionar por `CODMES`/`codmes` según el caso.

## Parámetros: del original (DDV) a EDV

- Conservados para lectura (DDV):
  - `PRM_CATALOG_NAME` (DDV).
  - `PRM_ESQUEMA_TABLA_DDV` o `PRM_ESQUEMA_TABLA_X` (DDV).
  - `PRM_TABLA_PRIMERATRANSPUESTA`, `PRM_TABLA_PARAMETROMATRIZVARIABLES`, `PRM_TABLA_HM_DETCONCEPTOCLIENTE`.
  - `PRM_FECHA_RUTINA` y otros de negocio.

- Nuevos/ajustados para escritura (EDV):
  - Añadir widgets:
    - `PRM_CATALOG_NAME_EDV` (p.ej. `catalog_lhcl_prod_bcp_expl`).
    - `PRM_ESQUEMA_TABLA_EDV` (p.ej. `bcp_edv_trdata_012`).
  - Derivados:
    - `PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV`
    - `VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + <tabla_destino>`

Snippet:

```python
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')

PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")
PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + (PRM_TABLE_NAME if 'PRM_TABLE_NAME' in globals() else PRM_TABLA_SEGUNDATRANSPUESTA)
```

Nota: en DDV algunas variantes usan `CONS_CONTAINER_NAME` (p.ej. `abfss://lhcldata@`). En EDV es común `abfss://bcp-edv-trdata-012@`. Si se dejan temporales managed, este valor deja de ser crítico.

## Temporales: de rutas físicas a managed tables

- Antes (DDV típico):

```python
ruta_salida = f"{PRM_CARPETA_RAIZ_DE_PROYECTO}/temp/{carpeta}_tmp"
tmp_table = f"{PRM_ESQUEMA_TABLA}.{carpeta}_tmp".lower()
(df.write
  .format("delta").mode("overwrite").partitionBy("CODMES")
  .saveAsTable(tmp_table, path=ruta_salida))

df = spark.read.format("delta").load(ruta_salida)
```

- Después (EDV recomendado):

```python
tmp_table = f"{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpeta}_tmp".lower()
(df.write
  .format("delta").mode("overwrite").partitionBy("CODMES")
  .saveAsTable(tmp_table))

df = spark.table(tmp_table)
```

- Limpieza de temporales:
  - Reemplazar limpieza de rutas (`cleanPaths`) por SQL de drop:
  - `spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")` tras la escritura final, para cada temporal.

Reglas de naming:
- Prefijo con esquema de escritura: `PRM_ESQUEMA_TABLA_ESCRITURA`.
- Sufijo `_tmp` consistente: `<carpeta>_tmp`.
- Minúsculas para evitar conflictos (como en Agente/Cajero EDV actual).

## Escritura final (managed en EDV) y particiones

Función de escritura estándar:

```python
def write_delta(df, output, partition):
    df.write \
      .format('delta') \
      .option('compression','snappy') \
      .option('partitionOverwriteMode','dynamic') \
      .partitionBy(partition).mode('overwrite') \
      .saveAsTable(output)

input_df = input_df.repartition('CODMES')  # usar 'codmes' si la partición es minúscula
write_delta(input_df, VAL_DESTINO_NAME, 'CODMES')
```

Atención a la columna de partición por familia:
- Macrogiro: `CODMES` (mayúsculas).
- Agente/Cajero: suele definirse `codmes` en constantes; validar caso en el script base EDV.

## Optimización en EDV (config de sesión)

```python
# AQE y skew
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64m")

# Shuffles y broadcast
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64*1024*1024))

# Overwrite dinámico de particiones
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Delta I/O
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

Persistencia/caching:
- Preferir `persist(StorageLevel.MEMORY_AND_DISK)` en DFs grandes.
- `dfInfo12Meses.cache()` si se usa en múltiples pasos.
- `unpersist()` tras el uso para liberar memoria.

## Guía por familia

- Macrogiro
  - Base vs EDV: ya alineados en optimizaciones y `repartition(CODMES)`.
  - Asegurar que `VAL_DESTINO_NAME` apunte a `PRM_ESQUEMA_TABLA_ESCRITURA` y que los temporales (si existen) estén en EDV managed.

- Agente
  - Añadir widgets EDV y construir `PRM_ESQUEMA_TABLA_ESCRITURA`.
  - Cambiar temporales a `saveAsTable(tmp)` sin `path` y leer con `spark.table`.
  - `VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLA_SEGUNDATRANSPUESTA`.
  - Validar partición (`codmes` vs `CODMES`) y ajustar `repartition`/`partitionBy`.

- Cajero
  - Igual que Agente: temporales managed y escritura final en EDV.
  - Evitar uso de rutas de `CONS_CONTAINER_NAME` salvo que sea imprescindible para lectura.

## Transformaciones automáticas (para la web app)

Heurísticas de transformación estática (regex) sobre el script base:

- Inyección EDV:
  - Insertar widgets `PRM_CATALOG_NAME_EDV`, `PRM_ESQUEMA_TABLA_EDV` y los `get()`.
  - Declarar `PRM_ESQUEMA_TABLA_ESCRITURA` y reasignar `VAL_DESTINO_NAME`.

- Temporales de path a managed:
  - Buscar `.saveAsTable\([^\)]*path\s*=\s*[^\)]*\)` y reemplazar por `.saveAsTable(<tmp_table>)`.
  - Forzar `<tmp_table>` a `f"{PRM_ESQUEMA_TABLA_ESCRITURA}.{nombre}_tmp".lower()`.
  - Reemplazar lecturas `spark.read.format(...).load(ruta)` por `spark.table(tmp_table)`.

- Optimización y repartition:
  - Si faltan, añadir las `spark.conf.set` listadas.
  - Insertar `input_df = input_df.repartition(<partición>)` antes de `write_delta`.

- Limpieza:
  - Eliminar/omitir `cleanPaths` y agregar `DROP TABLE IF EXISTS` para cada temporal.

Pseudocódigo del convertidor:

```text
1) Parsear archivo en bloques (líneas, no AST) para robustez ante SQL embebido.
2) Detectar sección de widgets; inyectar los EDV si faltan.
3) Detectar/ajustar definición de VAL_DESTINO_NAME a EDV.
4) Reescribir bloques saveAsTable/load para temporales.
5) Injectar/validar optimizaciones y repartition.
6) Agregar limpieza de temporales con DROP TABLE IF EXISTS.
7) Emitir diff + checklist de compliance.
```

Regex de ejemplo (JavaScript/Python compatible):
- `saveAsTable\((?P<args>[^)]*)path\s*=\s*[^)]*\)` → reemplazar por `saveAsTable(<tmp_table>)`.
- `spark\.read\.format\([^)]*\)\.load\([^)]*\)` → `spark.table(<tmp_table>)`.
- `VAL_DESTINO_NAME\s*=\s*[^\n]+` → apuntar a `PRM_ESQUEMA_TABLA_ESCRITURA`.

## Validación funcional y de paridad

Checks sugeridos (con mismos `PRM_FECHA_RUTINA` y fuentes DDV):
- Conteo de filas total y por partición (`CODMES`/`codmes`).
- Sumas por métricas clave (monto/tkt/cantidad) por clave: `CODMESANALISIS`,`CODUNICOCLI`,`CODINTERNOCOMPUTACIONAL`.
- Muestreo de 100 claves al azar y diff fila a fila.
- Validar esquema/nullable y tipos en `describe detail` de Delta.

Snippets útiles:

```python
assert spark.table(VAL_DESTINO_NAME).count() == df_origen.count()

# Ejemplo de hash por clave
df_hash = (spark.table(VAL_DESTINO_NAME)
  .select('CODMESANALISIS','CODUNICOCLI','CODINTERNOCOMPUTACIONAL', sha2(concat_ws('|', *metric_cols), 256).alias('h')))
```

## Validación con IA: qué es viable

- GitHub Pages (estático):
  - No ejecuta IA del lado servidor. Se puede validar solo con reglas estáticas (regex) en el navegador.
  - Usar IA desde Pages implica exponer un API key en el cliente (desaconsejado).

- GitHub Actions (recomendado):
  - Al abrir un PR, un workflow ejecuta:
    - Convertidor estático + validadores de compliance.
    - Paso de IA opcional (LLM) para auditar el diff y checklist (usando secretos del repo, p.ej. `OPENAI_API_KEY`).
  - Los resultados (Markdown/JSON) pueden publicarse como artefactos y enlazarse desde GitHub Pages.

Ejemplo de workflow mínimo:

```yaml
name: edv-conversion-validate
on:
  pull_request:
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - run: pip install openai==1.*
      - env:
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
        run: |
          python scripts/validate_edv.py
```

Esqueleto `scripts/validate_edv.py` (pseudo):

```python
from openai import OpenAI
import re, pathlib

client = OpenAI()
text = pathlib.Path('HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py').read_text('utf-8')
checks = [
  ('has_edv_widgets', r"PRM_CATALOG_NAME_EDV.*PRM_ESQUEMA_TABLA_EDV"),
  ('writes_to_edv', r"PRM_ESQUEMA_TABLA_ESCRITURA\s*=.*PRM_ESQUEMA_TABLA_EDV"),
  ('managed_temp', r"saveAsTable\([^\)]*\)(?!,\s*path=)"),
  ('repartition_before_write', r"repartition\([^\)]*(CODMES|codmes)[^\)]*\).*saveAsTable"),
]
report = {name: bool(re.search(pat, text, re.S|re.I)) for name, pat in checks}

# Paso IA opcional
prompt = f"Revisa compliance EDV y sugiere mejoras.\n\n{text[:10000]}"
resp = client.chat.completions.create(model='gpt-4o-mini', messages=[{'role':'user','content':prompt}])
print(report)
print(resp.choices[0].message.content)
```

- Alternativa: usar un proxy serverless (Cloudflare Workers, Vercel) para exponer IA a una UI en Pages sin exponer secrets.

## Diseño de la Web App (GitHub Pages)

Componentes (estático):
- Cargar script (input file) o seleccionar del repo vía fetch raw.
- Motor de transformación en JS (regex) aplicando reglas descritas.
- Vista de diff (antes/después) con resaltado.
- Checklist de compliance (regex) y warnings.
- Botón para descargar script EDV o abrir PR (integración con GitHub API a través de Actions).

Limitaciones:
- GitHub Pages no puede ejecutar Spark ni acceder a bases; validación de datos debe ser offline (Actions) o en un backend.

## Checklist de salida EDV

- [ ] Widgets `PRM_CATALOG_NAME_EDV` y `PRM_ESQUEMA_TABLA_EDV` presentes y resueltos.
- [ ] `PRM_ESQUEMA_TABLA_ESCRITURA` y `VAL_DESTINO_NAME` apuntan al esquema EDV.
- [ ] Temporales con `saveAsTable` SIN `path=` y lecturas con `spark.table`.
- [ ] `DROP TABLE IF EXISTS` para cada temporal al final.
- [ ] Config de sesión (AQE, broadcast, overwrite dinámico, Delta optimize/autoCompact) activas.
- [ ] `input_df.repartition(<partición>)` antes de `write_delta`.
- [ ] Partición correcta (`CODMES` vs `codmes`).
- [ ] Persistencias/caches y `unpersist()` equilibrados.
- [ ] Sin rutas absolutas ni credenciales hardcodeadas.

## Gobierno, permisos y seguridad

- Requiere permisos de `CREATE TABLE`/`WRITE` en el catálogo EDV.
- Mantener mismas versiones de `funciones.py` y librerías (`bcp_coe_data_cleaner`) entre ambientes.
- No exponer claves en el cliente; usar secretos en GitHub Actions o backends seguros.

## Referencias del repo

- Macrogiro: `HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py` vs `HM_MATRIZTRANSACCIONPOSMACROGIRO.py`.
- Agente: `MATRIZVARIABLES_HM_MATRIZTRANSACCIONAGENTE_EDV.py` vs `MATRIZVARIABLES_HM_MATRIZTRANSACCIONAGENTE.py`.
- Cajero: `MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO_EDV.py` vs `MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO.py`.

