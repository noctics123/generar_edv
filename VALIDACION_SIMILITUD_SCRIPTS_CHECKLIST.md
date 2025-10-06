# Checklist de Validación de Similitud entre Scripts EDV

Este checklist guía la validación para asegurar que dos scripts (p. ej., Macrogiro, Agente, Cajero) tienen lógica, estructura y datos equivalentes. Sirve como base para una futura app de validación automatizada.

## Objetivo
- Validar equivalencia de estructura (estática), lógica (proceso) y datos (salida) entre `SCRIPT_A` y `SCRIPT_B`.
- Entregar un reporte claro con PASS/FAIL por categoría y evidencias.

## Alcance
- Scripts Python EDV ubicados en la raíz del repo (p. ej., `HM_MATRIZTRANSACCIONPOSMACROGIRO_*.py`, `HM_MATRIZTRANSACCIONAGENTE_*.py`, `HM_MATRIZTRANSACCIONCAJERO_*.py`).
- Matrices de variables y transacciones (EDV). No incluye conexiones externas; se trabaja con datos locales y determinísticos.

## Preparación
- [ ] Python 3.10+ en un entorno virtual (`.venv`).
- [ ] Datos sintéticos pequeños y determinísticos que cubran:
  - casos vacíos, nulos, duplicados, formatos variados (fechas, decimales, strings con espacios).
  - variaciones por canal (Macrogiro/Agente/Cajero) con llaves de negocio representativas.
- [ ] Definir seed global para reproducibilidad (si hay aleatoriedad).
- [ ] Normalizar I/O con `pathlib` y `encoding="utf-8"`; sin rutas absolutas.
- [ ] Parametrizar entradas por CLI/ENV (mismos parámetros para ambos scripts).

## Checklist de Estructura (Estático)
- [ ] Ambos scripts exponen punto de entrada (`if __name__ == "__main__"`).
- [ ] Paridad de interfaz CLI (mismos nombres/semántica de argumentos requeridos y opcionales).
- [ ] Mismo patrón de organización: helpers comunes en `utils/` (no lógica duplicada pegada).
- [ ] Nombres y responsabilidades equivalentes: funciones principales, docstrings, constantes.
- [ ] Dependencias equivalentes (módulos importados, sin side-effects al importar).
- [ ] Manejo consistente de errores y logs (niveles, mensajes, formato).
- [ ] I/O consistente: no hardcode de rutas, uso de `pathlib`, escritura determinista (orden predefinido).

Sugerencia de automatización (app):
- Parseo AST para comparar número/nombres de funciones y firmas (mismo set esperado).
- Lint y formato (ruff/black) para detectar divergencias de estilo/estructura.

## Checklist de Lógica (Dinámico)
- [ ] Flujo de pasos equivalente: lectura → limpieza → transformaciones → joins/agregaciones → normalización → salida.
- [ ] Reglas de negocio iguales (filtros, mapeos, defaults, redondeos, manejo de nulos).
- [ ] Tipos y casts iguales por columna (evitar inferencias divergentes).
- [ ] Ordenamiento determinista antes de persistir (por llaves y columnas de control).

Sugerencia de automatización (app):
- Instrumentar métricas por etapa y comparar: conteo de filas, cardinalidad de llaves, sumatorias, nulos por columna.
- Snapshot testing de dataframes intermedios (CSV/Parquet ordenado) con tolerancias.

## Checklist de Datos (Salida)
- [ ] Esquema igual: mismas columnas (nombres, orden esperado) y tipos por columna.
- [ ] Mismo recuento de filas y mismas llaves (anti-join = 0 en ambos sentidos).
- [ ] Contenido igual por fila y columna, con reglas:
  - categóricas: igualdad exacta (aplicar `strip`, normalización de mayúsculas si es regla acordada).
  - numéricas: igualdad exacta o dentro de `epsilon` (definido por columna).
- [ ] Orden de salida determinista (mismas claves de ordenamiento).
- [ ] Hash total y por fila iguales (p. ej., SHA256 de fila normalizada) para trazabilidad.

Sugerencia de automatización (app):
- Comparación por llaves: `merge` y anti-join para detectar faltantes/diferentes.
- Reporte de diffs: top-N diferencias por columna y distribución de errores.

## Robustez y Rendimiento
- [ ] Manejo de errores equivalente (mensajes/exit codes). Sin fallas silenciosas.
- [ ] Idempotencia (ejecutar dos veces produce el mismo resultado).
- [ ] Rendimiento comparable en dataset mediano (tiempo y memoria dentro de un delta esperado).

## Reporte de Validación
- [ ] Generar `reports/<fecha>_validacion_<scriptA>_vs_<scriptB>.md` con:
  - Resumen PASS/FAIL por: Estructura, Lógica, Datos, Robustez.
  - Métricas clave (conteos, sumas, tipos, llaves, tolerancias aplicadas).
  - Enlaces a artefactos: `outputs/`, `logs/`, diffs (CSV de discrepancias).
- [ ] Guardar artefactos grandes fuera del VCS (`.gitignore`).

## Diseño de App de Validación (Propuesta)
Componentes principales:
1) Carga de entradas
   - Lectura de archivos de prueba (CSV/Parquet) con `pathlib` y `encoding="utf-8"`.
   - Config YAML en `config/validacion.yml` con: rutas, llaves, mapeos de columnas, `epsilon` por columna.

2) Ejecutor de scripts
   - Ejecutar `SCRIPT_A` y `SCRIPT_B` con los mismos argumentos (`--input`, `--output`, `--fecha`, etc.).
   - Capturar `stdout/stderr`, exit codes, tiempos y logs en `logs/`.

3) Comparador
   - Esquema: columnas, orden y `dtypes` (pandas/pyarrow).
   - Llaves: anti-join en ambos sentidos debe ser 0.
   - Datos: comparación por columna (exacta, casefold+strip para texto, `np.isclose` con `atol`/`rtol` para numérico).
   - Trazabilidad: hash por fila y hash global.
   - Diferencias: exportar CSV con filas discrepantes y columnas afectadas.

4) Reporte
   - Generar `.md` y opcional `.html` con el resumen y tablas de métricas.

CLI sugerida:
```
python validar.py \
  --script-a HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py \
  --script-b HM_MATRIZTRANSACCIONAGENTE_EDV.py \
  --input data/ejemplo.csv \
  --key COL1,COL2 \
  --epsilon-col COL_NUM=1e-9 \
  --output reports/validacion_macrogiro_vs_agente.md
```

Pruebas (`pytest`):
- `tests/test_validacion_equivalencia.py` con fixtures de micro-datasets y asserts sobre esquema, llaves y datos.

## Criterios de Aceptación (por defecto)
- Estructura: paridad de funciones clave y firma CLI: PASS.
- Esquema: columnas y tipos 100% iguales: PASS.
- Llaves: anti-joins = 0: PASS.
- Datos: 0 diferencias categóricas; diferencias numéricas ≤ `epsilon` por columna: PASS.
- Robustez: exit code 0 en ambos; tiempos similares (±20% referencia): PASS.

## Paso a Paso Operativo
1) Preparar entorno y datos de prueba.
2) Ejecutar ambos scripts con los mismos argumentos y entradas.
3) Normalizar salidas (orden, tipos, trims) antes de comparar.
4) Comparar esquema, llaves y datos con tolerancias.
5) Generar reporte y adjuntar diffs y logs.
6) Revisar PASS/FAIL y documentar hallazgos.

## Notas por Canal (Macrogiro, Agente, Cajero)
- Definir llaves primarias de negocio por matriz (ej.: combinación de `canal`, `terminal`, `fecha`, `hora`, `cod_trx`, etc.).
- Alinear reglas particulares (p. ej., normalización de IDs, redondeos de montos, códigos de estado) antes de la comparación.
- Cuando existan columnas exclusivas de un canal, documentar exclusiones o mapeos equivalentes en `config/validacion.yml`.

## Comandos Útiles (local)
```
# Activar venv (Windows)
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Ejecutar scripts (ajusta nombres y rutas)
python SCRIPT_A.py --input data/ejemplo.csv --output outputs/A.csv
python SCRIPT_B.py --input data/ejemplo.csv --output outputs/B.csv

# (Sugerido) Comparar con un validador futuro
python validar.py --script-a SCRIPT_A.py --script-b SCRIPT_B.py --input data/ejemplo.csv --key COL1,COL2 --output reports/validacion.md
```

---

Checklist rápido (copiable a issues/PRs):
- [ ] Paridad de interfaz CLI
- [ ] Estructura y funciones clave equivalentes
- [ ] Reglas de negocio alineadas
- [ ] Esquema de salida igual (columnas + tipos)
- [ ] Llaves iguales (anti-join 0)
- [ ] Datos equivalentes (texto exacto, numéricos con epsilon)
- [ ] Tiempos y memoria dentro del rango esperado
- [ ] Reporte `.md` generado con evidencias

