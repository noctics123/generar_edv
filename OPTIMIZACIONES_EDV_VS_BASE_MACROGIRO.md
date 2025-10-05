# Comparación de Optimizaciones: `HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py` vs. `HM_MATRIZTRANSACCIONPOSMACROGIRO.py`

Análisis realizado por el asistente Gemini.

A continuación se detallan las diferencias clave en optimización observadas entre la versión para EDV y la versión base del script. La versión `_EDV.py` implementa mejoras de rendimiento significativas que no se encuentran en la versión base.

### 1. Manejo de Datos Intermedios (I/O de Disco vs. Cache en Memoria)

La versión base materializa un DataFrame intermedio escribiéndolo a disco y releyéndolo, lo cual es una operación de I/O lenta. La versión EDV reemplaza esto con el uso de `.cache()`, que es mucho más eficiente.

*   **Versión Base (`HM_MATRIZTRANSACCIONPOSMACROGIRO.py`):**
    ```python
    # Escribimos el resultado en disco duro para forzar la ejecución del DAG SPARK
    dfInfo12Meses.write.format(...).save(...)
    
    # Leemos el resultado calculado
    dfInfo12Meses = spark.read.format(...).load(...)
    ```

*   **Versión EDV (`HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py`):**
    ```python
    # OPTIMIZACIÓN FASE 1: Reemplazar write/read por cache para forzar evaluación
    dfInfo12Meses = dfInfo12Meses.cache()
    dfInfo12Meses.count()
    ```

### 2. Cálculo de Métricas de Crecimiento (Múltiples Bucles vs. `select` Único)

La versión base utiliza tres bucles `for` secuenciales para calcular las métricas de crecimiento, lo que resulta en tres transformaciones separadas. La versión EDV consolida todos los cálculos en una única operación `select`, permitiendo una optimización más efectiva por parte de Spark.

*   **Versión Base:**
    ```python
    # Bucle 1
    for colName in colsToExpandMonto:
        ...
    # Bucle 2
    for colName in colsToExpandTkt:
        ...
    # Bucle 3
    for colName in colsToExpandCantidad:
        ...
    ```

*   **Versión EDV:**
    ```python
    # OPTIMIZACIÓN FASE 1: Consolidar 3 loops en 1 solo select para mejor performance
    all_new_cols = []
    for colName in colsToExpandMonto:
        all_new_cols.extend(...)
    for colName in colsToExpandTkt:
        all_new_cols.extend(...)
    for colName in colsToExpandCantidad:
        all_new_cols.extend(...)

    dfMatrizVarTransaccionPosMacrogiroMont = dfMatrizVarTransaccionPosMacrogiroMont.select("*", *all_new_cols)
    ```

### 3. Uso Ineficiente de `coalesce` en un Bucle

La versión base incluye una llamada a `.coalesce(160)` dentro de un bucle, una práctica que impacta negativamente el rendimiento al forzar múltiples y costosas remezclas de datos (shuffles). Esta llamada fue correctamente eliminada en la versión EDV.

*   **Versión Base:**
    ```python
    for colName in colsToExpandCantidad2daT:
        ...
        dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrogiro.coalesce(160)
        ...
    ```

*   **Versión EDV:**
    ```python
    for colName in colsToExpandCantidad2daT:
        # OPTIMIZACIÓN FASE 1: Eliminado coalesce(160) de dentro del loop (extremadamente costoso)
        ...
    ```

### 4. Estrategia de Persistencia en Memoria

La versión base utiliza `MEMORY_ONLY_2`, que puede causar errores de falta de memoria (OOM) si el conjunto de datos es grande. La versión EDV utiliza `MEMORY_AND_DISK`, una estrategia más robusta que permite a Spark mover datos a disco si no caben en memoria.

*   **Versión Base:**
    ```python
    dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrog.persist(StorageLevel.MEMORY_ONLY_2)
    ```

*   **Versión EDV:**
    ```python
    # OPTIMIZACIÓN FASE 1: Cambiar MEMORY_ONLY_2 a MEMORY_AND_DISK para evitar OOM
    dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrog.persist(StorageLevel.MEMORY_AND_DISK)
    ```