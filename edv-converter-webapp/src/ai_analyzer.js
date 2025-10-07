/**
 * AI Analyzer - LLM-powered Script Analysis
 * ===========================================
 *
 * Cliente para análisis profundo de scripts usando LLMs (OpenAI, Claude, Gemini).
 * Requiere API key del usuario (almacenada localmente).
 *
 * Autor: Claude Code
 * Version: 1.0
 */

class AIAnalyzer {
    constructor() {
        this.provider = this.loadProvider() || 'openai';
        this.apiKey = this.loadAPIKey();
        this.endpoints = {
            openai: 'https://api.openai.com/v1/chat/completions',
            claude: 'https://api.anthropic.com/v1/messages',
            gemini: 'https://generativelanguage.googleapis.com/v1/models/gemini-2.5-flash:generateContent'
        };
        this.models = {
            openai: 'gpt-5',
            claude: 'claude-3-5-sonnet-20241022',
            gemini: 'gemini-2.5-flash'
        };
    }

    /**
     * Configurar proveedor de IA
     */
    setProvider(provider) {
        this.provider = provider;
        localStorage.setItem('ai_provider', provider);
    }

    /**
     * Configurar API key (solo en memoria, NO en localStorage por seguridad)
     */
    setAPIKey(apiKey) {
        this.apiKey = apiKey;
        // NO guardamos en localStorage por seguridad
    }

    /**
     * Cargar proveedor desde localStorage
     */
    loadProvider() {
        return localStorage.getItem('ai_provider');
    }

    /**
     * Cargar API key desde localStorage - DEPRECADO por seguridad
     * Ahora siempre retorna null, requiere ingresar key cada vez
     */
    loadAPIKey() {
        return null; // Por seguridad, no guardamos API keys
    }

    /**
     * Verificar si está configurado
     */
    isConfigured() {
        return this.apiKey && this.provider;
    }

    /**
     * Analizar scripts con IA
     * @param {string} script1 - Contenido del script 1
     * @param {string} script2 - Contenido del script 2
     * @param {object} options - Opciones: { mode: 'ddv-edv' | 'individual', script1Name, script2Name }
     * @returns {Promise<object>} - Resultado del análisis
     */
    async analyzeScripts(script1, script2, options = {}) {
        if (!this.isConfigured()) {
            throw new Error('AI Analyzer no está configurado. Configura tu API key primero.');
        }

        const mode = options.mode || 'individual';
        const script1Name = options.script1Name || 'script1.py';
        const script2Name = options.script2Name || 'script2.py';

        // Generar prompt según el modo
        const prompt = this.generatePrompt(script1, script2, mode, script1Name, script2Name);

        // Llamar a la API según el proveedor
        let response;
        if (this.provider === 'openai') {
            response = await this.callOpenAI(prompt);
        } else if (this.provider === 'claude') {
            response = await this.callClaude(prompt);
        } else if (this.provider === 'gemini') {
            response = await this.callGemini(prompt);
        } else {
            throw new Error(`Proveedor desconocido: ${this.provider}`);
        }

        return this.parseAIResponse(response, mode);
    }

    /**
     * Generar script EDV desde script DDV usando IA
     * @param {string} ddvScript - Script DDV original
     * @param {object} options - Opciones: { applyOptimizations: boolean, scriptName: string }
     * @returns {Promise<object>} - { edvScript, explanation, changes }
     */
    async generateEDVScript(ddvScript, options = {}) {
        if (!this.isConfigured()) {
            throw new Error('AI Analyzer no está configurado. Configura tu API key primero.');
        }

        const applyOptimizations = options.applyOptimizations || false;
        const scriptName = options.scriptName || 'script.py';

        // Generar prompt para conversión DDV→EDV
        const prompt = this.generateEDVConversionPrompt(ddvScript, scriptName, applyOptimizations);

        // Llamar a la API según el proveedor
        let response;
        if (this.provider === 'openai') {
            response = await this.callOpenAI(prompt);
        } else if (this.provider === 'claude') {
            response = await this.callClaude(prompt);
        } else if (this.provider === 'gemini') {
            response = await this.callGemini(prompt);
        } else {
            throw new Error(`Proveedor desconocido: ${this.provider}`);
        }

        return this.parseEDVGenerationResponse(response);
    }

    /**
     * Generar prompt robusto según el modo
     */
    generatePrompt(script1, script2, mode, script1Name, script2Name) {
        if (mode === 'ddv-edv') {
            return this.generateDDVEDVPrompt(script1, script2, script1Name, script2Name);
        } else {
            return this.generateIndividualPrompt(script1, script2, script1Name, script2Name);
        }
    }

    /**
     * Prompt para análisis DDV vs EDV
     */
    generateDDVEDVPrompt(script1, script2, script1Name, script2Name) {
        return `Eres un experto en análisis de código PySpark para conversiones DDV→EDV en entornos Databricks del Banco de Crédito del Perú (BCP).

# OBJETIVO DEL ANÁLISIS DDV vs EDV

Queremos validar si ambos scripts son **equivalentes en estructura, lógica y datos generados**, entendiendo que EDV introduce:
- **Managed tables** (esquemas separados DDV/EDV)
- **Parámetros EDV** (PRM_CATALOG_NAME_EDV, PRM_ESQUEMA_TABLA_EDV, PRM_ESQUEMA_TABLA_ESCRITURA)
- **Optimizaciones de rendimiento** que NO afectan la lógica de negocio

## LO QUE DEBE SER IGUAL (CRÍTICO):

1. **Estructura de datos**: Mismas tablas de entrada/salida, mismas columnas calculadas
2. **Lógica de negocio**: Mismas transformaciones, joins, agregaciones, filtros
3. **Datos generados**: El resultado final debe ser idéntico (excepto diferencias de rendimiento/infraestructura)

## LO QUE PUEDE SER DIFERENTE (ESPERADO EN EDV):

1. **Separación de esquemas**:
   - DDV: Lee/escribe en bcp_ddv_*
   - EDV: Lee de bcp_ddv_*_v (views), escribe en bcp_edv_*

2. **Variables/Widgets adicionales EDV**:
   - PRM_CATALOG_NAME_EDV, PRM_ESQUEMA_TABLA_EDV, PRM_ESQUEMA_TABLA_ESCRITURA
   - dbutils.widgets para parámetros EDV

3. **Optimizaciones de rendimiento** (NO afectan lógica):
   - Cache en memoria (.cache() vs write/read a disco)
   - Consolidación de loops
   - Storage Level (MEMORY_AND_DISK vs MEMORY_ONLY_2)
   - Spark AQE, coalesce, repartition

## ERRORES CRÍTICOS (REPORTAR COMO CRITICAL):

1. **Lógica de negocio diferente**: withColumn, when/otherwise, UDFs, funciones diferentes
2. **Cambios en agregaciones**: groupBy, agg, window functions diferentes
3. **Joins diferentes**: Columnas de join, tipos de join (inner/left/outer) diferentes
4. **Transformaciones de datos diferentes**: cast, trim (si afectan lógica), rename diferentes
5. **Tablas de entrada/salida diferentes** (excepto separación DDV/EDV)

# FORMATO DE RESPUESTA

Responde ÚNICAMENTE con JSON válido (sin bloques de código markdown):

{
  "summary": {
    "is_valid_conversion": true/false,
    "conversion_type": "DDV→EDV" | "EDV→DDV" | "Same Type",
    "similarity_percentage": 0-100,
    "total_differences": number,
    "critical_issues": number,
    "optimizations_applied": number
  },
  "differences": [
    {
      "category": "SCHEMA" | "OPTIMIZATION" | "LOGIC" | "VARIABLES",
      "severity": "CRITICAL" | "INFO",
      "description": "Descripción breve",
      "details": "Detalles completos",
      "recommendation": "Qué hacer",
      "is_expected_ddv_edv": true/false
    }
  ],
  "optimizations": [
    {
      "name": "Nombre optimización",
      "impact": "Impacto estimado",
      "detected": true/false,
      "details": "Detalles"
    }
  ],
  "recommendations": ["Recomendación 1", "Recomendación 2"],
  "conclusion": "Conclusión general"
}

# SCRIPTS A ANALIZAR

## Script 1 (${script1Name}):
${script1}

## Script 2 (${script2Name}):
${script2}

IMPORTANTE: Responde solo con JSON, sin bloques de código markdown.`;
    }

    /**
     * Prompt para análisis de scripts individuales
     */
    generateIndividualPrompt(script1, script2, script1Name, script2Name) {
        return `Eres un experto en análisis de código PySpark para entornos Databricks.

# OBJETIVO DEL ANÁLISIS DE SCRIPTS INDIVIDUALES

Queremos un **análisis profundo e integral** para determinar si estos dos scripts son equivalentes en:

1. **ESTRUCTURA**: Misma arquitectura de funciones, flujo del programa, organización del código
2. **LÓGICA**: Mismas transformaciones, filtros, joins, agregaciones, cálculos
3. **DATOS**: Mismas tablas de entrada/salida, mismas columnas generadas, mismo resultado final

## QUÉ ANALIZAR A FONDO:

### 1. Imports y Dependencias
- ¿Usan las mismas librerías?
- ¿Faltan o sobran imports?
- ¿Imports diferentes implican lógica diferente?

### 2. Configuraciones Spark
- ¿Configuraciones de Spark idénticas o diferentes?
- ¿Diferencias en shuffle partitions, memoria, cache?
- ¿Impactan en el resultado o solo en rendimiento?

### 3. Lógica de Negocio (CRÍTICO)
- ¿withColumn con la misma lógica?
- ¿when/otherwise iguales o diferentes?
- ¿Funciones aplicadas (trim, cast, round, etc.) iguales?
- ¿Agregaciones (groupBy, agg, window) idénticas?
- ¿Joins con mismas columnas y tipo?

### 4. Flujo de Datos
- ¿Mismas tablas de entrada?
- ¿Misma secuencia de transformaciones?
- ¿Mismas tablas de salida?
- ¿Mismas columnas finales?

### 5. Calidad de Datos
- ¿Filtros iguales?
- ¿Manejo de nulos igual?
- ¿Deduplicación igual?
- ¿Validaciones iguales?

## SEVERIDADES:

- **CRITICAL**: Diferencias que generan datos distintos (lógica, joins, agregaciones)
- **HIGH**: Diferencias que pueden afectar el resultado (filtros, transformaciones)
- **MEDIUM**: Diferencias estructurales importantes (funciones, organización)
- **LOW**: Diferencias menores (nombres de variables, comentarios)
- **INFO**: Diferencias solo de rendimiento/optimización

# FORMATO DE RESPUESTA

Responde ÚNICAMENTE con JSON válido (sin bloques de código markdown):

{
  "summary": {
    "similarity_percentage": 0-100,
    "total_differences": number,
    "critical_differences": number,
    "are_equivalent": true/false
  },
  "differences": [
    {
      "category": "IMPORTS" | "CONFIG" | "LOGIC" | "JOINS" | "AGGREGATIONS" | "TRANSFORMATIONS" | "FILTERS" | "OUTPUT",
      "severity": "CRITICAL" | "HIGH" | "MEDIUM" | "LOW" | "INFO",
      "location": "Ubicación en el código",
      "description": "Descripción del cambio",
      "details": "Detalles con ejemplos de código",
      "recommendation": "Qué hacer"
    }
  ],
  "similarities": [
    "Lista de similitudes importantes"
  ],
  "recommendations": [
    "Recomendaciones finales"
  ],
  "conclusion": "Conclusión general sobre equivalencia"
}

# SCRIPTS A ANALIZAR

## Script 1 (${script1Name}):
${script1}

## Script 2 (${script2Name}):
${script2}

IMPORTANTE: Responde solo con JSON, sin bloques de código markdown.`;
    }

    /**
     * Prompt para generación de script EDV desde DDV
     */
    generateEDVConversionPrompt(ddvScript, scriptName, applyOptimizations) {
        return `Eres un experto en conversión de scripts PySpark DDV→EDV para el Banco de Crédito del Perú (BCP) en entornos Databricks.

# OBJETIVO

Convertir el siguiente script DDV a formato EDV, aplicando TODAS las reglas de conversión obligatorias${applyOptimizations ? ' y las 6 optimizaciones de rendimiento' : ''}.

# REGLAS OBLIGATORIAS DE CONVERSIÓN DDV→EDV

## 1. WIDGETS Y PARÁMETROS (CRÍTICO)

### Widgets existentes DDV a modificar:
\`\`\`python
# ANTES (DDV):
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables')
dbutils.widgets.text(name="PRM_TABLA_PRIMERATRANSPUESTA", defaultValue='hm_conceptotransaccion...')

# DESPUÉS (EDV):
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables_v')  # Agregar _v
dbutils.widgets.text(name="PRM_TABLA_PRIMERATRANSPUESTA", defaultValue='HM_CONCEPTOTRANSACCION...')  # UPPERCASE
\`\`\`

### Nuevos widgets EDV (agregar después de PRM_CATALOG_NAME):
\`\`\`python
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')
\`\`\`

### Nuevas variables EDV (agregar después de obtener widgets):
\`\`\`python
PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")
\`\`\`

## 2. SEPARACIÓN DE ESQUEMAS (CRÍTICO)

\`\`\`python
# ANTES (DDV - un solo esquema):
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME+"."+PRM_ESQUEMA_TABLA_DDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA+"."+PRM_TABLE_NAME

# DESPUÉS (EDV - esquemas separados):
# Para LECTURA (usa views DDV)
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME+"."+PRM_ESQUEMA_TABLA_DDV
PRM_CARPETA_RAIZ_DE_PROYECTO = CONS_CONTAINER_NAME+PRM_STORAGE_ACCOUNT_DDV+CONS_DFS_NAME+PRM_RUTA_ADLS_TABLES

# Para ESCRITURA (usa schema EDV)
PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLA_SEGUNDATRANSPUESTA
\`\`\`

**IMPORTANTE**:
- Lectura: Usar \`PRM_ESQUEMA_TABLA\` (DDV views)
- Escritura: Usar \`PRM_ESQUEMA_TABLA_ESCRITURA\` o \`VAL_DESTINO_NAME\` (EDV)

${applyOptimizations ? `
## 3. OPTIMIZACIONES DE RENDIMIENTO (APLICAR)

### Optimización #1: Configuraciones Spark AQE (agregar al inicio)
\`\`\`python
# OPTIMIZACIÓN: Configuración de Spark para mejor rendimiento
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64m")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64*1024*1024))
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
\`\`\`

### Optimización #2: Cache en memoria (reemplazar write/read a disco)
\`\`\`python
# ANTES:
dfInfo12Meses.write.format(...).save(...)
dfInfo12Meses = spark.read.format(...).load(...)

# DESPUÉS:
# OPTIMIZACIÓN: Reemplazar write/read a disco por cache en memoria
dfInfo12Meses = dfInfo12Meses.cache()
dfInfo12Meses.count()  # Fuerza la materialización
\`\`\`

### Optimización #3: Storage Level MEMORY_AND_DISK
\`\`\`python
# ANTES:
df.persist(StorageLevel.MEMORY_ONLY_2)

# DESPUÉS:
# OPTIMIZACIÓN: Cambiar MEMORY_ONLY_2 a MEMORY_AND_DISK para evitar OOM
df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()  # Materializar explícitamente
\`\`\`

### Optimización #4: Consolidación de loops
Si hay múltiples loops con \`.select()\` separados, consolidar en uno solo.

### Optimización #5: Eliminar coalesce en loops
Eliminar \`coalesce(160)\` dentro de loops.

### Optimización #6: Repartition pre-escritura
\`\`\`python
# Antes de write_delta:
input_df = input_df.repartition(CONS_PARTITION_DELTA_NAME)
write_delta(input_df, VAL_DESTINO_NAME, CONS_PARTITION_DELTA_NAME)
\`\`\`
` : ''}

# VALIDACIONES CRÍTICAS

1. ✅ PRM_TABLA_PRIMERATRANSPUESTA en UPPERCASE
2. ✅ PRM_ESQUEMA_TABLA_DDV con sufijo _v
3. ✅ Lectura usa PRM_ESQUEMA_TABLA (DDV)
4. ✅ Escritura usa PRM_ESQUEMA_TABLA_ESCRITURA (EDV)
5. ✅ NO cambiar lógica de negocio
6. ✅ NO cambiar nombres de funciones
7. ✅ NO cambiar tablas de entrada (excepto schema)

# FORMATO DE RESPUESTA

Responde ÚNICAMENTE con JSON válido (sin bloques de código markdown):

{
  "edv_script": "CÓDIGO PYTHON COMPLETO DEL SCRIPT EDV",
  "changes": [
    {
      "type": "WIDGET" | "SCHEMA" | "OPTIMIZATION" | "VARIABLE",
      "location": "Línea aproximada o función",
      "description": "Qué se cambió",
      "before": "Código antes (si aplica)",
      "after": "Código después"
    }
  ],
  "optimizations_applied": ["Lista de optimizaciones aplicadas (si apply_optimizations=true)"],
  "validation_checklist": {
    "uppercase_table_name": true/false,
    "schema_separation": true/false,
    "views_suffix": true/false,
    "edv_variables": true/false
  },
  "warnings": ["Lista de advertencias o consideraciones"],
  "summary": "Resumen breve de la conversión (2-3 líneas)"
}

# SCRIPT DDV A CONVERTIR (${scriptName}):

${ddvScript}

IMPORTANTE:
- Responde solo con JSON válido
- El campo "edv_script" debe contener el código Python completo
- NO incluyas bloques de código markdown en la respuesta
- Mantén TODA la lógica de negocio idéntica`;
    }

    /**
     * Parsear respuesta de generación EDV
     */
    parseEDVGenerationResponse(responseText) {
        try {
            let jsonText = responseText;

            // Método 1: Extraer de bloques ```json ... ```
            let jsonMatch = responseText.match(/```json\s*([\s\S]*?)\s*```/);
            if (jsonMatch) {
                jsonText = jsonMatch[1];
            } else {
                // Método 2: Extraer de bloques ``` ... ```
                jsonMatch = responseText.match(/```\s*([\s\S]*?)\s*```/);
                if (jsonMatch) {
                    jsonText = jsonMatch[1];
                }
            }

            // Método 3: Limpiar prefijos/sufijos
            jsonText = jsonText.trim();
            const jsonStart = jsonText.indexOf('{');
            if (jsonStart > 0) {
                jsonText = jsonText.substring(jsonStart);
            }
            const jsonEnd = jsonText.lastIndexOf('}');
            if (jsonEnd > 0 && jsonEnd < jsonText.length - 1) {
                jsonText = jsonText.substring(0, jsonEnd + 1);
            }

            const result = JSON.parse(jsonText);

            return {
                edvScript: result.edv_script || '',
                changes: result.changes || [],
                optimizationsApplied: result.optimizations_applied || [],
                validationChecklist: result.validation_checklist || {},
                warnings: result.warnings || [],
                summary: result.summary || ''
            };
        } catch (e) {
            console.error('[AIAnalyzer] Error parsing EDV generation response:', e);
            return {
                edvScript: null,
                error: e.message,
                rawResponse: responseText,
                changes: [],
                optimizationsApplied: [],
                validationChecklist: {},
                warnings: [],
                summary: 'Error al parsear respuesta de IA'
            };
        }
    }

    /**
     * Llamar a OpenAI API
     */
    async callOpenAI(prompt) {
        const response = await fetch(this.endpoints.openai, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.apiKey}`
            },
            body: JSON.stringify({
                model: this.models.openai,
                messages: [
                    {
                        role: 'system',
                        content: 'Eres un experto en análisis de código PySpark y conversiones DDV→EDV para el Banco de Crédito del Perú.'
                    },
                    {
                        role: 'user',
                        content: prompt
                    }
                ],
                temperature: 0.1,
                max_tokens: 4000
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(`OpenAI API error: ${error.error?.message || response.statusText}`);
        }

        const data = await response.json();
        return data.choices[0].message.content;
    }

    /**
     * Llamar a Claude API
     */
    async callClaude(prompt) {
        const response = await fetch(this.endpoints.claude, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': this.apiKey,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model: this.models.claude,
                max_tokens: 4000,
                messages: [
                    {
                        role: 'user',
                        content: prompt
                    }
                ]
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(`Claude API error: ${error.error?.message || response.statusText}`);
        }

        const data = await response.json();
        return data.content[0].text;
    }

    /**
     * Llamar a Gemini API
     */
    async callGemini(prompt) {
        const url = `${this.endpoints.gemini}?key=${this.apiKey}`;

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                contents: [{
                    parts: [{
                        text: prompt
                    }]
                }]
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(`Gemini API error: ${error.error?.message || response.statusText}`);
        }

        const data = await response.json();
        return data.candidates[0].content.parts[0].text;
    }

    /**
     * Parsear respuesta de IA
     */
    parseAIResponse(responseText, mode) {
        try {
            // Intentar múltiples métodos de extracción JSON
            let jsonText = responseText;

            // Método 1: Extraer de bloques de código markdown ```json ... ```
            let jsonMatch = responseText.match(/```json\s*([\s\S]*?)\s*```/);
            if (jsonMatch) {
                jsonText = jsonMatch[1];
            } else {
                // Método 2: Extraer de bloques de código sin lenguaje ``` ... ```
                jsonMatch = responseText.match(/```\s*([\s\S]*?)\s*```/);
                if (jsonMatch) {
                    jsonText = jsonMatch[1];
                }
            }

            // Método 3: Limpiar texto antes de parsear
            jsonText = jsonText.trim();

            // Eliminar posibles prefijos antes del JSON
            const jsonStart = jsonText.indexOf('{');
            if (jsonStart > 0) {
                jsonText = jsonText.substring(jsonStart);
            }

            // Eliminar posibles sufijos después del JSON
            const jsonEnd = jsonText.lastIndexOf('}');
            if (jsonEnd > 0 && jsonEnd < jsonText.length - 1) {
                jsonText = jsonText.substring(0, jsonEnd + 1);
            }

            return JSON.parse(jsonText);
        } catch (e) {
            console.error('[AIAnalyzer] Error parsing JSON:', e);
            console.error('[AIAnalyzer] Response text:', responseText);

            // Retornar formato básico con la respuesta en texto plano
            return {
                summary: {
                    is_valid_conversion: null,
                    similarity_percentage: null,
                    total_differences: null
                },
                differences: [],
                raw_response: responseText,
                parse_error: e.message
            };
        }
    }

    /**
     * Limpiar configuración
     */
    clearConfig() {
        this.apiKey = null;
        this.provider = null;
        // Solo limpiamos provider, API key ya no se guarda
        localStorage.removeItem('ai_provider');
    }
}

// Exportar para uso en módulos
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AIAnalyzer;
}
