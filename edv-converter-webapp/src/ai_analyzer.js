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
            gemini: 'https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent'
        };
        this.models = {
            openai: 'gpt-5-chat-latest',
            claude: 'claude-3-5-sonnet-20241022',
            gemini: 'gemini-pro'
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
     * Configurar API key
     */
    setAPIKey(apiKey) {
        this.apiKey = apiKey;
        // Almacenar con simple obfuscación (NO es encriptación real)
        const obfuscated = btoa(apiKey);
        localStorage.setItem('ai_api_key', obfuscated);
    }

    /**
     * Cargar proveedor desde localStorage
     */
    loadProvider() {
        return localStorage.getItem('ai_provider');
    }

    /**
     * Cargar API key desde localStorage
     */
    loadAPIKey() {
        const obfuscated = localStorage.getItem('ai_api_key');
        if (!obfuscated) return null;
        try {
            return atob(obfuscated);
        } catch (e) {
            return null;
        }
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

# CONTEXTO

Estás comparando dos scripts PySpark:
- **Script 1 (${script1Name})**: Puede ser DDV o EDV
- **Script 2 (${script2Name})**: Puede ser DDV o EDV

# DIFERENCIAS ESPERADAS DDV→EDV (NO SON ERRORES):

1. **Separación de Esquemas**:
   - DDV: Lee y escribe en el mismo schema (bcp_ddv_*)
   - EDV: Lee de views DDV (bcp_ddv_*_v), escribe en schema EDV (bcp_edv_*)

2. **Variables Adicionales EDV**:
   - PRM_CATALOG_NAME_EDV
   - PRM_ESQUEMA_TABLA_EDV
   - PRM_ESQUEMA_TABLA_ESCRITURA

3. **Widgets Adicionales EDV**:
   - dbutils.widgets para catalog y schema EDV

4. **Optimizaciones de Rendimiento (6 optimizaciones conocidas)**:
   - **Cache en memoria**: DDV usa write/read a disco, EDV usa .cache() + .count()
   - **Consolidación de loops**: DDV usa 3 .select separados, EDV consolida en 1
   - **Storage Level**: DDV usa MEMORY_ONLY_2, EDV usa MEMORY_AND_DISK
   - **Spark AQE**: EDV agrega configuraciones de Adaptive Query Execution
   - **Eliminación de coalesce**: EDV elimina coalesce(160) en loops
   - **Repartition pre-escritura**: EDV agrega repartition antes de write_delta

# DIFERENCIAS CRÍTICAS (ERRORES REALES):

1. **Lógica de negocio diferente** (funciones, transformaciones, joins)
2. **Cambios en campos calculados** (withColumn con lógica distinta)
3. **Agregaciones diferentes** (groupBy, agg)
4. **Tablas de entrada/salida diferentes** (excepto separación DDV/EDV esperada)
5. **Eliminación de funciones críticas**
6. **Cambios en trim()** que afecten deduplicación

# TAREA

Analiza ambos scripts y proporciona un reporte en formato JSON con esta estructura:

\`\`\`json
{
  "summary": {
    "is_valid_conversion": true/false,
    "conversion_type": "DDV→EDV" | "EDV→DDV" | "Same Type" | "Unknown",
    "similarity_percentage": 0-100,
    "total_differences": number,
    "critical_issues": number,
    "optimizations_applied": number
  },
  "differences": [
    {
      "category": "SCHEMA_CHANGE" | "OPTIMIZATION" | "LOGIC_CHANGE" | "VARIABLE_CHANGE" | ...,
      "severity": "CRITICAL" | "HIGH" | "MEDIUM" | "LOW" | "INFO",
      "description": "Descripción breve del cambio",
      "details": "Detalles completos con ejemplos de código",
      "recommendation": "Qué hacer con este cambio",
      "is_expected_ddv_edv": true/false
    }
  ],
  "optimizations": [
    {
      "name": "Cache en Memoria",
      "impact": "60-80% más rápido",
      "detected": true/false,
      "details": "..."
    }
  ],
  "recommendations": [
    "Recomendación 1",
    "Recomendación 2"
  ],
  "conclusion": "Conclusión general del análisis"
}
\`\`\`

# SCRIPTS A ANALIZAR

## Script 1 (${script1Name}):
\`\`\`python
${script1}
\`\`\`

## Script 2 (${script2Name}):
\`\`\`python
${script2}
\`\`\`

Proporciona el análisis completo en formato JSON válido.`;
    }

    /**
     * Prompt para análisis de scripts individuales
     */
    generateIndividualPrompt(script1, script2, script1Name, script2Name) {
        return `Eres un experto en análisis de código PySpark para entornos Databricks.

# TAREA

Compara estos dos scripts PySpark y proporciona un análisis detallado de sus diferencias.

**Script 1 (${script1Name})**:
\`\`\`python
${script1}
\`\`\`

**Script 2 (${script2Name})**:
\`\`\`python
${script2}
\`\`\`

# ANÁLISIS REQUERIDO

Proporciona un reporte en formato JSON con esta estructura:

\`\`\`json
{
  "summary": {
    "similarity_percentage": 0-100,
    "total_differences": number,
    "critical_differences": number,
    "are_equivalent": true/false
  },
  "differences": [
    {
      "category": "IMPORTS" | "FUNCTIONS" | "TRANSFORMATIONS" | "DATA_FLOW" | ...,
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
  "conclusion": "Conclusión general"
}
\`\`\`

Proporciona el análisis completo en formato JSON válido.`;
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
            // Extraer JSON de la respuesta (puede venir dentro de ```json ... ```)
            const jsonMatch = responseText.match(/```json\s*([\s\S]*?)\s*```/);
            const jsonText = jsonMatch ? jsonMatch[1] : responseText;

            return JSON.parse(jsonText);
        } catch (e) {
            console.error('[AIAnalyzer] Error parsing JSON:', e);
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
        localStorage.removeItem('ai_api_key');
        localStorage.removeItem('ai_provider');
    }
}

// Exportar para uso en módulos
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AIAnalyzer;
}
