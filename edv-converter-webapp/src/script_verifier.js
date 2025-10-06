/**
 * Script Verifier - Pure JavaScript Implementation
 * =================================================
 *
 * Verificador de similitud de scripts PySpark 100% en JavaScript.
 * No requiere servidor backend - funciona completamente en el browser.
 *
 * Compatible con GitHub Pages.
 *
 * Autor: Claude Code
 * Version: 2.0 (Pure JS)
 */

class ScriptVerifier {
    constructor() {
        this.differences = [];
        this.script1 = '';
        this.script2 = '';
        this.script1Name = 'script1.py';
        this.script2Name = 'script2.py';
        this.isDdvEdv = false;
    }

    /**
     * Verifica dos scripts y retorna reporte completo
     */
    verify(script1, script2, options = {}) {
        this.script1 = script1;
        this.script2 = script2;
        this.script1Name = options.script1_name || 'script1.py';
        this.script2Name = options.script2_name || 'script2.py';
        this.isDdvEdv = options.is_ddv_edv || false;
        this.differences = [];

        // Ejecutar analisis en orden
        this.analyzeStructure();
        this.analyzeDataFlow();

        if (this.isDdvEdv) {
            this.analyzeDdvEdvSpecific();
        }

        this.analyzeSemantics();

        // Generar reporte
        return this.generateReport();
    }

    /**
     * ANALISIS 1: Estructura
     */
    analyzeStructure() {
        const struct1 = this.extractStructure(this.script1);
        const struct2 = this.extractStructure(this.script2);

        // Comparar imports
        this.compareArrays(
            struct1.imports,
            struct2.imports,
            'IMPORTS',
            'CRITICAL'
        );

        // Comparar funciones
        this.compareArrays(
            struct1.functions,
            struct2.functions,
            'FUNCIONES',
            'HIGH'
        );

        // Comparar variables globales
        this.compareArrays(
            struct1.variables,
            struct2.variables,
            'VARIABLES_GLOBALES',
            'MEDIUM'
        );

        // Comparar widgets
        this.compareArrays(
            struct1.widgets,
            struct2.widgets,
            'WIDGETS',
            this.isDdvEdv ? 'INFO' : 'HIGH'
        );
    }

    /**
     * ANALISIS 2: Flujo de Datos
     */
    analyzeDataFlow() {
        const flow1 = this.extractDataFlow(this.script1);
        const flow2 = this.extractDataFlow(this.script2);

        // Comparar operaciones de lectura
        this.compareArrays(
            flow1.reads,
            flow2.reads,
            'OPERACIONES_LECTURA',
            this.isDdvEdv ? 'INFO' : 'CRITICAL'
        );

        // Comparar operaciones de escritura
        this.compareArrays(
            flow1.writes,
            flow2.writes,
            'OPERACIONES_ESCRITURA',
            'CRITICAL'
        );

        // Comparar transformaciones
        this.compareArrays(
            flow1.transformations,
            flow2.transformations,
            'TRANSFORMACIONES',
            'HIGH'
        );

        // Comparar joins
        this.compareArrays(
            flow1.joins,
            flow2.joins,
            'JOINS',
            'HIGH'
        );
    }

    /**
     * ANALISIS 3: DDV vs EDV Especifico
     */
    analyzeDdvEdvSpecific() {
        // Verificar que script1 tiene patrones DDV
        const hasDdvPatterns = this.hasDdvPatterns(this.script1);
        const hasEdvPatterns = this.hasEdvPatterns(this.script2);

        if (!hasDdvPatterns) {
            this.addDifference(
                'DDV_PATTERNS',
                'Script 1 no parece ser un script DDV',
                'Falta: PRM_ESQUEMA_TABLA_DDV, lecturas de DDV',
                'CRITICAL',
                'Verifica que el Script 1 sea realmente un script DDV'
            );
        }

        if (!hasEdvPatterns) {
            this.addDifference(
                'EDV_PATTERNS',
                'Script 2 no parece ser un script EDV',
                'Falta: PRM_ESQUEMA_TABLA_EDV, PRM_CATALOG_NAME_EDV, escrituras separadas',
                'CRITICAL',
                'Verifica que el Script 2 sea realmente un script EDV'
            );
        }

        // Verificar separacion de esquemas lectura/escritura
        const edvSchemas = this.extractEdvSchemas(this.script2);
        if (!edvSchemas.hasReadSchema || !edvSchemas.hasWriteSchema) {
            this.addDifference(
                'EDV_SCHEMAS',
                'Script EDV no tiene separacion correcta de esquemas',
                `Read schema: ${edvSchemas.hasReadSchema}, Write schema: ${edvSchemas.hasWriteSchema}`,
                'CRITICAL',
                'EDV debe tener PRM_ESQUEMA_TABLA (lectura DDV) y PRM_ESQUEMA_TABLA_ESCRITURA (escritura EDV)'
            );
        }

        // Verificar que logica de negocio es identica
        const logic1 = this.extractBusinessLogic(this.script1);
        const logic2 = this.extractBusinessLogic(this.script2);

        if (logic1 !== logic2) {
            this.addDifference(
                'BUSINESS_LOGIC',
                'La logica de negocio difiere entre DDV y EDV',
                'Las transformaciones core deben ser identicas',
                'CRITICAL',
                'Revisa que las funciones de negocio sean exactamente iguales'
            );
        }
    }

    /**
     * ANALISIS 4: Semantica
     */
    analyzeSemantics() {
        // Verificar que las tablas de salida coinciden
        const output1 = this.extractOutputTables(this.script1);
        const output2 = this.extractOutputTables(this.script2);

        this.compareArrays(
            output1,
            output2,
            'TABLAS_SALIDA',
            'CRITICAL'
        );

        // Verificar particiones
        const part1 = this.extractPartitions(this.script1);
        const part2 = this.extractPartitions(this.script2);

        if (part1 !== part2) {
            this.addDifference(
                'PARTICIONES',
                'Las particiones difieren',
                `Script 1: ${part1}, Script 2: ${part2}`,
                'HIGH',
                'Las particiones deben coincidir para correcta escritura'
            );
        }
    }

    /**
     * Extrae estructura del script
     */
    extractStructure(script) {
        return {
            imports: this.extractImports(script),
            functions: this.extractFunctions(script),
            variables: this.extractVariables(script),
            widgets: this.extractWidgets(script)
        };
    }

    /**
     * Extrae flujo de datos
     */
    extractDataFlow(script) {
        return {
            reads: this.extractReads(script),
            writes: this.extractWrites(script),
            transformations: this.extractTransformations(script),
            joins: this.extractJoins(script)
        };
    }

    /**
     * Extrae imports
     */
    extractImports(script) {
        const imports = [];
        const importRegex = /^(?:from|import)\s+([^\s#]+)/gm;
        let match;

        while ((match = importRegex.exec(script)) !== null) {
            imports.push(match[1].trim());
        }

        return [...new Set(imports)].sort();
    }

    /**
     * Extrae funciones
     */
    extractFunctions(script) {
        const functions = [];
        const funcRegex = /^def\s+(\w+)\s*\(/gm;
        let match;

        while ((match = funcRegex.exec(script)) !== null) {
            functions.push(match[1]);
        }

        return functions.sort();
    }

    /**
     * Extrae variables globales
     */
    extractVariables(script) {
        const variables = [];
        const varRegex = /^([A-Z_][A-Z0-9_]*)\s*=/gm;
        let match;

        while ((match = varRegex.exec(script)) !== null) {
            const varName = match[1];
            // Excluir constantes conocidas
            if (!varName.startsWith('CONS_')) {
                variables.push(varName);
            }
        }

        return [...new Set(variables)].sort();
    }

    /**
     * Extrae widgets
     */
    extractWidgets(script) {
        const widgets = [];
        const widgetRegex = /dbutils\.widgets\.\w+\([^)]*name\s*=\s*["']([^"']+)["']/g;
        let match;

        while ((match = widgetRegex.exec(script)) !== null) {
            widgets.push(match[1]);
        }

        return widgets.sort();
    }

    /**
     * Extrae operaciones de lectura
     */
    extractReads(script) {
        const reads = [];
        const readPatterns = [
            /spark\.read\.format\([^)]+\)\.load\([^)]+\)/g,
            /spark\.read\.table\([^)]+\)/g,
            /funciones\.crearDfGrupo\([^)]+\)/g
        ];

        readPatterns.forEach(pattern => {
            let match;
            while ((match = pattern.exec(script)) !== null) {
                reads.push(match[0]);
            }
        });

        return reads;
    }

    /**
     * Extrae operaciones de escritura
     */
    extractWrites(script) {
        const writes = [];
        const writePatterns = [
            /\.write\.format\([^)]+\)/g,
            /write_delta\([^)]+\)/g,
            /\.saveAsTable\([^)]+\)/g
        ];

        writePatterns.forEach(pattern => {
            let match;
            while ((match = pattern.exec(script)) !== null) {
                writes.push(match[0]);
            }
        });

        return writes;
    }

    /**
     * Extrae transformaciones
     */
    extractTransformations(script) {
        const transformations = [];
        const transformPatterns = [
            /\.select\([^)]+\)/g,
            /\.filter\([^)]+\)/g,
            /\.withColumn\([^)]+\)/g,
            /\.groupBy\([^)]+\)/g,
            /\.agg\([^)]+\)/g
        ];

        transformPatterns.forEach(pattern => {
            let match;
            while ((match = pattern.exec(script)) !== null) {
                transformations.push(match[0].substring(0, 50)); // Truncar para comparacion
            }
        });

        return transformations;
    }

    /**
     * Extrae joins
     */
    extractJoins(script) {
        const joins = [];
        const joinRegex = /\.join\([^)]+\)/g;
        let match;

        while ((match = joinRegex.exec(script)) !== null) {
            joins.push(match[0]);
        }

        return joins;
    }

    /**
     * Verifica patrones DDV
     */
    hasDdvPatterns(script) {
        return script.includes('PRM_ESQUEMA_TABLA_DDV') ||
               script.includes('bcp_ddv_');
    }

    /**
     * Verifica patrones EDV
     */
    hasEdvPatterns(script) {
        return script.includes('PRM_ESQUEMA_TABLA_EDV') &&
               script.includes('PRM_CATALOG_NAME_EDV') &&
               script.includes('PRM_ESQUEMA_TABLA_ESCRITURA');
    }

    /**
     * Extrae esquemas EDV
     */
    extractEdvSchemas(script) {
        return {
            hasReadSchema: script.includes('PRM_ESQUEMA_TABLA') &&
                          script.includes('PRM_ESQUEMA_TABLA_DDV'),
            hasWriteSchema: script.includes('PRM_ESQUEMA_TABLA_ESCRITURA') &&
                           script.includes('PRM_ESQUEMA_TABLA_EDV')
        };
    }

    /**
     * Extrae logica de negocio (funciones core)
     */
    extractBusinessLogic(script) {
        // Extraer funciones principales sin comentarios ni espacios
        const coreFunctions = [
            'extraccion_info_base_cliente',
            'extraccionInformacion12Meses',
            'agruparInformacionMesAnalisis',
            'logicaPostAgrupacionInformacionMesAnalisis'
        ];

        let logic = '';
        coreFunctions.forEach(funcName => {
            const funcRegex = new RegExp(`def ${funcName}\\s*\\([^)]*\\):([\\s\\S]*?)(?=\\ndef |$)`, 'm');
            const match = script.match(funcRegex);
            if (match) {
                // Normalizar: eliminar comentarios y espacios extras
                logic += match[1]
                    .replace(/#.*$/gm, '')  // Comentarios
                    .replace(/\s+/g, ' ')   // Espacios multiples
                    .trim();
            }
        });

        return logic;
    }

    /**
     * Extrae tablas de salida
     */
    extractOutputTables(script) {
        const tables = [];
        const tableRegex = /VAL_DESTINO_NAME\s*=\s*.*["']([^"']+)["']/g;
        let match;

        while ((match = tableRegex.exec(script)) !== null) {
            tables.push(match[1]);
        }

        // Tambien buscar en write_delta
        const writeDeltaRegex = /write_delta\([^,]+,\s*["']?([^,"']+)["']?/g;
        while ((match = writeDeltaRegex.exec(script)) !== null) {
            tables.push(match[1]);
        }

        return [...new Set(tables)].sort();
    }

    /**
     * Extrae particiones
     */
    extractPartitions(script) {
        const partRegex = /CONS_PARTITION_DELTA_NAME\s*=\s*["']([^"']+)["']/;
        const match = script.match(partRegex);
        return match ? match[1] : '';
    }

    /**
     * Compara dos arrays y registra diferencias
     */
    compareArrays(arr1, arr2, category, severity) {
        const set1 = new Set(arr1);
        const set2 = new Set(arr2);

        const onlyIn1 = [...set1].filter(x => !set2.has(x));
        const onlyIn2 = [...set2].filter(x => !set1.has(x));

        if (onlyIn1.length > 0 || onlyIn2.length > 0) {
            this.addDifference(
                category,
                `Diferencias encontradas en ${category}`,
                `Solo en ${this.script1Name}: ${onlyIn1.join(', ') || 'ninguno'}\n` +
                `Solo en ${this.script2Name}: ${onlyIn2.join(', ') || 'ninguno'}`,
                severity,
                this.getSuggestion(category, onlyIn1, onlyIn2)
            );
        }
    }

    /**
     * Agrega una diferencia
     */
    addDifference(category, description, details, severity, suggestion) {
        this.differences.push({
            category,
            description,
            details,
            severity,
            suggestion,
            location: `${this.script1Name} vs ${this.script2Name}`
        });
    }

    /**
     * Genera sugerencia segun categoria
     */
    getSuggestion(category, onlyIn1, onlyIn2) {
        if (this.isDdvEdv && category === 'WIDGETS') {
            return 'Es esperado que EDV tenga widgets adicionales (PRM_CATALOG_NAME_EDV, PRM_ESQUEMA_TABLA_EDV)';
        }

        if (this.isDdvEdv && category === 'VARIABLES_GLOBALES') {
            return 'Es esperado que EDV tenga variables adicionales (PRM_ESQUEMA_TABLA_ESCRITURA)';
        }

        if (this.isDdvEdv && category === 'OPERACIONES_LECTURA') {
            return 'Es esperado que ambos lean de las mismas fuentes (DDV views)';
        }

        if (onlyIn1.length > 0 && onlyIn2.length > 0) {
            return `Sincroniza los elementos entre ambos scripts`;
        } else if (onlyIn1.length > 0) {
            return `Agrega estos elementos a ${this.script2Name}`;
        } else {
            return `Agrega estos elementos a ${this.script1Name}`;
        }
    }

    /**
     * Calcula score de similitud
     */
    calculateSimilarityScore() {
        if (this.differences.length === 0) {
            return 100;
        }

        // Penalizacion por severidad
        const penalties = {
            'CRITICAL': 20,
            'HIGH': 10,
            'MEDIUM': 5,
            'LOW': 2,
            'INFO': 0
        };

        let totalPenalty = 0;
        this.differences.forEach(diff => {
            totalPenalty += penalties[diff.severity] || 5;
        });

        // Score base 100, restar penalizaciones
        const score = Math.max(0, 100 - totalPenalty);
        return Math.round(score);
    }

    /**
     * Determina si scripts son equivalentes
     */
    areEquivalent() {
        const criticalCount = this.differences.filter(d => d.severity === 'CRITICAL').length;
        const score = this.calculateSimilarityScore();

        if (this.isDdvEdv) {
            // Para DDV vs EDV, permitir diferencias INFO
            return criticalCount === 0 && score >= 85;
        } else {
            // Para scripts individuales, ser mas estricto
            return criticalCount === 0 && score >= 95;
        }
    }

    /**
     * Genera reporte completo
     */
    generateReport() {
        const score = this.calculateSimilarityScore();
        const isEquivalent = this.areEquivalent();

        // Contar por severidad
        const counts = {
            critical: 0,
            high: 0,
            medium: 0,
            low: 0,
            info: 0
        };

        this.differences.forEach(diff => {
            counts[diff.severity.toLowerCase()]++;
        });

        // NUEVO: Análisis detallado con DetailedDiffAnalyzer
        let detailedDifferences = this.differences;
        if (typeof DetailedDiffAnalyzer !== 'undefined') {
            const analyzer = new DetailedDiffAnalyzer();
            detailedDifferences = analyzer.analyzeAll(this.differences, this.script1, this.script2);
        }

        return {
            script1_name: this.script1Name,
            script2_name: this.script2Name,
            is_ddv_edv: this.isDdvEdv,
            similarity_score: score,
            is_equivalent: isEquivalent,
            total_differences: this.differences.length,
            critical_count: counts.critical,
            high_count: counts.high,
            medium_count: counts.medium,
            low_count: counts.low,
            info_count: counts.info,
            differences: detailedDifferences, // Usar diferencias con análisis detallado
            summary: this.generateSummary(score, isEquivalent, counts)
        };
    }

    /**
     * Genera resumen ejecutivo
     */
    generateSummary(score, isEquivalent, counts) {
        let summary = `Similitud: ${score}%\n\n`;

        if (isEquivalent) {
            summary += '[OK] Los scripts son equivalentes.\n\n';
        } else {
            summary += '[ERROR] Los scripts NO son equivalentes.\n\n';
        }

        summary += `Total de diferencias: ${this.differences.length}\n`;
        summary += `- Criticas: ${counts.critical}\n`;
        summary += `- Altas: ${counts.high}\n`;
        summary += `- Medias: ${counts.medium}\n`;
        summary += `- Bajas: ${counts.low}\n`;
        summary += `- Informativas: ${counts.info}\n`;

        return summary;
    }
}

// Exportar para uso en browser
if (typeof window !== 'undefined') {
    window.ScriptVerifier = ScriptVerifier;
}

// Exportar para uso en Node.js (testing)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ScriptVerifier;
}
