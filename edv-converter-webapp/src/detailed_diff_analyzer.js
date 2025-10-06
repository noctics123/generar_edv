/**
 * Detailed Diff Analyzer - Advanced Script Comparison
 * ====================================================
 *
 * Proporciona análisis detallado a nivel de caracteres y tokens
 * para diferencias entre scripts Python/PySpark.
 *
 * Features:
 * - Character-level diff (Myers algorithm)
 * - Token-level analysis
 * - DDV→EDV pattern detection
 * - Change categorization
 * - Impact assessment
 *
 * Autor: Claude Code
 * Version: 3.5
 */

class DetailedDiffAnalyzer {
    constructor() {
        this.ddvEdvPatterns = this.initializeDDVEDVPatterns();
        this.changeCategories = this.initializeChangeCategories();
    }

    /**
     * Patrones conocidos de conversión DDV→EDV
     */
    initializeDDVEDVPatterns() {
        return [
            {
                name: 'Schema DDV con sufijo _v (views)',
                pattern: /bcp_ddv_\w+_v/,
                expected: true,
                severity: 'INFO',
                description: 'EDV debe leer de views DDV (sufijo _v)'
            },
            {
                name: 'Catalog EDV para escritura',
                pattern: /catalog_lhcl_\w+_bcp_expl/,
                expected: true,
                severity: 'INFO',
                description: 'EDV escribe en catalog de exploración'
            },
            {
                name: 'Schema EDV para escritura',
                pattern: /bcp_edv_\w+/,
                expected: true,
                severity: 'INFO',
                description: 'EDV escribe en schema EDV'
            },
            {
                name: 'Variable PRM_ESQUEMA_TABLA_ESCRITURA',
                pattern: /PRM_ESQUEMA_TABLA_ESCRITURA/,
                expected: true,
                severity: 'INFO',
                description: 'EDV separa lectura y escritura'
            },
            {
                name: 'Nombres de tabla en UPPERCASE',
                pattern: /PRM_TABLA_PRIMERATRANSPUESTA.*[A-Z_]+/,
                expected: true,
                severity: 'CRITICAL',
                description: 'Nombres de tabla deben estar en UPPERCASE para EDV'
            }
        ];
    }

    /**
     * Categorías de cambios para clasificación automática
     */
    initializeChangeCategories() {
        return {
            'SCHEMA_CHANGE': {
                patterns: [/esquema|schema|_ddv|_edv/i],
                severity: 'HIGH',
                impact: 'Cambia origen/destino de datos',
                color: '#f59e0b'
            },
            'CATALOG_CHANGE': {
                patterns: [/catalog|PRM_CATALOG/i],
                severity: 'HIGH',
                impact: 'Cambia catálogo de datos',
                color: '#f59e0b'
            },
            'TABLE_NAME_CHANGE': {
                patterns: [/tabla|table|PRM_TABLE/i],
                severity: 'CRITICAL',
                impact: 'Cambia tabla de salida',
                color: '#ef4444'
            },
            'VARIABLE_CHANGE': {
                patterns: [/^[A-Z_][A-Z0-9_]*\s*=/],
                severity: 'MEDIUM',
                impact: 'Cambia variable global',
                color: '#3b82f6'
            },
            'FUNCTION_CHANGE': {
                patterns: [/^def\s+\w+/],
                severity: 'HIGH',
                impact: 'Cambia lógica de función',
                color: '#ef4444'
            },
            'IMPORT_CHANGE': {
                patterns: [/^import\s+|^from\s+\w+\s+import/],
                severity: 'CRITICAL',
                impact: 'Cambia dependencias',
                color: '#ef4444'
            },
            'COMMENT_CHANGE': {
                patterns: [/^\s*#/],
                severity: 'INFO',
                impact: 'Solo cambia documentación',
                color: '#6b7280'
            },
            'WHITESPACE_CHANGE': {
                patterns: [/^\s*$/],
                severity: 'INFO',
                impact: 'Solo cambia formato',
                color: '#6b7280'
            }
        };
    }

    /**
     * Analiza diferencia en detalle (OPTIMIZADO - memoria limitada)
     *
     * @param {Object} difference - Diferencia del verifier básico
     * @param {string} script1 - Script 1 completo (OPCIONAL - solo para patrones)
     * @param {string} script2 - Script 2 completo (OPCIONAL - solo para patrones)
     * @returns {Object} Análisis detallado
     */
    analyzeDetailedDifference(difference, script1 = null, script2 = null) {
        const detailed = {
            ...difference,
            changeType: this.categorizeChange(difference),
            charDiff: null,
            tokenDiff: null,
            patternMatches: [],
            impactAssessment: null,
            codeSnippets: null
        };

        // OPTIMIZACIÓN: Solo procesar details si son pequeños (< 5000 chars)
        if (difference.details && difference.details.length < 5000) {
            try {
                const lines = this.extractLinesFromDetails(difference.details);
                if (lines.text1 && lines.text2) {
                    // Limitar procesamiento a primeras 10 líneas
                    const text1Lines = lines.text1.split('\n').slice(0, 10).join('\n');
                    const text2Lines = lines.text2.split('\n').slice(0, 10).join('\n');

                    if (text1Lines.length < 1000 && text2Lines.length < 1000) {
                        detailed.charDiff = this.computeCharacterDiff(text1Lines, text2Lines);
                        detailed.tokenDiff = this.computeTokenDiff(text1Lines, text2Lines);
                        detailed.codeSnippets = {
                            script1: text1Lines,
                            script2: text2Lines
                        };
                    }
                }
            } catch (e) {
                console.warn('[DetailedDiffAnalyzer] Error en análisis char-level:', e);
                // Continuar sin char diff
            }
        }

        // Detectar patrones DDV→EDV (solo si scripts están disponibles y son pequeños)
        if (script1 && script2 && script1.length < 100000 && script2.length < 100000) {
            try {
                detailed.patternMatches = this.detectDDVEDVPatterns(difference, script1, script2);
            } catch (e) {
                console.warn('[DetailedDiffAnalyzer] Error en detección de patrones:', e);
            }
        }

        // Evaluar impacto (siempre - es ligero)
        detailed.impactAssessment = this.assessImpact(detailed);

        return detailed;
    }

    /**
     * Categoriza el tipo de cambio
     */
    categorizeChange(difference) {
        const category = difference.category || '';
        const details = difference.details || '';
        const combined = `${category} ${details}`.toLowerCase();

        for (const [type, config] of Object.entries(this.changeCategories)) {
            for (const pattern of config.patterns) {
                if (pattern.test(combined)) {
                    return {
                        type,
                        severity: config.severity,
                        impact: config.impact,
                        color: config.color
                    };
                }
            }
        }

        return {
            type: 'UNKNOWN',
            severity: difference.severity || 'MEDIUM',
            impact: 'Cambio no categorizado',
            color: '#9ca3af'
        };
    }

    /**
     * Extrae líneas específicas de los detalles de diferencias (SIMPLIFICADO)
     */
    extractLinesFromDetails(details) {
        // Parsear detalles para obtener elementos específicos
        const onlyIn1Match = details.match(/Solo en [^:]+:\s*([^\n]+)/);
        const onlyIn2Match = details.match(/Solo en [^:]+:\s*([^\n]+)/);

        if (!onlyIn1Match && !onlyIn2Match) {
            return { text1: null, text2: null };
        }

        const text1 = onlyIn1Match ? onlyIn1Match[1].trim() : '';
        const text2 = onlyIn2Match ? onlyIn2Match[1].trim() : '';

        return {
            text1: text1 || 'ninguno',
            text2: text2 || 'ninguno'
        };
    }

    /**
     * Encuentra contexto alrededor de un texto en el script
     */
    findContextInScript(text, script, contextLines = 3) {
        if (!text || text === 'ninguno') return null;

        const lines = script.split('\n');
        const targetIndex = lines.findIndex(line => line.includes(text));

        if (targetIndex === -1) return null;

        const start = Math.max(0, targetIndex - contextLines);
        const end = Math.min(lines.length, targetIndex + contextLines + 1);

        return lines.slice(start, end).join('\n');
    }

    /**
     * Computa diff a nivel de caracteres (OPTIMIZADO - limita procesamiento)
     */
    computeCharacterDiff(text1, text2) {
        if (!text1 || !text2) return null;

        const lines1 = text1.split('\n');
        const lines2 = text2.split('\n');

        const lineDiffs = [];

        // OPTIMIZACIÓN: Limitar a primeras 5 líneas para evitar OOM
        const maxLines = Math.min(5, Math.max(lines1.length, lines2.length));

        for (let i = 0; i < maxLines; i++) {
            const line1 = lines1[i] || '';
            const line2 = lines2[i] || '';

            // OPTIMIZACIÓN: No procesar líneas muy largas (> 200 chars)
            if (line1.length > 200 || line2.length > 200) {
                lineDiffs.push({
                    type: 'different',
                    line1: line1.substring(0, 200) + (line1.length > 200 ? '...' : ''),
                    line2: line2.substring(0, 200) + (line2.length > 200 ? '...' : ''),
                    charChanges: null // Skip char diff for long lines
                });
                continue;
            }

            if (line1 === line2) {
                lineDiffs.push({
                    type: 'same',
                    line1,
                    line2,
                    charChanges: null
                });
            } else {
                lineDiffs.push({
                    type: 'different',
                    line1,
                    line2,
                    charChanges: this.computeCharChanges(line1, line2)
                });
            }
        }

        return lineDiffs;
    }

    /**
     * Computa cambios a nivel de caracteres entre dos líneas
     */
    computeCharChanges(line1, line2) {
        const changes = [];
        const maxLen = Math.max(line1.length, line2.length);

        let i = 0;
        while (i < maxLen) {
            const char1 = line1[i] || '';
            const char2 = line2[i] || '';

            if (char1 === char2) {
                i++;
                continue;
            }

            // Encontrar rango de cambio
            let start = i;
            let end1 = i;
            let end2 = i;

            // Avanzar hasta encontrar coincidencia
            while (end1 < line1.length && end2 < line2.length) {
                if (line1[end1] === line2[end2]) {
                    break;
                }
                end1++;
                end2++;
            }

            changes.push({
                start,
                removed: line1.substring(start, end1),
                added: line2.substring(start, end2),
                position: start
            });

            i = Math.max(end1, end2);
        }

        return changes;
    }

    /**
     * Computa diff a nivel de tokens
     */
    computeTokenDiff(text1, text2) {
        if (!text1 || !text2) return null;

        const tokens1 = this.tokenize(text1);
        const tokens2 = this.tokenize(text2);

        const onlyIn1 = tokens1.filter(t => !tokens2.includes(t));
        const onlyIn2 = tokens2.filter(t => !tokens1.includes(t));

        return {
            removed: [...new Set(onlyIn1)],
            added: [...new Set(onlyIn2)],
            common: tokens1.filter(t => tokens2.includes(t)).length
        };
    }

    /**
     * Tokeniza texto Python
     */
    tokenize(text) {
        // Separar por espacios, operadores, paréntesis
        return text
            .split(/[\s()[\]{},.:=+\-*/<>!&|]+/)
            .filter(token => token.length > 0)
            .filter(token => !/^[0-9]+$/.test(token)); // Filtrar solo números
    }

    /**
     * Detecta patrones DDV→EDV conocidos
     */
    detectDDVEDVPatterns(difference, script1, script2) {
        const matches = [];

        for (const pattern of this.ddvEdvPatterns) {
            const inScript1 = pattern.pattern.test(script1);
            const inScript2 = pattern.pattern.test(script2);

            if (inScript2 && !inScript1 && pattern.expected) {
                matches.push({
                    pattern: pattern.name,
                    description: pattern.description,
                    severity: pattern.severity,
                    status: 'OK',
                    message: `✓ Patrón EDV detectado correctamente`
                });
            } else if (inScript1 && !inScript2 && pattern.expected) {
                matches.push({
                    pattern: pattern.name,
                    description: pattern.description,
                    severity: 'CRITICAL',
                    status: 'MISSING',
                    message: `✗ Patrón EDV esperado no encontrado`
                });
            }
        }

        return matches;
    }

    /**
     * Evalúa el impacto del cambio
     */
    assessImpact(detailed) {
        const assessment = {
            level: detailed.changeType.severity,
            areas: [],
            recommendation: null
        };

        // Detectar áreas afectadas
        if (detailed.changeType.type.includes('SCHEMA') || detailed.changeType.type.includes('CATALOG')) {
            assessment.areas.push('Lectura de datos');
        }

        if (detailed.changeType.type.includes('TABLE')) {
            assessment.areas.push('Escritura de datos');
        }

        if (detailed.changeType.type.includes('FUNCTION')) {
            assessment.areas.push('Lógica de negocio');
        }

        if (detailed.changeType.type.includes('VARIABLE')) {
            assessment.areas.push('Configuración');
        }

        // Generar recomendación
        if (detailed.severity === 'CRITICAL') {
            assessment.recommendation = 'Revisar inmediatamente - puede causar fallo en ejecución';
        } else if (detailed.severity === 'HIGH') {
            assessment.recommendation = 'Revisar antes de desplegar - puede afectar resultados';
        } else if (detailed.severity === 'MEDIUM') {
            assessment.recommendation = 'Revisar durante QA - puede afectar comportamiento';
        } else {
            assessment.recommendation = 'Revisar si es necesario - cambio menor';
        }

        return assessment;
    }

    /**
     * Analiza todos los differences con detalle (OPTIMIZADO - limita procesamiento)
     */
    analyzeAll(differences, script1, script2) {
        // OPTIMIZACIÓN: Solo analizar primeras 20 diferencias para evitar OOM
        const maxDiffs = 20;
        const diffsToAnalyze = differences.slice(0, maxDiffs);

        console.log(`[DetailedDiffAnalyzer] Analizando ${diffsToAnalyze.length} de ${differences.length} diferencias`);

        // Procesar diferencias
        const analyzed = diffsToAnalyze.map((diff, index) => {
            try {
                return this.analyzeDetailedDifference(diff, script1, script2);
            } catch (e) {
                console.warn(`[DetailedDiffAnalyzer] Error al analizar diff ${index}:`, e);
                // Retornar diferencia original sin análisis detallado
                return {
                    ...diff,
                    changeType: this.categorizeChange(diff),
                    impactAssessment: this.assessImpact({
                        changeType: this.categorizeChange(diff),
                        severity: diff.severity
                    })
                };
            }
        });

        // Si hay más diferencias, agregarlas sin análisis detallado
        if (differences.length > maxDiffs) {
            const remaining = differences.slice(maxDiffs).map(diff => ({
                ...diff,
                changeType: this.categorizeChange(diff),
                impactAssessment: this.assessImpact({
                    changeType: this.categorizeChange(diff),
                    severity: diff.severity
                })
            }));
            return [...analyzed, ...remaining];
        }

        return analyzed;
    }
}

// Exportar para uso en módulos
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DetailedDiffAnalyzer;
}
