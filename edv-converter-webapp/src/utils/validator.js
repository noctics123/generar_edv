/**
 * EDV Validator - Validador de compliance EDV
 * Verifica que un script cumpla con los estándares EDV
 *
 * @author BCP Analytics Team
 * @version 1.0.0
 */

class EDVValidator {
    constructor() {
        this.checks = [];
        this.errors = [];
        this.warnings = [];
        this.context = {};
    }

    /**
     * Valida un script EDV completo
     * @param {string} script - Código del script a validar
     * @returns {Object} - Resultado de validación con checks, errors y warnings
     */
    validate(script) {
        this.checks = [];
        this.errors = [];
        this.warnings = [];
        this.context = {};

        // Ejecutar todas las validaciones
        this.validateEDVWidgets(script);
        this.validateEDVVariables(script);
        this.validateEDVSchemaWriting(script);
        this.validateDestinationName(script);
        this.validateManagedTables(script);
        this.validateSparkOptimizations(script);
        this.validateRepartitionBeforeWrite(script);
        this.validatePartitionColumn(script);
        this.validateNoHardcodedPaths(script);
        this.validateDDVSchemaViews(script);
        this.validateDropTableCleanup(script);
        this.validateWriterOptions(script);
        this.validateTmpPlacementInEDV(script);
        this.validateNoResidualDDVDestination(script);

        return {
            passed: this.errors.length === 0,
            checks: this.checks,
            errors: this.errors,
            warnings: this.warnings,
            score: this.calculateScore()
        };
    }

    /**
     * Valida que existan los widgets EDV
     */
    validateEDVWidgets(script) {
        const hasEDVCatalog = /dbutils\.widgets\.text\([^)]*["']PRM_CATALOG_NAME_EDV["']/.test(script);
        const hasEDVSchema = /dbutils\.widgets\.text\([^)]*["']PRM_ESQUEMA_TABLA_EDV["']/.test(script);

        if (hasEDVCatalog && hasEDVSchema) {
            this.checks.push({
                name: 'Widgets EDV',
                passed: true,
                message: '✅ PRM_CATALOG_NAME_EDV y PRM_ESQUEMA_TABLA_EDV presentes'
            });
        } else {
            const missing = [];
            if (!hasEDVCatalog) missing.push('PRM_CATALOG_NAME_EDV');
            if (!hasEDVSchema) missing.push('PRM_ESQUEMA_TABLA_EDV');

            this.checks.push({
                name: 'Widgets EDV',
                passed: false,
                message: `❌ Faltan widgets: ${missing.join(', ')}`
            });
            this.errors.push(`Widgets EDV faltantes: ${missing.join(', ')}`);
        }
    }

    /**
     * Valida que existan las variables EDV
     */
    validateEDVVariables(script) {
        const hasEDVCatalogVar = /PRM_CATALOG_NAME_EDV\s*=\s*dbutils\.widgets\.get/.test(script);
        const hasEDVSchemaVar = /PRM_ESQUEMA_TABLA_EDV\s*=\s*dbutils\.widgets\.get/.test(script);

        if (hasEDVCatalogVar && hasEDVSchemaVar) {
            this.checks.push({
                name: 'Variables EDV',
                passed: true,
                message: '✅ Variables EDV correctamente asignadas'
            });
        } else {
            this.checks.push({
                name: 'Variables EDV',
                passed: false,
                message: '❌ Variables EDV no están correctamente asignadas'
            });
            this.errors.push('Variables EDV no definidas con dbutils.widgets.get');
        }
    }

    /**
     * Valida que exista PRM_ESQUEMA_TABLA_ESCRITURA
     */
    validateEDVSchemaWriting(script) {
        const hasWritingSchema = /PRM_ESQUEMA_TABLA_ESCRITURA\s*=\s*PRM_CATALOG_NAME_EDV\s*\+\s*["']\.[\"']\s*\+\s*PRM_ESQUEMA_TABLA_EDV/.test(script);

        if (hasWritingSchema) {
            this.checks.push({
                name: 'Esquema de escritura EDV',
                passed: true,
                message: '✅ PRM_ESQUEMA_TABLA_ESCRITURA correctamente definido'
            });
        } else {
            this.checks.push({
                name: 'Esquema de escritura EDV',
                passed: false,
                message: '❌ PRM_ESQUEMA_TABLA_ESCRITURA no está definido'
            });
            this.errors.push('PRM_ESQUEMA_TABLA_ESCRITURA no definido o incorrecto');
        }
    }

    /**
     * Valida que VAL_DESTINO_NAME apunte a esquema EDV
     */
    validateDestinationName(script) {
        const usesEDVSchema = /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA_ESCRITURA/.test(script);

        if (usesEDVSchema) {
            this.checks.push({
                name: 'Destino EDV',
                passed: true,
                message: '✅ VAL_DESTINO_NAME usa PRM_ESQUEMA_TABLA_ESCRITURA'
            });
        } else {
            this.checks.push({
                name: 'Destino EDV',
                passed: false,
                message: '❌ VAL_DESTINO_NAME no apunta al esquema EDV'
            });
            this.errors.push('VAL_DESTINO_NAME debe usar PRM_ESQUEMA_TABLA_ESCRITURA');
        }
    }

    /**
     * Valida que las tablas temporales sean managed (sin path)
     */
    validateManagedTables(script) {
        const hasSaveAsTableWithPath = /\.saveAsTable\([^)]*path\s*=/.test(script);

        if (!hasSaveAsTableWithPath) {
            this.checks.push({
                name: 'Tablas managed',
                passed: true,
                message: '✅ Tablas temporales son managed (sin path)'
            });
        } else {
            this.checks.push({
                name: 'Tablas managed',
                passed: false,
                message: '❌ Se encontró saveAsTable con path (debe ser managed)'
            });
            this.errors.push('Existen tablas con path= en saveAsTable. Deben ser managed tables.');
        }

        // Validar que se use spark.table para leer temporales
        const usesSparkTable = /spark\\.(read\\.)?table\\(/.test(script);
        if (usesSparkTable) {
            this.checks.push({
                name: 'Lectura con spark.table',
                passed: true,
                message: '✅ Se usa spark.table() para leer temporales'
            });
        } else {
            this.warnings.push('No se detectó uso de spark.table(). Verificar lecturas de temporales.');
        }
    }

    /**
     * Valida configuraciones de optimización Spark
     */
    validateSparkOptimizations(script) {
        const requiredConfigs = {
            'AQE': /spark\.conf\.set\(["']spark\.sql\.adaptive\.enabled["']/,
            'Coalesce Partitions': /spark\.conf\.set\(["']spark\.sql\.adaptive\.coalescePartitions\.enabled["']/,
            'Skew Join': /spark\.conf\.set\(["']spark\.sql\.adaptive\.skewJoin\.enabled["']/,
            'Shuffle Partitions': /spark\.conf\.set\(["']spark\.sql\.shuffle\.partitions["']/,
            'Broadcast Threshold': /spark\.conf\.set\(["']spark\.sql\.autoBroadcastJoinThreshold["']/,
            'Partition Overwrite': /spark\.conf\.set\(["']spark\.sql\.sources\.partitionOverwriteMode["']/,
            'Delta optimizeWrite': /spark\.conf\.set\(["']spark\.databricks\.delta\.optimizeWrite\.enabled["']/,
            'Delta autoCompact': /spark\.conf\.set\(["']spark\.databricks\.delta\.autoCompact\.enabled["']/
        };

        const missing = [];
        const present = [];

        for (const [name, pattern] of Object.entries(requiredConfigs)) {
            if (pattern.test(script)) {
                present.push(name);
            } else {
                missing.push(name);
            }
        }

        if (missing.length === 0) {
            this.checks.push({
                name: 'Optimizaciones Spark',
                passed: true,
                message: `✅ Todas las optimizaciones Spark presentes (${present.length}/8)`
            });
        } else if (missing.length <= 2) {
            this.checks.push({
                name: 'Optimizaciones Spark',
                passed: true,
                message: `⚠️  Casi completo (${present.length}/8). Faltan: ${missing.join(', ')}`
            });
            this.warnings.push(`Optimizaciones Spark faltantes: ${missing.join(', ')}`);
        } else {
            this.checks.push({
                name: 'Optimizaciones Spark',
                passed: false,
                message: `❌ Faltan ${missing.length} optimizaciones: ${missing.join(', ')}`
            });
            this.errors.push(`Configuraciones Spark faltantes: ${missing.join(', ')}`);
        }
    }

    /**
     * Valida que haya repartition antes de write_delta
     */
    validateRepartitionBeforeWrite(script) {
        const hasWriteDelta = /write_delta\(/.test(script);

        if (!hasWriteDelta) {
            this.warnings.push('No se encontró write_delta en el script');
            return;
        }

        // Buscar si hay repartition cerca de write_delta
        const hasRepartition = /repartition\([^)]*\)[\s\S]{0,500}write_delta/.test(script);

        if (hasRepartition) {
            this.checks.push({
                name: 'Repartition pre-escritura',
                passed: true,
                message: '✅ Repartition aplicado antes de write_delta'
            });
        } else {
            this.checks.push({
                name: 'Repartition pre-escritura',
                passed: false,
                message: '❌ Falta repartition antes de write_delta'
            });
            this.errors.push('Debe agregar repartition(CONS_PARTITION_DELTA_NAME) antes de write_delta');
        }
    }

    /**
     * Valida que la columna de partición sea correcta
     */
    validatePartitionColumn(script) {
        // Detectar si usa CODMES o codmes
        const hasCODMES = /CODMES/.test(script);
        const hasCodmes = /codmes/.test(script);

        if (hasCODMES && !hasCodmes) {
            this.checks.push({
                name: 'Columna de partición',
                passed: true,
                message: '✅ Usa CODMES (mayúsculas) - Macrogiro style'
            });
        } else if (hasCodmes && !hasCODMES) {
            this.checks.push({
                name: 'Columna de partición',
                passed: true,
                message: '✅ Usa codmes (minúsculas) - Agente/Cajero style'
            });
        } else if (hasCODMES && hasCodmes) {
            this.warnings.push('Se detectó tanto CODMES como codmes. Verificar consistencia.');
            this.checks.push({
                name: 'Columna de partición',
                passed: true,
                message: '⚠️  Usa ambos CODMES y codmes. Verificar consistencia.'
            });
        } else {
            this.warnings.push('No se detectó columna de partición CODMES/codmes');
        }
    }

    /**
     * Valida que no haya rutas hardcodeadas
     */
    validateNoHardcodedPaths(script) {
        const hardcodedPathPatterns = [
            /abfss:\/\/[^"'\s]+/,
            /\/mnt\/[^"'\s]+/,
            /path\s*=\s*["'][^"']+["']/
        ];

        const foundPaths = [];

        hardcodedPathPatterns.forEach(pattern => {
            const matches = script.match(pattern);
            if (matches) {
                foundPaths.push(...matches);
            }
        });

        if (foundPaths.length === 0) {
            this.checks.push({
                name: 'Sin rutas hardcodeadas',
                passed: true,
                message: '✅ No se detectaron rutas hardcodeadas'
            });
        } else {
            this.checks.push({
                name: 'Sin rutas hardcodeadas',
                passed: false,
                message: `❌ Se encontraron ${foundPaths.length} rutas hardcodeadas`
            });
            this.warnings.push(`Rutas hardcodeadas detectadas: ${foundPaths.slice(0, 3).join(', ')}...`);
        }
    }

    /**
     * Valida que el esquema DDV use views (_v)
     */
    validateDDVSchemaViews(script) {
        const schemaPattern = /PRM_ESQUEMA_TABLA_DDV["']\s*,\s*defaultValue\s*=\s*["']([^"']+)["']/;
        const match = script.match(schemaPattern);

        if (match) {
            const schema = match[1];
            if (schema.endsWith('_v')) {
                this.checks.push({
                    name: 'Schema DDV usa views',
                    passed: true,
                    message: `✅ Schema DDV usa views: ${schema}`
                });
            } else {
                this.checks.push({
                    name: 'Schema DDV usa views',
                    passed: false,
                    message: `❌ Schema DDV debe usar sufijo _v: ${schema}`
                });
                this.errors.push(`Schema DDV debe terminar en _v. Actual: ${schema}`);
            }
        } else {
            this.warnings.push('No se encontró definición de PRM_ESQUEMA_TABLA_DDV');
        }
    }

    /**
     * Valida limpieza de tablas temporales
     */
    validateDropTableCleanup(script) {
        const hasDropTable = /DROP\s+TABLE\s+IF\s+EXISTS/i.test(script);
        const hasCleanPaths = /cleanPaths/.test(script);

        if (hasDropTable && !hasCleanPaths) {
            this.checks.push({
                name: 'Limpieza de temporales',
                passed: true,
                message: '✅ Usa DROP TABLE IF EXISTS para limpieza'
            });
        } else if (!hasDropTable && !hasCleanPaths) {
            this.warnings.push('No se detectó limpieza de temporales (ni DROP TABLE ni cleanPaths)');
        } else if (hasCleanPaths) {
            this.checks.push({
                name: 'Limpieza de temporales',
                passed: false,
                message: '❌ Usa cleanPaths en vez de DROP TABLE IF EXISTS'
            });
            this.errors.push('Reemplazar cleanPaths por DROP TABLE IF EXISTS para cada temporal');
        }
    }


    /**
     * Extrae contexto util del script para checks compuestos
     */
    collectContext(script) {
        return {
            hasSaveWithPath: /\.saveAsTable\([^)]*path\s*=/.test(script),
            hasLoadTempFromPath: /spark\.read\.format\([^)]*\)\.load\([^)]*(\/temp\/|CONS_CONTAINER_NAME|PRM_CARPETA_RAIZ_DE_PROYECTO)/i.test(script),
            tmpAssignInDDV: /(tmp_table\w*)\s*=\s*f["']\{PRM_ESQUEMA_TABLA\}(.+?_tmp["'])/i.test(script),
            tmpAssignInEDV: /(tmp_table\w*)\s*=\s*f["']\{PRM_ESQUEMA_TABLA_ESCRITURA\}(.+?_tmp["'])/i.test(script),
            destUsesEDV: /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA_ESCRITURA\s*\+\s*['"]\.["']\s*\+\s*(PRM_TABLE_NAME|PRM_TABLA_SEGUNDATRANSPUESTA|\w+)/.test(script),
            destUsesDDV: /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*['"]\.["']\s*\+\s*(PRM_TABLE_NAME|PRM_TABLA_SEGUNDATRANSPUESTA|\w+)/.test(script),
            hasWriteDelta: /write_delta\(/.test(script),
            repartitionNearWrite: /repartition\([^)]*(CODMES|codmes)[^)]*\)[\s\S]{0,500}write_delta\(/.test(script),
            partitionByWriter: /\.partitionBy\(\s*(CODMES|codmes|CONS_PARTITION_DELTA_NAME)\s*\)/.test(script),
            confOverwriteDynamic: /spark\.conf\.set\(\s*['"]spark\.sql\.sources\.partitionOverwriteMode['"]/i.test(script),
            writerOverwriteDynamic: /option\(\s*['"]partitionOverwritemode['"]/i.test(script),
        };
    }

    /**
     * Valida writer options / conf para overwrite dinamico y particion
     */
    validateWriterOptions(script) {
        const hasDyn = this.context.writerOverwriteDynamic || this.context.confOverwriteDynamic;
        const hasPart = this.context.partitionByWriter;
        this.checks.push({ name: 'Overwrite dinamico', passed: !!hasDyn, message: hasDyn ? '? Overwrite dinamico activado (writer/conf)' : '? Falta overwrite dinamico' });
        if (!hasDyn) this.errors.push('Configurar partitionOverwriteMode=dynamic en writer o spark.conf');
        this.checks.push({ name: 'Writer particionado', passed: !!hasPart, message: hasPart ? '? Writer usa partitionBy' : '? Writer sin partitionBy' });
        if (!hasPart) this.errors.push('Agregar .partitionBy(<columna particion>) en write');
    }

    /**
     * Valida que las tmp esten en esquema EDV (no DDV)
     */
    validateTmpPlacementInEDV(script) {
        if (this.context.tmpAssignInDDV && !this.context.tmpAssignInEDV) {
            this.checks.push({ name: 'Temporales en EDV', passed: false, message: '? tmp_table apunta a PRM_ESQUEMA_TABLA (DDV)' });
            this.errors.push('Reubicar tmp_table a PRM_ESQUEMA_TABLA_ESCRITURA');
        } else {
            this.checks.push({ name: 'Temporales en EDV', passed: true, message: '? tmp_table en PRM_ESQUEMA_TABLA_ESCRITURA' });
        }
    }

    /**
     * Valida que no queden destinos residuales en DDV
     */
    validateNoResidualDDVDestination(script) {
        if (this.context.destUsesDDV) {
            this.checks.push({ name: 'Destino no-DDV', passed: false, message: '? Hay definiciones con PRM_ESQUEMA_TABLA (DDV)' });
            this.errors.push('Actualizar VAL_DESTINO_NAME para usar PRM_ESQUEMA_TABLA_ESCRITURA');
        } else {
            this.checks.push({ name: 'Destino no-DDV', passed: true, message: '? No hay destinos residuales en DDV' });
        }
    }
    /**
     * Calcula un score de compliance (0-100)
     */
    calculateScore() {
        if (this.checks.length === 0) return 0;

        const passed = this.checks.filter(c => c.passed).length;
        return Math.round((passed / this.checks.length) * 100);
    }

    /**
     * Genera reporte detallado
     */
    generateReport() {
        return {
            summary: {
                total: this.checks.length,
                passed: this.checks.filter(c => c.passed).length,
                failed: this.checks.filter(c => !c.passed).length,
                errors: this.errors.length,
                warnings: this.warnings.length,
                score: this.calculateScore()
            },
            checks: this.checks,
            errors: this.errors,
            warnings: this.warnings,
            compliance: this.calculateScore() >= 80 ? 'PASS' : 'FAIL'
        };
    }
}


// Exportar para uso en Node.js o navegador
if (typeof module !== 'undefined' && module.exports) {
    module.exports = EDVValidator;
}





