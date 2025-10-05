/**
 * EDV Validator RIGUROSO - Basado en reglas exactas de conversión
 * Valida punto por punto cada requisito crítico
 *
 * @author BCP Analytics Team
 * @version 2.0.0
 */

class EDVValidatorRiguroso {
    constructor() {
        this.checks = [];
        this.errors = [];
        this.warnings = [];
        this.score = 0;
    }

    /**
     * Valida un script EDV con máximo rigor
     */
    validate(script) {
        this.checks = [];
        this.errors = [];
        this.warnings = [];
        this.score = 0;

        // NIVEL 0: Ambiente EDV (CRÍTICO - validaciones sin puntos, pero obligatorias)
        this.validateContainerName(script);
        this.validateStorageAccount(script);
        this.validateCatalogName(script);

        // NIVEL 1: Parámetros EDV (90 puntos - CRÍTICO)
        this.validateWidgetCatalogEDV(script);      // 15pts
        this.validateWidgetSchemaEDV(script);       // 15pts
        this.validateGetCatalogEDV(script);         // 15pts
        this.validateGetSchemaEDV(script);          // 15pts
        this.validateEsquemaEscritura(script);      // 15pts
        this.validateValDestinoEDV(script);         // 15pts

        // NIVEL 2: Schema DDV Views (10 puntos - CRÍTICO)
        this.validateDDVSchemaViews(script);        // 10pts

        // NIVEL 3: Tablas Managed (validación sin puntos - incluido en Nivel 1)
        this.validateNoPathInSaveAsTable(script);
        this.validateTmpInEDVSchema(script);
        this.validateSparkTable(script);
        this.validateNoLoadFromPath(script);

        // NIVEL 4: Optimizaciones Spark (Bonus +10)
        const sparkOpts = this.validateSparkOptimizations(script);
        if (sparkOpts === 8) this.score += 10;

        // NIVEL 5: Limpieza (Bonus +5)
        if (this.validateCleanup(script)) this.score += 5;

        // NIVEL 6: Parámetros Opcionales (Advertencias - No afectan score)
        this.validateOptionalParameters(script);

        return {
            passed: this.score >= 90,
            checks: this.checks,
            errors: this.errors,
            warnings: this.warnings,
            score: Math.min(this.score, 100),
            level: this.getComplianceLevel()
        };
    }

    // ========== NIVEL 0: AMBIENTE EDV (CRÍTICOS) ==========

    validateContainerName(script) {
        const correctContainer = 'abfss://bcp-edv-trdata-012@';
        const wrongContainer = 'abfss://lhcldata@';

        const hasCorrect = script.includes(correctContainer);
        const hasWrong = script.includes(wrongContainer);

        if (hasCorrect && !hasWrong) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '0.1 Container EDV',
                passed: true,
                points: 0,
                message: `✅ CONS_CONTAINER_NAME correcto: ${correctContainer}`
            });
        } else if (hasWrong) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '0.1 Container EDV',
                passed: false,
                points: 0,
                message: `❌ CONS_CONTAINER_NAME INCORRECTO - debe ser: ${correctContainer}`
            });
            this.errors.push(`Container name debe ser "${correctContainer}" (no "lhcldata@")`);
            this.score -= 20; // Penalización grave
        } else {
            this.warnings.push('No se detectó CONS_CONTAINER_NAME');
        }
    }

    validateStorageAccount(script) {
        const correctAccount = 'adlscu1lhclbackp05';
        const wrongPattern = /adlscu1lhclbackd\d+/;

        const hasCorrect = script.includes(correctAccount);
        const hasWrong = wrongPattern.test(script);

        if (hasCorrect && !hasWrong) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '0.2 Storage Account Producción',
                passed: true,
                points: 0,
                message: `✅ PRM_STORAGE_ACCOUNT_DDV correcto: ${correctAccount}`
            });
        } else if (hasWrong) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '0.2 Storage Account Producción',
                passed: false,
                points: 0,
                message: `❌ PRM_STORAGE_ACCOUNT_DDV es desarrollo (d0X) - debe ser producción: ${correctAccount}`
            });
            this.errors.push(`Storage account debe ser "${correctAccount}" (producción, no desarrollo)`);
            this.score -= 20; // Penalización grave
        } else {
            this.warnings.push('No se detectó PRM_STORAGE_ACCOUNT_DDV');
        }
    }

    validateCatalogName(script) {
        const correctCatalog = 'catalog_lhcl_prod_bcp';
        const wrongCatalog = 'catalog_lhcl_desa_bcp';

        const hasCorrect = script.includes(correctCatalog);
        const hasWrong = script.includes(wrongCatalog);

        if (hasCorrect && !hasWrong) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '0.3 Catalog Producción',
                passed: true,
                points: 0,
                message: `✅ PRM_CATALOG_NAME correcto: ${correctCatalog}`
            });
        } else if (hasWrong) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '0.3 Catalog Producción',
                passed: false,
                points: 0,
                message: `❌ PRM_CATALOG_NAME es desarrollo (desa) - debe ser producción: ${correctCatalog}`
            });
            this.errors.push(`Catalog debe ser "${correctCatalog}" (no "desa_bcp")`);
            this.score -= 20; // Penalización grave
        } else {
            this.warnings.push('No se detectó PRM_CATALOG_NAME');
        }
    }

    // ========== NIVEL 1: PARÁMETROS EDV (CRÍTICOS) ==========

    validateWidgetCatalogEDV(script) {
        const pattern = /dbutils\.widgets\.text\(\s*name\s*=\s*["']PRM_CATALOG_NAME_EDV["']/;
        const hasWidget = pattern.test(script);

        if (hasWidget) {
            // Verificar defaultValue correcto
            const valuePattern = /dbutils\.widgets\.text\(\s*name\s*=\s*["']PRM_CATALOG_NAME_EDV["']\s*,\s*defaultValue\s*=\s*["']catalog_lhcl_prod_bcp_expl["']/;
            const hasCorrectValue = valuePattern.test(script);

            if (hasCorrectValue) {
                this.checks.push({
                    level: 'CRÍTICO',
                    name: '1.1 Widget PRM_CATALOG_NAME_EDV',
                    passed: true,
                    points: 15,
                    message: '✅ Widget con defaultValue correcto'
                });
                this.score += 15;
            } else {
                this.checks.push({
                    level: 'CRÍTICO',
                    name: '1.1 Widget PRM_CATALOG_NAME_EDV',
                    passed: false,
                    points: 0,
                    message: '❌ Widget existe pero defaultValue incorrecto (debe ser: catalog_lhcl_prod_bcp_expl)'
                });
                this.errors.push('PRM_CATALOG_NAME_EDV debe tener defaultValue="catalog_lhcl_prod_bcp_expl"');
            }
        } else {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.1 Widget PRM_CATALOG_NAME_EDV',
                passed: false,
                points: 0,
                message: '❌ Widget PRM_CATALOG_NAME_EDV NO existe'
            });
            this.errors.push('Falta widget: dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue="catalog_lhcl_prod_bcp_expl")');
        }
    }

    validateWidgetSchemaEDV(script) {
        const pattern = /dbutils\.widgets\.text\(\s*name\s*=\s*["']PRM_ESQUEMA_TABLA_EDV["']/;
        const hasWidget = pattern.test(script);

        if (hasWidget) {
            const valuePattern = /dbutils\.widgets\.text\(\s*name\s*=\s*["']PRM_ESQUEMA_TABLA_EDV["']\s*,\s*defaultValue\s*=\s*["']bcp_edv_trdata_012["']/;
            const hasCorrectValue = valuePattern.test(script);

            if (hasCorrectValue) {
                this.checks.push({
                    level: 'CRÍTICO',
                    name: '1.2 Widget PRM_ESQUEMA_TABLA_EDV',
                    passed: true,
                    points: 15,
                    message: '✅ Widget con defaultValue correcto'
                });
                this.score += 15;
            } else {
                this.checks.push({
                    level: 'CRÍTICO',
                    name: '1.2 Widget PRM_ESQUEMA_TABLA_EDV',
                    passed: false,
                    points: 0,
                    message: '❌ Widget existe pero defaultValue incorrecto (debe ser: bcp_edv_trdata_012)'
                });
                this.errors.push('PRM_ESQUEMA_TABLA_EDV debe tener defaultValue="bcp_edv_trdata_012"');
            }
        } else {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.2 Widget PRM_ESQUEMA_TABLA_EDV',
                passed: false,
                points: 0,
                message: '❌ Widget PRM_ESQUEMA_TABLA_EDV NO existe'
            });
            this.errors.push('Falta widget: dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue="bcp_edv_trdata_012")');
        }
    }

    validateGetCatalogEDV(script) {
        const pattern = /PRM_CATALOG_NAME_EDV\s*=\s*dbutils\.widgets\.get\(\s*["']PRM_CATALOG_NAME_EDV["']\s*\)/;
        const hasGet = pattern.test(script);

        if (hasGet) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.3 Get PRM_CATALOG_NAME_EDV',
                passed: true,
                points: 15,
                message: '✅ Variable correctamente asignada'
            });
            this.score += 15;
        } else {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.3 Get PRM_CATALOG_NAME_EDV',
                passed: false,
                points: 0,
                message: '❌ Falta asignación de variable'
            });
            this.errors.push('Falta: PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")');
        }
    }

    validateGetSchemaEDV(script) {
        const pattern = /PRM_ESQUEMA_TABLA_EDV\s*=\s*dbutils\.widgets\.get\(\s*["']PRM_ESQUEMA_TABLA_EDV["']\s*\)/;
        const hasGet = pattern.test(script);

        if (hasGet) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.4 Get PRM_ESQUEMA_TABLA_EDV',
                passed: true,
                points: 15,
                message: '✅ Variable correctamente asignada'
            });
            this.score += 15;
        } else {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.4 Get PRM_ESQUEMA_TABLA_EDV',
                passed: false,
                points: 0,
                message: '❌ Falta asignación de variable'
            });
            this.errors.push('Falta: PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")');
        }
    }

    validateEsquemaEscritura(script) {
        const pattern = /PRM_ESQUEMA_TABLA_ESCRITURA\s*=\s*PRM_CATALOG_NAME_EDV\s*\+\s*["']\s*\.\s*["']\s*\+\s*PRM_ESQUEMA_TABLA_EDV/;
        const hasVar = pattern.test(script);

        if (hasVar) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.5 PRM_ESQUEMA_TABLA_ESCRITURA',
                passed: true,
                points: 15,
                message: '✅ Esquema de escritura EDV definido'
            });
            this.score += 15;
        } else {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.5 PRM_ESQUEMA_TABLA_ESCRITURA',
                passed: false,
                points: 0,
                message: '❌ Falta definición de esquema de escritura'
            });
            this.errors.push('Falta: PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV');
        }
    }

    validateValDestinoEDV(script) {
        // Debe usar PRM_ESQUEMA_TABLA_ESCRITURA
        const correctPattern = /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA_ESCRITURA\s*\+\s*["']\s*\.\s*["']\s*\+\s*\w+/;
        const hasCorrect = correctPattern.test(script);

        // NO debe usar PRM_ESQUEMA_TABLA (sin _ESCRITURA)
        const wrongPattern = /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*["']\s*\.\s*["']/;
        const hasWrong = wrongPattern.test(script);

        if (hasCorrect && !hasWrong) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.6 VAL_DESTINO_NAME usa EDV',
                passed: true,
                points: 15,
                message: '✅ VAL_DESTINO_NAME apunta correctamente a esquema EDV'
            });
            this.score += 15;
        } else if (hasWrong) {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.6 VAL_DESTINO_NAME usa EDV',
                passed: false,
                points: 0,
                message: '❌ VAL_DESTINO_NAME aún usa PRM_ESQUEMA_TABLA (DDV) en vez de PRM_ESQUEMA_TABLA_ESCRITURA'
            });
            this.errors.push('CRÍTICO: VAL_DESTINO_NAME debe usar PRM_ESQUEMA_TABLA_ESCRITURA, NO PRM_ESQUEMA_TABLA');
        } else {
            this.checks.push({
                level: 'CRÍTICO',
                name: '1.6 VAL_DESTINO_NAME usa EDV',
                passed: false,
                points: 0,
                message: '❌ No se encontró definición correcta de VAL_DESTINO_NAME'
            });
            this.errors.push('Falta: VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME');
        }
    }

    // ========== NIVEL 2: SCHEMA DDV VIEWS ==========

    validateDDVSchemaViews(script) {
        const pattern = /dbutils\.widgets\.text\(\s*name\s*=\s*["']PRM_ESQUEMA_TABLA_DDV["']\s*,\s*defaultValue\s*=\s*["']([^"']+)["']/;
        const match = script.match(pattern);

        if (match) {
            const schemaValue = match[1];
            if (schemaValue.endsWith('_v')) {
                this.checks.push({
                    level: 'CRÍTICO',
                    name: '2.1 Schema DDV usa views (_v)',
                    passed: true,
                    points: 10,
                    message: `✅ Schema DDV correcto: ${schemaValue}`
                });
                this.score += 10;
            } else {
                this.checks.push({
                    level: 'CRÍTICO',
                    name: '2.1 Schema DDV usa views (_v)',
                    passed: false,
                    points: 0,
                    message: `❌ Schema DDV sin sufijo _v: ${schemaValue} (debe ser: ${schemaValue}_v)`
                });
                this.errors.push(`Schema DDV debe terminar en _v. Cambiar "${schemaValue}" por "${schemaValue}_v"`);
            }
        } else {
            this.warnings.push('No se encontró widget PRM_ESQUEMA_TABLA_DDV');
        }
    }

    // ========== NIVEL 3: MANAGED TABLES ==========

    validateNoPathInSaveAsTable(script) {
        const hasPath = /\.saveAsTable\([^)]*,\s*path\s*=/.test(script);

        if (!hasPath) {
            this.checks.push({
                level: 'Managed Tables',
                name: '3.1 Sin path en saveAsTable',
                passed: true,
                points: 0,
                message: '✅ Todas las tablas son managed (sin path)'
            });
        } else {
            this.checks.push({
                level: 'Managed Tables',
                name: '3.1 Sin path en saveAsTable',
                passed: false,
                points: 0,
                message: '❌ Se encontró saveAsTable con path= (debe ser managed)'
            });
            this.errors.push('CRÍTICO: Eliminar path= de saveAsTable. Las tablas deben ser managed.');
        }
    }

    validateTmpInEDVSchema(script) {
        const hasDDV = /tmp_table\w*\s*=\s*f["']\s*\{\s*PRM_ESQUEMA_TABLA\s*\}/.test(script);
        const hasEDV = /tmp_table\w*\s*=\s*f["']\s*\{\s*PRM_ESQUEMA_TABLA_ESCRITURA\s*\}/.test(script);

        if (hasEDV || !hasDDV) {
            this.checks.push({
                level: 'Managed Tables',
                name: '3.2 Temporales en esquema EDV',
                passed: true,
                points: 0,
                message: '✅ Temporales usan PRM_ESQUEMA_TABLA_ESCRITURA'
            });
        } else {
            this.checks.push({
                level: 'Managed Tables',
                name: '3.2 Temporales en esquema EDV',
                passed: false,
                points: 0,
                message: '❌ Temporales aún usan PRM_ESQUEMA_TABLA (DDV)'
            });
            this.errors.push('CRÍTICO: Temporales deben usar PRM_ESQUEMA_TABLA_ESCRITURA, NO PRM_ESQUEMA_TABLA');
        }
    }

    validateSparkTable(script) {
        const hasSparkTable = /spark\.table\(/.test(script);

        if (hasSparkTable) {
            this.checks.push({
                level: 'Managed Tables',
                name: '3.3 Lectura con spark.table',
                passed: true,
                points: 0,
                message: '✅ Se usa spark.table() para leer managed tables'
            });
        } else {
            this.warnings.push('No se detectó spark.table(). Verificar si hay temporales.');
        }
    }

    validateNoLoadFromPath(script) {
        const hasLoadPath = /spark\.read\.format\([^)]*\)\.load\([^)]*\/temp\//.test(script);

        if (!hasLoadPath) {
            this.checks.push({
                level: 'Managed Tables',
                name: '3.4 Sin lecturas de path',
                passed: true,
                points: 0,
                message: '✅ No hay lecturas de rutas temporales'
            });
        } else {
            this.checks.push({
                level: 'Managed Tables',
                name: '3.4 Sin lecturas de path',
                passed: false,
                points: 0,
                message: '❌ Se detectó lectura de ruta temporal (usar spark.table)'
            });
            this.errors.push('Reemplazar spark.read...load(ruta_temp) por spark.table(tabla_temp)');
        }
    }

    // ========== NIVEL 4: OPTIMIZACIONES SPARK (BONUS) ==========

    validateSparkOptimizations(script) {
        const configs = [
            'spark.sql.adaptive.enabled',
            'spark.sql.adaptive.coalescePartitions.enabled',
            'spark.sql.adaptive.skewJoin.enabled',
            'spark.sql.shuffle.partitions',
            'spark.sql.autoBroadcastJoinThreshold',
            'spark.sql.sources.partitionOverwriteMode',
            'spark.databricks.delta.optimizeWrite.enabled',
            'spark.databricks.delta.autoCompact.enabled'
        ];

        let count = 0;
        configs.forEach(config => {
            if (script.includes(config)) count++;
        });

        this.checks.push({
            level: 'Optimizaciones',
            name: '4. Configuraciones Spark AQE',
            passed: count === 8,
            points: count === 8 ? 10 : 0,
            message: count === 8 ? '✅ Todas las optimizaciones Spark presentes (+10 bonus)' : `⚠️ ${count}/8 optimizaciones Spark`
        });

        return count;
    }

    // ========== NIVEL 5: LIMPIEZA (BONUS) ==========

    validateCleanup(script) {
        const hasCleanPaths = /cleanPaths/.test(script);
        const hasDropTable = /DROP\s+TABLE\s+IF\s+EXISTS/i.test(script);

        if (!hasCleanPaths && hasDropTable) {
            this.checks.push({
                level: 'Limpieza',
                name: '5. Limpieza con DROP TABLE',
                passed: true,
                points: 5,
                message: '✅ Usa DROP TABLE IF EXISTS (+5 bonus)'
            });
            return true;
        } else if (hasCleanPaths) {
            this.checks.push({
                level: 'Limpieza',
                name: '5. Limpieza con DROP TABLE',
                passed: false,
                points: 0,
                message: '⚠️ Aún usa cleanPaths (reemplazar por DROP TABLE)'
            });
            this.warnings.push('Reemplazar cleanPaths por DROP TABLE IF EXISTS');
        }
        return false;
    }

    // ========== NIVEL 6: PARÁMETROS OPCIONALES (ADVERTENCIAS) ==========

    validateOptionalParameters(script) {
        // Advertencia: PRM_TABLE_NAME debería tener sufijo para evitar colisiones
        const tableNamePattern = /dbutils\.widgets\.text\(\s*name\s*=\s*["']PRM_TABLE_NAME["']\s*,\s*defaultValue\s*=\s*["']([^"']+)["']/;
        const tableNameMatch = script.match(tableNamePattern);

        if (tableNameMatch) {
            const tableName = tableNameMatch[1];
            const hasSuffix = /_EDV|_RUBEN|_TEST|_PROD|_[A-Z]+_\d+/i.test(tableName);

            if (!hasSuffix) {
                this.checks.push({
                    level: 'RECOMENDADO',
                    name: '6.1 Sufijo en PRM_TABLE_NAME',
                    passed: false,
                    points: 0,
                    message: `⚠️ Considera agregar sufijo a "${tableName}" (ej: ${tableName}_EDV) para evitar colisiones`
                });
                this.warnings.push(`PRM_TABLE_NAME sin sufijo: "${tableName}". Considera agregar _EDV, _RUBEN, etc.`);
            } else {
                this.checks.push({
                    level: 'RECOMENDADO',
                    name: '6.1 Sufijo en PRM_TABLE_NAME',
                    passed: true,
                    points: 0,
                    message: `✅ PRM_TABLE_NAME tiene sufijo: ${tableName}`
                });
            }
        }

        // Advertencia: PRM_FECHA_RUTINA debería actualizarse
        const fechaPattern = /dbutils\.widgets\.text\(\s*name\s*=\s*["']PRM_FECHA_RUTINA["']\s*,\s*defaultValue\s*=\s*["']([^"']+)["']/;
        const fechaMatch = script.match(fechaPattern);

        if (fechaMatch) {
            const fecha = fechaMatch[1];
            const year = parseInt(fecha.split('-')[0]);
            const currentYear = new Date().getFullYear();

            if (year < 2024 || year > currentYear + 1) {
                this.checks.push({
                    level: 'RECOMENDADO',
                    name: '6.2 PRM_FECHA_RUTINA actualizada',
                    passed: false,
                    points: 0,
                    message: `⚠️ PRM_FECHA_RUTINA parece antigua: ${fecha}. Actualizar a fecha reciente`
                });
                this.warnings.push(`PRM_FECHA_RUTINA="${fecha}" parece antigua. Actualizar a fecha de ejecución.`);
            } else {
                this.checks.push({
                    level: 'RECOMENDADO',
                    name: '6.2 PRM_FECHA_RUTINA actualizada',
                    passed: true,
                    points: 0,
                    message: `✅ PRM_FECHA_RUTINA reciente: ${fecha}`
                });
            }
        }

        // Advertencia: PRM_CARPETA_OUTPUT debería ser de usuario
        const carpetaPattern = /dbutils\.widgets\.text\(\s*name\s*=\s*["']PRM_CARPETA_OUTPUT["']\s*,\s*defaultValue\s*=\s*["']([^"']+)["']/;
        const carpetaMatch = script.match(carpetaPattern);

        if (carpetaMatch) {
            const carpeta = carpetaMatch[1];
            const hasUserFolder = /RUBEN|PEDRO|TEST|DEUDA_TECNICA/i.test(carpeta);

            if (carpeta.includes('desa/bcp/ddv') && !hasUserFolder) {
                this.checks.push({
                    level: 'RECOMENDADO',
                    name: '6.3 Carpeta de trabajo personalizada',
                    passed: false,
                    points: 0,
                    message: `⚠️ PRM_CARPETA_OUTPUT usa carpeta por defecto. Considera usar carpeta personal`
                });
                this.warnings.push(`PRM_CARPETA_OUTPUT="${carpeta}" usa carpeta por defecto. Personalizar con nombre de usuario/proyecto.`);
            }
        }
    }

    // ========== UTILIDADES ==========

    getComplianceLevel() {
        if (this.score >= 90) return 'PASS';
        if (this.score >= 70) return 'WARNING';
        return 'FAIL';
    }

    generateReport() {
        return {
            summary: {
                score: this.score,
                maxScore: 100,
                level: this.getComplianceLevel(),
                criticalPassed: this.score >= 90,
                totalChecks: this.checks.length,
                errors: this.errors.length,
                warnings: this.warnings.length
            },
            checks: this.checks,
            errors: this.errors,
            warnings: this.warnings
        };
    }
}

// Exportar
if (typeof module !== 'undefined' && module.exports) {
    module.exports = EDVValidatorRiguroso;
}
