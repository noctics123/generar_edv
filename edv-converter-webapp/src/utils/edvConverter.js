/**
 * EDV Converter - Motor de conversión DDV a EDV
 * Convierte scripts de PySpark del ambiente DDV al ambiente EDV
 *
 * @author BCP Analytics Team
 * @version 1.0.0
 */

class EDVConverter {
    constructor() {
        this.conversionLog = [];
        this.warnings = [];
    }

    /**
     * Convierte un script DDV a EDV
     * @param {string} ddvScript - Código del script DDV
     * @returns {Object} - { edvScript: string, log: array, warnings: array }
     */
    convert(ddvScript) {
        this.conversionLog = [];
        this.warnings = [];

        let edvScript = ddvScript;

        // 1. Agregar widgets EDV
        edvScript = this.addEDVWidgets(edvScript);

        // 2. Agregar variables EDV
        edvScript = this.addEDVVariables(edvScript);

        // 3. Actualizar VAL_DESTINO_NAME
        edvScript = this.updateDestinationName(edvScript);

        // 4. Convertir temporales a managed tables
        edvScript = this.convertTempTablesToManaged(edvScript);

        // 5. Agregar optimizaciones Spark
        edvScript = this.addSparkOptimizations(edvScript);

        // 6. Agregar repartition antes de write
        edvScript = this.addRepartitionBeforeWrite(edvScript);

        // 7. Reemplazar cleanPaths por DROP TABLE
        edvScript = this.replaceCleanPathsWithDrop(edvScript);

        // 8. Actualizar schema DDV para usar views (_v)
        edvScript = this.updateDDVSchemaToViews(edvScript);

        return {
            edvScript,
            log: this.conversionLog,
            warnings: this.warnings
        };
    }

    /**
     * Agrega widgets EDV al script
     */
    addEDVWidgets(script) {
        // Verificar si ya existen
        if (script.includes('PRM_CATALOG_NAME_EDV')) {
            this.warnings.push('Los widgets EDV ya existen en el script');
            return script;
        }

        const edvWidgets = `dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')
`;

        // Buscar PRM_TABLA_PARAM_GRUPO (último widget común) para insertar después
        const lastWidgetPattern = /dbutils\.widgets\.text\(name="PRM_TABLA_PARAM_GRUPO"[^\n]+\n/;
        const match = script.match(lastWidgetPattern);

        if (match) {
            const insertPos = match.index + match[0].length;
            script = script.slice(0, insertPos) + edvWidgets + script.slice(insertPos);
            this.conversionLog.push('✅ Widgets EDV agregados después de PRM_TABLA_PARAM_GRUPO');
        } else {
            // Buscar cualquier último widget como fallback
            const fallbackPattern = /(dbutils\.widgets\.text\([^)]+\)\s*\n)+/g;
            let lastMatch = null;
            let match2;
            while ((match2 = fallbackPattern.exec(script)) !== null) {
                lastMatch = match2;
            }

            if (lastMatch) {
                const insertPos = lastMatch.index + lastMatch[0].length;
                script = script.slice(0, insertPos) + '\n' + edvWidgets + script.slice(insertPos);
                this.conversionLog.push('✅ Widgets EDV agregados al final de sección de widgets');
            } else {
                this.warnings.push('⚠️  No se encontró sección de widgets. Agregar manualmente.');
            }
        }

        return script;
    }

    /**
     * Agrega variables EDV después de los gets de widgets
     */
    addEDVVariables(script) {
        // Verificar si ya existen
        if (script.includes('PRM_CATALOG_NAME_EDV = dbutils.widgets.get')) {
            this.warnings.push('Las variables EDV ya existen');
            return script;
        }

        const edvVarsCode = `PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")
`;

        // Buscar PRM_TABLA_PARAM_GRUPO get (último común)
        const lastGetPattern = /PRM_TABLA_PARAM_GRUPO\s*=\s*dbutils\.widgets\.get\("PRM_TABLA_PARAM_GRUPO"\)\s*\n/;
        const match = script.match(lastGetPattern);

        if (match) {
            const insertPos = match.index + match[0].length;
            script = script.slice(0, insertPos) + edvVarsCode + script.slice(insertPos);
            this.conversionLog.push('✅ Variables EDV agregadas después de PRM_TABLA_PARAM_GRUPO');
        } else {
            // Buscar último get como fallback
            const lines = script.split('\n');
            let lastGetIndex = -1;

            for (let i = 0; i < lines.length; i++) {
                if (lines[i].includes('= dbutils.widgets.get(')) {
                    lastGetIndex = i;
                }
            }

            if (lastGetIndex >= 0) {
                lines.splice(lastGetIndex + 1, 0, edvVarsCode);
                script = lines.join('\n');
                this.conversionLog.push('✅ Variables EDV agregadas');
            } else {
                this.warnings.push('⚠️  No se encontró sección de gets. Agregar variables EDV manualmente.');
            }
        }

        return script;
    }

    /**
     * Actualiza la definición de VAL_DESTINO_NAME para usar esquema EDV
     */
    updateDestinationName(script) {
        // Verificar si ya usa PRM_ESQUEMA_TABLA_ESCRITURA
        if (script.includes('PRM_ESQUEMA_TABLA_ESCRITURA')) {
            this.warnings.push('PRM_ESQUEMA_TABLA_ESCRITURA ya existe');
            return script;
        }

        // Buscar línea de PRM_CARPETA_RAIZ_DE_PROYECTO o PRM_ESQUEMA_TABLA para insertar después
        const schemaPattern = /PRM_ESQUEMA_TABLA\s*=\s*PRM_CATALOG_NAME\s*\+\s*"\.".*\n/;
        const match = script.match(schemaPattern);

        if (match) {
            const insertPos = match.index + match[0].length;

            // Código a insertar
            const edvSchemaCode = `PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
`;

            // Insertar después de PRM_ESQUEMA_TABLA
            script = script.slice(0, insertPos) + edvSchemaCode + script.slice(insertPos);

            // Actualizar VAL_DESTINO_NAME
            script = script.replace(
                /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*"\.".*PRM_TABLE_NAME/,
                'VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME'
            );

            this.conversionLog.push('✅ PRM_ESQUEMA_TABLA_ESCRITURA agregado');
            this.conversionLog.push('✅ VAL_DESTINO_NAME actualizado para EDV');
        } else {
            this.warnings.push('⚠️  No se encontró PRM_ESQUEMA_TABLA. Verificar manualmente.');
        }

        return script;
    }

    /**
     * Convierte tablas temporales de path a managed tables
     */
    convertTempTablesToManaged(script) {
        let converted = false;

        // Pattern 1: saveAsTable con path
        const saveAsTablePattern = /\.saveAsTable\(([^,)]+),\s*path\s*=\s*[^)]+\)/g;
        if (script.match(saveAsTablePattern)) {
            script = script.replace(saveAsTablePattern, '.saveAsTable($1)');
            converted = true;
        }

        // Pattern 2: spark.read.format().load(ruta) -> spark.table()
        // Detectar variables de ruta temporal
        const tempPathPattern = /(\w+)\s*=\s*[^"\n]+["']\/temp\/[^"'\n]+["']/g;
        const tempPaths = [];
        let pathMatch;

        while ((pathMatch = tempPathPattern.exec(script)) !== null) {
            tempPaths.push(pathMatch[1]);
        }

        // Reemplazar lecturas de path por spark.table
        tempPaths.forEach(pathVar => {
            const loadPattern = new RegExp(`spark\\.read\\.format\\([^)]+\\)\\.load\\(${pathVar}\\)`, 'g');
            // Necesitamos obtener el nombre de la tabla desde el saveAsTable anterior
            // Por simplicidad, usar el nombre de variable anterior
            script = script.replace(loadPattern, 'spark.table(tmp_table)');
        });

        if (converted) {
            this.conversionLog.push('✅ Tablas temporales convertidas a managed tables');
            this.warnings.push('⚠️  Verificar que las lecturas con spark.table() usen el nombre correcto');
        }

        return script;
    }

    /**
     * Agrega configuraciones de optimización Spark
     */
    addSparkOptimizations(script) {
        const optimizationCode = `
# OPTIMIZACIÓN: Configuración de Spark para mejor rendimiento
# Adaptive Query Execution (AQE) - Optimiza shuffles y maneja skew automáticamente
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64m")

# Ajustar particiones de shuffle para cluster mediano
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Broadcast automático para dimensiones pequeñas
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64*1024*1024))

# Overwrite dinámico de particiones
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Delta Lake - Optimización de escritura y compactación automática
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

`;

        // Verificar si ya existen
        if (script.includes('spark.sql.adaptive.enabled')) {
            this.warnings.push('Las optimizaciones Spark ya existen');
            return script;
        }

        // Insertar después de los imports y antes de las constantes
        // Buscar la primera línea que no sea import, comentario o línea vacía
        const lines = script.split('\n');
        let insertIndex = 0;

        for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            if (line && !line.startsWith('#') && !line.startsWith('import') && !line.startsWith('from')) {
                insertIndex = i;
                break;
            }
        }

        lines.splice(insertIndex, 0, optimizationCode);
        script = lines.join('\n');

        this.conversionLog.push('✅ Optimizaciones Spark agregadas');

        return script;
    }

    /**
     * Agrega repartition antes de write_delta
     */
    addRepartitionBeforeWrite(script) {
        // Buscar llamadas a write_delta sin repartition previo
        const writeDeltaPattern = /(write_delta\(input_df,\s*VAL_DESTINO_NAME,\s*(\w+)\))/g;

        const matches = [...script.matchAll(writeDeltaPattern)];

        if (matches.length > 0) {
            matches.forEach(match => {
                const fullMatch = match[0];
                const partitionVar = match[2];

                // Verificar si ya hay un repartition antes
                const beforeWrite = script.substring(0, match.index);
                if (!beforeWrite.slice(-500).includes('repartition')) {
                    const repartitionCode = `\n# OPTIMIZACIÓN: Repartition antes de escribir para balancear particiones\ninput_df = input_df.repartition(${partitionVar})\n\n`;
                    script = script.replace(fullMatch, repartitionCode + fullMatch);
                }
            });

            this.conversionLog.push('✅ Repartition agregado antes de write_delta');
        }

        return script;
    }

    /**
     * Reemplaza cleanPaths por DROP TABLE IF EXISTS
     */
    replaceCleanPathsWithDrop(script) {
        // Buscar llamadas a cleanPaths o funciones similares
        const cleanPattern = /funciones\.cleanPaths\([^)]+\)/g;

        if (script.match(cleanPattern)) {
            // Necesitamos identificar las tablas temporales para generar DROP TABLE
            this.warnings.push('⚠️  cleanPaths detectado. Reemplazar con DROP TABLE IF EXISTS para cada temporal.');

            // Comentar la línea de cleanPaths
            script = script.replace(cleanPattern, '# $& # REEMPLAZAR con DROP TABLE IF EXISTS');

            this.conversionLog.push('⚠️  cleanPaths comentado - agregar DROP TABLE manualmente');
        }

        return script;
    }

    /**
     * Actualiza esquema DDV para usar views (sufijo _v)
     */
    updateDDVSchemaToViews(script) {
        // Buscar PRM_ESQUEMA_TABLA_DDV widget y agregar _v si no lo tiene
        const schemaPattern = /dbutils\.widgets\.text\(name="PRM_ESQUEMA_TABLA_DDV",\s*defaultValue='([^']+)'\)/;
        const match = script.match(schemaPattern);

        if (match) {
            const currentSchema = match[1];
            if (!currentSchema.endsWith('_v')) {
                const newSchema = currentSchema + '_v';
                script = script.replace(
                    `defaultValue='${currentSchema}'`,
                    `defaultValue='${newSchema}'`
                );
                this.conversionLog.push(`✅ Schema DDV actualizado a views: ${currentSchema} → ${newSchema}`);
            } else {
                this.warnings.push('Schema DDV ya usa sufijo _v (views)');
            }
        } else {
            this.warnings.push('⚠️  No se encontró PRM_ESQUEMA_TABLA_DDV widget');
        }

        return script;
    }

    /**
     * Genera un reporte de la conversión
     */
    generateReport() {
        return {
            totalChanges: this.conversionLog.length,
            warnings: this.warnings.length,
            log: this.conversionLog,
            warningsList: this.warnings
        };
    }
}

// Exportar para uso en Node.js o navegador
if (typeof module !== 'undefined' && module.exports) {
    module.exports = EDVConverter;
}
