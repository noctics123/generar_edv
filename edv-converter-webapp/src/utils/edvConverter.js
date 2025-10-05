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
     * CRÍTICO: Actualiza CONS_CONTAINER_NAME de lhcldata a bcp-edv-trdata-012
     * Basado en conversiones reales DDV→EDV
     */
    updateContainerName(script) {
        const oldContainer = 'abfss://lhcldata@';
        const newContainer = 'abfss://bcp-edv-trdata-012@';

        if (script.includes(oldContainer)) {
            script = script.replace(
                /CONS_CONTAINER_NAME\s*=\s*["']abfss:\/\/lhcldata@["']/g,
                `CONS_CONTAINER_NAME = "${newContainer}"`
            );
            this.conversionLog.push(`✅ Container actualizado: lhcldata → bcp-edv-trdata-012`);
        } else if (script.includes(newContainer)) {
            this.warnings.push('Container ya está actualizado a EDV');
        }

        return script;
    }

    /**
     * CRÍTICO: Actualiza PRM_STORAGE_ACCOUNT_DDV de desarrollo (d03) a producción (p05)
     * Basado en conversiones reales DDV→EDV
     */
    updateStorageAccount(script) {
        const pattern = /dbutils\.widgets\.text\(name=["']PRM_STORAGE_ACCOUNT_DDV["'],\s*defaultValue=["']adlscu1lhclbackd\d+["']\)/;

        if (pattern.test(script)) {
            script = script.replace(
                pattern,
                `dbutils.widgets.text(name="PRM_STORAGE_ACCOUNT_DDV", defaultValue='adlscu1lhclbackp05')`
            );
            this.conversionLog.push(`✅ Storage account actualizado: desarrollo (d0X) → producción (p05)`);
        } else if (script.includes('adlscu1lhclbackp05')) {
            this.warnings.push('Storage account ya está en producción (p05)');
        }

        return script;
    }

    /**
     * CRÍTICO: Actualiza PRM_CATALOG_NAME de desarrollo (desa) a producción (prod)
     * Basado en conversiones reales DDV→EDV
     */
    updateCatalogName(script) {
        const pattern = /dbutils\.widgets\.text\(name=["']PRM_CATALOG_NAME["'],\s*defaultValue=["']catalog_lhcl_desa_bcp["']\)/;

        if (pattern.test(script)) {
            script = script.replace(
                pattern,
                `dbutils.widgets.text(name="PRM_CATALOG_NAME", defaultValue='catalog_lhcl_prod_bcp')`
            );
            this.conversionLog.push(`✅ Catalog actualizado: catalog_lhcl_desa_bcp → catalog_lhcl_prod_bcp`);
        } else if (script.includes('catalog_lhcl_prod_bcp')) {
            this.warnings.push('Catalog ya está en producción (prod_bcp)');
        }

        return script;
    }

    /**
     * Refuerza actualizacion de VAL_DESTINO_NAME para esquemas EDV (casos generales)
     * Aplica sufijo a la tabla final si está activado
     */
    fixDestinationName(script) {
        // Obtener opciones de switches
        const options = window.edvConversionOptions || {};
        const usarSufijoRuben = options.usarSufijoRuben !== false; // default true

        if (usarSufijoRuben) {
            // PRM_TABLE_NAME con sufijo
            script = script.replace(
                /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*"\."\s*\+\s*PRM_TABLE_NAME/g,
                'VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME + "_" + PRM_SUFIJO_TABLA'
            );
            // PRM_TABLA_SEGUNDATRANSPUESTA con sufijo
            script = script.replace(
                /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*"\."\s*\+\s*PRM_TABLA_SEGUNDATRANSPUESTA/g,
                'VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLA_SEGUNDATRANSPUESTA + "_" + PRM_SUFIJO_TABLA'
            );
            // Captura generica de variable a la derecha con sufijo
            script = script.replace(
                /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*"\."\s*\+\s*(\w+)/g,
                'VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + $1 + "_" + PRM_SUFIJO_TABLA'
            );
        } else {
            // Sin sufijo
            script = script.replace(
                /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*"\."\s*\+\s*PRM_TABLE_NAME/g,
                'VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME'
            );
            script = script.replace(
                /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*"\."\s*\+\s*PRM_TABLA_SEGUNDATRANSPUESTA/g,
                'VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLA_SEGUNDATRANSPUESTA'
            );
            script = script.replace(
                /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*"\."\s*\+\s*(\w+)/g,
                'VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + $1'
            );
        }
        return script;
    }

    /**
     * Reasigna tmp_table / tmp_table_N al esquema EDV si aun apuntan a PRM_ESQUEMA_TABLA
     */
    updateTmpAssignmentsToEDV(script) {
        const tmpAssignPattern = /(tmp_table\w*)\s*=\s*f["']\{PRM_ESQUEMA_TABLA\}(.+?_tmp["'])/g;
        return script.replace(tmpAssignPattern, "$1 = f'{PRM_ESQUEMA_TABLA_ESCRITURA}$2");
    }

    /**
     * Reemplaza lecturas inline de rutas /temp/ por spark.table(schemaEDV.carpeta_tmp)
     */
    replaceInlineTempLoads(script) {
        const loadAnyTempPattern = /spark\.read\.format\([^)]*\)\.load\(([^)]*\/temp\/[^)]*)\)/g;
        return script.replace(loadAnyTempPattern, (full, inner) => {
            const m = inner.match(/\/temp\/["']\s*\+\s*(\w+)/);
            if (m && m[1]) {
                const varName = m[1];
                // Construye: spark.table(f'{PRM_ESQUEMA_TABLA_ESCRITURA}.{<varName>}_tmp')
                return "spark.table(f'" + "{" + "PRM_ESQUEMA_TABLA_ESCRITURA" + "}" + ".{" + varName + "}_tmp')";
            }
            return full;
        });
    }

    /**
     * Reemplaza cualquier uso de cleanPaths por un comentario con instruccion de DROP TABLE
     */
    replaceAnyCleanPaths(script) {
        const cleanPattern = /\w+\.cleanPaths\([^)]+\)/g;
        return script.replace(cleanPattern, '# $&  # REEMPLAZAR con DROP TABLE IF EXISTS');
    }

    /**
     * Aplica sufijo (PRM_SUFIJO_TABLA) a nombres de tablas temporales si está activado
     * Modifica: tmp_table = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.nombre_tmp'
     * A: tmp_table = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.nombre_tmp_{PRM_SUFIJO_TABLA}'
     */
    applySufijoToTempTables(script) {
        // Obtener opciones de switches
        const options = window.edvConversionOptions || {};
        const usarSufijoRuben = options.usarSufijoRuben !== false; // default true

        if (!usarSufijoRuben) {
            return script; // No aplicar sufijo si está desactivado
        }

        // Patrón: tmp_table = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.XXX_tmp'
        const tmpPattern = /(tmp_table\w*)\s*=\s*f['"](\{PRM_ESQUEMA_TABLA_ESCRITURA\}\.[\w]+_tmp)['"]/g;

        const updated = script.replace(tmpPattern, (match, varName, tablePath) => {
            // Insertar sufijo antes del cierre de comilla
            return `${varName} = f'${tablePath}_{PRM_SUFIJO_TABLA}'`;
        });

        if (updated !== script) {
            this.conversionLog.push('✅ Sufijo PRM_SUFIJO_TABLA aplicado a tablas temporales');
        }

        return updated;
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

        // 0. CRÍTICO: Actualizar constantes y parámetros de ambiente (DDV → EDV)
        edvScript = this.updateContainerName(edvScript);
        edvScript = this.updateStorageAccount(edvScript);
        edvScript = this.updateCatalogName(edvScript);

        // 0.5. Agregar comandos de setup EDV (restartPython, pip install, removeAll)
        edvScript = this.addEDVSetupCommands(edvScript);

        // 1. Agregar widgets EDV
        edvScript = this.addEDVWidgets(edvScript);

        // 2. Agregar variables EDV
        edvScript = this.addEDVVariables(edvScript);

        // 3. Actualizar VAL_DESTINO_NAME
        edvScript = this.updateDestinationName(edvScript);
        // Refuerzo: forzar que VAL_DESTINO_NAME use esquema EDV en variantes no cubiertas
        edvScript = this.fixDestinationName(edvScript);

        // 4. Convertir temporales a managed tables
        edvScript = this.convertTempTablesToManaged(edvScript);
        // Refuerzos para temporales: reasignar tmp_table(s) al esquema EDV y reemplazar lecturas inline de rutas
        edvScript = this.updateTmpAssignmentsToEDV(edvScript);
        edvScript = this.replaceInlineTempLoads(edvScript);
        // Aplicar sufijo a tablas temporales
        edvScript = this.applySufijoToTempTables(edvScript);

        // 5. Agregar optimizaciones Spark
        edvScript = this.addSparkOptimizations(edvScript);

        // 6. Agregar repartition antes de write
        edvScript = this.addRepartitionBeforeWrite(edvScript);

        // 7. Reemplazar cleanPaths por DROP TABLE
        edvScript = this.replaceCleanPathsWithDrop(edvScript);
        // Refuerzo: cubrir variantes de cualquierObject.cleanPaths(...)
        edvScript = this.replaceAnyCleanPaths(edvScript);

        // 8. Actualizar schema DDV para usar views (_v)
        edvScript = this.updateDDVSchemaToViews(edvScript);

        return {
            edvScript,
            log: this.conversionLog,
            warnings: this.warnings
        };
    }

    /**
     * Agrega comandos de setup EDV al inicio del script
     * Incluye: restartPython, pip install COE Data Cleaner, removeAll widgets
     */
    addEDVSetupCommands(script) {
        // Verificar si ya existen
        if (script.includes('dbutils.library.restartPython()') &&
            script.includes('bcp_coe_data_cleaner') &&
            script.includes('dbutils.widgets.removeAll()')) {
            this.warnings.push('Comandos de setup EDV ya existen');
            return script;
        }

        // Obtener opciones de switches (serán configurables desde la UI)
        const options = window.edvSetupOptions || {
            restartPython: true,
            pipInstall: true,
            removeAllWidgets: true
        };

        let setupCommands = '\n';

        // Comando 1: restartPython
        if (options.restartPython) {
            setupCommands += `# COMMAND ----------

dbutils.library.restartPython()

`;
        }

        // Comando 2: pip install COE Data Cleaner
        if (options.pipInstall) {
            setupCommands += `# COMMAND ----------

!pip3 install --trusted-host 10.79.236.20 https://10.79.236.20:443/artifactory/LHCL.Pypi.Snapshot/lhcl/bcp_coe_data_cleaner/1.0.1/bcp_coe_data_cleaner-1.0.1-py3-none-any.whl

`;
        }

        // Comando 3: removeAll widgets
        if (options.removeAllWidgets) {
            setupCommands += `# COMMAND ----------

dbutils.widgets.removeAll()

`;
        }

        setupCommands += '# COMMAND ----------\n';

        // Insertar después del header (# ||****... o primera línea)
        const headerEndPattern = /# \*+\s*\n/;
        const headerMatch = script.match(headerEndPattern);

        if (headerMatch) {
            const insertPos = headerMatch.index + headerMatch[0].length;
            script = script.slice(0, insertPos) + setupCommands + script.slice(insertPos);

            const comandosAgregados = [];
            if (options.restartPython) comandosAgregados.push('restartPython');
            if (options.pipInstall) comandosAgregados.push('pip install');
            if (options.removeAllWidgets) comandosAgregados.push('removeAll');

            this.conversionLog.push(`✅ Comandos de setup EDV agregados: ${comandosAgregados.join(', ')}`);
        } else {
            // Fallback: insertar al inicio después de "# Databricks notebook source"
            const notebookPattern = /# Databricks notebook source\s*\n/;
            const notebookMatch = script.match(notebookPattern);

            if (notebookMatch) {
                const insertPos = notebookMatch.index + notebookMatch[0].length;
                script = script.slice(0, insertPos) + setupCommands + script.slice(insertPos);
                this.conversionLog.push('✅ Comandos de setup EDV agregados después del header Databricks');
            } else {
                // Último fallback: insertar al inicio
                script = setupCommands + script;
                this.conversionLog.push('✅ Comandos de setup EDV agregados al inicio del script');
            }
        }

        return script;
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

        // Obtener opciones de switches
        const options = window.edvConversionOptions || {};
        const usarRutasRuben = options.usarRutasRuben !== false; // default true

        let edvWidgets = `dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')
dbutils.widgets.text(name="PRM_SUFIJO_TABLA", defaultValue='RUBEN')
`;

        // Agregar widgets de rutas si está activado
        if (usarRutasRuben) {
            edvWidgets += `dbutils.widgets.text(name="PRM_CARPETA_OUTPUT", defaultValue='data/RUBEN/DEUDA_TECNICA/out')
dbutils.widgets.text(name="PRM_RUTA_ADLS_TABLES", defaultValue='data/RUBEN/DEUDA_TECNICA/matrizvariables')
`;
        }

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

        // Obtener opciones de switches
        const options = window.edvConversionOptions || {};
        const usarRutasRuben = options.usarRutasRuben !== false; // default true

        let edvVarsCode = `PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")
PRM_SUFIJO_TABLA = dbutils.widgets.get("PRM_SUFIJO_TABLA")
`;

        // Agregar variables de rutas si está activado
        if (usarRutasRuben) {
            edvVarsCode += `PRM_CARPETA_OUTPUT = dbutils.widgets.get("PRM_CARPETA_OUTPUT")
PRM_RUTA_ADLS_TABLES = dbutils.widgets.get("PRM_RUTA_ADLS_TABLES")
`;
        }

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

            // Actualizar VAL_DESTINO_NAME con sufijo
            script = script.replace(
                /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*"\.".*PRM_TABLE_NAME/,
                'VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME + "_" + PRM_SUFIJO_TABLA'
            );

            this.conversionLog.push('✅ PRM_ESQUEMA_TABLA_ESCRITURA agregado');
            this.conversionLog.push('✅ VAL_DESTINO_NAME actualizado para EDV con sufijo PRM_SUFIJO_TABLA');
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
