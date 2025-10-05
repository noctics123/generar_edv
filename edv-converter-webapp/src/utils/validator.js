/**
 * EDV Validator - Validador de compliance EDV
 * - Verifica cumplimiento de reglas EDV
 * - Extrae parámetros DDV/EDV para mostrar en UI
 */

class EDVValidator {
  constructor() {
    this.checks = [];
    this.errors = [];
    this.warnings = [];
    this.context = {};
    this.parameters = {};
  }

  validate(script) {
    this.checks = [];
    this.errors = [];
    this.warnings = [];
    this.context = this.collectContext(script);
    this.parameters = this.extractParameters(script);

    // Validaciones principales
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
      score: this.calculateScore(),
      parameters: this.parameters,
    };
  }

  // ===== Contexto y parámetros =====
  collectContext(script) {
    return {
      hasEdvWidgets:
        /dbutils\.widgets\.text\([^)]*["']PRM_CATALOG_NAME_EDV["']/.test(script) &&
        /dbutils\.widgets\.text\([^)]*["']PRM_ESQUEMA_TABLA_EDV["']/.test(script),
      hasEdvVars:
        /PRM_CATALOG_NAME_EDV\s*=\s*dbutils\.widgets\.get/.test(script) &&
        /PRM_ESQUEMA_TABLA_EDV\s*=\s*dbutils\.widgets\.get/.test(script),
      hasWritingSchema:
        /PRM_ESQUEMA_TABLA_ESCRITURA\s*=\s*PRM_CATALOG_NAME_EDV\s*\+\s*['"]\.["']\s*\+\s*PRM_ESQUEMA_TABLA_EDV/.test(
          script,
        ),
      destUsesEDV: /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA_ESCRITURA/.test(script),
      destUsesDDV: /VAL_DESTINO_NAME\s*=\s*PRM_ESQUEMA_TABLA\s*\+\s*['"]\.["']\s*\+/.test(script),
      hasSaveWithPath: /\.saveAsTable\([^)]*path\s*=\//.test(script),
      usesSparkTable: /spark\.(read\.)?table\(/.test(script),
      hasLoadTempFromPath:
        /spark\.read\.format\([^)]*\)\.load\([^)]*(\/temp\/|abfss:\/\/|\/mnt\/)\s*[^)]*\)/i.test(script),
      tmpAssignInDDV:
        /(tmp_table\w*)\s*=\s*f["']\{PRM_ESQUEMA_TABLA\}(.+?_tmp["'])/i.test(script),
      tmpAssignInEDV:
        /(tmp_table\w*)\s*=\s*f["']\{PRM_ESQUEMA_TABLA_ESCRITURA\}(.+?_tmp["'])/i.test(script),
      hasCleanPaths: /\w+\.cleanPaths\(/.test(script),
      hasDropTable: /DROP\s+TABLE\s+IF\s+EXISTS/i.test(script),
      hasWriteDelta: /write_delta\(/.test(script),
      repartitionNearWrite:
        /repartition\([^)]*(CODMES|codmes|CONS_PARTITION_DELTA_NAME)[^)]*\)[\s\S]{0,500}write_delta\(/.test(
          script,
        ),
      partitionByWriter:
        /\.partitionBy\(\s*(CODMES|codmes|CONS_PARTITION_DELTA_NAME)\s*\)/.test(script),
      confOverwriteDynamic:
        /spark\.conf\.set\(\s*['"]spark\.sql\.sources\.partitionOverwriteMode['"]/.test(script),
      writerOverwriteDynamic: /option\(\s*['"]partitionOverwritemode['"]/.test(script),
      hasAQE: /spark\.sql\.adaptive\.enabled/.test(script),
      hasBroadcast: /spark\.sql\.autoBroadcastJoinThreshold/.test(script),
      hasOptimizeWrite: /spark\.databricks\.delta\.optimizeWrite\.enabled/.test(script),
      hasAutoCompact: /spark\.databricks\.delta\.autoCompact\.enabled/.test(script),
      hasShufflePartitions: /spark\.sql\.shuffle\.partitions/.test(script),
    };
  }

  extractParameters(script) {
    const getWidgetDefault = (name) => {
      const re = new RegExp(
        `dbutils\\.widgets\\.text\\(name=\\\"${name}\\\",\\s*defaultValue=\\'([\\\']*)`
      );
      const m = script.match(re);
      return m ? m[1] : null;
    };

    const ddvCatalog = getWidgetDefault('PRM_CATALOG_NAME');
    const ddvSchema =
      getWidgetDefault('PRM_ESQUEMA_TABLA_DDV') || getWidgetDefault('PRM_ESQUEMA_TABLA_X');
    const edvCatalog = getWidgetDefault('PRM_CATALOG_NAME_EDV');
    const edvSchema = getWidgetDefault('PRM_ESQUEMA_TABLA_EDV');
    const tableName = getWidgetDefault('PRM_TABLE_NAME');
    const tablaSegunda = getWidgetDefault('PRM_TABLA_SEGUNDATRANSPUESTA');
    const tablaSegundaTmp = getWidgetDefault('PRM_TABLA_SEGUNDATRANSPUESTA_TMP');
    const container = (script.match(/CONS_CONTAINER_NAME\s*=\s*['"]([^'"]+)['"]/i) || [])[1] || null;

    // Deducción de familia
    let familia = 'desconocida';
    if (/MATRIZTRANSACCIONPOSMACROGIRO/i.test(script)) familia = 'macrogiro';
    else if (/MATRIZTRANSACCIONAGENTE/i.test(script)) familia = 'agente';
    else if (/MATRIZTRANSACCIONCAJERO/i.test(script)) familia = 'cajero';

    return {
      ddv: { catalog: ddvCatalog, schema: ddvSchema },
      edv: { catalog: edvCatalog, schema: edvSchema },
      destino: {
        table_name: tableName,
        tabla_segunda: tablaSegunda,
        tabla_segunda_tmp: tablaSegundaTmp,
        familia,
      },
      storage: { container },
    };
  }

  // ===== Validaciones =====
  validateEDVWidgets(script) {
    if (this.context.hasEdvWidgets) {
      this.checks.push({
        name: 'Widgets EDV',
        passed: true,
        message: '✅ PRM_CATALOG_NAME_EDV y PRM_ESQUEMA_TABLA_EDV presentes',
      });
    } else {
      this.checks.push({
        name: 'Widgets EDV',
        passed: false,
        message: '❌ Faltan widgets EDV (PRM_CATALOG_NAME_EDV, PRM_ESQUEMA_TABLA_EDV)',
      });
      this.errors.push('Faltan widgets PRM_CATALOG_NAME_EDV / PRM_ESQUEMA_TABLA_EDV');
    }
  }

  validateEDVVariables(script) {
    if (this.context.hasEdvVars) {
      this.checks.push({ name: 'Variables EDV', passed: true, message: '✅ Variables EDV OK' });
    } else {
      this.checks.push({
        name: 'Variables EDV',
        passed: false,
        message: '❌ Variables EDV no están correctamente asignadas',
      });
      this.errors.push('Variables EDV no definidas con dbutils.widgets.get');
    }
  }

  validateEDVSchemaWriting(script) {
    if (this.context.hasWritingSchema) {
      this.checks.push({ name: 'Esquema escritura EDV', passed: true, message: '✅ PRM_ESQUEMA_TABLA_ESCRITURA definido' });
    } else {
      this.checks.push({ name: 'Esquema escritura EDV', passed: false, message: '❌ Falta PRM_ESQUEMA_TABLA_ESCRITURA' });
      this.errors.push('PRM_ESQUEMA_TABLA_ESCRITURA no definido');
    }
  }

  validateDestinationName(script) {
    if (this.context.destUsesEDV) {
      this.checks.push({ name: 'Destino EDV', passed: true, message: '✅ VAL_DESTINO_NAME usa PRM_ESQUEMA_TABLA_ESCRITURA' });
    } else {
      this.checks.push({ name: 'Destino EDV', passed: false, message: '❌ VAL_DESTINO_NAME no apunta a esquema EDV' });
      this.errors.push('VAL_DESTINO_NAME debe usar PRM_ESQUEMA_TABLA_ESCRITURA');
    }
  }

  validateManagedTables(script) {
    if (this.context.hasSaveWithPath) {
      this.checks.push({ name: 'Tablas managed', passed: false, message: '❌ saveAsTable con path=' });
      this.errors.push('Convertir saveAsTable(path=...) a managed (sin path)');
    } else {
      this.checks.push({ name: 'Tablas managed', passed: true, message: '✅ Sin saveAsTable(path=...)' });
    }

    if (this.context.hasLoadTempFromPath) {
      this.checks.push({ name: 'Lecturas temporales', passed: false, message: '❌ Lecturas desde ruta (/temp/ o abfss://). Use spark.table(...)' });
      this.errors.push('Reemplazar spark.read.load(<ruta temp>) por spark.table(<tmp_table>)');
    } else {
      this.checks.push({ name: 'Lecturas temporales', passed: true, message: '✅ Sin lecturas por ruta' });
    }

    if (this.context.usesSparkTable) {
      this.checks.push({ name: 'spark.table()', passed: true, message: '✅ Se usa spark.table()' });
    } else {
      this.warnings.push('No se detectó spark.table(). Verificar lecturas de temporales.');
    }
  }

  validateSparkOptimizations(script) {
    const missing = [];
    if (!this.context.hasAQE) missing.push('AQE');
    if (!this.context.hasBroadcast) missing.push('Broadcast');
    if (!this.context.hasShufflePartitions) missing.push('Shuffle Partitions');
    if (!this.context.confOverwriteDynamic) missing.push('Overwrite dinámico (conf)');
    if (!this.context.hasOptimizeWrite) missing.push('Delta optimizeWrite');
    if (!this.context.hasAutoCompact) missing.push('Delta autoCompact');

    if (missing.length === 0) {
      this.checks.push({ name: 'Optimización Spark', passed: true, message: '✅ Configuración completa' });
    } else {
      this.checks.push({ name: 'Optimización Spark', passed: false, message: `❌ Faltan: ${missing.join(', ')}` });
    }
  }

  validateRepartitionBeforeWrite(script) {
    if (!this.context.hasWriteDelta) {
      this.warnings.push('No se encontró write_delta');
      return;
    }
    if (this.context.repartitionNearWrite) {
      this.checks.push({ name: 'Repartition pre-escritura', passed: true, message: '✅ Repartition antes de write_delta' });
    } else {
      this.checks.push({ name: 'Repartition pre-escritura', passed: false, message: '❌ Falta repartition antes de write_delta' });
      this.errors.push('Agregar input_df = input_df.repartition(<col partición>) antes de write_delta');
    }
  }

  validatePartitionColumn(script) {
    const hasCODMES = /CODMES/.test(script);
    const hasCodmes = /codmes/.test(script);
    if (hasCODMES && !hasCodmes) this.checks.push({ name: 'Partición', passed: true, message: '✅ CODMES (Macrogiro)' });
    else if (!hasCODMES && hasCodmes) this.checks.push({ name: 'Partición', passed: true, message: '✅ codmes (Agente/Cajero)' });
    else if (hasCODMES && hasCodmes) this.checks.push({ name: 'Partición', passed: true, message: '⚠️ CODMES y codmes detectados' });
    else this.warnings.push('No se detectó columna de partición CODMES/codmes');
  }

  validateNoHardcodedPaths(script) {
    const hasWriterPath = /.saveAsTable\([^)]*path\s*=\s*['"][^'"]+['"]/i.test(script);
    const hasLoadPath = /spark\.read\.format\([^)]*\)\.load\([^)]*(abfss:\/\/|\/mnt\/)\s*[^)]*\)/i.test(script);
    if (!hasWriterPath && !hasLoadPath) this.checks.push({ name: 'Rutas hardcodeadas', passed: true, message: '✅ Sin rutas hardcodeadas (writer/load)' });
    else {
      this.checks.push({ name: 'Rutas hardcodeadas', passed: false, message: '❌ Rutas hardcodeadas en writer/load' });
      this.errors.push('Eliminar path= en saveAsTable y .load(abfss:/// /mnt/...)');
    }
  }

  validateDDVSchemaViews(script) {
    const m = script.match(/dbutils\.widgets\.text\(name=\"PRM_ESQUEMA_TABLA_DDV\",\s*defaultValue=\'([^\']+)\'\)/);
    if (m) {
      const schema = m[1];
      if (schema.endsWith('_v')) this.checks.push({ name: 'Schema DDV (_v)', passed: true, message: `✅ ${schema}` });
      else {
        this.checks.push({ name: 'Schema DDV (_v)', passed: false, message: `❌ Debe terminar en _v: ${schema}` });
        this.errors.push(`Schema DDV debe terminar en _v. Actual: ${schema}`);
      }
    } else this.warnings.push('No se encontró PRM_ESQUEMA_TABLA_DDV');
  }

  validateDropTableCleanup(script) {
    const hasDrop = this.context.hasDropTable;
    const hasClean = this.context.hasCleanPaths;
    if (hasDrop && !hasClean) this.checks.push({ name: 'Limpieza temporales', passed: true, message: '✅ DROP TABLE IF EXISTS' });
    else if (!hasDrop && !hasClean) {
      if (/(tmp_table\w*\s*=)|(_tmp["'])/i.test(script)) this.warnings.push('No se detectó limpieza de temporales');
    } else if (hasClean) {
      this.checks.push({ name: 'Limpieza temporales', passed: false, message: '❌ Usa cleanPaths' });
      this.errors.push('Reemplazar cleanPaths por DROP TABLE IF EXISTS');
    }
  }

  validateWriterOptions(script) {
    const hasDyn = this.context.writerOverwriteDynamic || this.context.confOverwriteDynamic;
    const hasPart = this.context.partitionByWriter;
    this.checks.push({ name: 'Overwrite dinámico', passed: !!hasDyn, message: hasDyn ? '✅ Activado (writer/conf)' : '❌ Falta overwrite dinámico' });
    if (!hasDyn) this.errors.push('Configurar partitionOverwriteMode=dynamic en writer o spark.conf');
    this.checks.push({ name: 'Writer particionado', passed: !!hasPart, message: hasPart ? '✅ Usa partitionBy' : '❌ Sin partitionBy' });
    if (!hasPart) this.errors.push('Agregar .partitionBy(<col partición>) en write');
  }

  validateTmpPlacementInEDV(script) {
    if (this.context.tmpAssignInDDV && !this.context.tmpAssignInEDV) {
      this.checks.push({ name: 'Temporales en EDV', passed: false, message: '❌ tmp_table en PRM_ESQUEMA_TABLA (DDV)' });
      this.errors.push('Reubicar tmp_table a PRM_ESQUEMA_TABLA_ESCRITURA');
    } else this.checks.push({ name: 'Temporales en EDV', passed: true, message: '✅ tmp_table en PRM_ESQUEMA_TABLA_ESCRITURA' });
  }

  validateNoResidualDDVDestination(script) {
    if (this.context.destUsesDDV) {
      this.checks.push({ name: 'Destino no-DDV', passed: false, message: '❌ Hay VAL_DESTINO_NAME con PRM_ESQUEMA_TABLA (DDV)' });
      this.errors.push('Actualizar VAL_DESTINO_NAME a PRM_ESQUEMA_TABLA_ESCRITURA');
    } else this.checks.push({ name: 'Destino no-DDV', passed: true, message: '✅ Sin destinos residuales en DDV' });
  }

  // ===== Utilidades =====
  calculateScore() {
    if (this.checks.length === 0) return 0;
    const passed = this.checks.filter((c) => c.passed).length;
    return Math.round((passed / this.checks.length) * 100);
  }

  generateReport() {
    return {
      summary: {
        total: this.checks.length,
        passed: this.checks.filter((c) => c.passed).length,
        failed: this.checks.filter((c) => !c.passed).length,
        errors: this.errors.length,
        warnings: this.warnings.length,
        score: this.calculateScore(),
      },
      checks: this.checks,
      errors: this.errors,
      warnings: this.warnings,
      compliance: this.calculateScore() >= 80 ? 'PASS' : 'FAIL',
      parameters: this.parameters,
    };
  }
}

// Export
if (typeof module !== 'undefined' && module.exports) {
  module.exports = EDVValidator;
}


// Browser globals aliases for backward-compat
if (typeof window !== 'undefined') {
  window.EDVValidator = EDVValidator;
  if (!window.EDVValidatorRiguroso) window.EDVValidatorRiguroso = EDVValidator;
}