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

    extractParameters(script) {
        const getWidgetDefault = (name) => {
            const re = new RegExp(
                `dbutils\\.widgets\\.text(name="${name}",\\s*defaultValue=\'([^\\]*)\'`
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

    validate(script) {
        this.checks = [];
        this.errors = [];
        this.warnings = [];
        this.score = 0;
        this.parameters = this.extractParameters(script);

        // NIVEL 0: Ambiente EDV (CRÍTICO)
        this.validateContainerName(script);
        this.validateStorageAccount(script);
        this.validateCatalogName(script);

        // NIVEL 1: Parámetros EDV (90 puntos - CRÍTICO)
        this.validateWidgetCatalogEDV(script);
        this.validateWidgetSchemaEDV(script);
        this.validateGetCatalogEDV(script);
        this.validateGetSchemaEDV(script);
        this.validateEsquemaEscritura(script);
        this.validateValDestinoEDV(script);

        // NIVEL 2: Schema DDV Views (10 puntos - CRÍTICO)
        this.validateDDVSchemaViews(script);

        // NIVEL 3: Tablas Managed
        this.validateNoPathInSaveAsTable(script);
        this.validateTmpInEDVSchema(script);
        this.validateSparkTable(script);
        this.validateNoLoadFromPath(script);

        // NIVEL 4: Optimizaciones Spark (Bonus +10)
        const sparkOpts = this.validateSparkOptimizations(script);
        if (sparkOpts === 8) this.score += 10;

        // NIVEL 5: Limpieza (Bonus +5)
        if (this.validateCleanup(script)) this.score += 5;

        return {
            passed: this.score >= 90,
            checks: this.checks,
            errors: this.errors,
            warnings: this.warnings,
            score: Math.min(this.score, 100),
            level: this.getComplianceLevel(),
            parameters: this.parameters
        };
    }

    // ... (resto de los métodos de validación) ...
}

// Exportar
if (typeof module !== 'undefined' && module.exports) {
    module.exports = EDVValidatorRiguroso;
}