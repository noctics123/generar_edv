/**
 * Verification Client - Cliente JavaScript para Script Verifier
 * ==============================================================
 *
 * Cliente que usa ScriptVerifier.js local (sin servidor backend).
 * 100% JavaScript - Compatible con GitHub Pages.
 *
 * Autor: Claude Code
 * Version: 2.0 (Pure JS - No Server)
 */

class VerificationClient {
    constructor() {
        this.verifier = new ScriptVerifier();
        this.lastVerificationReport = null;
    }

    /**
     * Health check (siempre retorna true ya que es local)
     * @returns {Promise<boolean>}
     */
    async checkHealth() {
        // Modo local - siempre disponible
        return Promise.resolve(true);
    }

    /**
     * Verifica dos scripts
     * @param {string} script1 - Contenido del primer script
     * @param {string} script2 - Contenido del segundo script
     * @param {Object} options - Opciones de verificacion
     * @returns {Promise<Object>} - Reporte de verificacion
     */
    async verifyScripts(script1, script2, options = {}) {
        const {
            script1_name = 'script1.py',
            script2_name = 'script2.py',
            is_ddv_edv = false
        } = options;

        try {
            console.log('[VERIFY] Iniciando verificacion local (sin servidor)...');
            console.log(`  - Script 1: ${script1_name}`);
            console.log(`  - Script 2: ${script2_name}`);
            console.log(`  - Modo DDV/EDV: ${is_ddv_edv}`);

            // Ejecutar verificacion local
            const report = this.verifier.verify(script1, script2, {
                script1_name,
                script2_name,
                is_ddv_edv
            });

            this.lastVerificationReport = report;

            console.log('[OK] Verificacion completada');
            console.log(`  - Score: ${report.similarity_score}%`);
            console.log(`  - Equivalentes: ${report.is_equivalent}`);
            console.log(`  - Total diferencias: ${report.total_differences}`);

            return report;

        } catch (error) {
            console.error('[ERROR] Error en verificacion:', error);
            throw error;
        }
    }

    /**
     * Verifica conversion DDV->EDV
     * @param {string} ddvScript - Script DDV original
     * @param {string} edvScript - Script EDV convertido
     * @returns {Promise<Object>} - Reporte de verificacion
     */
    async verifyDDVtoEDV(ddvScript, edvScript) {
        console.log('[VERIFY] Modo DDV vs EDV activado');

        // Detectar nombre del script
        let scriptName = 'SCRIPT';
        if (ddvScript.includes('MATRIZTRANSACCIONAGENTE')) {
            scriptName = 'MATRIZTRANSACCIONAGENTE';
        } else if (ddvScript.includes('MATRIZTRANSACCIONCAJERO')) {
            scriptName = 'MATRIZTRANSACCIONCAJERO';
        } else if (ddvScript.includes('MATRIZTRANSACCIONPOSMACROGIRO')) {
            scriptName = 'MATRIZTRANSACCIONPOSMACROGIRO';
        }

        return this.verifyScripts(ddvScript, edvScript, {
            script1_name: `${scriptName}_DDV.py`,
            script2_name: `${scriptName}_EDV.py`,
            is_ddv_edv: true
        });
    }

    /**
     * Exporta reporte a JSON
     * @returns {string} - JSON string del reporte
     */
    exportToJSON() {
        if (!this.lastVerificationReport) {
            throw new Error('No hay reporte disponible para exportar');
        }

        return JSON.stringify(this.lastVerificationReport, null, 2);
    }

    /**
     * Obtiene ultimo reporte
     * @returns {Object|null}
     */
    getLastReport() {
        return this.lastVerificationReport;
    }
}


/**
 * Verification UI Renderer
 * =========================
 *
 * Renderiza reportes de verificacion en HTML
 */
class VerificationUI {
    constructor() {
        this.currentReport = null;
    }

    /**
     * Renderiza un reporte completo
     * @param {Object} report - Reporte de verificacion
     */
    render(report) {
        this.currentReport = report;

        const container = document.getElementById('verification-report');
        if (!container) {
            console.error('[ERROR] Contenedor de reporte no encontrado');
            return;
        }

        // Header del reporte
        let html = this.renderHeader(report);

        // Estadisticas
        html += this.renderStats(report);

        // Diferencias
        html += this.renderDifferences(report);

        // Acciones
        html += this.renderActions();

        container.innerHTML = html;

        // Agregar event listeners
        this.attachEventListeners();
    }

    /**
     * Renderiza header
     */
    renderHeader(report) {
        const statusClass = report.is_equivalent ? 'status-success' : 'status-error';
        const statusIcon = report.is_equivalent ? 'OK' : 'ERROR';
        const statusText = report.is_equivalent ? 'Scripts Equivalentes' : 'Scripts NO Equivalentes';

        return `
            <div class="report-header ${statusClass}">
                <h2>[${statusIcon}] ${statusText}</h2>
                <div class="report-meta">
                    <span><strong>Script 1:</strong> ${report.script1_name}</span>
                    <span><strong>Script 2:</strong> ${report.script2_name}</span>
                    ${report.is_ddv_edv ? '<span class="badge-ddv-edv">DDV vs EDV</span>' : ''}
                </div>
            </div>
        `;
    }

    /**
     * Renderiza estadisticas
     */
    renderStats(report) {
        const scoreClass = report.similarity_score >= 95 ? 'score-high' :
                          report.similarity_score >= 80 ? 'score-medium' : 'score-low';

        return `
            <div class="report-stats">
                <div class="stat-card score-card ${scoreClass}">
                    <div class="stat-value">${report.similarity_score}%</div>
                    <div class="stat-label">Similitud</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${report.total_differences}</div>
                    <div class="stat-label">Total Diferencias</div>
                </div>
                <div class="stat-card stat-critical">
                    <div class="stat-value">${report.critical_count}</div>
                    <div class="stat-label">Criticas</div>
                </div>
                <div class="stat-card stat-high">
                    <div class="stat-value">${report.high_count}</div>
                    <div class="stat-label">Altas</div>
                </div>
                <div class="stat-card stat-medium">
                    <div class="stat-value">${report.medium_count}</div>
                    <div class="stat-label">Medias</div>
                </div>
                <div class="stat-card stat-low">
                    <div class="stat-value">${report.low_count + report.info_count}</div>
                    <div class="stat-label">Bajas/Info</div>
                </div>
            </div>
        `;
    }

    /**
     * Renderiza diferencias
     */
    renderDifferences(report) {
        if (report.differences.length === 0) {
            return `
                <div class="no-differences">
                    <h3>[OK] No se encontraron diferencias</h3>
                    <p>Los scripts son identicos o equivalentes.</p>
                </div>
            `;
        }

        let html = '<div class="differences-list"><h3>Diferencias Encontradas</h3>';

        // Agrupar por severidad
        const grouped = this.groupBySeverity(report.differences);

        ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO'].forEach(severity => {
            if (grouped[severity] && grouped[severity].length > 0) {
                html += this.renderSeverityGroup(severity, grouped[severity]);
            }
        });

        html += '</div>';
        return html;
    }

    /**
     * Agrupa diferencias por severidad
     */
    groupBySeverity(differences) {
        const grouped = {
            CRITICAL: [],
            HIGH: [],
            MEDIUM: [],
            LOW: [],
            INFO: []
        };

        differences.forEach(diff => {
            if (grouped[diff.severity]) {
                grouped[diff.severity].push(diff);
            }
        });

        return grouped;
    }

    /**
     * Renderiza grupo de severidad
     */
    renderSeverityGroup(severity, differences) {
        const severityLabel = {
            CRITICAL: 'Criticas',
            HIGH: 'Altas',
            MEDIUM: 'Medias',
            LOW: 'Bajas',
            INFO: 'Informativas'
        }[severity];

        let html = `
            <div class="severity-group severity-${severity.toLowerCase()}">
                <h4>[${severity}] ${severityLabel} (${differences.length})</h4>
                <div class="differences-items">
        `;

        differences.forEach((diff, index) => {
            html += this.renderDifference(diff, index);
        });

        html += '</div></div>';
        return html;
    }

    /**
     * Renderiza una diferencia individual
     */
    renderDifference(diff, index) {
        return `
            <div class="difference-item">
                <div class="diff-header">
                    <strong>${diff.category}</strong>
                    <span class="diff-severity">${diff.severity}</span>
                </div>
                <div class="diff-description">${diff.description}</div>
                <div class="diff-details">
                    <pre>${this.escapeHtml(diff.details)}</pre>
                </div>
                <div class="diff-suggestion">
                    <strong>Sugerencia:</strong> ${diff.suggestion}
                </div>
            </div>
        `;
    }

    /**
     * Renderiza acciones
     */
    renderActions() {
        return `
            <div class="report-actions">
                <button id="export-json-btn" class="btn btn-secondary">
                    Exportar JSON
                </button>
                <button id="export-html-btn" class="btn btn-secondary">
                    Exportar HTML
                </button>
                <button id="new-verification-btn" class="btn btn-primary">
                    Nueva Verificacion
                </button>
            </div>
        `;
    }

    /**
     * Agrega event listeners
     */
    attachEventListeners() {
        // Export JSON
        const exportJsonBtn = document.getElementById('export-json-btn');
        if (exportJsonBtn) {
            exportJsonBtn.addEventListener('click', () => this.exportJSON());
        }

        // Export HTML
        const exportHtmlBtn = document.getElementById('export-html-btn');
        if (exportHtmlBtn) {
            exportHtmlBtn.addEventListener('click', () => this.exportHTML());
        }

        // Nueva verificacion
        const newVerifyBtn = document.getElementById('new-verification-btn');
        if (newVerifyBtn) {
            newVerifyBtn.addEventListener('click', () => {
                document.getElementById('verification-results').style.display = 'none';
            });
        }
    }

    /**
     * Exporta a JSON
     */
    exportJSON() {
        if (!this.currentReport) return;

        const json = JSON.stringify(this.currentReport, null, 2);
        const blob = new Blob([json], { type: 'application/json' });
        const url = URL.createObjectURL(blob);

        const a = document.createElement('a');
        a.href = url;
        a.download = `verification_report_${Date.now()}.json`;
        a.click();

        URL.revokeObjectURL(url);
        console.log('[OK] Reporte JSON exportado');
    }

    /**
     * Exporta a HTML
     */
    exportHTML() {
        if (!this.currentReport) return;

        const container = document.getElementById('verification-report');
        const htmlContent = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Verification Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .report-header { padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .status-success { background: #d4edda; color: #155724; }
        .status-error { background: #f8d7da; color: #721c24; }
        .report-stats { display: flex; gap: 10px; margin-bottom: 20px; }
        .stat-card { padding: 15px; background: #f8f9fa; border-radius: 8px; flex: 1; }
        .stat-value { font-size: 24px; font-weight: bold; }
        .difference-item { padding: 15px; margin: 10px 0; border-left: 4px solid #ccc; background: #f8f9fa; }
        pre { background: #fff; padding: 10px; overflow-x: auto; }
    </style>
</head>
<body>
    ${container.innerHTML}
</body>
</html>
        `;

        const blob = new Blob([htmlContent], { type: 'text/html' });
        const url = URL.createObjectURL(blob);

        const a = document.createElement('a');
        a.href = url;
        a.download = `verification_report_${Date.now()}.html`;
        a.click();

        URL.revokeObjectURL(url);
        console.log('[OK] Reporte HTML exportado');
    }

    /**
     * Escapa HTML
     */
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Crear instancias globales
const verificationClient = new VerificationClient();
const verificationUI = new VerificationUI();
