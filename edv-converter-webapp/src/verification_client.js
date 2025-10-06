/**
 * Verification Client - Cliente JavaScript para Script Verifier
 * ==============================================================
 *
 * Cliente para comunicarse con el servidor de verificación de scripts.
 * Se integra con la app web EDV Converter.
 *
 * Autor: Claude Code
 * Versión: 1.0
 */

class VerificationClient {
    constructor(serverUrl = 'http://localhost:5000') {
        this.serverUrl = serverUrl;
        this.lastVerificationReport = null;
    }

    /**
     * Verifica conexión con el servidor
     * @returns {Promise<boolean>}
     */
    async checkHealth() {
        try {
            const response = await fetch(`${this.serverUrl}/health`);
            const data = await response.json();
            return data.status === 'ok';
        } catch (error) {
            console.error('❌ Error conectando con servidor de verificación:', error);
            return false;
        }
    }

    /**
     * Verifica dos scripts
     * @param {string} script1 - Contenido del primer script
     * @param {string} script2 - Contenido del segundo script
     * @param {Object} options - Opciones de verificación
     * @returns {Promise<Object>} - Reporte de verificación
     */
    async verifyScripts(script1, script2, options = {}) {
        const {
            script1_name = 'script1.py',
            script2_name = 'script2.py',
            is_ddv_edv = false
        } = options;

        try {
            const response = await fetch(`${this.serverUrl}/verify`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    script1,
                    script2,
                    script1_name,
                    script2_name,
                    is_ddv_edv
                })
            });

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Error desconocido en verificación');
            }

            this.lastVerificationReport = result.report;
            return result.report;

        } catch (error) {
            console.error('❌ Error en verificación:', error);
            throw error;
        }
    }

    /**
     * Verifica conversión DDV→EDV
     * @param {string} ddvScript - Script DDV original
     * @param {string} edvScript - Script EDV convertido
     * @returns {Promise<Object>} - Reporte de verificación
     */
    async verifyDDVtoEDV(ddvScript, edvScript) {
        try {
            const response = await fetch(`${this.serverUrl}/verify-ddv-edv`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    ddv_script: ddvScript,
                    edv_script: edvScript
                })
            });

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Error desconocido en verificación DDV→EDV');
            }

            this.lastVerificationReport = result.report;
            return result.report;

        } catch (error) {
            console.error('❌ Error en verificación DDV→EDV:', error);
            throw error;
        }
    }

    /**
     * Obtiene el último reporte de verificación
     * @returns {Object|null}
     */
    getLastReport() {
        return this.lastVerificationReport;
    }
}


/**
 * Verification UI - Componente de UI para mostrar resultados
 * ===========================================================
 */
class VerificationUI {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.report = null;
    }

    /**
     * Renderiza el reporte de verificación
     * @param {Object} report - Reporte de verificación
     */
    render(report) {
        this.report = report;

        if (!this.container) {
            console.error('❌ Container no encontrado');
            return;
        }

        // Generar HTML
        const html = this.generateHTML(report);
        this.container.innerHTML = html;

        // Attach event listeners
        this.attachEventListeners();
    }

    /**
     * Genera HTML del reporte
     * @param {Object} report
     * @returns {string}
     */
    generateHTML(report) {
        const statusIcon = report.is_equivalent ? '✅' : '⚠️';
        const statusText = report.is_equivalent ? 'EQUIVALENTES' : 'DIFERENCIAS ENCONTRADAS';
        const statusColor = report.is_equivalent ? '#10b981' : '#f59e0b';

        // Score color
        let scoreColor = '#10b981'; // verde
        if (report.similarity_score < 80) scoreColor = '#f59e0b'; // amarillo
        if (report.similarity_score < 60) scoreColor = '#ef4444'; // rojo

        return `
            <div class="verification-report">
                <!-- Header -->
                <div class="verification-header" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 2rem; border-radius: 12px; margin-bottom: 2rem;">
                    <h3 style="margin: 0 0 0.5rem 0; font-size: 1.5rem;">
                        ${statusIcon} Reporte de Verificación
                    </h3>
                    <p style="margin: 0; opacity: 0.9;">
                        ${report.script1_name} vs ${report.script2_name}
                    </p>
                </div>

                <!-- Summary Cards -->
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem;">
                    <div class="verification-card" style="background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                        <div style="font-size: 0.875rem; color: #6b7280; margin-bottom: 0.5rem;">Estado</div>
                        <div style="font-size: 1.25rem; font-weight: bold; color: ${statusColor};">
                            ${statusIcon} ${statusText}
                        </div>
                    </div>

                    <div class="verification-card" style="background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                        <div style="font-size: 0.875rem; color: #6b7280; margin-bottom: 0.5rem;">Score de Similitud</div>
                        <div style="font-size: 1.25rem; font-weight: bold; color: ${scoreColor};">
                            ${report.similarity_score}%
                        </div>
                    </div>

                    <div class="verification-card" style="background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                        <div style="font-size: 0.875rem; color: #6b7280; margin-bottom: 0.5rem;">Total Diferencias</div>
                        <div style="font-size: 1.25rem; font-weight: bold; color: #3b82f6;">
                            ${report.total_differences}
                        </div>
                    </div>

                    <div class="verification-card" style="background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                        <div style="font-size: 0.875rem; color: #6b7280; margin-bottom: 0.5rem;">Críticas / Altas</div>
                        <div style="font-size: 1.25rem; font-weight: bold; color: ${report.critical_count > 0 ? '#ef4444' : '#10b981'};">
                            ${report.critical_count} / ${report.high_count}
                        </div>
                    </div>
                </div>

                <!-- Filters -->
                ${report.differences.length > 0 ? `
                <div class="verification-filters" style="margin-bottom: 1.5rem;">
                    <div style="display: flex; gap: 0.5rem; flex-wrap: wrap;">
                        <button class="filter-btn active" data-level="all">
                            Todas (${report.differences.length})
                        </button>
                        <button class="filter-btn" data-level="CRÍTICO">
                            🔴 Críticas (${report.critical_count})
                        </button>
                        <button class="filter-btn" data-level="ALTO">
                            🟠 Altas (${report.high_count})
                        </button>
                        <button class="filter-btn" data-level="MEDIO">
                            🟡 Medias (${this.countByLevel(report.differences, 'MEDIO')})
                        </button>
                        <button class="filter-btn" data-level="BAJO">
                            🟢 Bajas (${this.countByLevel(report.differences, 'BAJO')})
                        </button>
                        <button class="filter-btn" data-level="INFO">
                            ℹ️ Info (${this.countByLevel(report.differences, 'INFO')})
                        </button>
                    </div>
                </div>
                ` : ''}

                <!-- Differences List -->
                ${report.differences.length > 0 ? this.generateDifferencesList(report.differences) : `
                    <div style="text-align: center; padding: 3rem; background: #f0fdf4; border-radius: 8px; border: 2px solid #10b981;">
                        <div style="font-size: 3rem; margin-bottom: 1rem;">✅</div>
                        <h3 style="color: #10b981; margin: 0 0 0.5rem 0;">¡Scripts Equivalentes!</h3>
                        <p style="color: #6b7280; margin: 0;">No se encontraron diferencias significativas entre los scripts.</p>
                    </div>
                `}

                <!-- Metadata -->
                <div style="margin-top: 2rem; padding: 1.5rem; background: #f8fafc; border-radius: 8px; border: 1px solid #e2e8f0;">
                    <h4 style="margin: 0 0 1rem 0; color: #334155;">📊 Metadata</h4>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; font-size: 0.875rem;">
                        <div>
                            <strong>Script 1:</strong><br>
                            • Funciones: ${report.metadata.script1_functions || 'N/A'}<br>
                            • Operaciones: ${report.metadata.script1_operations || 'N/A'}<br>
                            • Tablas fuente: ${this.formatList(report.metadata.script1_source_tables)}
                        </div>
                        <div>
                            <strong>Script 2:</strong><br>
                            • Funciones: ${report.metadata.script2_functions || 'N/A'}<br>
                            • Operaciones: ${report.metadata.script2_operations || 'N/A'}<br>
                            • Tablas fuente: ${this.formatList(report.metadata.script2_source_tables)}
                        </div>
                    </div>
                </div>

                <!-- Export Buttons -->
                <div style="margin-top: 2rem; text-align: center;">
                    <button class="btn btn-primary" onclick="verificationUI.exportJSON()">
                        💾 Exportar JSON
                    </button>
                    <button class="btn btn-secondary" onclick="verificationUI.exportHTML()">
                        📄 Exportar HTML
                    </button>
                </div>
            </div>
        `;
    }

    /**
     * Genera lista de diferencias
     * @param {Array} differences
     * @returns {string}
     */
    generateDifferencesList(differences) {
        const items = differences.map((diff, index) => {
            const levelIcons = {
                'CRÍTICO': '🔴',
                'ALTO': '🟠',
                'MEDIO': '🟡',
                'BAJO': '🟢',
                'INFO': 'ℹ️'
            };

            const levelColors = {
                'CRÍTICO': '#ef4444',
                'ALTO': '#f59e0b',
                'MEDIO': '#eab308',
                'BAJO': '#10b981',
                'INFO': '#3b82f6'
            };

            const icon = levelIcons[diff.level] || '•';
            const color = levelColors[diff.level] || '#6b7280';

            return `
                <div class="difference-item" data-level="${diff.level}" style="background: white; border-left: 4px solid ${color}; padding: 1.5rem; margin-bottom: 1rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                    <!-- Header -->
                    <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 1rem;">
                        <div style="flex: 1;">
                            <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
                                <span style="font-size: 1.25rem;">${icon}</span>
                                <span style="font-weight: bold; color: ${color};">${diff.level}</span>
                                <span style="color: #6b7280;">•</span>
                                <span style="color: #6b7280; font-size: 0.875rem;">${diff.category}</span>
                            </div>
                            <h4 style="margin: 0; color: #1f2937;">${this.escapeHtml(diff.description)}</h4>
                        </div>
                        <button class="btn btn-sm" onclick="verificationUI.toggleDetails(${index})" style="white-space: nowrap;">
                            <span id="toggle-icon-${index}">▼</span> Detalles
                        </button>
                    </div>

                    <!-- Quick Summary -->
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem;">
                        <div style="padding: 0.75rem; background: #fef2f2; border-radius: 6px;">
                            <div style="font-size: 0.75rem; color: #991b1b; font-weight: bold; margin-bottom: 0.25rem;">SCRIPT 1</div>
                            <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.875rem; color: #374151; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                                ${this.escapeHtml(String(diff.script1_value).substring(0, 100))}${String(diff.script1_value).length > 100 ? '...' : ''}
                            </div>
                        </div>
                        <div style="padding: 0.75rem; background: #f0fdf4; border-radius: 6px;">
                            <div style="font-size: 0.75rem; color: #166534; font-weight: bold; margin-bottom: 0.25rem;">SCRIPT 2</div>
                            <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.875rem; color: #374151; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                                ${this.escapeHtml(String(diff.script2_value).substring(0, 100))}${String(diff.script2_value).length > 100 ? '...' : ''}
                            </div>
                        </div>
                    </div>

                    <!-- Details (hidden by default) -->
                    <div id="details-${index}" style="display: none; padding-top: 1rem; border-top: 1px solid #e5e7eb;">
                        ${diff.location ? `
                        <div style="margin-bottom: 1rem;">
                            <strong style="color: #4b5563;">📍 Ubicación:</strong>
                            <div style="margin-top: 0.25rem; color: #6b7280;">${this.escapeHtml(diff.location)}</div>
                        </div>
                        ` : ''}

                        ${diff.impact ? `
                        <div style="margin-bottom: 1rem; padding: 0.75rem; background: #fef3c7; border-radius: 6px;">
                            <strong style="color: #92400e;">⚠️ Impacto:</strong>
                            <div style="margin-top: 0.25rem; color: #78350f;">${this.escapeHtml(diff.impact)}</div>
                        </div>
                        ` : ''}

                        ${diff.recommendation ? `
                        <div style="padding: 0.75rem; background: #dbeafe; border-radius: 6px;">
                            <strong style="color: #1e40af;">💡 Recomendación:</strong>
                            <div style="margin-top: 0.25rem; color: #1e3a8a;">${this.escapeHtml(diff.recommendation)}</div>
                        </div>
                        ` : ''}

                        <!-- Full Values -->
                        <div style="margin-top: 1rem; display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                            <div>
                                <strong style="color: #4b5563;">Script 1 (completo):</strong>
                                <pre style="margin-top: 0.5rem; padding: 0.75rem; background: #1f2937; color: #f9fafb; border-radius: 6px; overflow-x: auto; font-size: 0.75rem;">${this.escapeHtml(String(diff.script1_value))}</pre>
                            </div>
                            <div>
                                <strong style="color: #4b5563;">Script 2 (completo):</strong>
                                <pre style="margin-top: 0.5rem; padding: 0.75rem; background: #1f2937; color: #f9fafb; border-radius: 6px; overflow-x: auto; font-size: 0.75rem;">${this.escapeHtml(String(diff.script2_value))}</pre>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        return `<div class="differences-list">${items}</div>`;
    }

    /**
     * Attach event listeners
     */
    attachEventListeners() {
        // Filter buttons
        const filterBtns = this.container.querySelectorAll('.filter-btn');
        filterBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const level = btn.dataset.level;
                this.filterDifferences(level);

                // Update active button
                filterBtns.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
            });
        });
    }

    /**
     * Filtra diferencias por nivel
     * @param {string} level
     */
    filterDifferences(level) {
        const items = this.container.querySelectorAll('.difference-item');
        items.forEach(item => {
            if (level === 'all' || item.dataset.level === level) {
                item.style.display = 'block';
            } else {
                item.style.display = 'none';
            }
        });
    }

    /**
     * Toggle detalles de una diferencia
     * @param {number} index
     */
    toggleDetails(index) {
        const details = document.getElementById(`details-${index}`);
        const icon = document.getElementById(`toggle-icon-${index}`);

        if (details.style.display === 'none') {
            details.style.display = 'block';
            icon.textContent = '▲';
        } else {
            details.style.display = 'none';
            icon.textContent = '▼';
        }
    }

    /**
     * Exporta reporte como JSON
     */
    exportJSON() {
        if (!this.report) return;

        const blob = new Blob([JSON.stringify(this.report, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `verification_report_${Date.now()}.json`;
        a.click();
        URL.revokeObjectURL(url);
    }

    /**
     * Exporta reporte como HTML
     */
    exportHTML() {
        if (!this.report) return;

        const html = `
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte de Verificación - ${this.report.script1_name} vs ${this.report.script2_name}</title>
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; margin: 2rem; background: #f3f4f6; }
        .container { max-width: 1200px; margin: 0 auto; }
        ${this.container.innerHTML}
    </style>
</head>
<body>
    <div class="container">
        ${this.container.innerHTML}
    </div>
</body>
</html>
        `;

        const blob = new Blob([html], { type: 'text/html' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `verification_report_${Date.now()}.html`;
        a.click();
        URL.revokeObjectURL(url);
    }

    // Utility methods
    countByLevel(differences, level) {
        return differences.filter(d => d.level === level).length;
    }

    formatList(list) {
        if (!list || list.length === 0) return 'N/A';
        return list.join(', ');
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}


// Global instances
let verificationClient = null;
let verificationUI = null;

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    verificationClient = new VerificationClient();
    verificationUI = new VerificationUI('verification-container');

    console.log('✅ Verification Client initialized');
});
