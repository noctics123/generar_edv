/**
 * AI UI - Interface and Event Handlers for AI Analysis
 * ======================================================
 *
 * Maneja la UI para configuración y ejecución de análisis con IA.
 *
 * Autor: Claude Code
 * Version: 1.0
 */

// Instancia global del analizador IA
let aiAnalyzer = null;

/**
 * Inicializar UI de IA
 */
function initializeAIUI() {
    aiAnalyzer = new AIAnalyzer();

    // Configurar event listeners
    setupAIEventListeners();

    // Verificar configuración inicial
    updateAIStatus();

    console.log('[AI UI] Initialized');
}

/**
 * Configurar event listeners
 */
function setupAIEventListeners() {
    // Botón configurar IA
    const configureBtn = document.getElementById('configure-ai-btn');
    if (configureBtn) {
        configureBtn.addEventListener('click', openAIConfigModal);
    }

    // Toggle habilitar IA
    const enableToggle = document.getElementById('enable-ai-analysis');
    if (enableToggle) {
        enableToggle.addEventListener('change', (e) => {
            const verifyAIBtn = document.getElementById('verify-with-ai-btn');
            if (verifyAIBtn) {
                verifyAIBtn.style.display = e.target.checked ? 'inline-flex' : 'none';
            }
        });
    }

    // Botón verificar con IA
    const verifyAIBtn = document.getElementById('verify-with-ai-btn');
    if (verifyAIBtn) {
        verifyAIBtn.addEventListener('click', runAIAnalysis);
    }

    // Modal: Guardar configuración
    const saveConfigBtn = document.getElementById('save-ai-config');
    if (saveConfigBtn) {
        saveConfigBtn.addEventListener('click', saveAIConfiguration);
    }

    // Modal: Cerrar
    const closeModalBtn = document.getElementById('close-ai-modal');
    if (closeModalBtn) {
        closeModalBtn.addEventListener('click', closeAIConfigModal);
    }

    // Modal: Limpiar configuración
    const clearConfigBtn = document.getElementById('clear-ai-config');
    if (clearConfigBtn) {
        clearConfigBtn.addEventListener('click', clearAIConfiguration);
    }

    // Cerrar modal al hacer click fuera
    const modal = document.getElementById('ai-config-modal');
    if (modal) {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                closeAIConfigModal();
            }
        });
    }
}

/**
 * Abrir modal de configuración
 */
function openAIConfigModal() {
    const modal = document.getElementById('ai-config-modal');
    if (!modal) return;

    // Cargar configuración actual (solo provider, NO API key por seguridad)
    const providerSelect = document.getElementById('ai-provider');
    const apiKeyInput = document.getElementById('ai-api-key');

    if (providerSelect && aiAnalyzer.provider) {
        providerSelect.value = aiAnalyzer.provider;
    }

    // NO cargar API key por seguridad - siempre limpio
    if (apiKeyInput) {
        apiKeyInput.value = '';
        apiKeyInput.placeholder = 'Ingresa tu API key...';
    }

    modal.classList.add('active');
}

/**
 * Cerrar modal de configuración
 */
function closeAIConfigModal() {
    const modal = document.getElementById('ai-config-modal');
    if (modal) {
        modal.classList.remove('active');
    }
}

/**
 * Guardar configuración de IA
 */
function saveAIConfiguration() {
    const providerSelect = document.getElementById('ai-provider');
    const apiKeyInput = document.getElementById('ai-api-key');

    if (!providerSelect || !apiKeyInput) return;

    const provider = providerSelect.value;
    const apiKey = apiKeyInput.value.trim();

    if (!apiKey) {
        alert('⚠️ Por favor ingresa tu API key');
        return;
    }

    // Guardar configuración
    aiAnalyzer.setProvider(provider);
    aiAnalyzer.setAPIKey(apiKey);

    // Actualizar UI
    updateAIStatus();

    // Cerrar modal
    closeAIConfigModal();

    // Mostrar notificación
    showNotification('✅ Configuración guardada correctamente', 'success');
}

/**
 * Limpiar configuración de IA
 */
function clearAIConfiguration() {
    if (!confirm('¿Estás seguro de que quieres eliminar la configuración de IA?')) {
        return;
    }

    aiAnalyzer.clearConfig();
    updateAIStatus();

    // Limpiar inputs
    const apiKeyInput = document.getElementById('ai-api-key');
    if (apiKeyInput) {
        apiKeyInput.value = '';
    }

    showNotification('🗑️ Configuración eliminada', 'info');
}

/**
 * Actualizar estado de configuración en UI
 */
function updateAIStatus() {
    const statusEl = document.getElementById('ai-config-status');
    const configureBtn = document.getElementById('configure-ai-btn');

    if (!statusEl) return;

    if (aiAnalyzer.isConfigured()) {
        statusEl.innerHTML = `
            <span style="color: #10b981; font-weight: 600;">
                ✅ Configurado (${aiAnalyzer.provider.toUpperCase()})
            </span>
        `;
        if (configureBtn) {
            configureBtn.textContent = '⚙️ Cambiar Configuración';
        }
    } else {
        statusEl.innerHTML = `
            <span style="color: #ef4444; font-weight: 600;">
                ❌ No configurado
            </span>
        `;
        if (configureBtn) {
            configureBtn.textContent = '⚙️ Configurar API';
        }
    }
}

/**
 * Ejecutar análisis con IA
 */
async function runAIAnalysis() {
    // Verificar que hay scripts cargados
    if (!verifyScript1Content || !verifyScript2Content) {
        alert('⚠️ Debes cargar ambos scripts antes de analizar');
        return;
    }

    // Verificar configuración
    if (!aiAnalyzer.isConfigured()) {
        alert('⚠️ Debes configurar tu API key primero');
        openAIConfigModal();
        return;
    }

    // Obtener modo de verificación
    const modeRadio = document.querySelector('input[name="verification-mode"]:checked');
    const mode = modeRadio ? modeRadio.value : 'individual';

    // Obtener nombres de archivos
    const script1Name = document.getElementById('verify-file-name-1')?.textContent || 'script1.py';
    const script2Name = document.getElementById('verify-file-name-2')?.textContent || 'script2.py';

    // Mostrar loading
    const verifyBtn = document.getElementById('verify-with-ai-btn');
    const originalText = verifyBtn.innerHTML;
    verifyBtn.innerHTML = '<span class="spinner"></span> Analizando con IA...';
    verifyBtn.disabled = true;

    try {
        // Llamar a IA
        const result = await aiAnalyzer.analyzeScripts(
            verifyScript1Content,
            verifyScript2Content,
            {
                mode: mode,
                script1Name: script1Name,
                script2Name: script2Name
            }
        );

        // Renderizar resultados
        renderAIResults(result, mode);

        showNotification('✅ Análisis con IA completado', 'success');
    } catch (error) {
        console.error('[AI Analysis] Error:', error);
        showNotification(`❌ Error: ${error.message}`, 'error');
    } finally {
        // Restaurar botón
        verifyBtn.innerHTML = originalText;
        verifyBtn.disabled = false;
    }
}

/**
 * Renderizar resultados de IA
 */
function renderAIResults(result, mode) {
    const container = document.getElementById('ai-results-container');
    if (!container) {
        console.error('[AI UI] Container #ai-results-container not found');
        return;
    }

    // Si hay error de parsing, mostrar respuesta raw
    if (result.parse_error) {
        container.innerHTML = `
            <div class="ai-results">
                <div class="alert alert-warning">
                    <h3>⚠️ Error al parsear respuesta JSON</h3>
                    <p>La IA respondió pero no pude parsear el JSON. Aquí está la respuesta completa:</p>
                    <pre style="white-space: pre-wrap; background: #f5f5f5; padding: 1rem; border-radius: 6px; max-height: 500px; overflow: auto;">${escapeHtml(result.raw_response)}</pre>
                </div>
            </div>
        `;
        container.style.display = 'block';
        return;
    }

    let html = '<div class="ai-results">';

    // Header
    html += `
        <div class="ai-results-header">
            <h2>🤖 Análisis con IA</h2>
            <span class="ai-provider-badge">${aiAnalyzer.provider.toUpperCase()}</span>
        </div>
    `;

    // Summary
    if (result.summary) {
        html += renderAISummary(result.summary, mode);
    }

    // Differences
    if (result.differences && result.differences.length > 0) {
        html += renderAIDifferences(result.differences);
    }

    // Optimizations (solo para modo DDV-EDV)
    if (mode === 'ddv-edv' && result.optimizations && result.optimizations.length > 0) {
        html += renderAIOptimizations(result.optimizations);
    }

    // Recommendations
    if (result.recommendations && result.recommendations.length > 0) {
        html += renderAIRecommendations(result.recommendations);
    }

    // Conclusion
    if (result.conclusion) {
        html += `
            <div class="ai-conclusion">
                <h3>📝 Conclusión</h3>
                <p>${escapeHtml(result.conclusion)}</p>
            </div>
        `;
    }

    html += '</div>';

    container.innerHTML = html;
    container.style.display = 'block';

    // Scroll to results
    container.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

/**
 * Renderizar resumen
 */
function renderAISummary(summary, mode) {
    let html = '<div class="ai-summary">';

    if (mode === 'ddv-edv') {
        const statusClass = summary.is_valid_conversion ? 'status-success' : 'status-error';
        const statusIcon = summary.is_valid_conversion ? '✅' : '❌';
        const statusText = summary.is_valid_conversion ? 'Conversión Válida' : 'Conversión Inválida';

        html += `
            <div class="summary-card ${statusClass}">
                <h3>${statusIcon} ${statusText}</h3>
                <div class="summary-stats">
                    <div class="stat">
                        <span class="stat-label">Tipo:</span>
                        <span class="stat-value">${summary.conversion_type || 'Unknown'}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Similitud:</span>
                        <span class="stat-value">${summary.similarity_percentage}%</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Total Diferencias:</span>
                        <span class="stat-value">${summary.total_differences}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Issues Críticos:</span>
                        <span class="stat-value">${summary.critical_issues}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Optimizaciones:</span>
                        <span class="stat-value">${summary.optimizations_applied}</span>
                    </div>
                </div>
            </div>
        `;
    } else {
        const statusClass = summary.are_equivalent ? 'status-success' : 'status-warning';
        const statusIcon = summary.are_equivalent ? '✅' : '⚠️';
        const statusText = summary.are_equivalent ? 'Scripts Equivalentes' : 'Scripts Diferentes';

        html += `
            <div class="summary-card ${statusClass}">
                <h3>${statusIcon} ${statusText}</h3>
                <div class="summary-stats">
                    <div class="stat">
                        <span class="stat-label">Similitud:</span>
                        <span class="stat-value">${summary.similarity_percentage}%</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Total Diferencias:</span>
                        <span class="stat-value">${summary.total_differences}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Diferencias Críticas:</span>
                        <span class="stat-value">${summary.critical_differences}</span>
                    </div>
                </div>
            </div>
        `;
    }

    html += '</div>';
    return html;
}

/**
 * Renderizar diferencias
 */
function renderAIDifferences(differences) {
    let html = '<div class="ai-differences"><h3>📊 Diferencias Encontradas</h3>';

    differences.forEach((diff, index) => {
        const severityClass = `severity-${diff.severity.toLowerCase()}`;
        const expectedBadge = diff.is_expected_ddv_edv ? '<span class="badge badge-success">Esperado DDV→EDV</span>' : '';

        html += `
            <div class="ai-difference-item ${severityClass}">
                <div class="diff-header">
                    <span class="diff-category">${diff.category}</span>
                    <span class="diff-severity badge-${diff.severity.toLowerCase()}">${diff.severity}</span>
                    ${expectedBadge}
                </div>
                <h4>${escapeHtml(diff.description)}</h4>
                <div class="diff-details">
                    <pre>${escapeHtml(diff.details)}</pre>
                </div>
                ${diff.recommendation ? `<div class="diff-recommendation">💡 ${escapeHtml(diff.recommendation)}</div>` : ''}
            </div>
        `;
    });

    html += '</div>';
    return html;
}

/**
 * Renderizar optimizaciones
 */
function renderAIOptimizations(optimizations) {
    let html = '<div class="ai-optimizations"><h3>⚡ Optimizaciones Detectadas</h3>';

    optimizations.forEach(opt => {
        const detectedClass = opt.detected ? 'opt-detected' : 'opt-not-detected';
        const icon = opt.detected ? '✅' : '❌';

        html += `
            <div class="optimization-item ${detectedClass}">
                <div class="opt-header">
                    <span class="opt-icon">${icon}</span>
                    <span class="opt-name">${escapeHtml(opt.name)}</span>
                    <span class="opt-impact">${escapeHtml(opt.impact)}</span>
                </div>
                ${opt.details ? `<p class="opt-details">${escapeHtml(opt.details)}</p>` : ''}
            </div>
        `;
    });

    html += '</div>';
    return html;
}

/**
 * Renderizar recomendaciones
 */
function renderAIRecommendations(recommendations) {
    let html = '<div class="ai-recommendations"><h3>💡 Recomendaciones</h3><ul>';

    recommendations.forEach(rec => {
        html += `<li>${escapeHtml(rec)}</li>`;
    });

    html += '</ul></div>';
    return html;
}

/**
 * Escape HTML para prevenir XSS
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Mostrar notificación
 */
function showNotification(message, type = 'info') {
    // Reusar función existente si está disponible
    if (typeof addLogMessage === 'function') {
        addLogMessage(message, type);
    } else {
        console.log(`[${type.toUpperCase()}] ${message}`);
    }
}

// Inicializar cuando el DOM esté listo
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeAIUI);
} else {
    initializeAIUI();
}
