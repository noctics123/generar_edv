/**
 * EDV Converter - Main Application
 * BCP Analytics
 */

// Global state
let currentInputScript = '';
let currentOutputScript = '';
let conversionResult = null;
let validationResult = null;
let editableParams = {};

// Initialize application
document.addEventListener('DOMContentLoaded', () => {
    initializeEventListeners();
    initializeSyntaxHighlighting();
    initializeVerificationSection(); // Initialize verification section
    initializeComparisonModal(); // Initialize comparison modal
    initializeMainNavigation(); // Initialize page navigation
    console.log('✅ EDV Converter initialized');
});

// ===== MAIN NAVIGATION =====
function initializeMainNavigation() {
    const mainTabs = document.querySelectorAll('.main-tab');

    mainTabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const page = tab.dataset.page;
            switchMainPage(page);
        });
    });
}

function switchMainPage(pageName) {
    // Update tabs
    document.querySelectorAll('.main-tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.page === pageName);
    });

    // Update pages
    document.querySelectorAll('.main-page').forEach(page => {
        page.classList.toggle('active', page.id === `page-${pageName}`);
    });

    // Scroll to top
    window.scrollTo({ top: 0, behavior: 'smooth' });

    console.log(`[INFO] Pagina cambiada: ${pageName}`);
}

// ===== EVENT LISTENERS =====
function initializeEventListeners() {
    // File upload
    const fileInput = document.getElementById('file-upload');
    fileInput.addEventListener('change', handleFileUpload);

    // Example buttons
    const exampleButtons = document.querySelectorAll('[data-example]');
    exampleButtons.forEach(btn => {
        btn.addEventListener('click', () => loadExample(btn.dataset.example));
    });

    // Clear input
    document.getElementById('clear-input').addEventListener('click', clearInput);

    // Convert button
    document.getElementById('convert-btn').addEventListener('click', convertScript);

    // Input script monitoring
    const inputScript = document.getElementById('input-script');
    inputScript.addEventListener('input', updateInputStats);

    // Tab switching
    const tabs = document.querySelectorAll('.tab');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => switchTab(tab.dataset.tab));
    });

    // Copy output
    document.getElementById('copy-output').addEventListener('click', copyOutput);

    // Download output
    document.getElementById('download-output').addEventListener('click', downloadOutput);

    // Download .txt
    document.getElementById('download-txt').addEventListener('click', downloadTxt);

    // Regenerate EDV
    document.getElementById('regenerate-edv').addEventListener('click', regenerateEDV);

    // Reset params
    document.getElementById('reset-params').addEventListener('click', resetParams);

    // Multi-periodo
    document.getElementById('multiperiodo-hoy').addEventListener('click', setTodayMultiperiodo);
    document.getElementById('generate-multiperiodo').addEventListener('click', generateMultiperiodo);

    // Conversion method selection
    const methodRadios = document.querySelectorAll('input[name="conversion-method"]');
    methodRadios.forEach(radio => {
        radio.addEventListener('change', handleConversionMethodChange);
    });

    // Add visual feedback for method selection
    const methodCards = document.querySelectorAll('.conversion-method-card');
    methodCards.forEach(card => {
        card.addEventListener('click', () => {
            methodCards.forEach(c => c.style.borderColor = '#dee2e6');
            card.style.borderColor = '#0066cc';
        });
    });
}

// ===== FILE HANDLING =====
function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    // Validate file type
    if (!file.name.endsWith('.py')) {
        alert('⚠️ Por favor selecciona un archivo .py');
        return;
    }

    // Update file name display
    document.getElementById('file-name').textContent = file.name;

    // Read file
    const reader = new FileReader();
    reader.onload = (e) => {
        const content = e.target.result;
        setEditorContent('input-script', content);
        currentInputScript = content;
        updateInputStats();
        console.log(`✅ Archivo cargado: ${file.name}`);
    };
    reader.readAsText(file);
}

async function loadExample(exampleName) {
    try {
        // Fetch the DDV example script
        const response = await fetch(`edv-converter-webapp/examples/${exampleName}/ddv.py`);

        if (!response.ok) {
            throw new Error(`No se pudo cargar el ejemplo: ${response.statusText}`);
        }

        const content = await response.text();

        // Load the script into the input area
        setEditorContent('input-script', content);
        currentInputScript = content;
        updateInputStats();

        // Update file name display
        document.getElementById('file-name').textContent = `📂 Ejemplo: ${exampleName.toUpperCase()}`;

        console.log(`✅ Ejemplo ${exampleName} cargado correctamente`);
    } catch (error) {
        console.error('Error cargando ejemplo:', error);
        alert(`❌ Error al cargar el ejemplo "${exampleName}".\n\n${error.message}`);
    }
}

function clearInput() {
    setEditorContent('input-script', '');
    document.getElementById('file-name').textContent = 'Ningún archivo seleccionado';
    document.getElementById('file-upload').value = '';
    currentInputScript = '';
    updateInputStats();
    console.log('🗑️ Input limpiado');
}

// ===== STATS =====
/**
 * Generic function to update script statistics (lines and size)
 * @param {string} script - Script content
 * @param {string} linesElementId - Element ID for lines display
 * @param {string} sizeElementId - Element ID for size display
 */
function updateStats(script, linesElementId, sizeElementId) {
    const lines = script.split('\n').length;
    const bytes = new Blob([script]).size;
    const kb = (bytes / 1024).toFixed(2);

    document.getElementById(linesElementId).textContent = `${lines} líneas`;
    document.getElementById(sizeElementId).textContent = `${kb} KB`;
}

function updateInputStats() {
    const script = getEditorContent('input-script');
    currentInputScript = script;
    updateStats(script, 'input-lines', 'input-size');
}

function updateOutputStats() {
    updateStats(currentOutputScript, 'output-lines', 'output-size');
}

// ===== CONVERSION =====
async function convertScript() {
    // Validate input
    if (!currentInputScript || currentInputScript.trim() === '') {
        alert('⚠️ Por favor carga o pega un script DDV primero');
        return;
    }

    // Detectar método de conversión seleccionado
    const methodRadio = document.querySelector('input[name="conversion-method"]:checked');
    const method = methodRadio ? methodRadio.value : 'classic';

    console.log(`🔄 Iniciando conversión (método: ${method})...`);

    // Leer configuración de switches de setup
    window.edvSetupOptions = {
        restartPython: document.getElementById('switch-restart-python').checked,
        pipInstall: document.getElementById('switch-pip-install').checked,
        removeAllWidgets: document.getElementById('switch-remove-widgets').checked
    };

    // Leer configuración de switches de conversión (sufijos y rutas)
    window.edvConversionOptions = {
        usarSufijoRuben: document.getElementById('switch-sufijo-ruben').checked,
        usarRutasRuben: document.getElementById('switch-rutas-ruben').checked
    };

    console.log('⚙️ Opciones de setup:', window.edvSetupOptions);
    console.log('🏷️ Opciones de conversión:', window.edvConversionOptions);

    // Show loading state
    const convertBtn = document.getElementById('convert-btn');
    const originalText = convertBtn.innerHTML;
    convertBtn.innerHTML = method === 'ai' ? '🤖 Generando con IA...' : '⏳ Convirtiendo...';
    convertBtn.disabled = true;

    try {
        // Convert based on selected method
        if (method === 'ai') {
            // AI-powered conversion
            conversionResult = await convertWithAI();
            currentOutputScript = conversionResult.edvScript;
        } else {
            // Classic rule-based conversion
            const converter = new EDVConverter();
            conversionResult = converter.convert(currentInputScript);
            currentOutputScript = conversionResult.edvScript;
        }

        // Validar con validador EDV
        const validator = new EDVValidatorRiguroso();
        validationResult = validator.validate(currentOutputScript);

        // Update UI
        updateConversionResults();
        updateValidationResults();
        generateDiff();

        // Show output section
        document.getElementById('output-section').style.display = 'block';

        // Scroll to results
        document.getElementById('output-section').scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });

        console.log('✅ Conversión completada');
        console.log('📊 Cambios:', conversionResult.log.length);
        console.log('⚠️ Advertencias:', conversionResult.warnings.length);
        console.log('🎯 Score:', validationResult.score + '%');

        // Si fue generado con IA, mostrar información adicional
        if (conversionResult.aiGenerated) {
            console.log('🤖 Generado con IA');
            console.log('📝 Resumen:', conversionResult.aiSummary);
            if (conversionResult.aiOptimizationsApplied && conversionResult.aiOptimizationsApplied.length > 0) {
                console.log('⚡ Optimizaciones aplicadas:', conversionResult.aiOptimizationsApplied.length);
            }
        }

    } catch (error) {
        console.error('❌ Error en conversión:', error);

        // Mostrar el error en la UI para depuración
        const errorLogEl = document.getElementById('log-errors');
        if (errorLogEl) {
            errorLogEl.innerHTML = `<div class="log-item error"><strong>Error:</strong> ${escapeHtml(error.message)}<br><pre>${escapeHtml(error.stack || '')}</pre></div>`;
        }

        // Asegurarse de que la sección de salida sea visible y mostrar el log
        document.getElementById('output-section').style.display = 'block';
        switchTab('log');

        alert('❌ Error al convertir el script. Revisa la pestaña "Log" para más detalles.');
    } finally {
        // Restore button
        convertBtn.innerHTML = originalText;
        convertBtn.disabled = false;
    }
}

// ===== UPDATE RESULTS =====
function updateConversionResults() {
    // Update stats
    document.getElementById('stat-changes').textContent = conversionResult.log.length;
    document.getElementById('stat-warnings').textContent = conversionResult.warnings.length;

    // Update output editor
    setEditorContent('output-script', currentOutputScript);
    updateOutputStats();

    // Update log tab
    updateLogTab();

    // Update params table
    updateParamsTable();

    // Update managed tables info
    updateManagedTablesInfo();
}

function updateValidationResults() {
    // Update stats
    document.getElementById('stat-score').textContent = validationResult.score + '%';

    const status = validationResult.score >= 80 ? '✅ PASS' : '❌ FAIL';
    const statusEl = document.getElementById('stat-status');
    statusEl.textContent = status;
    statusEl.style.color = validationResult.score >= 80 ? 'var(--success)' : 'var(--danger)';

    // Update checklist tab
    updateChecklistTab();
    // Update parameters tab (if present)
    if (document.getElementById('params-container')) {
        updateParamsTab();
    }
}

function updateLogTab() {
    // Si fue generado con IA, mostrar banner informativo al inicio
    let aiInfoHtml = '';
    if (conversionResult.aiGenerated) {
        aiInfoHtml = `
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 1rem; border-radius: 8px; margin-bottom: 1rem;">
                <h3 style="margin: 0 0 0.5rem 0; display: flex; align-items: center; gap: 0.5rem;">
                    🤖 Generado con IA
                </h3>
                <p style="margin: 0 0 0.5rem 0; opacity: 0.95; font-size: 0.9rem;">
                    ${escapeHtml(conversionResult.aiSummary || 'Script convertido usando inteligencia artificial')}
                </p>
                ${conversionResult.aiOptimizationsApplied && conversionResult.aiOptimizationsApplied.length > 0 ? `
                    <div style="margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid rgba(255,255,255,0.2);">
                        <strong style="font-size: 0.85rem;">⚡ Optimizaciones aplicadas (${conversionResult.aiOptimizationsApplied.length}):</strong>
                        <ul style="margin: 0.5rem 0 0 1.5rem; padding: 0; font-size: 0.85rem; opacity: 0.95;">
                            ${conversionResult.aiOptimizationsApplied.map(opt => `<li>${escapeHtml(opt.name || opt)}</li>`).join('')}
                        </ul>
                    </div>
                ` : ''}
            </div>
        `;
    }

    // Changes
    const changesEl = document.getElementById('log-changes');
    if (conversionResult.log.length === 0) {
        changesEl.innerHTML = aiInfoHtml + '<div class="log-empty">No hay cambios registrados</div>';
    } else {
        const changesHtml = conversionResult.log
            .map(log => {
                // Si log es objeto (de IA)
                if (typeof log === 'object') {
                    return `
                        <div class="log-item">
                            <strong>${escapeHtml(log.message || log.description || '')}</strong>
                            ${log.details ? `<div style="margin-top: 0.5rem; padding-left: 1rem; border-left: 3px solid #ddd; font-size: 0.85rem; color: #666;"><pre style="white-space: pre-wrap; margin: 0;">${escapeHtml(log.details)}</pre></div>` : ''}
                        </div>
                    `;
                }
                // Si log es string (clásico)
                return `<div class="log-item">${escapeHtml(log)}</div>`;
            })
            .join('');
        changesEl.innerHTML = aiInfoHtml + changesHtml;
    }

    // Warnings
    const warningsEl = document.getElementById('log-warnings');
    if (conversionResult.warnings.length === 0) {
        warningsEl.innerHTML = '<div class="log-empty">No hay advertencias</div>';
    } else {
        warningsEl.innerHTML = conversionResult.warnings
            .map(warning => {
                // Si warning es objeto
                if (typeof warning === 'object') {
                    return `<div class="log-item">⚠️ ${escapeHtml(warning.message || warning)}</div>`;
                }
                return `<div class="log-item">⚠️ ${escapeHtml(warning)}</div>`;
            })
            .join('');
    }

    // Errors
    const errorsEl = document.getElementById('log-errors');
    if (validationResult.errors.length === 0) {
        errorsEl.innerHTML = '<div class="log-empty">No hay errores</div>';
    } else {
        errorsEl.innerHTML = validationResult.errors
            .map(error => `<div class="log-item">❌ ${escapeHtml(error)}</div>`)
            .join('');
    }
}

function updateChecklistTab() {
    const checklistEl = document.getElementById('checklist-container');

    if (validationResult.checks.length === 0) {
        checklistEl.innerHTML = '<div class="log-empty">No hay validaciones disponibles</div>';
        return;
    }

    checklistEl.innerHTML = validationResult.checks
        .map(check => `
            <div class="checklist-item ${check.passed ? 'passed' : 'failed'}">
                <div class="checklist-icon">${check.passed ? '✅' : '❌'}</div>
                <div class="checklist-content">
                    <div class="checklist-name">
                        <span style="color: ${check.level === 'CRÍTICO' ? 'red' : 'gray'}; font-weight: bold;">[${check.level || 'CHECK'}]</span>
                        ${escapeHtml(check.name)}
                        ${check.points ? `<span style="color: blue; font-size: 0.9em;"> (+${check.points}pts)</span>` : ''}
                    </div>
                    <div class="checklist-message">${escapeHtml(check.message)}</div>
                </div>
            </div>
        `)
        .join('');
}

// ===== DIFF GENERATION =====
function generateDiff() {
    const beforeEl = document.getElementById('diff-before');
    const afterEl = document.getElementById('diff-after');

    // Simple line-by-line diff
    const beforeLines = currentInputScript.split('\n');
    const afterLines = currentOutputScript.split('\n');

    // For simplicity, just show both sides with line numbers
    beforeEl.innerHTML = beforeLines
        .map((line, i) => {
            const lineNum = i + 1;
            const escaped = escapeHtml(line);
            return `<div class="diff-line"><span class="diff-line-number">${lineNum}</span>${escaped || ' '}</div>`;
        })
        .join('');

    afterEl.innerHTML = afterLines
        .map((line, i) => {
            const lineNum = i + 1;
            const escaped = escapeHtml(line);

            // Highlight added lines (simple heuristic: contains OPTIMIZACIÓN or EDV keywords)
            const isAdded = line.includes('OPTIMIZACIÓN') ||
                           line.includes('PRM_CATALOG_NAME_EDV') ||
                           line.includes('PRM_ESQUEMA_TABLA_EDV') ||
                           line.includes('PRM_ESQUEMA_TABLA_ESCRITURA');

            const cssClass = isAdded ? 'diff-line diff-line-added' : 'diff-line';

            return `<div class="${cssClass}"><span class="diff-line-number">${lineNum}</span>${escaped || ' '}</div>`;
        })
        .join('');
}

// ===== TABS =====
function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.tab === tabName);
    });

    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `tab-${tabName}`);
    });

    console.log(`📑 Tab cambiado: ${tabName}`);
}

// ===== OUTPUT ACTIONS =====
function copyOutput() {
    const content = getEditorContent('output-script');

    // Use modern clipboard API
    navigator.clipboard.writeText(content).then(() => {
        // Visual feedback
        const btn = document.getElementById('copy-output');
        const originalText = btn.innerHTML;
        btn.innerHTML = '✅ Copiado';
        setTimeout(() => {
            btn.innerHTML = originalText;
        }, 2000);

        console.log('📋 Script copiado al portapapeles');
    }).catch(err => {
        console.error('Error al copiar:', err);
        alert('❌ Error al copiar al portapapeles');
    });
}

/**
 * Detect script type based on content patterns
 * @returns {string} Script base filename (without extension)
 */
function detectScriptType() {
    if (currentInputScript.includes('MATRIZTRANSACCIONAGENTE')) {
        return 'MATRIZVARIABLES_HM_MATRIZTRANSACCIONAGENTE_EDV';
    } else if (currentInputScript.includes('MATRIZTRANSACCIONCAJERO')) {
        return 'MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO_EDV';
    } else if (currentInputScript.includes('MATRIZTRANSACCIONPOSMACROGIRO')) {
        return 'HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV';
    }
    return 'script_edv';
}

/**
 * Generic file download function
 * @param {string} content - File content
 * @param {string} extension - File extension (py, txt, etc.)
 */
function downloadFile(content, extension) {
    if (!content) {
        alert('⚠️ No hay script para descargar');
        return;
    }

    const filename = `${detectScriptType()}.${extension}`;
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);

    console.log(`💾 Script descargado: ${filename}`);
}

function downloadOutput() {
    downloadFile(currentOutputScript, 'py');
}

// ===== UTILITIES =====
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ===== PARAMS TABLE =====
function updateParamsTable() {
    const tbody = document.getElementById('params-table-body');

    if (!conversionResult) {
        tbody.innerHTML = '<tr><td colspan="4">No hay parámetros para mostrar</td></tr>';
        return;
    }

    const params = extractParamsFromScripts(currentInputScript, currentOutputScript);
    editableParams = { ...params.edv }; // Store for editing

    const rows = [];

    // Row 1: Container (DDV lee → EDV escribe)
    rows.push(`
        <tr>
            <td class="param-name">CONS_CONTAINER_NAME</td>
            <td class="param-value" title="Para lectura DDV">${escapeHtml(params.ddv.container || '-')}</td>
            <td><input type="text" id="edit-container" value="${escapeHtml(params.edv.container || '')}" placeholder="abfss://bcp-edv-trdata-012@" /></td>
            <td>✏️</td>
        </tr>
    `);

    // Row 2: Storage Account (DDV desarrollo → EDV producción)
    rows.push(`
        <tr>
            <td class="param-name">PRM_STORAGE_ACCOUNT_DDV</td>
            <td class="param-value" title="Desarrollo">${escapeHtml(params.ddv.storageAccount || '-')}</td>
            <td><input type="text" id="edit-storage" value="${escapeHtml(params.edv.storageAccount || '')}" placeholder="adlscu1lhclbackp05" /></td>
            <td>✏️</td>
        </tr>
    `);

    // Row 3: Catalog (DDV desarrollo → EDV producción, MISMO PARAM)
    rows.push(`
        <tr>
            <td class="param-name">PRM_CATALOG_NAME <span style="font-size: 0.8em; color: #666;">(lectura)</span></td>
            <td class="param-value" title="Desarrollo">${escapeHtml(params.ddv.catalogDDV || '-')}</td>
            <td><input type="text" id="edit-catalog-ddv" value="${escapeHtml(params.edv.catalogDDV || '')}" placeholder="catalog_lhcl_prod_bcp" /></td>
            <td>✏️</td>
        </tr>
    `);

    // Row 4: Catalog EDV (nuevo parámetro para escritura)
    rows.push(`
        <tr>
            <td class="param-name">PRM_CATALOG_NAME_EDV <span style="font-size: 0.8em; color: #666;">(escritura)</span></td>
            <td class="param-value" style="color: #999;">N/A (solo EDV)</td>
            <td><input type="text" id="edit-catalog-edv" value="${escapeHtml(params.edv.catalogEDV || '')}" placeholder="catalog_lhcl_prod_bcp_expl" /></td>
            <td>✏️</td>
        </tr>
    `);

    // Row 5: Schema DDV (MISMO PARAM, solo cambia sufijo _v)
    rows.push(`
        <tr>
            <td class="param-name">PRM_ESQUEMA_TABLA_DDV <span style="font-size: 0.8em; color: #666;">(lectura con views)</span></td>
            <td class="param-value">${escapeHtml(params.ddv.schemaDDV || '-')}</td>
            <td><input type="text" id="edit-schema-ddv" value="${escapeHtml(params.edv.schemaDDV || '')}" placeholder="bcp_ddv_matrizvariables_v" /></td>
            <td>✏️</td>
        </tr>
    `);

    // Row 6: Schema EDV (nuevo parámetro para escritura)
    rows.push(`
        <tr>
            <td class="param-name">PRM_ESQUEMA_TABLA_EDV <span style="font-size: 0.8em; color: #666;">(escritura)</span></td>
            <td class="param-value" style="color: #999;">N/A (solo EDV)</td>
            <td><input type="text" id="edit-schema-edv" value="${escapeHtml(params.edv.schemaEDV || '')}" placeholder="bcp_edv_trdata_012" /></td>
            <td>✏️</td>
        </tr>
    `);

    // Row 7: Table Name (opcional agregar sufijo)
    rows.push(`
        <tr>
            <td class="param-name">PRM_TABLE_NAME <span style="font-size: 0.8em; color: #666;">(+ sufijo opcional)</span></td>
            <td class="param-value">${escapeHtml(params.ddv.tableName || '-')}</td>
            <td><input type="text" id="edit-table-name" value="${escapeHtml(params.edv.tableName || '')}" placeholder="${escapeHtml(params.ddv.tableName || '')}_EDV" /></td>
            <td>✏️</td>
        </tr>
    `);

    // Row 8: Fecha Rutina (opcional)
    rows.push(`
        <tr>
            <td class="param-name">PRM_FECHA_RUTINA <span style="font-size: 0.8em; color: #666;">(opcional)</span></td>
            <td class="param-value">${escapeHtml(params.ddv.fecha || '-')}</td>
            <td><input type="date" id="edit-fecha" value="${params.edv.fecha || new Date().toISOString().split('T')[0]}" /></td>
            <td>✏️</td>
        </tr>
    `);

    // Row 9: Carpeta Output (si se agregó)
    if (params.edv.carpetaOutput) {
        rows.push(`
            <tr>
                <td class="param-name">PRM_CARPETA_OUTPUT <span style="font-size: 0.8em; color: #666;">(ruta save)</span></td>
                <td class="param-value">${escapeHtml(params.ddv.carpetaOutput || '-')}</td>
                <td><input type="text" id="edit-carpeta-output" value="${escapeHtml(params.edv.carpetaOutput || '')}" placeholder="data/RUBEN/DEUDA_TECNICA/out" /></td>
                <td>✏️</td>
            </tr>
        `);
    }

    // Row 10: Ruta ADLS Tables (si se agregó)
    if (params.edv.rutaAdls) {
        rows.push(`
            <tr>
                <td class="param-name">PRM_RUTA_ADLS_TABLES <span style="font-size: 0.8em; color: #666;">(ruta ADLS)</span></td>
                <td class="param-value">${escapeHtml(params.ddv.rutaAdls || '-')}</td>
                <td><input type="text" id="edit-ruta-adls" value="${escapeHtml(params.edv.rutaAdls || '')}" placeholder="data/RUBEN/DEUDA_TECNICA/matrizvariables" /></td>
                <td>✏️</td>
            </tr>
        `);
    }

    tbody.innerHTML = rows.join('');
}

/**
 * Utility: Extract parameter value from script using regex
 * @param {string} script - Script content
 * @param {string} name - Parameter/constant name
 * @param {string} type - Type: 'param' (dbutils.widgets) or 'const' (variable assignment)
 * @returns {string|null} Extracted value or null if not found
 */
function extractValue(script, name, type = 'param') {
    const pattern = type === 'const'
        ? new RegExp(`${name}\\s*=\\s*['"]([^'"]+)['"]`)
        : new RegExp(`${name}.*?defaultValue\\s*=\\s*['"]([^'"]+)['"]`);
    const match = script.match(pattern);
    return match ? match[1] : null;
}

function extractParamsFromScripts(ddvScript, edvScript) {
    const paramNames = {
        const: ['CONS_CONTAINER_NAME'],
        param: ['PRM_STORAGE_ACCOUNT_DDV', 'PRM_CATALOG_NAME', 'PRM_ESQUEMA_TABLA_DDV', 'PRM_TABLE_NAME', 'PRM_FECHA_RUTINA']
    };

    const extractFromScript = (script, includeEDVParams = false) => {
        const result = {};

        // Extract constants
        paramNames.const.forEach(name => {
            const key = name === 'CONS_CONTAINER_NAME' ? 'container' : name;
            result[key] = extractValue(script, name, 'const');
        });

        // Extract parameters
        result.storageAccount = extractValue(script, 'PRM_STORAGE_ACCOUNT_DDV');
        result.catalogDDV = extractValue(script, 'PRM_CATALOG_NAME');
        result.schemaDDV = extractValue(script, 'PRM_ESQUEMA_TABLA_DDV');
        result.tableName = extractValue(script, 'PRM_TABLE_NAME');
        result.fecha = extractValue(script, 'PRM_FECHA_RUTINA');
        result.carpetaOutput = extractValue(script, 'PRM_CARPETA_OUTPUT');
        result.rutaAdls = extractValue(script, 'PRM_RUTA_ADLS_TABLES');

        // EDV-specific parameters
        if (includeEDVParams) {
            result.catalogEDV = extractValue(script, 'PRM_CATALOG_NAME_EDV');
            result.schemaEDV = extractValue(script, 'PRM_ESQUEMA_TABLA_EDV');
        }

        return result;
    };

    return {
        ddv: extractFromScript(ddvScript, false),
        edv: extractFromScript(edvScript, true)
    };
}

function updateManagedTablesInfo() {
    const infoEl = document.getElementById('managed-info');

    const tempTables = (currentOutputScript.match(/tmp_table\w*\s*=\s*f["'].*?["']/g) || []).length;
    const saveAsTables = (currentOutputScript.match(/\.saveAsTable\([^)]+\)/g) || []).length;
    const hasPath = /\.saveAsTable\([^)]*,\s*path\s*=/.test(currentOutputScript);

    infoEl.innerHTML = `
        <div style="background: white; padding: 1.5rem; border-radius: 8px; margin-top: 1rem;">
            <h4 style="margin-bottom: 1rem;">📊 Resumen de Tablas</h4>
            <ul style="list-style: none; padding: 0;">
                <li style="margin-bottom: 0.75rem;">
                    <strong>✅ Tablas Temporales detectadas:</strong> ${tempTables}
                </li>
                <li style="margin-bottom: 0.75rem;">
                    <strong>✅ Operaciones saveAsTable:</strong> ${saveAsTables}
                </li>
                <li style="margin-bottom: 0.75rem;">
                    <strong>${hasPath ? '❌' : '✅'} Managed Tables (sin path):</strong> ${hasPath ? 'NO - Aún tiene path=' : 'SÍ - Todas son managed'}
                </li>
            </ul>
            <div style="margin-top: 1.5rem; padding: 1rem; background: ${hasPath ? '#fee2e2' : '#dcfce7'}; border-radius: 6px;">
                <p style="margin: 0; color: ${hasPath ? '#991b1b' : '#166534'};">
                    ${hasPath ?
                        '⚠️ ADVERTENCIA: Se detectó uso de path= en saveAsTable. Las tablas deben ser managed (sin path) para EDV.' :
                        '✅ CORRECTO: Todas las tablas son managed. Los datos serán administrados por Databricks Unity Catalog.'
                    }
                </p>
            </div>
        </div>
    `;
}

function regenerateEDV() {
    // Parameter mapping: elementId -> {pattern, replacement}
    const paramUpdates = [
        {
            id: 'edit-container',
            pattern: /CONS_CONTAINER_NAME\s*=\s*["'][^'"]+["']/,
            replacement: (val) => `CONS_CONTAINER_NAME = "${val}"`,
            name: 'CONS_CONTAINER_NAME'
        },
        {
            id: 'edit-storage',
            pattern: /PRM_STORAGE_ACCOUNT_DDV["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_STORAGE_ACCOUNT_DDV", defaultValue='${val}'`,
            name: 'PRM_STORAGE_ACCOUNT_DDV'
        },
        {
            id: 'edit-catalog-ddv',
            pattern: /PRM_CATALOG_NAME["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_CATALOG_NAME", defaultValue='${val}'`,
            name: 'PRM_CATALOG_NAME'
        },
        {
            id: 'edit-catalog-edv',
            pattern: /PRM_CATALOG_NAME_EDV["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_CATALOG_NAME_EDV", defaultValue='${val}'`,
            name: 'PRM_CATALOG_NAME_EDV'
        },
        {
            id: 'edit-schema-ddv',
            pattern: /PRM_ESQUEMA_TABLA_DDV["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_ESQUEMA_TABLA_DDV", defaultValue='${val}'`,
            name: 'PRM_ESQUEMA_TABLA_DDV'
        },
        {
            id: 'edit-schema-edv',
            pattern: /PRM_ESQUEMA_TABLA_EDV["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_ESQUEMA_TABLA_EDV", defaultValue='${val}'`,
            name: 'PRM_ESQUEMA_TABLA_EDV'
        },
        {
            id: 'edit-table-name',
            pattern: /PRM_TABLE_NAME["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_TABLE_NAME", defaultValue='${val}'`,
            name: 'PRM_TABLE_NAME'
        },
        {
            id: 'edit-fecha',
            pattern: /PRM_FECHA_RUTINA["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_FECHA_RUTINA", defaultValue='${val}'`,
            name: 'PRM_FECHA_RUTINA'
        },
        {
            id: 'edit-carpeta-output',
            pattern: /PRM_CARPETA_OUTPUT["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_CARPETA_OUTPUT", defaultValue='${val}'`,
            name: 'PRM_CARPETA_OUTPUT'
        },
        {
            id: 'edit-ruta-adls',
            pattern: /PRM_RUTA_ADLS_TABLES["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            replacement: (val) => `PRM_RUTA_ADLS_TABLES", defaultValue='${val}'`,
            name: 'PRM_RUTA_ADLS_TABLES'
        }
    ];

    let newScript = currentOutputScript;
    const changes = [];

    paramUpdates.forEach(param => {
        const value = document.getElementById(param.id)?.value;
        if (value) {
            newScript = newScript.replace(param.pattern, param.replacement(value));
            changes.push(param.name);
        }
    });

    currentOutputScript = newScript;
    setEditorContent('output-script', newScript);
    updateOutputStats();

    // Re-validate
    const validator = new EDVValidatorRiguroso();
    validationResult = validator.validate(currentOutputScript);
    updateValidationResults();

    // Re-generate diff with updated script
    generateDiff();

    alert(`✅ Script EDV re-generado con ${changes.length} cambios:\n${changes.join(', ')}`);
}

function resetParams() {
    updateParamsTable();
    alert('🔄 Parámetros restaurados a valores originales');
}

function downloadTxt() {
    downloadFile(currentOutputScript, 'txt');
}

// ===== KEYBOARD SHORTCUTS =====
document.addEventListener('keydown', (e) => {
    // Ctrl+Enter: Convert
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        convertScript();
    }

    // Ctrl+S: Download
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        if (currentOutputScript) {
            downloadOutput();
        }
    }
});

console.log('🚀 EDV Converter v1.0.0');
console.log('📚 Atajos: Ctrl+Enter (convertir), Ctrl+S (descargar)');

// ===== SYNTAX HIGHLIGHTING =====

/**
 * Get text content from contenteditable pre>code editor
 */
function getEditorContent(editorId) {
    const editor = document.getElementById(editorId);
    const codeElement = editor.querySelector('code');
    return codeElement ? codeElement.textContent : '';
}

/**
 * Set text content in contenteditable pre>code editor with syntax highlighting
 */
function setEditorContent(editorId, content) {
    const editor = document.getElementById(editorId);
    const codeElement = editor.querySelector('code');

    if (codeElement) {
        codeElement.textContent = content;
        Prism.highlightElement(codeElement);
    }
}

function initializeSyntaxHighlighting() {
    // Input editor
    const inputEditor = document.getElementById('input-script');
    const inputCode = inputEditor.querySelector('code');

    // Event listener para actualizar highlighting en tiempo real
    inputEditor.addEventListener('input', () => {
        // Re-highlight después de cada cambio
        if (inputCode) {
            Prism.highlightElement(inputCode);
        }
        // Update stats
        updateInputStats();
    });

    // Prevent pasting HTML, only paste plain text
    inputEditor.addEventListener('paste', (e) => {
        e.preventDefault();
        const text = e.clipboardData.getData('text/plain');
        document.execCommand('insertText', false, text);
    });

    // Actualizar highlight inicial si hay contenido
    if (inputCode && inputCode.textContent) {
        Prism.highlightElement(inputCode);
    }
}

// ===== MULTI-PERIODO =====

function setTodayMultiperiodo() {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const dateStr = `${year}${month}${day}`;

    document.getElementById('multiperiodo-fecha').value = dateStr;
}

/**
 * Calculate previous months from a date in YYYYMMDD format
 * @param {string} dateStr - Date in YYYYMMDD format (e.g., "20250131")
 * @param {number} monthsBack - Number of months to go back
 * @returns {string} - Date in YYYYMMDD format
 */
function getPreviousMonth(dateStr, monthsBack) {
    const year = parseInt(dateStr.substring(0, 4));
    const month = parseInt(dateStr.substring(4, 6));
    const day = parseInt(dateStr.substring(6, 8));

    const date = new Date(year, month - 1, day);
    date.setMonth(date.getMonth() - monthsBack);

    const newYear = date.getFullYear();
    const newMonth = String(date.getMonth() + 1).padStart(2, '0');
    const newDay = String(date.getDate()).padStart(2, '0');

    return `${newYear}${newMonth}${newDay}`;
}

function generateMultiperiodo() {
    if (!currentOutputScript) {
        alert('⚠️ Primero debes convertir un script DDV a EDV');
        return;
    }

    const fecha = document.getElementById('multiperiodo-fecha').value;

    if (!fecha || !/^\d{8}$/.test(fecha)) {
        alert('⚠️ Ingresa una fecha válida en formato YYYYMMDD (8 dígitos)');
        return;
    }

    // Calculate 3 periods: current, -1 month, -2 months
    const periodos = [
        { offset: 0, label: 'Periodo Actual', fecha: fecha },
        { offset: 1, label: 'Periodo -1 Mes', fecha: getPreviousMonth(fecha, 1) },
        { offset: 2, label: 'Periodo -2 Meses', fecha: getPreviousMonth(fecha, 2) }
    ];

    const scripts = [];

    periodos.forEach(periodo => {
        // Replace PRM_FECHA_RUTINA in the script
        let periodScript = currentOutputScript.replace(
            /PRM_FECHA_RUTINA["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            `PRM_FECHA_RUTINA", defaultValue='${periodo.fecha}'`
        );

        scripts.push({
            label: periodo.label,
            fecha: periodo.fecha,
            codmes: periodo.fecha.substring(0, 6), // YYYYMM
            script: periodScript
        });
    });

    // Display results
    const resultsDiv = document.getElementById('multiperiodo-results');
    const listDiv = document.getElementById('multiperiodo-list');

    listDiv.innerHTML = scripts.map((s, i) => `
        <div style="background: #f8f9fa; padding: 1.5rem; border-radius: 8px; border: 1px solid #dee2e6;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                <div>
                    <h5 style="margin: 0; color: #0066cc;">${s.label}</h5>
                    <p style="margin: 0.25rem 0 0 0; color: #666; font-family: 'JetBrains Mono', monospace;">
                        📅 Fecha: ${s.fecha} | 📊 CODMES: ${s.codmes}
                    </p>
                </div>
                <div style="display: flex; gap: 0.5rem;">
                    <button class="btn btn-sm btn-primary" onclick="downloadPeriodoScript(${i}, 'py')">
                        💾 .py
                    </button>
                    <button class="btn btn-sm btn-secondary" onclick="downloadPeriodoScript(${i}, 'txt')">
                        📄 .txt
                    </button>
                </div>
            </div>
            <div style="font-size: 0.875rem; color: #666;">
                ${s.script.split('\\n').length} líneas | ${(new Blob([s.script]).size / 1024).toFixed(2)} KB
            </div>
        </div>
    `).join('');

    resultsDiv.style.display = 'block';

    // Store scripts globally for download
    window.multiperiodoScripts = scripts;

    alert(`✅ ${scripts.length} scripts generados para los últimos 3 periodos`);
}

function downloadPeriodoScript(index, extension) {
    if (!window.multiperiodoScripts || !window.multiperiodoScripts[index]) {
        alert('⚠️ Error: Script no encontrado');
        return;
    }

    const scriptData = window.multiperiodoScripts[index];
    const baseFilename = detectScriptType();
    const filename = `${baseFilename}_${scriptData.codmes}.${extension}`;

    const blob = new Blob([scriptData.script], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);

    console.log(`💾 Script multi-periodo descargado: ${filename}`);
}


function updateParamsTab() {
    const el = document.getElementById("params-container");
    if (!el) return;
    if (!validationResult || !validationResult.parameters) {
        el.innerHTML = "<div class=\"log-empty\">No hay parámetros</div>";
        return;
    }
    const p = validationResult.parameters;
    const esc = (v) => escapeHtml(String(v ?? "-"));
    const rows = [];
    rows.push("<div class=\"log-item\"><strong>DDV</strong>: catalog=" + esc((p.ddv && p.ddv.catalog)) + ", schema=" + esc((p.ddv && p.ddv.schema)) + "</div>");
    rows.push("<div class=\"log-item\"><strong>EDV</strong>: catalog=" + esc((p.edv && p.edv.catalog)) + ", schema=" + esc((p.edv && p.edv.schema)) + "</div>");
    rows.push("<div class=\"log-item\"><strong>Destino</strong>: table_name=" + esc(p.destino?.table_name) + ", tabla_segunda=" + esc(p.destino?.tabla_segunda) + ", tmp=" + esc(p.destino?.tabla_segunda_tmp) + ", familia=" + esc(p.destino?.familia) + "</div>");
    rows.push("<div class=\"log-item\"><strong>Storage</strong>: container=" + esc((p.storage && p.storage.container)) + "</div>");
    el.innerHTML = rows.join("");
}

// ===== AI-POWERED CONVERSION =====

/**
 * Handle conversion method selection change
 */
function handleConversionMethodChange(event) {
    const method = event.target.value;
    const aiOptions = document.getElementById('ai-method-options');

    if (method === 'ai') {
        aiOptions.style.display = 'block';

        // Verificar si aiAnalyzer está configurado
        if (typeof aiAnalyzer !== 'undefined' && aiAnalyzer && !aiAnalyzer.isConfigured()) {
            // Mostrar notificación y abrir modal de configuración
            setTimeout(() => {
                if (confirm('🤖 Para usar la conversión con IA necesitas configurar tu API key.\n\n¿Deseas configurarla ahora?')) {
                    // Abrir modal de configuración de IA (de la pestaña de verificación)
                    if (typeof openAIConfigModal === 'function') {
                        openAIConfigModal();
                    } else {
                        alert('⚠️ Ve a la pestaña "Verificación de Similitud" para configurar tu API key de IA.');
                    }
                }
            }, 100);
        }
    } else {
        aiOptions.style.display = 'none';
    }

    console.log(`[INFO] Método de conversión seleccionado: ${method}`);
}

/**
 * Convert script using AI
 */
async function convertWithAI() {
    // Verificar que aiAnalyzer está disponible
    if (typeof aiAnalyzer === 'undefined' || !aiAnalyzer) {
        throw new Error('AI Analyzer no está inicializado. Asegúrate de que ai_ui.js esté cargado.');
    }

    // Verificar configuración - si no está configurada, abrir modal
    if (!aiAnalyzer.isConfigured()) {
        // Abrir modal de configuración
        if (typeof openAIConfigModal === 'function') {
            openAIConfigModal();
        }
        throw new Error('⚠️ Debes configurar tu API key de IA primero.\n\nSe ha abierto la ventana de configuración.');
    }

    // Obtener opciones
    const applyOptimizations = document.getElementById('ai-apply-optimizations')?.checked || false;
    const scriptName = extractScriptName(currentInputScript) || 'script.py';

    console.log(`[AI] Generando script EDV con IA (optimizaciones: ${applyOptimizations})`);

    // Llamar a IA
    const result = await aiAnalyzer.generateEDVScript(currentInputScript, {
        applyOptimizations: applyOptimizations,
        scriptName: scriptName
    });

    // Verificar si hubo error
    if (result.error) {
        throw new Error(`Error de IA: ${result.error}\n\nRespuesta raw:\n${result.rawResponse}`);
    }

    if (!result.edvScript) {
        throw new Error('La IA no generó un script EDV válido');
    }

    // Crear un resultado de conversión compatible con el formato esperado
    return {
        edvScript: result.edvScript,
        log: result.changes.map(change => ({
            type: 'change',
            message: change.description || change,
            details: change.before && change.after ? `ANTES: ${change.before}\nDESPUÉS: ${change.after}` : ''
        })),
        warnings: result.warnings.map(w => ({
            type: 'warning',
            message: w
        })),
        aiGenerated: true,
        aiSummary: result.summary,
        aiOptimizationsApplied: result.optimizationsApplied || [],
        aiValidationChecklist: result.validationChecklist || {}
    };
}

/**
 * Extract script name from content (look for file name patterns)
 */
function extractScriptName(scriptContent) {
    // Try to find script name in comments
    const nameMatch = scriptContent.match(/(?:#|""").*?([A-Z_]+\.py)/i);
    if (nameMatch) {
        return nameMatch[1];
    }

    // Try to find table name
    const tableMatch = scriptContent.match(/PRM_TABLE_NAME.*?['"]([A-Z_]+)['"]/);
    if (tableMatch) {
        return `${tableMatch[1]}_EDV.py`;
    }

    return 'script_EDV.py';
}



