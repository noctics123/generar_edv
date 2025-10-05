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
    console.log('✅ EDV Converter initialized');
});

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
        document.getElementById('input-script').value = content;
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
        document.getElementById('input-script').value = content;
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
    document.getElementById('input-script').value = '';
    document.getElementById('file-name').textContent = 'Ningún archivo seleccionado';
    document.getElementById('file-upload').value = '';
    currentInputScript = '';
    updateInputStats();
    console.log('🗑️ Input limpiado');
}

// ===== STATS =====
function updateInputStats() {
    const script = document.getElementById('input-script').value;
    currentInputScript = script;

    const lines = script.split('\n').length;
    const bytes = new Blob([script]).size;
    const kb = (bytes / 1024).toFixed(2);

    document.getElementById('input-lines').textContent = `${lines} líneas`;
    document.getElementById('input-size').textContent = `${kb} KB`;
}

function updateOutputStats() {
    const script = currentOutputScript;

    const lines = script.split('\n').length;
    const bytes = new Blob([script]).size;
    const kb = (bytes / 1024).toFixed(2);

    document.getElementById('output-lines').textContent = `${lines} líneas`;
    document.getElementById('output-size').textContent = `${kb} KB`;
}

// ===== CONVERSION =====
function convertScript() {
    // Validate input
    if (!currentInputScript || currentInputScript.trim() === '') {
        alert('⚠️ Por favor carga o pega un script DDV primero');
        return;
    }

    console.log('🔄 Iniciando conversión...');

    // Show loading state
    const convertBtn = document.getElementById('convert-btn');
    const originalText = convertBtn.innerHTML;
    convertBtn.innerHTML = '⏳ Convirtiendo...';
    convertBtn.disabled = true;

    // Simulate async processing
    setTimeout(() => {
        try {
            // Convert
            const converter = new EDVConverter();
            conversionResult = converter.convert(currentInputScript);
            currentOutputScript = conversionResult.edvScript;

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

        } catch (error) {
            console.error('❌ Error en conversión:', error);

            // Mostrar el error en la UI para depuración
            const errorLogEl = document.getElementById('log-errors');
            if (errorLogEl) {
                errorLogEl.innerHTML = `<div class="log-item error"><strong>Error:</strong> ${escapeHtml(error.message)}<br><pre>${escapeHtml(error.stack)}</pre></div>`;
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
    }, 500);
}

// ===== UPDATE RESULTS =====
function updateConversionResults() {
    // Update stats
    document.getElementById('stat-changes').textContent = conversionResult.log.length;
    document.getElementById('stat-warnings').textContent = conversionResult.warnings.length;

    // Update output editor
    document.getElementById('output-script').value = currentOutputScript;
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
    // Changes
    const changesEl = document.getElementById('log-changes');
    if (conversionResult.log.length === 0) {
        changesEl.innerHTML = '<div class="log-empty">No hay cambios registrados</div>';
    } else {
        changesEl.innerHTML = conversionResult.log
            .map(log => `<div class="log-item">${escapeHtml(log)}</div>`)
            .join('');
    }

    // Warnings
    const warningsEl = document.getElementById('log-warnings');
    if (conversionResult.warnings.length === 0) {
        warningsEl.innerHTML = '<div class="log-empty">No hay advertencias</div>';
    } else {
        warningsEl.innerHTML = conversionResult.warnings
            .map(warning => `<div class="log-item">⚠️ ${escapeHtml(warning)}</div>`)
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
    const outputScript = document.getElementById('output-script');
    outputScript.select();
    document.execCommand('copy');

    // Visual feedback
    const btn = document.getElementById('copy-output');
    const originalText = btn.innerHTML;
    btn.innerHTML = '✅ Copiado';
    setTimeout(() => {
        btn.innerHTML = originalText;
    }, 2000);

    console.log('📋 Script copiado al portapapeles');
}

function downloadOutput() {
    if (!currentOutputScript) {
        alert('⚠️ No hay script para descargar');
        return;
    }

    // Detect script type from original filename or content
    let filename = 'script_edv.py';

    if (currentInputScript.includes('MATRIZTRANSACCIONAGENTE')) {
        filename = 'MATRIZVARIABLES_HM_MATRIZTRANSACCIONAGENTE_EDV.py';
    } else if (currentInputScript.includes('MATRIZTRANSACCIONCAJERO')) {
        filename = 'MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO_EDV.py';
    } else if (currentInputScript.includes('MATRIZTRANSACCIONPOSMACROGIRO')) {
        filename = 'HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py';
    }

    // Create blob and download
    const blob = new Blob([currentOutputScript], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);

    console.log(`💾 Script descargado: ${filename}`);
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

    tbody.innerHTML = rows.join('');
}

function extractParamsFromScripts(ddvScript, edvScript) {
    const extractParam = (script, param) => {
        const pattern = new RegExp(`${param}.*?defaultValue\\s*=\\s*['"]([^'"]+)['"]`);
        const match = script.match(pattern);
        return match ? match[1] : null;
    };

    const extractConst = (script, constName) => {
        const pattern = new RegExp(`${constName}\\s*=\\s*['"]([^'"]+)['"]`);
        const match = script.match(pattern);
        return match ? match[1] : null;
    };

    return {
        ddv: {
            container: extractConst(ddvScript, 'CONS_CONTAINER_NAME'),
            storageAccount: extractParam(ddvScript, 'PRM_STORAGE_ACCOUNT_DDV'),
            catalogDDV: extractParam(ddvScript, 'PRM_CATALOG_NAME'),
            schemaDDV: extractParam(ddvScript, 'PRM_ESQUEMA_TABLA_DDV'),
            tableName: extractParam(ddvScript, 'PRM_TABLE_NAME'),
            fecha: extractParam(ddvScript, 'PRM_FECHA_RUTINA')
        },
        edv: {
            container: extractConst(edvScript, 'CONS_CONTAINER_NAME'),
            storageAccount: extractParam(edvScript, 'PRM_STORAGE_ACCOUNT_DDV'),
            catalogDDV: extractParam(edvScript, 'PRM_CATALOG_NAME'),
            catalogEDV: extractParam(edvScript, 'PRM_CATALOG_NAME_EDV'),
            schemaDDV: extractParam(edvScript, 'PRM_ESQUEMA_TABLA_DDV'),
            schemaEDV: extractParam(edvScript, 'PRM_ESQUEMA_TABLA_EDV'),
            tableName: extractParam(edvScript, 'PRM_TABLE_NAME'),
            fecha: extractParam(edvScript, 'PRM_FECHA_RUTINA')
        }
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
    // Get all edited values
    const container = document.getElementById('edit-container')?.value;
    const storageAccount = document.getElementById('edit-storage')?.value;
    const catalogDDV = document.getElementById('edit-catalog-ddv')?.value;
    const catalogEDV = document.getElementById('edit-catalog-edv')?.value;
    const schemaDDV = document.getElementById('edit-schema-ddv')?.value;
    const schemaEDV = document.getElementById('edit-schema-edv')?.value;
    const tableName = document.getElementById('edit-table-name')?.value;
    const fecha = document.getElementById('edit-fecha')?.value;

    // Update script with new values
    let newScript = currentOutputScript;
    let changes = [];

    if (container) {
        newScript = newScript.replace(
            /CONS_CONTAINER_NAME\s*=\s*["'][^'"]+["']/,
            `CONS_CONTAINER_NAME = "${container}"`
        );
        changes.push('CONS_CONTAINER_NAME');
    }

    if (storageAccount) {
        newScript = newScript.replace(
            /PRM_STORAGE_ACCOUNT_DDV["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            `PRM_STORAGE_ACCOUNT_DDV", defaultValue='${storageAccount}'`
        );
        changes.push('PRM_STORAGE_ACCOUNT_DDV');
    }

    if (catalogDDV) {
        newScript = newScript.replace(
            /PRM_CATALOG_NAME["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            `PRM_CATALOG_NAME", defaultValue='${catalogDDV}'`
        );
        changes.push('PRM_CATALOG_NAME');
    }

    if (catalogEDV) {
        newScript = newScript.replace(
            /PRM_CATALOG_NAME_EDV["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            `PRM_CATALOG_NAME_EDV", defaultValue='${catalogEDV}'`
        );
        changes.push('PRM_CATALOG_NAME_EDV');
    }

    if (schemaDDV) {
        newScript = newScript.replace(
            /PRM_ESQUEMA_TABLA_DDV["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            `PRM_ESQUEMA_TABLA_DDV", defaultValue='${schemaDDV}'`
        );
        changes.push('PRM_ESQUEMA_TABLA_DDV');
    }

    if (schemaEDV) {
        newScript = newScript.replace(
            /PRM_ESQUEMA_TABLA_EDV["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            `PRM_ESQUEMA_TABLA_EDV", defaultValue='${schemaEDV}'`
        );
        changes.push('PRM_ESQUEMA_TABLA_EDV');
    }

    if (tableName) {
        newScript = newScript.replace(
            /PRM_TABLE_NAME["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            `PRM_TABLE_NAME", defaultValue='${tableName}'`
        );
        changes.push('PRM_TABLE_NAME');
    }

    if (fecha) {
        newScript = newScript.replace(
            /PRM_FECHA_RUTINA["'],\s*defaultValue\s*=\s*['"][^'"]+['"]/,
            `PRM_FECHA_RUTINA", defaultValue='${fecha}'`
        );
        changes.push('PRM_FECHA_RUTINA');
    }

    currentOutputScript = newScript;
    document.getElementById('output-script').value = newScript;
    updateOutputStats();

    // Re-validate
    const validator = new EDVValidatorRiguroso();
    validationResult = validator.validate(currentOutputScript);
    updateValidationResults();

    alert(`✅ Script EDV re-generado con ${changes.length} cambios:\n${changes.join(', ')}`);
}

function resetParams() {
    updateParamsTable();
    alert('🔄 Parámetros restaurados a valores originales');
}

function downloadTxt() {
    if (!currentOutputScript) {
        alert('⚠️ No hay script para descargar');
        return;
    }

    let filename = 'script_edv.txt';

    if (currentInputScript.includes('MATRIZTRANSACCIONAGENTE')) {
        filename = 'MATRIZVARIABLES_HM_MATRIZTRANSACCIONAGENTE_EDV.txt';
    } else if (currentInputScript.includes('MATRIZTRANSACCIONCAJERO')) {
        filename = 'MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO_EDV.txt';
    } else if (currentInputScript.includes('MATRIZTRANSACCIONPOSMACROGIRO')) {
        filename = 'HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.txt';
    }

    const blob = new Blob([currentOutputScript], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);

    console.log(`📄 Script descargado como TXT: ${filename}`);
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



