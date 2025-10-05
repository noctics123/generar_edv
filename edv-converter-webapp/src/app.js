/**
 * EDV Converter - Main Application
 * BCP Analytics
 */

// Global state
let currentInputScript = '';
let currentOutputScript = '';
let conversionResult = null;
let validationResult = null;

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

function loadExample(exampleName) {
    // For now, show alert that examples need to be added
    alert(`🔜 Ejemplo "${exampleName}" próximamente.\n\nPor ahora, carga tu propio script o pega el código.`);

    // TODO: Load example from examples/ directory
    // fetch(`../examples/${exampleName}_ddv.py`)
    //     .then(response => response.text())
    //     .then(content => {
    //         document.getElementById('input-script').value = content;
    //         currentInputScript = content;
    //         updateInputStats();
    //     });
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

            // Validate con validador RIGUROSO
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
            alert('❌ Error al convertir el script. Revisa la consola para más detalles.');
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
    rows.push("<div class=\"log-item\"><strong>Destino</strong>: table_name=" + esc((p.destino && p.destino.table_name)) + ", tabla_segunda=" + esc((p.destino && p.destino.tabla_segunda)) + ", tmp=" + esc((p.destino && p.destino.tabla_segunda)_tmp) + ", familia=" + esc((p.destino && p.destino.familia)) + "</div>");
    rows.push("<div class=\"log-item\"><strong>Storage</strong>: container=" + esc((p.storage && p.storage.container)) + "</div>");
    el.innerHTML = rows.join("");
}



