/**
 * Verification UI Functions - Interfaz de Usuario para Verificación
 * ==================================================================
 *
 * Funciones para manejar la sección de verificación de similitud
 * independiente en la app web.
 */

// State para verificación
let verifyScript1Content = '';
let verifyScript2Content = '';

/**
 * Agrega mensaje al log visual
 */
function addLogMessage(message, type = 'info') {
    const logContainer = document.getElementById('verification-log');
    const logWrapper = document.getElementById('verification-log-container');

    if (!logContainer || !logWrapper) return;

    // Mostrar el contenedor de log
    logWrapper.style.display = 'block';

    // Colores según tipo
    const colors = {
        'info': '#4ec9b0',
        'success': '#4ec9b0',
        'warn': '#dcdcaa',
        'error': '#f48771',
        'verify': '#569cd6'
    };

    const color = colors[type] || '#d4d4d4';

    // Timestamp
    const now = new Date();
    const timestamp = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`;

    // Crear elemento de log
    const logEntry = document.createElement('div');
    logEntry.style.marginBottom = '0.25rem';
    logEntry.style.color = color;
    logEntry.innerHTML = `<span style="color: #808080;">[${timestamp}]</span> ${message}`;

    logContainer.appendChild(logEntry);

    // Auto-scroll al final
    logContainer.parentElement.scrollTop = logContainer.parentElement.scrollHeight;
}

/**
 * Limpia el log visual
 */
function clearLog() {
    const logContainer = document.getElementById('verification-log');
    if (logContainer) {
        logContainer.innerHTML = '';
    }
}

/**
 * Inicializa event listeners para sección de verificación
 */
function initializeVerificationSection() {
    // File uploads
    document.getElementById('verify-file-1').addEventListener('change', (e) => handleVerifyFileUpload(e, 1));
    document.getElementById('verify-file-2').addEventListener('change', (e) => handleVerifyFileUpload(e, 2));

    // Clear buttons
    document.getElementById('verify-clear-1').addEventListener('click', () => clearVerifyScript(1));
    document.getElementById('verify-clear-2').addEventListener('click', () => clearVerifyScript(2));

    // Script editors
    document.getElementById('verify-script-1').addEventListener('input', () => updateVerifyStats(1));
    document.getElementById('verify-script-2').addEventListener('input', () => updateVerifyStats(2));

    // Quick actions
    document.getElementById('verify-use-current-conversion').addEventListener('click', useCurrentConversion);
    document.getElementById('verify-load-examples').addEventListener('click', loadVerificationExamples);
    document.getElementById('verify-swap-scripts').addEventListener('click', swapVerifyScripts);

    // Main verify button
    document.getElementById('verify-scripts-btn').addEventListener('click', verifyScriptsSimilarity);

    // Clear log button
    const clearLogBtn = document.getElementById('clear-log-btn');
    if (clearLogBtn) {
        clearLogBtn.addEventListener('click', clearLog);
    }

    // Syntax highlighting para editors
    initializeVerifyEditorHighlighting();

    console.log('[OK] Verification section initialized');
}

/**
 * Maneja carga de archivos para verificación
 */
function handleVerifyFileUpload(event, scriptNumber) {
    const file = event.target.files[0];
    if (!file) return;

    if (!file.name.endsWith('.py')) {
        alert('[WARN] Por favor selecciona un archivo .py');
        return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
        const content = e.target.result;
        setVerifyEditorContent(`verify-script-${scriptNumber}`, content);

        if (scriptNumber === 1) {
            verifyScript1Content = content;
        } else {
            verifyScript2Content = content;
        }

        updateVerifyStats(scriptNumber);
        document.getElementById(`verify-file-name-${scriptNumber}`).textContent = file.name;

        // Update UI to show file loaded
        const card = document.querySelector(`.script-upload-card:nth-of-type(${scriptNumber}) .file-info-card`);
        if (card) card.classList.add('has-file');

        const clearBtn = document.getElementById(`verify-clear-${scriptNumber}`);
        if (clearBtn) clearBtn.style.display = 'block';

        console.log(`[OK] Script ${scriptNumber} cargado: ${file.name}`);
    };
    reader.readAsText(file);
}

/**
 * Limpia un script de verificación
 */
function clearVerifyScript(scriptNumber) {
    setVerifyEditorContent(`verify-script-${scriptNumber}`, '');
    document.getElementById(`verify-file-name-${scriptNumber}`).textContent = 'Ningún archivo seleccionado';
    document.getElementById(`verify-file-${scriptNumber}`).value = '';

    if (scriptNumber === 1) {
        verifyScript1Content = '';
    } else {
        verifyScript2Content = '';
    }

    // Update UI to show no file
    const card = document.querySelector(`.script-upload-card:nth-of-type(${scriptNumber}) .file-info-card`);
    if (card) card.classList.remove('has-file');

    const clearBtn = document.getElementById(`verify-clear-${scriptNumber}`);
    if (clearBtn) clearBtn.style.display = 'none';

    updateVerifyStats(scriptNumber);
    console.log(`[CLEAR] Script ${scriptNumber} limpiado`);
}

/**
 * Actualiza estadísticas de un script de verificación
 */
function updateVerifyStats(scriptNumber) {
    const script = getVerifyEditorContent(`verify-script-${scriptNumber}`);

    if (scriptNumber === 1) {
        verifyScript1Content = script;
    } else {
        verifyScript2Content = script;
    }

    const lines = script.split('\n').length;
    const bytes = new Blob([script]).size;
    const kb = (bytes / 1024).toFixed(2);

    document.getElementById(`verify-lines-${scriptNumber}`).textContent = `${lines} líneas`;
    document.getElementById(`verify-size-${scriptNumber}`).textContent = `${kb} KB`;
}

/**
 * Usa la conversión actual (DDV → EDV)
 */
function useCurrentConversion() {
    if (!currentInputScript || !currentOutputScript) {
        alert('[WARN] Primero debes realizar una conversion DDV -> EDV en la seccion 1');
        return;
    }

    // Cargar DDV en script 1, EDV en script 2
    setVerifyEditorContent('verify-script-1', currentInputScript);
    setVerifyEditorContent('verify-script-2', currentOutputScript);

    verifyScript1Content = currentInputScript;
    verifyScript2Content = currentOutputScript;

    document.getElementById('verify-file-name-1').textContent = 'Script DDV (conversión actual)';
    document.getElementById('verify-file-name-2').textContent = 'Script EDV (conversión actual)';

    // Seleccionar modo DDV vs EDV
    document.querySelector('input[name="verification-mode"][value="ddv-edv"]').checked = true;

    updateVerifyStats(1);
    updateVerifyStats(2);

    // Scroll a sección de verificación
    document.getElementById('verification-section').scrollIntoView({
        behavior: 'smooth',
        block: 'start'
    });

    console.log('[OK] Scripts de conversion cargados para verificacion');
}

/**
 * Carga ejemplos para verificación
 */
async function loadVerificationExamples() {
    try {
        // Cargar Agente en script 1
        const response1 = await fetch('edv-converter-webapp/examples/agente/ddv.py');
        if (!response1.ok) throw new Error('No se pudo cargar ejemplo Agente');
        const script1 = await response1.text();

        // Cargar Cajero en script 2
        const response2 = await fetch('edv-converter-webapp/examples/cajero/ddv.py');
        if (!response2.ok) throw new Error('No se pudo cargar ejemplo Cajero');
        const script2 = await response2.text();

        // Cargar scripts
        setVerifyEditorContent('verify-script-1', script1);
        setVerifyEditorContent('verify-script-2', script2);

        verifyScript1Content = script1;
        verifyScript2Content = script2;

        document.getElementById('verify-file-name-1').textContent = 'Ejemplo: AGENTE';
        document.getElementById('verify-file-name-2').textContent = 'Ejemplo: CAJERO';

        // Seleccionar modo individual
        document.querySelector('input[name="verification-mode"][value="individual"]').checked = true;

        updateVerifyStats(1);
        updateVerifyStats(2);

        console.log('[OK] Ejemplos cargados: Agente vs Cajero');

    } catch (error) {
        console.error('Error cargando ejemplos:', error);
        alert(`[ERROR] Error al cargar ejemplos:\n${error.message}`);
    }
}

/**
 * Intercambia los scripts
 */
function swapVerifyScripts() {
    if (!verifyScript1Content && !verifyScript2Content) {
        alert('[WARN] Carga scripts primero antes de intercambiar');
        return;
    }

    // Intercambiar contenido
    const temp = verifyScript1Content;
    verifyScript1Content = verifyScript2Content;
    verifyScript2Content = temp;

    setVerifyEditorContent('verify-script-1', verifyScript1Content);
    setVerifyEditorContent('verify-script-2', verifyScript2Content);

    // Intercambiar nombres
    const name1 = document.getElementById('verify-file-name-1').textContent;
    const name2 = document.getElementById('verify-file-name-2').textContent;
    document.getElementById('verify-file-name-1').textContent = name2;
    document.getElementById('verify-file-name-2').textContent = name1;

    updateVerifyStats(1);
    updateVerifyStats(2);

    console.log('[INFO] Scripts intercambiados');
}

/**
 * Verifica similitud entre scripts
 */
async function verifyScriptsSimilarity() {
    // Validar que hay scripts
    if (!verifyScript1Content || !verifyScript2Content) {
        alert('[WARN] Debes cargar ambos scripts antes de verificar');
        return;
    }

    // Obtener modo
    const mode = document.querySelector('input[name="verification-mode"]:checked').value;
    const isDdvEdv = (mode === 'ddv-edv');

    console.log(`[VERIFY] Iniciando verificacion en modo: ${mode}`);
    addLogMessage(`Iniciando verificacion en modo: ${mode}`, 'verify');

    // Mostrar loading
    const verifyBtn = document.getElementById('verify-scripts-btn');
    const originalText = verifyBtn.innerHTML;
    verifyBtn.innerHTML = 'Verificando...';
    verifyBtn.disabled = true;

    try {
        // Modo local - sin servidor necesario
        console.log('[INFO] Usando verificador local (sin servidor)');
        addLogMessage('Usando verificador local (100% JavaScript)', 'info');

        // Obtener nombres de archivos
        const script1Name = document.getElementById('verify-file-name-1').textContent;
        const script2Name = document.getElementById('verify-file-name-2').textContent;

        addLogMessage(`Script 1: ${script1Name}`, 'info');
        addLogMessage(`Script 2: ${script2Name}`, 'info');

        // Llamar al verificador según modo
        let report;
        if (isDdvEdv) {
            console.log('[MODE] DDV vs EDV');
            addLogMessage('Modo: DDV vs EDV', 'verify');
            report = await verificationClient.verifyDDVtoEDV(
                verifyScript1Content,
                verifyScript2Content
            );
        } else {
            console.log('[MODE] Scripts Individuales');
            addLogMessage('Modo: Scripts Individuales', 'verify');
            report = await verificationClient.verifyScripts(
                verifyScript1Content,
                verifyScript2Content,
                {
                    script1_name: script1Name === 'Ningún archivo seleccionado' ? 'script1.py' : script1Name,
                    script2_name: script2Name === 'Ningún archivo seleccionado' ? 'script2.py' : script2Name,
                    is_ddv_edv: false
                }
            );
        }

        // Renderizar reporte
        verificationUI.render(report);

        // Mostrar resultados
        document.getElementById('verification-results').style.display = 'block';

        // Scroll a resultados
        document.getElementById('verification-results').scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });

        // Notificación según resultado
        const criticalCount = report.critical_count;
        const highCount = report.high_count;
        const score = report.similarity_score;

        addLogMessage('────────────────────────────────', 'info');
        addLogMessage(`Similitud: ${score}%`, 'verify');
        addLogMessage(`Total diferencias: ${report.total_differences}`, 'info');
        addLogMessage(`Criticas: ${criticalCount}, Altas: ${highCount}`, 'info');

        if (criticalCount === 0 && score >= 95) {
            console.log('[OK] Verificacion APROBADA');
            console.log(`[OK] Scripts Equivalentes (${score}%)`);
            addLogMessage(`✓ Scripts Equivalentes (${score}%)`, 'success');
        } else if (criticalCount === 0 && score >= 80) {
            console.log('[WARN] Verificacion con ADVERTENCIAS');
            console.log(`[WARN] Scripts Similares con Diferencias (${score}%)`);
            addLogMessage(`⚠ Scripts Similares con Diferencias (${score}%)`, 'warn');
        } else {
            console.log('[ERROR] Verificacion RECHAZADA');
            console.log(`[ERROR] Scripts NO Equivalentes - ${criticalCount} Errores Criticos`);
            addLogMessage(`✗ Scripts NO Equivalentes - ${criticalCount} Errores Criticos`, 'error');
        }

        // Log detallado
        console.log('[REPORTE]', {
            score: score,
            equivalent: report.is_equivalent,
            critical: criticalCount,
            high: highCount,
            total: report.total_differences
        });

    } catch (error) {
        console.error('[ERROR] Error en verificacion:', error);
        addLogMessage(`ERROR: ${error.message}`, 'error');

        // Logging detallado del error
        const errorDetails = {
            message: error.message,
            stack: error.stack,
            timestamp: new Date().toISOString(),
            mode: mode,
            script1Length: verifyScript1Content.length,
            script2Length: verifyScript2Content.length,
            serverUrl: verificationClient.serverUrl
        };

        console.group('[ERROR DETALLADO] Verificacion');
        console.error('Mensaje:', errorDetails.message);
        console.error('Timestamp:', errorDetails.timestamp);
        console.error('Modo:', errorDetails.mode);
        console.error('Script 1:', errorDetails.script1Length, 'caracteres');
        console.error('Script 2:', errorDetails.script2Length, 'caracteres');
        console.error('Servidor:', errorDetails.serverUrl);
        console.error('Stack trace:', errorDetails.stack);
        console.groupEnd();

        // Mostrar error detallado al usuario
        let userMessage = `[ERROR] Error al verificar scripts:\n\n${error.message}\n\n`;
        userMessage += `Detalles tecnicos:\n`;
        userMessage += `- Modo: ${mode}\n`;
        userMessage += `- Script 1: ${errorDetails.script1Length} caracteres\n`;
        userMessage += `- Script 2: ${errorDetails.script2Length} caracteres\n\n`;
        userMessage += `Verifica la consola del navegador (F12) para el stack trace completo.`;

        alert(userMessage);
    } finally {
        // Restaurar botón
        verifyBtn.innerHTML = originalText;
        verifyBtn.disabled = false;
    }
}

/**
 * Get editor content para verificación
 */
function getVerifyEditorContent(editorId) {
    const editor = document.getElementById(editorId);
    const codeElement = editor.querySelector('code');
    return codeElement ? codeElement.textContent : '';
}

/**
 * Set editor content para verificación
 */
function setVerifyEditorContent(editorId, content) {
    const editor = document.getElementById(editorId);
    const codeElement = editor.querySelector('code');

    if (codeElement) {
        codeElement.textContent = content;
        Prism.highlightElement(codeElement);
    }
}

/**
 * Inicializa syntax highlighting para editors de verificación
 */
function initializeVerifyEditorHighlighting() {
    ['verify-script-1', 'verify-script-2'].forEach(editorId => {
        const editor = document.getElementById(editorId);
        const codeElement = editor.querySelector('code');

        // Event listener para actualizar highlighting
        editor.addEventListener('input', () => {
            if (codeElement) {
                Prism.highlightElement(codeElement);
            }
        });

        // Prevent pasting HTML
        editor.addEventListener('paste', (e) => {
            e.preventDefault();
            const text = e.clipboardData.getData('text/plain');
            document.execCommand('insertText', false, text);
        });
    });
}

// Exportar funciones si se usa como módulo
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        initializeVerificationSection,
        useCurrentConversion,
        loadVerificationExamples,
        swapVerifyScripts,
        verifyScriptsSimilarity
    };
}
