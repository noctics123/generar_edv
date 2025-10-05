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
    // ... (listeners)
}

// ... (otras funciones) ...

// ===== CONVERSION =====
function convertScript() {
    // ... (código de conversión) ...
    setTimeout(() => {
        try {
            const converter = new EDVConverter();
            conversionResult = converter.convert(currentInputScript);
            currentOutputScript = conversionResult.edvScript;

            const validator = new EDVValidatorRiguroso(); // Usando el validador riguroso
            validationResult = validator.validate(currentOutputScript);

            // ... (actualización de UI) ...

        } catch (error) {
            console.error('❌ Error en conversión:', error);
            const errorLogEl = document.getElementById('log-errors');
            if (errorLogEl) {
                errorLogEl.innerHTML = `<div class="log-item error"><strong>Error:</strong> ${escapeHtml(error.message)}<br><pre>${escapeHtml(error.stack)}</pre></div>`;
            }
            document.getElementById('output-section').style.display = 'block';
            switchTab('log');
            alert('❌ Error al convertir el script. Revisa la pestaña "Log" para más detalles.');
        } finally {
            // ...
        }
    }, 500);
}

// ... (resto de app.js) ...