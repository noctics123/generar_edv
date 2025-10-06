/**
 * Comparison Modal - Side-by-Side Script Comparison
 * ===================================================
 *
 * Funcionalidad para comparar scripts lado a lado en pantalla completa.
 */

// Inicializar modal de comparación
function initializeComparisonModal() {
    const compareBtn = document.getElementById('compare-side-by-side-btn');
    const modal = document.getElementById('comparison-modal');
    const closeBtn = document.getElementById('comparison-modal-close');

    if (!compareBtn || !modal || !closeBtn) return;

    // Abrir modal
    compareBtn.addEventListener('click', openComparisonModal);

    // Cerrar modal
    closeBtn.addEventListener('click', closeComparisonModal);

    // Cerrar con ESC
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && modal.classList.contains('active')) {
            closeComparisonModal();
        }
    });

    // Cerrar al hacer click fuera del contenido
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            closeComparisonModal();
        }
    });

    // Sync scroll entre paneles
    setupSyncScroll();

    console.log('[OK] Comparison modal initialized');
}

/**
 * Abre el modal de comparación
 */
function openComparisonModal() {
    // Validar que hay scripts cargados
    if (!verifyScript1Content || !verifyScript2Content) {
        alert('[WARN] Debes cargar ambos scripts antes de comparar');
        return;
    }

    const modal = document.getElementById('comparison-modal');
    const code1 = document.getElementById('comparison-code-1');
    const code2 = document.getElementById('comparison-code-2');
    const header1 = document.getElementById('comparison-header-1');
    const header2 = document.getElementById('comparison-header-2');

    // Obtener nombres de archivos
    const script1Name = document.getElementById('verify-file-name-1').textContent;
    const script2Name = document.getElementById('verify-file-name-2').textContent;

    // Actualizar headers
    header1.textContent = script1Name;
    header2.textContent = script2Name;

    // Cargar código con syntax highlighting
    code1.textContent = verifyScript1Content;
    code2.textContent = verifyScript2Content;

    // Aplicar syntax highlighting
    Prism.highlightElement(code1);
    Prism.highlightElement(code2);

    // Mostrar modal
    modal.classList.add('active');
    document.body.style.overflow = 'hidden'; // Prevent background scroll

    addLogMessage('Comparacion lado a lado abierta', 'info');
}

/**
 * Cierra el modal de comparación
 */
function closeComparisonModal() {
    const modal = document.getElementById('comparison-modal');
    modal.classList.remove('active');
    document.body.style.overflow = ''; // Restore scroll

    addLogMessage('Comparacion cerrada', 'info');
}

/**
 * Configura scroll sincronizado entre paneles
 */
function setupSyncScroll() {
    const content1 = document.getElementById('comparison-content-1');
    const content2 = document.getElementById('comparison-content-2');

    if (!content1 || !content2) return;

    let isSyncing = false;

    content1.addEventListener('scroll', () => {
        if (isSyncing) return;
        isSyncing = true;
        content2.scrollTop = content1.scrollTop;
        content2.scrollLeft = content1.scrollLeft;
        setTimeout(() => isSyncing = false, 10);
    });

    content2.addEventListener('scroll', () => {
        if (isSyncing) return;
        isSyncing = true;
        content1.scrollTop = content2.scrollTop;
        content1.scrollLeft = content2.scrollLeft;
        setTimeout(() => isSyncing = false, 10);
    });
}
