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

    // REMOVED: Sync scroll - ahora permitimos scroll independiente
    // setupSyncScroll();

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

    // Comparar y resaltar diferencias
    const lines1 = verifyScript1Content.split('\n');
    const lines2 = verifyScript2Content.split('\n');

    code1.innerHTML = highlightDifferences(lines1, lines2, 'left');
    code2.innerHTML = highlightDifferences(lines2, lines1, 'right');

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
 * Highlight differences between two sets of lines
 * Returns HTML with VSCode Dark+ syntax highlighting and difference markers
 */
function highlightDifferences(linesA, linesB, side) {
    const maxLines = Math.max(linesA.length, linesB.length);
    let html = '';

    for (let i = 0; i < maxLines; i++) {
        const lineA = linesA[i] || '';
        const lineB = linesB[i] || '';
        const lineNumber = i + 1;

        // Determine if line is different, added, or removed
        let cssClass = 'line-same';
        let marker = ' ';

        if (i >= linesB.length) {
            // Line only exists in A (removed in B perspective, added in A perspective)
            cssClass = side === 'left' ? 'line-added' : 'line-removed';
            marker = side === 'left' ? '+' : '-';
        } else if (i >= linesA.length) {
            // Line only exists in B (added in B perspective, removed in A perspective)
            cssClass = side === 'left' ? 'line-removed' : 'line-added';
            marker = side === 'left' ? '-' : '+';
        } else if (lineA.trim() !== lineB.trim()) {
            // Lines exist but are different
            cssClass = 'line-modified';
            marker = '~';
        }

        // Escape HTML
        const escapedLine = escapeHtml(lineA);

        html += `<div class="diff-line ${cssClass}">`;
        html += `<span class="line-number">${lineNumber}</span>`;
        html += `<span class="line-marker">${marker}</span>`;
        html += `<span class="line-content">${escapedLine || ' '}</span>`;
        html += `</div>\n`;
    }

    return html;
}

/**
 * Escape HTML special characters
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
