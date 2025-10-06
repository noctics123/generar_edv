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

    // Computar diff real usando LCS
    const lines1 = verifyScript1Content.split('\n');
    const lines2 = verifyScript2Content.split('\n');

    const diffOps = computeDiff(lines1, lines2);

    // Renderizar ambos lados con mapeo correcto
    code1.innerHTML = renderDiffSide(diffOps, 'left');
    code2.innerHTML = renderDiffSide(diffOps, 'right');

    // Setup click handlers para resaltar líneas equivalentes
    setupLineClickHandlers();

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
 * Compute diff using Myers algorithm (similar to git diff)
 * Returns array of diff operations
 */
function computeDiff(linesA, linesB) {
    const diff = [];
    const n = linesA.length;
    const m = linesB.length;

    // LCS (Longest Common Subsequence) usando programación dinámica
    const lcs = Array(n + 1).fill(null).map(() => Array(m + 1).fill(0));

    for (let i = 1; i <= n; i++) {
        for (let j = 1; j <= m; j++) {
            if (linesA[i - 1].trim() === linesB[j - 1].trim()) {
                lcs[i][j] = lcs[i - 1][j - 1] + 1;
            } else {
                lcs[i][j] = Math.max(lcs[i - 1][j], lcs[i][j - 1]);
            }
        }
    }

    // Backtrack para construir el diff
    let i = n, j = m;
    const result = [];

    while (i > 0 || j > 0) {
        if (i > 0 && j > 0 && linesA[i - 1].trim() === linesB[j - 1].trim()) {
            result.unshift({ type: 'same', lineA: i - 1, lineB: j - 1, content: linesA[i - 1] });
            i--;
            j--;
        } else if (j > 0 && (i === 0 || lcs[i][j - 1] >= lcs[i - 1][j])) {
            result.unshift({ type: 'added', lineA: null, lineB: j - 1, content: linesB[j - 1] });
            j--;
        } else if (i > 0) {
            result.unshift({ type: 'removed', lineA: i - 1, lineB: null, content: linesA[i - 1] });
            i--;
        }
    }

    return result;
}

/**
 * Render diff for one side with proper mapping
 */
function renderDiffSide(diffOps, side) {
    let html = '';
    let lineNumberA = 0;
    let lineNumberB = 0;

    diffOps.forEach((op, index) => {
        let cssClass = '';
        let marker = ' ';
        let lineNumber = '';
        let mappedIndex = null;

        if (op.type === 'same') {
            cssClass = 'line-same';
            marker = '='; // Mostrar = para líneas iguales
            lineNumberA++;
            lineNumberB++;
            lineNumber = side === 'left' ? lineNumberA : lineNumberB;
            mappedIndex = index; // Índice en el array de diff
        } else if (op.type === 'added') {
            lineNumberB++;
            if (side === 'left') {
                // En el lado izquierdo, las líneas agregadas en B no se muestran
                return;
            } else {
                cssClass = 'line-added';
                marker = '+';
                lineNumber = lineNumberB;
                mappedIndex = index;
            }
        } else if (op.type === 'removed') {
            lineNumberA++;
            if (side === 'left') {
                cssClass = 'line-removed';
                marker = '-';
                lineNumber = lineNumberA;
                mappedIndex = index;
            } else {
                // En el lado derecho, las líneas removidas de A no se muestran
                return;
            }
        }

        const highlightedLine = highlightPythonSyntax(op.content);

        html += `<div class="diff-line ${cssClass}" data-diff-index="${mappedIndex}">`;
        html += `<span class="line-number">${lineNumber}</span>`;
        html += `<span class="line-marker">${marker}</span>`;
        html += `<span class="line-content">${highlightedLine}</span>`;
        html += `</div>\n`;
    });

    return html;
}

/**
 * Setup click handlers para resaltar líneas equivalentes
 */
function setupLineClickHandlers() {
    const code1 = document.getElementById('comparison-code-1');
    const code2 = document.getElementById('comparison-code-2');

    if (!code1 || !code2) return;

    // Click en panel izquierdo
    code1.addEventListener('click', (e) => {
        const line = e.target.closest('.diff-line');
        if (!line) return;

        const diffIndex = line.getAttribute('data-diff-index');
        highlightEquivalentLines(diffIndex);
    });

    // Click en panel derecho
    code2.addEventListener('click', (e) => {
        const line = e.target.closest('.diff-line');
        if (!line) return;

        const diffIndex = line.getAttribute('data-diff-index');
        highlightEquivalentLines(diffIndex);
    });
}

/**
 * Resalta líneas equivalentes en ambos paneles usando diff index
 */
function highlightEquivalentLines(diffIndex) {
    const code1 = document.getElementById('comparison-code-1');
    const code2 = document.getElementById('comparison-code-2');

    // Remover highlights anteriores
    code1.querySelectorAll('.line-highlighted').forEach(el => el.classList.remove('line-highlighted'));
    code2.querySelectorAll('.line-highlighted').forEach(el => el.classList.remove('line-highlighted'));

    // Buscar líneas con el mismo diff-index
    const line1 = code1.querySelector(`.diff-line[data-diff-index="${diffIndex}"]`);
    const line2 = code2.querySelector(`.diff-line[data-diff-index="${diffIndex}"]`);

    if (line1) {
        line1.classList.add('line-highlighted');
        scrollToLine(line1, 'left');
    }

    if (line2) {
        line2.classList.add('line-highlighted');
        scrollToLine(line2, 'right');
    }
}

/**
 * Scroll suave a una línea específica
 */
function scrollToLine(lineElement, side) {
    const container = document.getElementById(side === 'left' ? 'comparison-content-1' : 'comparison-content-2');
    if (!container) return;

    const containerRect = container.getBoundingClientRect();
    const lineRect = lineElement.getBoundingClientRect();

    // Scroll solo si la línea no está visible
    if (lineRect.top < containerRect.top || lineRect.bottom > containerRect.bottom) {
        lineElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
}

/**
 * Escape HTML special characters
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Syntax highlighting para Python - VSCode Dark+ Theme
 */
function highlightPythonSyntax(code) {
    if (!code || code.trim() === '') return ' ';

    // Python keywords
    const keywords = /\b(def|class|if|elif|else|for|while|return|import|from|as|try|except|finally|with|lambda|yield|async|await|pass|break|continue|raise|assert|del|global|nonlocal|in|is|not|and|or|None|True|False)\b/g;

    // Built-in functions
    const builtins = /\b(print|len|range|str|int|float|list|dict|set|tuple|bool|type|isinstance|enumerate|zip|map|filter|sorted|sum|max|min|abs|all|any|open|read|write|split|join|format|append|extend|insert|remove|pop|get|keys|values|items)\b/g;

    // Strings (single, double, triple)
    const strings = /("""[\s\S]*?"""|'''[\s\S]*?'''|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/g;

    // Comments
    const comments = /(#.*$)/gm;

    // Numbers
    const numbers = /\b(\d+\.?\d*)\b/g;

    // Function calls (function followed by parenthesis)
    const functionCalls = /\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?=\()/g;

    // Decorators
    const decorators = /(@[a-zA-Z_][a-zA-Z0-9_]*)/g;

    // Self parameter
    const selfParam = /\bself\b/g;

    // Escape HTML first
    let highlighted = escapeHtml(code);

    // Apply highlighting in order (most specific to least specific)
    // 1. Comments (highest priority)
    highlighted = highlighted.replace(comments, '<span class="token-comment">$1</span>');

    // 2. Strings (before keywords to avoid highlighting keywords in strings)
    highlighted = highlighted.replace(strings, '<span class="token-string">$1</span>');

    // 3. Decorators
    highlighted = highlighted.replace(decorators, '<span class="token-decorator">$1</span>');

    // 4. Keywords
    highlighted = highlighted.replace(keywords, '<span class="token-keyword">$1</span>');

    // 5. Built-in functions
    highlighted = highlighted.replace(builtins, '<span class="token-builtin">$1</span>');

    // 6. Function calls
    highlighted = highlighted.replace(functionCalls, '<span class="token-function">$1</span>');

    // 7. Numbers
    highlighted = highlighted.replace(numbers, '<span class="token-number">$1</span>');

    // 8. Self parameter
    highlighted = highlighted.replace(selfParam, '<span class="token-self">self</span>');

    return highlighted;
}
