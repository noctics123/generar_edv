# 🌐 Integración del Verificador con App Web EDV Converter

## 📋 Guía de Integración

Este documento explica cómo integrar el **Script Verifier** con la aplicación web **EDV Converter** existente.

---

## 🎯 Objetivo

Agregar una nueva funcionalidad de **"Verificar Conversión"** que:
1. Compara script DDV original vs script EDV generado
2. Muestra reporte visual con diferencias
3. Indica si la conversión es válida (aprobada/rechazada)

---

## 📦 Pasos de Integración

### 1. Iniciar Servidor de Verificación

**Opción A: En terminal separada**
```bash
python verification_server.py
```

**Opción B: Como servicio en background (Windows)**
```bash
start /B python verification_server.py
```

**Opción C: Con quick start**
```bash
.\quick_start_verifier.bat
```

El servidor estará disponible en `http://localhost:5000`

---

### 2. Agregar Script JavaScript al HTML

Editar `index.html` y agregar antes del cierre de `</body>`:

```html
<!-- Verification Client -->
<script src="edv-converter-webapp/src/verification_client.js"></script>
```

---

### 3. Agregar Nueva Pestaña en HTML

Editar `index.html`, agregar nueva tab en la sección de tabs (después de "Log"):

```html
<!-- En la lista de tabs -->
<button class="tab" data-tab="verification">🔍 Verificación</button>

<!-- En el contenido de tabs -->
<div class="tab-content" id="tab-verification">
    <div id="verification-container"></div>
</div>
```

---

### 4. Modificar `app.js` - Agregar Botón de Verificación

En `app.js`, función `initializeEventListeners()`, agregar:

```javascript
// Verificar conversión
document.getElementById('verify-conversion-btn').addEventListener('click', verifyConversion);
```

Y agregar el botón en el HTML (sección de output):

```html
<button id="verify-conversion-btn" class="btn btn-primary">
    🔍 Verificar Conversión
</button>
```

---

### 5. Implementar Función de Verificación

Agregar al final de `app.js`:

```javascript
/**
 * Verifica la conversión DDV→EDV
 */
async function verifyConversion() {
    // Validar que hay scripts para verificar
    if (!currentInputScript || !currentOutputScript) {
        alert('⚠️ Debes convertir un script primero antes de verificar');
        return;
    }

    console.log('🔍 Iniciando verificación...');

    // Mostrar loading
    const verifyBtn = document.getElementById('verify-conversion-btn');
    const originalText = verifyBtn.innerHTML;
    verifyBtn.innerHTML = '⏳ Verificando...';
    verifyBtn.disabled = true;

    try {
        // Verificar conexión con servidor
        const isHealthy = await verificationClient.checkHealth();

        if (!isHealthy) {
            throw new Error('Servidor de verificación no disponible. Asegúrate de que verification_server.py esté corriendo.');
        }

        // Llamar al verificador
        const report = await verificationClient.verifyDDVtoEDV(
            currentInputScript,
            currentOutputScript
        );

        // Renderizar reporte
        verificationUI.render(report);

        // Cambiar a tab de verificación
        switchTab('verification');

        // Scroll a resultados
        document.getElementById('tab-verification').scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });

        // Notificación según resultado
        const criticalCount = report.critical_count;
        const score = report.similarity_score;

        if (criticalCount === 0 && score >= 95) {
            console.log('✅ Verificación APROBADA');
            showNotification('✅ Conversión Aprobada', 'success');
        } else if (criticalCount === 0 && score >= 80) {
            console.log('⚠️ Verificación con ADVERTENCIAS');
            showNotification('⚠️ Conversión con Advertencias - Revisar Diferencias', 'warning');
        } else {
            console.log('❌ Verificación RECHAZADA');
            showNotification('❌ Conversión Rechazada - Errores Críticos Encontrados', 'error');
        }

    } catch (error) {
        console.error('❌ Error en verificación:', error);
        alert(`❌ Error al verificar:\n\n${error.message}\n\nAsegúrate de que el servidor de verificación esté corriendo:\npython verification_server.py`);
    } finally {
        // Restaurar botón
        verifyBtn.innerHTML = originalText;
        verifyBtn.disabled = false;
    }
}

/**
 * Muestra notificación visual
 */
function showNotification(message, type) {
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 1rem 1.5rem;
        background: ${type === 'success' ? '#10b981' : type === 'warning' ? '#f59e0b' : '#ef4444'};
        color: white;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        z-index: 10000;
        animation: slideIn 0.3s ease-out;
    `;

    document.body.appendChild(notification);

    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease-in';
        setTimeout(() => notification.remove(), 300);
    }, 5000);
}

// Agregar estilos de animación
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from { transform: translateX(400px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    @keyframes slideOut {
        from { transform: translateX(0); opacity: 1; }
        to { transform: translateX(400px); opacity: 0; }
    }
`;
document.head.appendChild(style);
```

---

### 6. Agregar CSS para Tab de Verificación

Agregar en `edv-converter-webapp/src/styles/main.css`:

```css
/* Verification Tab Styles */
#verification-container {
    min-height: 400px;
}

#verification-container .verification-report {
    animation: fadeIn 0.5s ease-in;
}

@keyframes fadeIn {
    from { opacity: 0; transform: translateY(20px); }
    to { opacity: 1; transform: translateY(0); }
}

#verification-container .filter-btn {
    padding: 0.5rem 1rem;
    border: 2px solid #e5e7eb;
    background: white;
    border-radius: 6px;
    cursor: pointer;
    font-size: 0.875rem;
    transition: all 0.2s;
}

#verification-container .filter-btn:hover {
    border-color: #3b82f6;
    background: #eff6ff;
}

#verification-container .filter-btn.active {
    border-color: #3b82f6;
    background: #3b82f6;
    color: white;
}

#verification-container .difference-item {
    transition: all 0.2s;
}

#verification-container .difference-item:hover {
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    transform: translateY(-2px);
}

#verification-container .btn {
    padding: 0.5rem 1rem;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-size: 0.875rem;
    font-weight: 500;
    transition: all 0.2s;
}

#verification-container .btn-primary {
    background: #3b82f6;
    color: white;
}

#verification-container .btn-primary:hover {
    background: #2563eb;
}

#verification-container .btn-secondary {
    background: #6b7280;
    color: white;
}

#verification-container .btn-secondary:hover {
    background: #4b5563;
}

#verification-container .btn-sm {
    padding: 0.375rem 0.75rem;
    font-size: 0.8125rem;
}
```

---

## 🎨 Ubicación del Botón de Verificación

### Opción A: En la barra de acciones (recomendado)

Agregar botón junto a "Copiar" y "Descargar":

```html
<!-- En la sección de output -->
<div class="action-buttons" style="margin-top: 1.5rem;">
    <button id="copy-output" class="btn btn-secondary">📋 Copiar</button>
    <button id="download-output" class="btn btn-secondary">💾 Descargar .py</button>
    <button id="download-txt" class="btn btn-secondary">📄 Descargar .txt</button>

    <!-- NUEVO BOTÓN -->
    <button id="verify-conversion-btn" class="btn btn-primary">
        🔍 Verificar Conversión
    </button>
</div>
```

### Opción B: Como card destacada

```html
<div class="verification-card" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 2rem; border-radius: 12px; margin: 2rem 0;">
    <h3 style="margin: 0 0 0.5rem 0;">🔍 Verificación de Conversión</h3>
    <p style="margin: 0 0 1.5rem 0; opacity: 0.9;">
        Valida que la conversión DDV→EDV sea correcta y detecta posibles errores
    </p>
    <button id="verify-conversion-btn" class="btn" style="background: white; color: #667eea;">
        Verificar Ahora
    </button>
</div>
```

---

## 🧪 Testing de Integración

### 1. Verificar Servidor

```bash
# En terminal 1: Iniciar servidor
python verification_server.py

# En terminal 2: Test health check
curl http://localhost:5000/health
```

**Respuesta esperada:**
```json
{
  "status": "ok",
  "service": "verification-server",
  "version": "1.0.0"
}
```

### 2. Probar Flujo Completo

1. Abrir `index.html` en browser
2. Cargar script DDV (ejemplo: Macrogiro)
3. Click "Convertir a EDV"
4. Click "Verificar Conversión"
5. Verificar que:
   - ✅ Tab "Verificación" aparece
   - ✅ Se muestra reporte con score
   - ✅ Diferencias están categorizadas
   - ✅ Botones de exportar funcionan

---

## 🎬 Flujo de Usuario

```
┌─────────────────────┐
│  1. Cargar DDV      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  2. Convertir EDV   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  3. Verificar       │ ◄─── NUEVO
└──────────┬──────────┘
           │
           ▼
     ┌─────────┐
     │ Score?  │
     └────┬────┘
          │
    ┌─────┴─────┐
    │           │
    ▼           ▼
 ≥95% +      <95% o
 0 críticas  críticas
    │           │
    ▼           ▼
 ✅ APROBAR  ❌ RECHAZAR
    │           │
    ▼           ▼
 Descargar   Revisar
   .py       Diferencias
```

---

## 🚨 Troubleshooting

### Error: "Servidor no disponible"

**Causa:** `verification_server.py` no está corriendo

**Solución:**
```bash
# Terminal separada
python verification_server.py
```

### Error: CORS en browser

**Causa:** Navegador bloquea requests cross-origin

**Solución:** Ya incluida en `verification_server.py` con `flask-cors`

Si persiste, usar extensión de browser o configurar:
```python
# En verification_server.py
CORS(app, resources={r"/*": {"origins": "*"}})
```

### Verificación tarda mucho

**Causa:** Scripts muy grandes (>5000 líneas)

**Solución:** Normal para scripts grandes. Agregar indicador de progreso:

```javascript
// Agregar en verifyConversion()
setTimeout(() => {
    if (verifyBtn.disabled) {
        verifyBtn.innerHTML = '⏳ Analizando estructura...';
    }
}, 2000);

setTimeout(() => {
    if (verifyBtn.disabled) {
        verifyBtn.innerHTML = '⏳ Comparando flujo de datos...';
    }
}, 4000);
```

---

## 🎯 Mejoras Futuras (Opcional)

### 1. Auto-verificación

Verificar automáticamente después de convertir:

```javascript
// En convertScript(), después de éxito
if (document.getElementById('auto-verify-switch').checked) {
    setTimeout(verifyConversion, 500);
}
```

Agregar switch en configuración:
```html
<label class="switch-container">
    <input type="checkbox" id="auto-verify-switch">
    <span class="switch-slider"></span>
    <span class="switch-label">✅ Auto-verificar después de convertir</span>
</label>
```

### 2. Comparación de Múltiples Scripts

Permitir cargar varios scripts y compararlos todos:

```javascript
async function verifyMultiple(scripts) {
    const results = [];
    for (let i = 0; i < scripts.length - 1; i++) {
        const report = await verificationClient.verifyScripts(
            scripts[i],
            scripts[i + 1]
        );
        results.push(report);
    }
    return results;
}
```

### 3. Histórico de Verificaciones

Guardar en localStorage:

```javascript
function saveVerificationHistory(report) {
    const history = JSON.parse(localStorage.getItem('verificationHistory') || '[]');
    history.unshift({
        timestamp: Date.now(),
        script1: report.script1_name,
        script2: report.script2_name,
        score: report.similarity_score,
        is_equivalent: report.is_equivalent
    });
    // Mantener solo últimas 10
    history.splice(10);
    localStorage.setItem('verificationHistory', JSON.stringify(history));
}
```

---

## 📊 Métricas Recomendadas

Agregar analytics para tracking:

```javascript
// En verifyConversion(), después de obtener reporte
trackVerificationMetrics({
    scriptType: detectScriptType(),
    score: report.similarity_score,
    criticalCount: report.critical_count,
    totalDifferences: report.total_differences,
    duration: Date.now() - startTime
});

function trackVerificationMetrics(data) {
    // Enviar a analytics
    console.log('📊 Métricas:', data);

    // Opcional: enviar a backend
    // fetch('/api/metrics', { method: 'POST', body: JSON.stringify(data) });
}
```

---

## ✅ Checklist de Integración

- [ ] Servidor `verification_server.py` iniciado
- [ ] Script `verification_client.js` agregado a HTML
- [ ] Tab "Verificación" agregada
- [ ] Botón "Verificar Conversión" agregado
- [ ] Función `verifyConversion()` implementada
- [ ] CSS para tab de verificación agregado
- [ ] Tested en browser (Chrome/Edge/Firefox)
- [ ] Tested con scripts reales (Agente, Cajero, Macrogiro)
- [ ] Documentación actualizada

---

## 📝 Notas Finales

**Recomendaciones:**

1. **Producción:** Usar servidor Flask con Gunicorn/uWSGI
2. **SSL:** Configurar HTTPS si se expone externamente
3. **Rate Limiting:** Agregar límites si se usa por muchos usuarios
4. **Caching:** Cachear reportes para scripts ya verificados

**Contacto:**
- Documentación completa: `SCRIPT_VERIFIER_README.md`
- Tests: `test_verifier.py`
- Quick start: `quick_start_verifier.bat`

---

*Documento de integración - BCP Analytics Project*
*Versión: 1.0.0 | Fecha: 2025-01-06*
