# 🤖 Instalación del Sistema de Análisis con IA

## ✅ Archivos Creados

Los siguientes archivos ya están listos:

1. ✅ `edv-converter-webapp/src/ai_analyzer.js` - Cliente para APIs (OpenAI, Claude, Gemini)
2. ✅ `edv-converter-webapp/src/ai_ui.js` - Manejo de UI y eventos
3. ✅ `edv-converter-webapp/src/styles/ai.css` - Estilos para la interfaz IA

## 📝 Pasos para Integrar en index.html

### 1. Agregar CSS (línea ~14)

**Buscar:**
```html
<link rel="stylesheet" href="edv-converter-webapp/src/styles/detailed-diff.css?v=3.5.2">
```

**Agregar después:**
```html
<link rel="stylesheet" href="edv-converter-webapp/src/styles/ai.css">
```

---

### 2. Agregar Controles IA (línea ~418)

**Buscar:**
```html
                </div>
            </div>

            <!-- Dual Script Upload -->
```

**Insertar ANTES de `<!-- Dual Script Upload -->`:**
```html
                <!-- AI Analysis Controls -->
                <div class="ai-controls">
                    <div class="ai-toggle-container">
                        <label class="ai-toggle-label">
                            <input type="checkbox" id="enable-ai-analysis">
                            <span>🤖 Habilitar Análisis con IA (Opcional)</span>
                        </label>
                        <div style="display: flex; align-items: center; gap: 1rem; flex-wrap: wrap;">
                            <div id="ai-config-status">
                                <span style="color: rgba(255,255,255,0.8); font-weight: 600;">
                                    ❌ No configurado
                                </span>
                            </div>
                            <button type="button" id="configure-ai-btn">
                                ⚙️ Configurar API
                            </button>
                        </div>
                    </div>
                    <div style="margin-top: 1rem; font-size: 0.9rem; color: rgba(255,255,255,0.9);">
                        <strong>💡 Análisis Profundo:</strong> Usa OpenAI GPT-4, Claude Sonnet, o Google Gemini para detectar optimizaciones,
                        cambios de lógica y validar conversiones DDV→EDV con contexto completo del proyecto BCP.
                    </div>
                </div>

```

---

### 3. Agregar Botón "Verificar con IA" (línea ~560)

**Buscar:**
```html
                        <button id="verify-scripts-btn" class="btn btn-primary btn-large">
                            🔍 Verificar Similitud
                        </button>
```

**Agregar DESPUÉS:**
```html
                        <button id="verify-with-ai-btn" class="btn btn-primary btn-large" style="display: none;">
                            🤖 Verificar con IA
                        </button>
```

---

### 4. Agregar Contenedor de Resultados IA (línea ~565)

**Buscar:**
```html
                <!-- Verification Results -->
                <div id="verification-results" style="display: none;">
                    <!-- Populated by JavaScript -->
                </div>
```

**Agregar DESPUÉS:**
```html
                <!-- AI Results Container -->
                <div id="ai-results-container" style="display: none;">
                    <!-- Populated by JavaScript -->
                </div>
```

---

### 5. Agregar Modal de Configuración (línea ~585, ANTES de `<!-- Prism.js Scripts -->`)

**Buscar:**
```html
    <!-- Prism.js Scripts -->
```

**Insertar ANTES:**
```html
    <!-- AI Configuration Modal -->
    <div id="ai-config-modal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h3>🤖 Configurar Análisis con IA</h3>
                <button type="button" id="close-ai-modal" class="modal-close">×</button>
            </div>

            <div class="form-group">
                <label for="ai-provider">Proveedor de IA:</label>
                <select id="ai-provider">
                    <option value="openai">OpenAI (GPT-4 Turbo)</option>
                    <option value="claude">Anthropic Claude (Sonnet 3.5)</option>
                    <option value="gemini">Google Gemini Pro</option>
                </select>
                <p class="form-help">
                    Selecciona el proveedor de IA que prefieras. Cada uno tiene diferentes fortalezas en análisis de código.
                </p>
            </div>

            <div class="form-group">
                <label for="ai-api-key">API Key:</label>
                <input type="password" id="ai-api-key" placeholder="sk-..." autocomplete="off">
                <p class="form-help">
                    Tu API key se almacena localmente en tu navegador (localStorage). Nunca se envía a nuestros servidores.
                    <br><strong>Obtén tu API key:</strong>
                    <a href="https://platform.openai.com/api-keys" target="_blank" rel="noopener">OpenAI</a> |
                    <a href="https://console.anthropic.com/settings/keys" target="_blank" rel="noopener">Claude</a> |
                    <a href="https://makersuite.google.com/app/apikey" target="_blank" rel="noopener">Gemini</a>
                </p>
            </div>

            <div class="modal-actions">
                <button type="button" id="clear-ai-config" class="btn-modal btn-modal-danger">
                    🗑️ Limpiar
                </button>
                <button type="button" id="save-ai-config" class="btn-modal btn-modal-primary">
                    💾 Guardar Configuración
                </button>
            </div>
        </div>
    </div>

```

---

### 6. Agregar Imports de Scripts (línea ~595, ANTES de `app.js`)

**Buscar:**
```html
    <script src="edv-converter-webapp/src/comparison-modal.js?v=3.4"></script>
    <script src="edv-converter-webapp/src/app.js?v=3.3"></script>
```

**Insertar ENTRE comparison-modal.js y app.js:**
```html
    <!-- AI Analysis Scripts -->
    <script src="edv-converter-webapp/src/ai_analyzer.js"></script>
    <script src="edv-converter-webapp/src/ai_ui.js"></script>
```

---

## 🧪 Prueba del Sistema

1. Abrir `index.html` en el navegador
2. Ir a la pestaña **"Verificación de Similitud"**
3. Marcar el checkbox **"🤖 Habilitar Análisis con IA"**
4. Hacer clic en **"⚙️ Configurar API"**
5. Seleccionar proveedor (OpenAI, Claude, o Gemini)
6. Ingresar tu API key
7. Hacer clic en **"💾 Guardar Configuración"**
8. Cargar dos scripts para comparar
9. Hacer clic en **"🤖 Verificar con IA"**

## 🔑 Obtener API Keys

- **OpenAI**: https://platform.openai.com/api-keys (Requiere cuenta y créditos)
- **Claude**: https://console.anthropic.com/settings/keys (Requiere cuenta)
- **Gemini**: https://makersuite.google.com/app/apikey (Gratis con límites)

## 📊 Características del Análisis IA

### Modo DDV→EDV:
- ✅ Detecta las 6 optimizaciones conocidas
- ✅ Identifica cambios esperados vs errores críticos
- ✅ Valida separación de esquemas DDV/EDV
- ✅ Evalúa variables y widgets adicionales
- ✅ Detecta cambios en lógica de negocio (trim, funciones, etc.)

### Modo Scripts Individuales:
- ✅ Análisis estructural completo
- ✅ Comparación de imports, funciones, transformaciones
- ✅ Evaluación de equivalencia lógica
- ✅ Recomendaciones de mejora

## 🔒 Seguridad

- Tu API key se guarda en `localStorage` del navegador (local)
- NO se envía a ningún servidor nuestro
- Solo se usa para llamar directamente a la API del proveedor seleccionado
- Puedes limpiar la configuración en cualquier momento

## 💰 Costos Estimados

- **OpenAI GPT-4 Turbo**: ~$0.01-$0.05 por análisis (depende del tamaño del script)
- **Claude Sonnet**: ~$0.003-$0.015 por análisis
- **Gemini Pro**: GRATIS con límites diarios

## ✅ Checklist de Instalación

- [ ] CSS agregado en `<head>`
- [ ] Controles IA agregados en sección verificación
- [ ] Botón "Verificar con IA" agregado
- [ ] Contenedor de resultados agregado
- [ ] Modal de configuración agregado
- [ ] Scripts de IA agregados antes de `app.js`
- [ ] Probado en navegador
- [ ] API key configurada
- [ ] Análisis de prueba ejecutado

---

**¿Necesitas ayuda?** Si encuentras problemas, revisa la consola del navegador (F12) para ver mensajes de error.
