# 🚀 Quick Start - EDV Converter + Verificador

## 🎯 Inicio Rápido (2 pasos)

### 1️⃣ Iniciar Servidor de Verificación

**Opción A - Quick Start (Recomendado):**
```bash
.\quick_start_verifier.bat
```

**Opción B - Manual:**
```bash
# Terminal 1
pip install -r requirements_verifier.txt
python verification_server.py
```

**Deberías ver:**
```
========================================
  SERVIDOR LISTO
========================================

  URL: http://localhost:5000
  Health: http://localhost:5000/health

  Presiona Ctrl+C para detener
========================================
```

### 2️⃣ Abrir App Web

**Abrir en browser:**
```
index.html
```

O doble click en `index.html`

---

## 📋 Uso

### Conversor DDV → EDV

1. Click en pestaña **🔄 Conversor DDV → EDV**
2. Cargar script DDV (.py)
3. Click "Convertir a EDV"
4. Revisar resultados
5. Descargar script EDV

### Verificador de Similitud

1. Click en pestaña **🔍 Verificador de Similitud**
2. Verificar que banner muestre "✅ Servidor Online"
   - Si muestra "❌ Offline", iniciar servidor (paso 1)
3. Cargar 2 scripts:
   - Script 1: DDV o cualquier script
   - Script 2: EDV o cualquier script
4. Seleccionar modo:
   - **DDV vs EDV**: Para validar conversiones
   - **Scripts Individuales**: Para comparar cualquier par
5. Click "🔍 Verificar Similitud"
6. Ver reporte con diferencias

---

## ❌ Troubleshooting

### "Servidor no disponible"

**Causa:** `verification_server.py` no está corriendo

**Solución:**
```bash
# Abre una terminal nueva
cd "ruta/al/proyecto"
python verification_server.py
```

### "Puerto 5000 en uso"

**Causa:** Otro proceso usa el puerto

**Solución:**
```bash
# Windows
netstat -ano | findstr :5000
taskkill /PID <numero_proceso> /F

# Reiniciar servidor
python verification_server.py
```

### Firewall bloquea conexión

**Causa:** Firewall de Windows bloquea puerto 5000

**Solución:**
1. Permitir Python en Firewall
2. O desactivar temporalmente firewall para pruebas

---

## 🎨 Características

### Conversor
- ✅ Conversión automática DDV → EDV
- ✅ 6 optimizaciones de rendimiento
- ✅ Validación rigurosa (80+ checks)
- ✅ Diff visual
- ✅ Multi-periodo
- ✅ Parámetros editables

### Verificador
- ✅ 4 niveles de análisis (estructural, flujo, DDV/EDV, semántico)
- ✅ Score de similitud (0-100%)
- ✅ Diferencias categorizadas (Crítico, Alto, Medio, Bajo, Info)
- ✅ Reportes exportables (JSON, HTML)
- ✅ Health check automático
- ✅ Logging detallado de errores

---

## 📞 Ayuda

**Documentación completa:**
- `SCRIPT_VERIFIER_README.md` - Verificador
- `VERIFIER_RESUMEN_EJECUTIVO.md` - Resumen ejecutivo
- `INTEGRACION_APP_WEB.md` - Integración

**Tests:**
```bash
python test_verifier.py
```

**GitHub:**
https://github.com/noctics123/generar_edv

---

*Generado con Claude Code - BCP Analytics*
