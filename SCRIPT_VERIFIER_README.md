# Script Verifier - Sistema de Verificación Riguroso

## 📋 Descripción

Sistema multi-nivel de verificación para scripts PySpark, diseñado específicamente para validar equivalencia estructural, lógica y semántica entre scripts, con especialización en conversiones DDV→EDV del proyecto BCP Analytics.

**Objetivo Principal:** Ser la **primera línea de validación** antes de revisión manual, detectando diferencias críticas que puedan alterar resultados.

---

## 🎯 Características

### ✅ Verificación Multi-Nivel

1. **Análisis Estructural (AST)**
   - Parseo completo del Abstract Syntax Tree
   - Comparación de imports, funciones, constantes
   - Detección de cambios en estructura del código

2. **Análisis de Flujo de Datos**
   - Extracción de pipeline PySpark completo
   - Identificación de tablas fuente y destino
   - Secuencia de operaciones (select, filter, groupBy, join, etc.)

3. **Análisis DDV vs EDV Especializado**
   - Validación de diferencias esperadas vs inesperadas
   - Verificación de widgets requeridos
   - Validación de schemas separados (lectura vs escritura)
   - Verificación de nombres UPPERCASE

4. **Validación Semántica**
   - Equivalencia de lógica de negocio
   - Comparación de funciones críticas
   - Validación de constantes de negocio
   - Análisis de joins y agregaciones

### 📊 Sistema de Reportes

- **Score de Similitud:** 0-100% basado en peso de diferencias
- **Categorización por Nivel:**
  - 🔴 CRÍTICO: Altera resultado final
  - 🟠 ALTO: Puede alterar resultado
  - 🟡 MEDIO: Diferencia importante pero no crítica
  - 🟢 BAJO: Diferencia menor/cosmética
  - ℹ️ INFO: Información, no es diferencia

- **Reportes Exportables:**
  - Consola (resumen ejecutivo)
  - JSON (programático)
  - HTML (visual e interactivo)

---

## 🚀 Instalación

### Requisitos

```bash
pip install flask flask-cors
```

Python 3.8+ requerido.

### Archivos del Sistema

```
.
├── script_verifier.py         # Motor de verificación principal
├── verification_server.py     # API REST Flask
├── test_verifier.py           # Suite de tests
├── edv-converter-webapp/
│   └── src/
│       └── verification_client.js  # Cliente JavaScript
└── SCRIPT_VERIFIER_README.md  # Esta documentación
```

---

## 💻 Uso

### 1. Uso Standalone (CLI)

```bash
# Verificar dos scripts individuales
python script_verifier.py script1.py script2.py

# Verificar conversión DDV→EDV
python script_verifier.py ddv_script.py edv_script.py --ddv-edv

# Generar reportes
python script_verifier.py script1.py script2.py --ddv-edv \
    --html report.html \
    --json report.json
```

### 2. Uso Programático (Python)

```python
from script_verifier import verify_scripts

# Verificar scripts
report = verify_scripts(
    script1_path="HM_MATRIZTRANSACCIONPOSMACROGIRO.py",
    script2_path="HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py",
    is_ddv_edv=True,
    output_html="report.html"
)

# Analizar resultados
print(f"Equivalentes: {report.is_equivalent}")
print(f"Score: {report.similarity_score}%")
print(f"Diferencias críticas: {len(report.get_critical_differences())}")

# Acceder a diferencias
for diff in report.get_critical_differences():
    print(f"[{diff.level.value}] {diff.description}")
    print(f"  Impacto: {diff.impact}")
    print(f"  Recomendación: {diff.recommendation}")
```

### 3. Uso con API REST

#### Iniciar servidor

```bash
python verification_server.py
```

El servidor inicia en `http://localhost:5000`

#### Endpoints

**Health Check**
```bash
curl http://localhost:5000/health
```

**Verificar Scripts**
```bash
curl -X POST http://localhost:5000/verify \
  -H "Content-Type: application/json" \
  -d '{
    "script1": "contenido script 1...",
    "script2": "contenido script 2...",
    "script1_name": "script1.py",
    "script2_name": "script2.py",
    "is_ddv_edv": false
  }'
```

**Verificar DDV→EDV**
```bash
curl -X POST http://localhost:5000/verify-ddv-edv \
  -H "Content-Type: application/json" \
  -d '{
    "ddv_script": "contenido DDV...",
    "edv_script": "contenido EDV..."
  }'
```

### 4. Integración con App Web

```javascript
// Inicializar cliente
const verificationClient = new VerificationClient('http://localhost:5000');

// Verificar health
const isHealthy = await verificationClient.checkHealth();

// Verificar DDV→EDV
const report = await verificationClient.verifyDDVtoEDV(
    ddvScriptContent,
    edvScriptContent
);

// Renderizar reporte
const verificationUI = new VerificationUI('verification-container');
verificationUI.render(report);
```

---

## 🧪 Testing

### Ejecutar Suite de Tests

```bash
python test_verifier.py
```

### Tests Incluidos

1. **Test 1: DDV vs EDV - MACROGIRO**
   - Verifica conversión correcta DDV→EDV
   - Valida diferencias esperadas vs inesperadas
   - Genera reporte HTML

2. **Test 2: Self-Comparison**
   - Control: script vs sí mismo
   - Debe ser 100% similar, 0 diferencias

3. **Test 3: Scripts Diferentes**
   - Verifica detección de diferencias
   - Agente vs Cajero (estructuras diferentes)

4. **Test 4: Similitud Individual**
   - Valida todos los pares DDV/EDV disponibles
   - Threshold: 85% similitud, 0 diferencias críticas

### Interpretar Resultados

```
✅ PASS - Test exitoso
❌ FAIL - Test falló, revisar detalles
⏭️  SKIP - Test omitido (archivos no encontrados)
```

---

## 📖 Estructura del Reporte

### Campos Principales

```python
{
    "script1_name": "script_ddv.py",
    "script2_name": "script_edv.py",
    "is_equivalent": true,           # ¿Son equivalentes?
    "similarity_score": 95.5,        # Score 0-100
    "total_differences": 12,         # Total diferencias
    "critical_count": 0,             # Diferencias críticas
    "high_count": 2,                 # Diferencias altas
    "differences": [                 # Lista de diferencias
        {
            "level": "CRÍTICO",
            "category": "Lógica de Negocio",
            "description": "...",
            "script1_value": "...",
            "script2_value": "...",
            "location": "función X, línea Y",
            "impact": "...",
            "recommendation": "..."
        }
    ],
    "metadata": {                    # Metadata adicional
        "script1_functions": 10,
        "script2_functions": 11,
        "script1_operations": 45,
        "script2_operations": 48,
        ...
    }
}
```

---

## 🔍 Validaciones DDV→EDV

### Diferencias Esperadas (NO son errores)

✅ **Widget PRM_ESQUEMA_TABLA_DDV**
- DDV: `bcp_ddv_matrizvariables`
- EDV: `bcp_ddv_matrizvariables_v` (suffix `_v` para views)

✅ **Widgets Nuevos en EDV**
- `PRM_CATALOG_NAME_EDV`
- `PRM_ESQUEMA_TABLA_EDV`

✅ **Variable Nueva en EDV**
- `PRM_ESQUEMA_TABLA_ESCRITURA` (schema separado para escritura)

✅ **Tabla Destino Diferente**
- DDV escribe a schema DDV
- EDV escribe a schema EDV

### Diferencias Inesperadas (SON errores críticos)

❌ **Tabla Primera Transpuesta en lowercase**
- EDV DEBE usar UPPERCASE (case-sensitive en parameter table)
- ✗ `hm_conceptotransaccionpos`
- ✓ `HM_CONCEPTOTRANSACCIONPOS`

❌ **Schema DDV sin suffix `_v`**
- EDV DEBE leer de views (`_v`)
- ✗ `bcp_ddv_matrizvariables`
- ✓ `bcp_ddv_matrizvariables_v`

❌ **Falta PRM_ESQUEMA_TABLA_ESCRITURA**
- EDV requiere variable para schema de escritura
- Debe ser: `PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV`

❌ **Cambios en Lógica de Negocio**
- Funciones críticas eliminadas/renombradas
- Constantes de negocio modificadas (ej. `CONS_GRUPO`)
- Secuencia de operaciones alterada significativamente

---

## 🎨 Ejemplo de Uso Completo

```python
#!/usr/bin/env python
"""
Ejemplo: Verificar conversión DDV→EDV con análisis completo
"""
from script_verifier import verify_scripts, ReportGenerator

# 1. Verificar scripts
report = verify_scripts(
    "HM_MATRIZTRANSACCIONPOSMACROGIRO.py",
    "HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py",
    is_ddv_edv=True,
    output_html="verification_report.html",
    output_json="verification_report.json"
)

# 2. Análisis rápido
print(f"\n{'='*60}")
print(f"Score: {report.similarity_score}%")
print(f"Equivalentes: {'SÍ' if report.is_equivalent else 'NO'}")
print(f"{'='*60}\n")

# 3. Revisar diferencias críticas
critical = report.get_critical_differences()
if critical:
    print("🔴 DIFERENCIAS CRÍTICAS ENCONTRADAS:")
    for i, diff in enumerate(critical, 1):
        print(f"\n{i}. {diff.description}")
        print(f"   📍 Ubicación: {diff.location}")
        print(f"   ⚠️  Impacto: {diff.impact}")
        print(f"   💡 Recomendación: {diff.recommendation}")
else:
    print("✅ No se encontraron diferencias críticas")

# 4. Revisar diferencias altas
high = report.get_high_differences()
if high:
    print(f"\n🟠 DIFERENCIAS ALTAS: {len(high)}")
    for diff in high:
        print(f"   • {diff.description}")

# 5. Metadata
print(f"\n📊 METADATA:")
print(f"   Funciones Script 1: {report.metadata['script1_functions']}")
print(f"   Funciones Script 2: {report.metadata['script2_functions']}")
print(f"   Operaciones Script 1: {report.metadata['script1_operations']}")
print(f"   Operaciones Script 2: {report.metadata['script2_operations']}")

# 6. Decisión final
if report.is_equivalent:
    print("\n✅ APROBADO: Scripts son equivalentes")
    print("   Puede proceder con despliegue")
else:
    print("\n❌ RECHAZADO: Scripts NO son equivalentes")
    print("   Revisar manualmente las diferencias críticas/altas")
    print(f"   Reporte detallado: verification_report.html")
```

---

## 🛠️ Personalización

### Ajustar Pesos de Diferencias

Editar en `script_verifier.py`, método `_calculate_similarity_score`:

```python
weights = {
    DifferenceLevel.CRITICAL: 20.0,   # Reducir penalización
    DifferenceLevel.HIGH: 10.0,
    DifferenceLevel.MEDIUM: 5.0,
    DifferenceLevel.LOW: 2.0,
    DifferenceLevel.INFO: 0.0
}
```

### Agregar Nuevas Validaciones DDV→EDV

Editar en `script_verifier.py`, clase `DDVEDVComparator`:

```python
EXPECTED_DIFFERENCES = {
    'widgets': {
        'new_widgets_edv': [
            'PRM_CATALOG_NAME_EDV',
            'PRM_ESQUEMA_TABLA_EDV',
            'TU_NUEVO_WIDGET',  # <-- Agregar aquí
        ]
    }
}
```

### Agregar Constantes Críticas

Editar en `script_verifier.py`, clase `SemanticValidator`, método `_compare_business_constants`:

```python
critical_constants = {
    'CONS_GRUPO',
    'CONS_PARTITION_DELTA_NAME',
    'TU_CONSTANTE_CRITICA',  # <-- Agregar aquí
}
```

---

## 📦 Despliegue en Producción

### Opción 1: Docker (Recomendado)

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY script_verifier.py verification_server.py ./
EXPOSE 5000

CMD ["python", "verification_server.py"]
```

```bash
docker build -t script-verifier .
docker run -p 5000:5000 script-verifier
```

### Opción 2: Systemd Service

```ini
[Unit]
Description=Script Verification Server
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/script-verifier
ExecStart=/usr/bin/python3 verification_server.py
Restart=always

[Install]
WantedBy=multi-user.target
```

### Opción 3: Cloud Functions

El código es compatible con:
- AWS Lambda (con adaptador Flask)
- Google Cloud Functions
- Azure Functions

---

## 🐛 Troubleshooting

### Error: "No module named 'ast'"

**Solución:** `ast` es built-in, verificar versión Python >= 3.8

### Error: "Connection refused" en cliente JavaScript

**Solución:**
1. Verificar que servidor esté corriendo: `curl http://localhost:5000/health`
2. Verificar CORS habilitado en `verification_server.py`
3. Verificar puerto no esté en uso: `netstat -an | grep 5000`

### Score siempre 100% en scripts diferentes

**Posible causa:** Scripts no están siendo parseados correctamente

**Solución:**
1. Verificar encoding UTF-8
2. Verificar sintaxis Python válida
3. Revisar logs del servidor

### Diferencias esperadas marcadas como críticas

**Solución:** Revisar configuración `EXPECTED_DIFFERENCES` en `DDVEDVComparator`

---

## 📚 Referencias

- [AST Module - Python Docs](https://docs.python.org/3/library/ast.html)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Flask Documentation](https://flask.palletsprojects.com/)

---

## 👥 Contribución

Para agregar nuevas validaciones o mejorar el verificador:

1. Fork el repositorio
2. Crear feature branch: `git checkout -b feature/nueva-validacion`
3. Agregar tests en `test_verifier.py`
4. Verificar que todos los tests pasen
5. Crear Pull Request con descripción detallada

---

## 📄 Licencia

Proyecto interno BCP Analytics - Uso restringido

---

## 📞 Contacto

Para preguntas o soporte:
- Equipo: BCP Analytics
- Proyecto: Generación Scripts EDV
- Herramienta: Claude Code

---

## 🔄 Changelog

### v1.0.0 (2025-01-06)
- ✨ Release inicial
- 🎯 Análisis estructural, flujo de datos, DDV vs EDV, semántico
- 📊 Reportes en consola, JSON, HTML
- 🌐 API REST Flask
- 🎨 Cliente JavaScript para app web
- 🧪 Suite de tests completa

---

**¡Sistema listo para producción!** 🚀
