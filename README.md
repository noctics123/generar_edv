# 🚀 Generador EDV - BCP Analytics

Repositorio de scripts optimizados de matriz de transacciones y herramienta web de conversión automática DDV a EDV para BCP Analytics.

[![GitHub Pages](https://img.shields.io/badge/GitHub%20Pages-Online-brightgreen)](https://noctics123.github.io/generar_edv/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🌐 Web App - Acceso Directo

**🔗 Convierte tu script ahora:** [https://noctics123.github.io/generar_edv/](https://noctics123.github.io/generar_edv/)

Aplicación web para conversión automática DDV → EDV sin instalación. Carga tu script, obtén el resultado optimizado en segundos.

## 📋 Contenido del Repositorio

1. **🌐 EDV Converter Web App** - Conversión automática con validación en tiempo real
2. **📜 Scripts Optimizados** - Matriz de transacciones (Agente, Cajero, Macrogiro)
3. **📚 Documentación Completa** - Guías, análisis y reglas de conversión

## ✨ Características de la Web App

- ✅ Conversión automática DDV → EDV
- ✅ 8 transformaciones aplicadas
- ✅ 11 validaciones de compliance
- ✅ Vista de diff interactiva
- ✅ Checklist en tiempo real
- ✅ Descarga directa del script
- ✅ Sin instalación requerida

## 🚀 Uso Rápido

```
1. Abre: https://noctics123.github.io/generar_edv/
2. Carga tu script DDV (.py)
3. Clic en "⚡ Convertir a EDV"
4. Revisa diff y checklist
5. Descarga el script EDV
```

## 📁 Estructura

```
├── index.html                           # 🌐 Web App (entrada)
├── edv-converter-webapp/                # Código fuente Web App
│   ├── src/                            # JavaScript + CSS
│   ├── docs/                           # Guía de uso
│   └── README.md
├── Scripts:
│   ├── *_MACROGIRO.py                  # DDV/EDV Macrogiro ✅
│   ├── *_AGENTE.py                     # DDV/EDV Agente ✅
│   ├── *_CAJERO.py                     # DDV/EDV Cajero ✅
│   └── funciones.py
└── Documentación:
    ├── CLAUDE.md                        # Guía completa
    ├── GUIA_CREAR_VERSION_EDV.md        # Conversión manual
    └── OPTIMIZACIONES_*.md              # Análisis técnicos
```

## 🎯 Scripts de Matriz de Transacciones

### Estado de Optimizaciones

| Script | Versión | Optimizaciones | Estado |
|--------|---------|----------------|--------|
| Macrogiro DDV | v3.2 | 6 + trim | ✅ 100% |
| Macrogiro EDV | v3.2 | 6 + trim | ✅ 100% |
| Agente EDV | v1.0 | Aplicadas | ✅ Listo |
| Cajero EDV | v1.0 | Aplicadas | ✅ Listo |

### Optimizaciones (Mejora 2-5x)

1. **Spark AQE** (20-30%) - Adaptive Query Execution
2. **Cache en memoria** (60-80%) - Elimina I/O a disco
3. **Storage Level** (10-20%) - MEMORY_AND_DISK
4. **Consolidación loops** (15-25%) - 3→1 transformación
5. **Eliminación coalesce** (70-90%) - Sin shuffles en loop
6. **Repartition optimizado** (10-15%) - Balance pre-escritura

## 🔄 DDV → EDV: Diferencias Clave

| Aspecto | DDV | EDV |
|---------|-----|-----|
| Catálogo lectura | `catalog_lhcl_desa_bcp` | `catalog_lhcl_prod_bcp` |
| Schema lectura | `bcp_ddv_matrizvariables` | `bcp_ddv_matrizvariables_v` |
| Catálogo escritura | (mismo) | `catalog_lhcl_prod_bcp_expl` |
| Schema escritura | (mismo) | `bcp_edv_trdata_012` |

La Web App aplica automáticamente todos los cambios necesarios.

## 📚 Documentación

- **[CLAUDE.md](CLAUDE.md)** - Guía completa del proyecto
- **[GUIA_CREAR_VERSION_EDV.md](GUIA_CREAR_VERSION_EDV.md)** - Conversión manual
- **[edv-converter-webapp/docs/GUIA_USO.md](edv-converter-webapp/docs/GUIA_USO.md)** - Uso de Web App
- **[OPTIMIZACIONES_APLICADAS_DDV.md](OPTIMIZACIONES_APLICADAS_DDV.md)** - Detalle técnico

## ✅ Validación (11 Checks)

- [x] Widgets EDV
- [x] Variables EDV
- [x] Esquema de escritura
- [x] Destino EDV
- [x] Managed tables
- [x] 8 configs Spark AQE
- [x] Repartition
- [x] Partición correcta
- [x] Sin paths hardcoded
- [x] Schema DDV con views
- [x] DROP TABLE cleanup

**Score objetivo: ≥80% para PASS**

## 🛠️ Tecnologías

- **Scripts**: PySpark 3.x, Delta Lake, Unity Catalog
- **Web App**: HTML5, CSS3, Vanilla JS, GitHub Pages

## 📞 Soporte

- **Web App**: [https://noctics123.github.io/generar_edv/](https://noctics123.github.io/generar_edv/)
- **Issues**: [GitHub Issues](https://github.com/noctics123/generar_edv/issues)
- **Docs**: [CLAUDE.md](CLAUDE.md)

## 📄 Licencia

MIT

## 👥 Autores

BCP Analytics Team | Generado con [Claude Code](https://claude.com/claude-code)

---

**Última actualización:** 2025-10-04 | **Web App:** v1.0.0 | **Scripts:** ✅ Optimizados