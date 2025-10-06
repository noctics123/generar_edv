@echo off
REM ============================================
REM Script Verifier - Quick Start
REM ============================================

echo.
echo ========================================
echo   SCRIPT VERIFIER - QUICK START
echo ========================================
echo.

REM Check Python installation
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python no esta instalado o no esta en PATH
    echo Por favor instala Python 3.8+ desde https://python.org
    pause
    exit /b 1
)

echo [OK] Python encontrado
echo.

REM Install dependencies
echo [1/4] Instalando dependencias...
pip install -q -r requirements_verifier.txt
if errorlevel 1 (
    echo [ERROR] Fallo instalacion de dependencias
    pause
    exit /b 1
)
echo [OK] Dependencias instaladas
echo.

REM Run tests
echo [2/4] Ejecutando suite de tests...
python test_verifier.py
if errorlevel 1 (
    echo [WARNING] Algunos tests fallaron, pero el servidor puede iniciarse
) else (
    echo [OK] Todos los tests pasaron
)
echo.

REM Start server
echo [3/4] Iniciando servidor de verificacion...
echo.
echo ========================================
echo   SERVIDOR LISTO
echo ========================================
echo.
echo   URL: http://localhost:5000
echo   Health: http://localhost:5000/health
echo.
echo   Presiona Ctrl+C para detener
echo ========================================
echo.

python verification_server.py

pause
