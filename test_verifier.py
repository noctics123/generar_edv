"""
Test Suite para Script Verifier
================================

Suite de pruebas para validar el verificador con scripts reales del proyecto.

Tests:
1. Verificación DDV vs EDV - MACROGIRO
2. Verificación script individual vs sí mismo (debe ser 100% similar)
3. Verificación scripts diferentes (Agente vs Cajero)

Autor: Claude Code
Versión: 1.0
"""

import os
from script_verifier import verify_scripts, ReportGenerator
from pathlib import Path

# Colores para terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(text):
    """Imprime header con formato"""
    print("\n" + "="*80)
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print("="*80)


def print_test_result(test_name, passed, details=""):
    """Imprime resultado de test"""
    icon = f"{Colors.GREEN}✅" if passed else f"{Colors.RED}❌"
    status = f"{Colors.GREEN}PASS" if passed else f"{Colors.RED}FAIL"
    print(f"\n{icon} {test_name}: {status}{Colors.END}")
    if details:
        print(f"   {details}")


def test_ddv_vs_edv_macrogiro():
    """Test 1: Verificación DDV vs EDV - MACROGIRO"""
    print_header("TEST 1: Verificación DDV vs EDV - MACROGIRO")

    # Paths
    ddv_script = "HM_MATRIZTRANSACCIONPOSMACROGIRO.py"
    edv_script = "HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py"

    # Verificar que existen
    if not os.path.exists(ddv_script):
        print_test_result("Test 1", False, f"Script DDV no encontrado: {ddv_script}")
        return False

    if not os.path.exists(edv_script):
        print_test_result("Test 1", False, f"Script EDV no encontrado: {edv_script}")
        return False

    # Verificar
    report = verify_scripts(ddv_script, edv_script, is_ddv_edv=True)

    # Analizar resultados
    critical_count = len(report.get_critical_differences())
    high_count = len(report.get_high_differences())

    # Test pasa si no hay diferencias críticas
    passed = critical_count == 0

    details = f"Score: {report.similarity_score}% | Críticas: {critical_count} | Altas: {high_count}"
    print_test_result("Test 1: DDV vs EDV - MACROGIRO", passed, details)

    # Mostrar diferencias críticas si existen
    if critical_count > 0:
        print(f"\n{Colors.RED}🔴 Diferencias CRÍTICAS encontradas:{Colors.END}")
        for diff in report.get_critical_differences():
            print(f"   • {diff.description}")
            print(f"     Impacto: {diff.impact}")

    # Generar reporte HTML
    output_path = "test_reports/test1_ddv_vs_edv_macrogiro.html"
    os.makedirs("test_reports", exist_ok=True)
    ReportGenerator.generate_html_report(report, output_path)
    print(f"\n📄 Reporte HTML generado: {output_path}")

    return passed


def test_self_comparison():
    """Test 2: Script vs sí mismo (debe ser 100% similar)"""
    print_header("TEST 2: Script vs Sí Mismo (Control)")

    script = "HM_MATRIZTRANSACCIONPOSMACROGIRO.py"

    if not os.path.exists(script):
        print_test_result("Test 2", False, f"Script no encontrado: {script}")
        return False

    # Verificar script contra sí mismo
    report = verify_scripts(script, script, is_ddv_edv=False)

    # Test pasa si score es 100% y sin diferencias
    passed = (report.similarity_score == 100.0 and
              len(report.differences) == 0)

    details = f"Score: {report.similarity_score}% | Diferencias: {len(report.differences)}"
    print_test_result("Test 2: Self-Comparison", passed, details)

    if not passed:
        print(f"\n{Colors.YELLOW}⚠️ ADVERTENCIA: Script vs sí mismo debería ser 100% similar{Colors.END}")

    return passed


def test_different_scripts():
    """Test 3: Scripts diferentes (Agente vs Cajero)"""
    print_header("TEST 3: Scripts Diferentes (Agente vs Cajero)")

    script1 = "MATRIZVARIABLES_HM_MATRIZTRANSACCIONAGENTE.py"
    script2 = "MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO.py"

    # Verificar existencia
    if not os.path.exists(script1):
        print_test_result("Test 3", False, f"Script no encontrado: {script1}")
        return False

    if not os.path.exists(script2):
        print_test_result("Test 3", False, f"Script no encontrado: {script2}")
        return False

    # Verificar
    report = verify_scripts(script1, script2, is_ddv_edv=False)

    # Test pasa si detecta diferencias (scripts son diferentes)
    passed = len(report.differences) > 0 and report.similarity_score < 100.0

    details = f"Score: {report.similarity_score}% | Diferencias: {len(report.differences)}"
    print_test_result("Test 3: Diferentes Scripts", passed, details)

    # Generar reporte
    output_path = "test_reports/test3_agente_vs_cajero.html"
    ReportGenerator.generate_html_report(report, output_path)
    print(f"\n📄 Reporte HTML generado: {output_path}")

    return passed


def test_individual_scripts_similarity():
    """Test 4: Verificar similitud entre scripts del mismo tipo"""
    print_header("TEST 4: Similitud Entre Scripts del Mismo Tipo")

    # Comparar variantes DDV vs EDV de diferentes scripts
    test_pairs = [
        ("HM_MATRIZTRANSACCIONPOSMACROGIRO.py", "HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py", "MACROGIRO"),
    ]

    all_passed = True

    for ddv, edv, name in test_pairs:
        if not os.path.exists(ddv) or not os.path.exists(edv):
            print(f"\n{Colors.YELLOW}⏭️  Saltando {name}: Scripts no encontrados{Colors.END}")
            continue

        print(f"\n{Colors.BLUE}🔍 Verificando: {name}{Colors.END}")

        report = verify_scripts(ddv, edv, is_ddv_edv=True)

        critical_count = len(report.get_critical_differences())
        high_count = len(report.get_high_differences())

        # Consideramos exitoso si score > 85% y sin críticas
        passed = report.similarity_score >= 85.0 and critical_count == 0

        details = f"Score: {report.similarity_score}% | Críticas: {critical_count} | Altas: {high_count}"
        print_test_result(f"   {name}", passed, details)

        if not passed:
            all_passed = False

            # Mostrar top 3 diferencias
            print(f"\n   {Colors.YELLOW}⚠️ Top diferencias:{Colors.END}")
            for i, diff in enumerate(report.differences[:3]):
                print(f"      {i+1}. [{diff.level}] {diff.description}")

        # Generar reporte
        output_path = f"test_reports/test4_{name.lower()}_ddv_vs_edv.html"
        ReportGenerator.generate_html_report(report, output_path)
        print(f"   📄 Reporte: {output_path}")

    return all_passed


def run_all_tests():
    """Ejecuta todos los tests"""
    print("\n" + "🚀 "*20)
    print(f"{Colors.BOLD}{Colors.BLUE}SCRIPT VERIFIER - TEST SUITE{Colors.END}")
    print("🚀 "*20 + "\n")

    results = []

    # Test 1: DDV vs EDV MACROGIRO
    try:
        result1 = test_ddv_vs_edv_macrogiro()
        results.append(("DDV vs EDV - MACROGIRO", result1))
    except Exception as e:
        print(f"{Colors.RED}❌ Test 1 falló con error: {e}{Colors.END}")
        results.append(("DDV vs EDV - MACROGIRO", False))

    # Test 2: Self-comparison
    try:
        result2 = test_self_comparison()
        results.append(("Self-Comparison", result2))
    except Exception as e:
        print(f"{Colors.RED}❌ Test 2 falló con error: {e}{Colors.END}")
        results.append(("Self-Comparison", False))

    # Test 3: Different scripts
    try:
        result3 = test_different_scripts()
        results.append(("Scripts Diferentes", result3))
    except Exception as e:
        print(f"{Colors.RED}❌ Test 3 falló con error: {e}{Colors.END}")
        results.append(("Scripts Diferentes", False))

    # Test 4: Individual similarity
    try:
        result4 = test_individual_scripts_similarity()
        results.append(("Similitud Individual", result4))
    except Exception as e:
        print(f"{Colors.RED}❌ Test 4 falló con error: {e}{Colors.END}")
        results.append(("Similitud Individual", False))

    # Resumen final
    print_header("RESUMEN DE TESTS")

    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)

    for test_name, passed in results:
        icon = f"{Colors.GREEN}✅" if passed else f"{Colors.RED}❌"
        status = f"{Colors.GREEN}PASS" if passed else f"{Colors.RED}FAIL"
        print(f"{icon} {test_name}: {status}{Colors.END}")

    print("\n" + "="*80)
    success_rate = (passed_count / total_count) * 100 if total_count > 0 else 0

    if success_rate == 100:
        color = Colors.GREEN
        emoji = "🎉"
    elif success_rate >= 75:
        color = Colors.YELLOW
        emoji = "⚠️"
    else:
        color = Colors.RED
        emoji = "❌"

    print(f"\n{color}{Colors.BOLD}{emoji} Tests Pasados: {passed_count}/{total_count} ({success_rate:.1f}%){Colors.END}\n")

    return success_rate == 100


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
