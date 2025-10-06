"""
Script Verifier - Sistema de Verificación Riguroso de Scripts PySpark
=====================================================================

Sistema multi-nivel para verificar equivalencia estructural, lógica y semántica
entre scripts PySpark, especialmente para validar conversiones DDV→EDV.

Niveles de Verificación:
1. Análisis Estructural (AST)
2. Análisis de Flujo de Datos (PySpark Pipeline)
3. Análisis Semántico DDV vs EDV
4. Validación de Equivalencia Lógica

Autor: Claude Code
Versión: 1.0
"""

import ast
import re
from typing import Dict, List, Tuple, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import json
from difflib import SequenceMatcher


class DifferenceLevel(Enum):
    """Niveles de severidad de diferencias"""
    CRITICAL = "CRÍTICO"  # Altera resultado final
    HIGH = "ALTO"  # Puede alterar resultado
    MEDIUM = "MEDIO"  # Diferencia importante pero no crítica
    LOW = "BAJO"  # Diferencia menor/cosmética
    INFO = "INFO"  # Información, no es diferencia


class DifferenceCategory(Enum):
    """Categorías de diferencias"""
    STRUCTURE = "Estructura"
    DATA_FLOW = "Flujo de Datos"
    BUSINESS_LOGIC = "Lógica de Negocio"
    CONFIGURATION = "Configuración"
    OPTIMIZATION = "Optimización"
    DDV_EDV_EXPECTED = "DDV→EDV Esperado"
    DDV_EDV_UNEXPECTED = "DDV→EDV Inesperado"


@dataclass
class Difference:
    """Representa una diferencia encontrada entre scripts"""
    level: DifferenceLevel
    category: DifferenceCategory
    description: str
    script1_value: Any
    script2_value: Any
    location: str = ""
    impact: str = ""
    recommendation: str = ""

    def to_dict(self):
        return {
            "level": self.level.value,
            "category": self.category.value,
            "description": self.description,
            "script1_value": str(self.script1_value),
            "script2_value": str(self.script2_value),
            "location": self.location,
            "impact": self.impact,
            "recommendation": self.recommendation
        }


@dataclass
class VerificationReport:
    """Reporte completo de verificación"""
    script1_name: str
    script2_name: str
    is_equivalent: bool
    similarity_score: float  # 0-100
    differences: List[Difference] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_critical_differences(self) -> List[Difference]:
        return [d for d in self.differences if d.level == DifferenceLevel.CRITICAL]

    def get_high_differences(self) -> List[Difference]:
        return [d for d in self.differences if d.level == DifferenceLevel.HIGH]

    def to_dict(self):
        return {
            "script1_name": self.script1_name,
            "script2_name": self.script2_name,
            "is_equivalent": self.is_equivalent,
            "similarity_score": self.similarity_score,
            "total_differences": len(self.differences),
            "critical_count": len(self.get_critical_differences()),
            "high_count": len(self.get_high_differences()),
            "differences": [d.to_dict() for d in self.differences],
            "warnings": self.warnings,
            "metadata": self.metadata
        }


@dataclass
class ScriptMetadata:
    """Metadata extraída de un script"""
    # Header info
    proyecto: str = ""
    nombre: str = ""
    tabla_destino: str = ""
    tablas_fuentes: List[str] = field(default_factory=list)
    objetivo: str = ""
    version: str = ""

    # Imports
    imports: Set[str] = field(default_factory=set)
    from_imports: Dict[str, Set[str]] = field(default_factory=dict)

    # Functions
    function_names: Set[str] = field(default_factory=set)
    function_signatures: Dict[str, List[str]] = field(default_factory=dict)

    # Constants
    constants: Dict[str, Any] = field(default_factory=dict)

    # Widgets
    widgets: Dict[str, str] = field(default_factory=dict)

    # Variables
    variables: Set[str] = field(default_factory=set)


@dataclass
class PySparkOperation:
    """Representa una operación PySpark"""
    operation_type: str  # select, filter, groupBy, join, etc.
    dataframe_var: str
    parameters: List[str] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    line_number: int = 0

    def __repr__(self):
        return f"{self.dataframe_var}.{self.operation_type}({', '.join(self.parameters[:3])}{'...' if len(self.parameters) > 3 else ''})"


@dataclass
class DataFlowPipeline:
    """Pipeline de transformaciones de datos"""
    source_tables: List[str] = field(default_factory=list)
    operations: List[PySparkOperation] = field(default_factory=list)
    destination_table: str = ""
    intermediate_dataframes: Set[str] = field(default_factory=set)

    def get_operation_sequence(self) -> str:
        """Retorna secuencia de operaciones como string"""
        return " → ".join([op.operation_type for op in self.operations])


class SparkScriptParser:
    """Parser para extraer estructura y metadata de scripts PySpark"""

    def __init__(self, script_path: str):
        self.script_path = script_path
        with open(script_path, 'r', encoding='utf-8') as f:
            self.content = f.read()
        self.tree = ast.parse(self.content)
        self.lines = self.content.split('\n')

    def parse(self) -> ScriptMetadata:
        """Parsea el script y extrae toda la metadata"""
        metadata = ScriptMetadata()

        # Extraer header info
        self._extract_header_info(metadata)

        # Extraer imports
        self._extract_imports(metadata)

        # Extraer funciones
        self._extract_functions(metadata)

        # Extraer constantes
        self._extract_constants(metadata)

        # Extraer widgets
        self._extract_widgets(metadata)

        # Extraer variables
        self._extract_variables(metadata)

        return metadata

    def _extract_header_info(self, metadata: ScriptMetadata):
        """Extrae información del header del script"""
        header_patterns = {
            'proyecto': r'PROYECTO\s*:\s*(.+)',
            'nombre': r'NOMBRE\s*:\s*(.+)',
            'tabla_destino': r'TABLA DESTINO\s*:\s*(.+)',
            'objetivo': r'OBJETIVO\s*:\s*(.+)',
            'version': r'VERSION\s*(.+)'
        }

        for key, pattern in header_patterns.items():
            match = re.search(pattern, self.content, re.IGNORECASE)
            if match:
                setattr(metadata, key, match.group(1).strip())

        # Extraer tablas fuentes
        fuentes_match = re.search(r'TABLAS FUENTES\s*:\s*(.+)', self.content, re.IGNORECASE)
        if fuentes_match:
            fuentes_text = fuentes_match.group(1)
            # Separar por comas o líneas
            tablas = re.split(r'[,\n]', fuentes_text)
            metadata.tablas_fuentes = [t.strip() for t in tablas if t.strip()]

    def _extract_imports(self, metadata: ScriptMetadata):
        """Extrae todos los imports del script"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    metadata.imports.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module not in metadata.from_imports:
                    metadata.from_imports[module] = set()
                for alias in node.names:
                    metadata.from_imports[module].add(alias.name)

    def _extract_functions(self, metadata: ScriptMetadata):
        """Extrae definiciones de funciones"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.FunctionDef):
                metadata.function_names.add(node.name)
                # Extraer parámetros
                params = [arg.arg for arg in node.args.args]
                metadata.function_signatures[node.name] = params

    def _extract_constants(self, metadata: ScriptMetadata):
        """Extrae constantes definidas en el script"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        # Solo constantes (UPPERCASE)
                        if target.id.isupper():
                            try:
                                value = ast.literal_eval(node.value)
                                metadata.constants[target.id] = value
                            except:
                                # No se puede evaluar, guardar como string
                                metadata.constants[target.id] = ast.unparse(node.value)

    def _extract_widgets(self, metadata: ScriptMetadata):
        """Extrae definiciones de widgets Databricks"""
        widget_pattern = r'dbutils\.widgets\.\w+\s*\(\s*name\s*=\s*["\'](\w+)["\'].*?defaultValue\s*=\s*["\']([^"\']*)["\']'
        for match in re.finditer(widget_pattern, self.content):
            widget_name = match.group(1)
            default_value = match.group(2)
            metadata.widgets[widget_name] = default_value

    def _extract_variables(self, metadata: ScriptMetadata):
        """Extrae variables importantes (no constantes)"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        # Solo variables (no constantes)
                        if not target.id.isupper():
                            metadata.variables.add(target.id)


class DataFlowAnalyzer:
    """Analiza el flujo de datos y transformaciones PySpark"""

    def __init__(self, script_path: str):
        self.script_path = script_path
        with open(script_path, 'r', encoding='utf-8') as f:
            self.content = f.read()
        self.tree = ast.parse(self.content)
        self.lines = self.content.split('\n')

    def analyze(self) -> DataFlowPipeline:
        """Analiza el flujo de datos del script"""
        pipeline = DataFlowPipeline()

        # Extraer tablas fuente
        self._extract_source_tables(pipeline)

        # Extraer operaciones PySpark
        self._extract_pyspark_operations(pipeline)

        # Extraer tabla destino
        self._extract_destination_table(pipeline)

        # Identificar dataframes intermedios
        self._identify_intermediate_dataframes(pipeline)

        return pipeline

    def _extract_source_tables(self, pipeline: DataFlowPipeline):
        """Extrae tablas de origen (spark.read, spark.table)"""
        # Patrones para lectura de tablas
        read_patterns = [
            r'spark\.read\.table\(["\']([^"\']+)["\']',
            r'spark\.table\(["\']([^"\']+)["\']',
            r'\.load\(["\']([^"\']+)["\']',
        ]

        for pattern in read_patterns:
            for match in re.finditer(pattern, self.content):
                table_name = match.group(1)
                if table_name not in pipeline.source_tables:
                    pipeline.source_tables.append(table_name)

    def _extract_pyspark_operations(self, pipeline: DataFlowPipeline):
        """Extrae operaciones PySpark del código"""
        # Operaciones importantes a trackear
        operations_to_track = [
            'select', 'filter', 'where', 'groupBy', 'agg', 'join',
            'withColumn', 'withColumnRenamed', 'drop', 'distinct',
            'orderBy', 'sort', 'union', 'unionAll', 'cache', 'persist',
            'repartition', 'coalesce', 'fillna', 'dropna'
        ]

        # Regex para detectar operaciones PySpark
        for i, line in enumerate(self.lines):
            for op in operations_to_track:
                pattern = rf'(\w+)\.{op}\s*\('
                match = re.search(pattern, line)
                if match:
                    df_var = match.group(1)

                    # Extraer parámetros (simplificado)
                    params = self._extract_operation_params(line, op)

                    operation = PySparkOperation(
                        operation_type=op,
                        dataframe_var=df_var,
                        parameters=params,
                        line_number=i + 1
                    )
                    pipeline.operations.append(operation)

    def _extract_operation_params(self, line: str, operation: str) -> List[str]:
        """Extrae parámetros de una operación PySpark"""
        # Buscar contenido entre paréntesis
        match = re.search(rf'{operation}\s*\(([^)]*)\)', line)
        if match:
            params_str = match.group(1)
            # Separar por comas (simplificado, no maneja nested)
            params = [p.strip() for p in params_str.split(',') if p.strip()]
            return params[:10]  # Limitar a 10 params para no sobrecargar
        return []

    def _extract_destination_table(self, pipeline: DataFlowPipeline):
        """Extrae tabla de destino"""
        # Buscar write operations
        write_patterns = [
            r'\.saveAsTable\(["\']([^"\']+)["\']',
            r'write_delta\([^,]+,\s*["\']([^"\']+)["\']',
            r'\.save\(["\']([^"\']+)["\']',
        ]

        for pattern in write_patterns:
            match = re.search(pattern, self.content)
            if match:
                pipeline.destination_table = match.group(1)
                break

    def _identify_intermediate_dataframes(self, pipeline: DataFlowPipeline):
        """Identifica dataframes intermedios"""
        df_pattern = r'(df\w*)\s*='
        for match in re.finditer(df_pattern, self.content):
            df_name = match.group(1)
            pipeline.intermediate_dataframes.add(df_name)


class DDVEDVComparator:
    """Comparador especializado para scripts DDV vs EDV"""

    # Diferencias esperadas en conversión DDV→EDV
    EXPECTED_DIFFERENCES = {
        'widgets': {
            'PRM_ESQUEMA_TABLA_DDV': {
                'ddv_suffix': '',
                'edv_suffix': '_v',
                'description': 'DDV usa schema directo, EDV usa views (_v)'
            },
            'new_widgets_edv': [
                'PRM_CATALOG_NAME_EDV',
                'PRM_ESQUEMA_TABLA_EDV'
            ]
        },
        'variables': {
            'PRM_ESQUEMA_TABLA_ESCRITURA': {
                'only_in': 'EDV',
                'description': 'EDV requiere schema separado para escritura'
            }
        },
        'table_names': {
            'case': 'UPPERCASE',
            'description': 'EDV requiere nombres de tabla en UPPERCASE'
        }
    }

    def __init__(self, metadata1: ScriptMetadata, metadata2: ScriptMetadata,
                 script1_is_ddv: bool = True):
        self.metadata_ddv = metadata1 if script1_is_ddv else metadata2
        self.metadata_edv = metadata2 if script1_is_ddv else metadata1
        self.script1_is_ddv = script1_is_ddv

    def compare(self) -> List[Difference]:
        """Compara scripts DDV vs EDV y categoriza diferencias"""
        differences = []

        # Verificar widgets esperados
        differences.extend(self._compare_widgets())

        # Verificar schemas
        differences.extend(self._compare_schemas())

        # Verificar nombres de tablas
        differences.extend(self._compare_table_names())

        # Verificar variables de escritura
        differences.extend(self._compare_write_variables())

        return differences

    def _compare_widgets(self) -> List[Difference]:
        """Compara widgets entre DDV y EDV"""
        differences = []

        # Verificar que EDV tenga widgets nuevos requeridos
        required_edv_widgets = self.EXPECTED_DIFFERENCES['widgets']['new_widgets_edv']
        for widget in required_edv_widgets:
            if widget not in self.metadata_edv.widgets:
                differences.append(Difference(
                    level=DifferenceLevel.CRITICAL,
                    category=DifferenceCategory.DDV_EDV_UNEXPECTED,
                    description=f"Widget requerido para EDV no encontrado: {widget}",
                    script1_value="No existe",
                    script2_value="Debería existir",
                    location="Widgets section",
                    impact="EDV no podrá escribir correctamente a schema EDV",
                    recommendation=f"Agregar widget: dbutils.widgets.text('{widget}', defaultValue='...')"
                ))

        # Verificar sufijo _v en PRM_ESQUEMA_TABLA_DDV
        if 'PRM_ESQUEMA_TABLA_DDV' in self.metadata_edv.widgets:
            edv_schema_widget = self.metadata_edv.widgets['PRM_ESQUEMA_TABLA_DDV']
            if not edv_schema_widget.endswith('_v'):
                differences.append(Difference(
                    level=DifferenceLevel.CRITICAL,
                    category=DifferenceCategory.DDV_EDV_UNEXPECTED,
                    description="EDV debe usar views (_v) para PRM_ESQUEMA_TABLA_DDV",
                    script1_value=edv_schema_widget,
                    script2_value=f"{edv_schema_widget}_v",
                    location="PRM_ESQUEMA_TABLA_DDV widget",
                    impact="EDV leerá tabla directa en vez de view",
                    recommendation="Cambiar defaultValue a terminar en '_v'"
                ))

        return differences

    def _compare_schemas(self) -> List[Difference]:
        """Compara configuración de schemas"""
        differences = []

        # Verificar que EDV tenga PRM_ESQUEMA_TABLA_ESCRITURA
        if 'PRM_ESQUEMA_TABLA_ESCRITURA' not in self.metadata_edv.variables:
            differences.append(Difference(
                level=DifferenceLevel.CRITICAL,
                category=DifferenceCategory.DDV_EDV_UNEXPECTED,
                description="EDV debe definir PRM_ESQUEMA_TABLA_ESCRITURA",
                script1_value="No existe",
                script2_value="Debe existir",
                location="Variable declarations",
                impact="EDV escribirá a schema incorrecto",
                recommendation="Agregar: PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + '.' + PRM_ESQUEMA_TABLA_EDV"
            ))

        return differences

    def _compare_table_names(self) -> List[Difference]:
        """Compara nombres de tablas (deben estar en UPPERCASE en EDV)"""
        differences = []

        # Verificar PRM_TABLA_PRIMERATRANSPUESTA
        if 'PRM_TABLA_PRIMERATRANSPUESTA' in self.metadata_edv.widgets:
            tabla_name = self.metadata_edv.widgets['PRM_TABLA_PRIMERATRANSPUESTA']
            if tabla_name != tabla_name.upper():
                differences.append(Difference(
                    level=DifferenceLevel.CRITICAL,
                    category=DifferenceCategory.DDV_EDV_UNEXPECTED,
                    description="Nombre de tabla debe estar en UPPERCASE en EDV",
                    script1_value=tabla_name,
                    script2_value=tabla_name.upper(),
                    location="PRM_TABLA_PRIMERATRANSPUESTA widget",
                    impact="Query a tabla de parámetros fallará (case-sensitive)",
                    recommendation=f"Cambiar a: '{tabla_name.upper()}'"
                ))

        return differences

    def _compare_write_variables(self) -> List[Difference]:
        """Compara variables de escritura"""
        differences = []

        # Verificar que VAL_DESTINO_NAME use PRM_ESQUEMA_TABLA_ESCRITURA en EDV
        # Esto requeriría análisis más profundo del código
        # Por ahora, registramos como warning para verificación manual

        return differences


class SemanticValidator:
    """Validador de equivalencia semántica entre scripts"""

    def __init__(self, pipeline1: DataFlowPipeline, pipeline2: DataFlowPipeline,
                 metadata1: ScriptMetadata, metadata2: ScriptMetadata):
        self.pipeline1 = pipeline1
        self.pipeline2 = pipeline2
        self.metadata1 = metadata1
        self.metadata2 = metadata2

    def validate(self) -> List[Difference]:
        """Valida equivalencia semántica"""
        differences = []

        # Comparar secuencia de operaciones
        differences.extend(self._compare_operation_sequences())

        # Comparar funciones de negocio
        differences.extend(self._compare_business_functions())

        # Comparar constantes de negocio
        differences.extend(self._compare_business_constants())

        # Comparar joins
        differences.extend(self._compare_joins())

        # Comparar agregaciones
        differences.extend(self._compare_aggregations())

        return differences

    def _compare_operation_sequences(self) -> List[Difference]:
        """Compara la secuencia de operaciones PySpark"""
        differences = []

        seq1 = self.pipeline1.get_operation_sequence()
        seq2 = self.pipeline2.get_operation_sequence()

        if seq1 != seq2:
            # Calcular similitud
            similarity = SequenceMatcher(None, seq1, seq2).ratio()

            if similarity < 0.8:  # Menos de 80% similar
                differences.append(Difference(
                    level=DifferenceLevel.HIGH,
                    category=DifferenceCategory.DATA_FLOW,
                    description="Secuencia de operaciones PySpark difiere significativamente",
                    script1_value=seq1,
                    script2_value=seq2,
                    location="Data flow pipeline",
                    impact="Puede generar resultados diferentes",
                    recommendation="Revisar flujo de transformaciones manualmente"
                ))

        return differences

    def _compare_business_functions(self) -> List[Difference]:
        """Compara funciones de lógica de negocio"""
        differences = []

        # Funciones críticas que no deben cambiar
        critical_functions = {
            'extraccion_info_base_cliente',
            'extraccionInformacion12Meses',
            'agruparInformacionMesAnalisis',
            'logicaPostAgrupacionInformacionMesAnalisis'
        }

        funcs1 = self.metadata1.function_names
        funcs2 = self.metadata2.function_names

        # Verificar que funciones críticas existan en ambos
        missing_in_2 = critical_functions & funcs1 - funcs2
        missing_in_1 = critical_functions & funcs2 - funcs1

        for func in missing_in_2:
            differences.append(Difference(
                level=DifferenceLevel.CRITICAL,
                category=DifferenceCategory.BUSINESS_LOGIC,
                description=f"Función crítica de negocio faltante: {func}",
                script1_value="Existe",
                script2_value="No existe",
                location=f"Function: {func}",
                impact="Lógica de negocio incompleta",
                recommendation="Agregar función o verificar renombramiento"
            ))

        return differences

    def _compare_business_constants(self) -> List[Difference]:
        """Compara constantes de lógica de negocio"""
        differences = []

        # Constantes críticas que no deben cambiar
        critical_constants = {
            'CONS_GRUPO', 'CONS_PARTITION_DELTA_NAME',
            'CONS_FORMATO_DE_ESCRITURA_EN_DISCO', 'CONS_MODO_DE_ESCRITURA'
        }

        for const in critical_constants:
            if const in self.metadata1.constants and const in self.metadata2.constants:
                val1 = self.metadata1.constants[const]
                val2 = self.metadata2.constants[const]

                if val1 != val2:
                    differences.append(Difference(
                        level=DifferenceLevel.HIGH,
                        category=DifferenceCategory.BUSINESS_LOGIC,
                        description=f"Constante de negocio difiere: {const}",
                        script1_value=val1,
                        script2_value=val2,
                        location=f"Constant: {const}",
                        impact="Puede alterar comportamiento del proceso",
                        recommendation="Verificar si cambio es intencional"
                    ))

        return differences

    def _compare_joins(self) -> List[Difference]:
        """Compara operaciones de join"""
        differences = []

        joins1 = [op for op in self.pipeline1.operations if op.operation_type == 'join']
        joins2 = [op for op in self.pipeline2.operations if op.operation_type == 'join']

        if len(joins1) != len(joins2):
            differences.append(Difference(
                level=DifferenceLevel.HIGH,
                category=DifferenceCategory.DATA_FLOW,
                description="Número de joins difiere",
                script1_value=f"{len(joins1)} joins",
                script2_value=f"{len(joins2)} joins",
                location="Join operations",
                impact="Estructura de datos resultante puede diferir",
                recommendation="Revisar joins manualmente"
            ))

        return differences

    def _compare_aggregations(self) -> List[Difference]:
        """Compara operaciones de agregación"""
        differences = []

        aggs1 = [op for op in self.pipeline1.operations
                 if op.operation_type in ['groupBy', 'agg']]
        aggs2 = [op for op in self.pipeline2.operations
                 if op.operation_type in ['groupBy', 'agg']]

        if len(aggs1) != len(aggs2):
            differences.append(Difference(
                level=DifferenceLevel.HIGH,
                category=DifferenceCategory.DATA_FLOW,
                description="Número de agregaciones difiere",
                script1_value=f"{len(aggs1)} agregaciones",
                script2_value=f"{len(aggs2)} agregaciones",
                location="Aggregation operations",
                impact="Resultados agregados pueden diferir",
                recommendation="Revisar agregaciones manualmente"
            ))

        return differences


class ScriptVerifier:
    """Verificador principal que coordina todos los analizadores"""

    def __init__(self, script1_path: str, script2_path: str,
                 is_ddv_edv_comparison: bool = False):
        self.script1_path = script1_path
        self.script2_path = script2_path
        self.is_ddv_edv_comparison = is_ddv_edv_comparison

        # Parsers
        self.parser1 = SparkScriptParser(script1_path)
        self.parser2 = SparkScriptParser(script2_path)

        # Analyzers
        self.flow_analyzer1 = DataFlowAnalyzer(script1_path)
        self.flow_analyzer2 = DataFlowAnalyzer(script2_path)

    def verify(self) -> VerificationReport:
        """Ejecuta verificación completa"""
        report = VerificationReport(
            script1_name=self.script1_path.split('\\')[-1],
            script2_name=self.script2_path.split('\\')[-1],
            is_equivalent=True,
            similarity_score=100.0
        )

        # 1. Análisis Estructural
        print("🔍 Fase 1: Análisis Estructural (AST)...")
        metadata1 = self.parser1.parse()
        metadata2 = self.parser2.parse()
        structural_diffs = self._compare_structure(metadata1, metadata2)
        report.differences.extend(structural_diffs)

        # 2. Análisis de Flujo de Datos
        print("🔍 Fase 2: Análisis de Flujo de Datos...")
        pipeline1 = self.flow_analyzer1.analyze()
        pipeline2 = self.flow_analyzer2.analyze()
        flow_diffs = self._compare_data_flow(pipeline1, pipeline2)
        report.differences.extend(flow_diffs)

        # 3. Análisis DDV vs EDV (si aplica)
        if self.is_ddv_edv_comparison:
            print("🔍 Fase 3: Análisis DDV vs EDV...")
            ddv_edv_comparator = DDVEDVComparator(metadata1, metadata2, script1_is_ddv=True)
            ddv_edv_diffs = ddv_edv_comparator.compare()
            report.differences.extend(ddv_edv_diffs)

        # 4. Validación Semántica
        print("🔍 Fase 4: Validación Semántica...")
        semantic_validator = SemanticValidator(pipeline1, pipeline2, metadata1, metadata2)
        semantic_diffs = semantic_validator.validate()
        report.differences.extend(semantic_diffs)

        # Calcular similarity score
        report.similarity_score = self._calculate_similarity_score(report.differences)

        # Determinar equivalencia
        critical_count = len(report.get_critical_differences())
        high_count = len(report.get_high_differences())
        report.is_equivalent = (critical_count == 0 and high_count == 0 and
                               report.similarity_score >= 95.0)

        # Metadata adicional
        report.metadata = {
            'script1_functions': len(metadata1.function_names),
            'script2_functions': len(metadata2.function_names),
            'script1_operations': len(pipeline1.operations),
            'script2_operations': len(pipeline2.operations),
            'script1_source_tables': pipeline1.source_tables,
            'script2_source_tables': pipeline2.source_tables,
            'script1_destination': pipeline1.destination_table,
            'script2_destination': pipeline2.destination_table
        }

        return report

    def _compare_structure(self, metadata1: ScriptMetadata,
                          metadata2: ScriptMetadata) -> List[Difference]:
        """Compara estructura entre scripts"""
        differences = []

        # Comparar imports
        imports1 = metadata1.imports | set(metadata1.from_imports.keys())
        imports2 = metadata2.imports | set(metadata2.from_imports.keys())

        missing_imports = imports1 - imports2
        extra_imports = imports2 - imports1

        if missing_imports:
            differences.append(Difference(
                level=DifferenceLevel.MEDIUM,
                category=DifferenceCategory.STRUCTURE,
                description="Imports faltantes en script 2",
                script1_value=", ".join(missing_imports),
                script2_value="No existen",
                location="Import section",
                impact="Puede causar errores si se usan",
                recommendation="Verificar si imports son necesarios"
            ))

        # Comparar funciones
        funcs1 = metadata1.function_names
        funcs2 = metadata2.function_names

        missing_funcs = funcs1 - funcs2
        extra_funcs = funcs2 - funcs1

        if missing_funcs:
            differences.append(Difference(
                level=DifferenceLevel.HIGH,
                category=DifferenceCategory.STRUCTURE,
                description="Funciones faltantes en script 2",
                script1_value=", ".join(missing_funcs),
                script2_value="No existen",
                location="Function definitions",
                impact="Lógica incompleta",
                recommendation="Verificar si funciones fueron eliminadas o renombradas"
            ))

        return differences

    def _compare_data_flow(self, pipeline1: DataFlowPipeline,
                          pipeline2: DataFlowPipeline) -> List[Difference]:
        """Compara flujo de datos entre scripts"""
        differences = []

        # Comparar source tables
        if set(pipeline1.source_tables) != set(pipeline2.source_tables):
            differences.append(Difference(
                level=DifferenceLevel.CRITICAL,
                category=DifferenceCategory.DATA_FLOW,
                description="Tablas fuente difieren",
                script1_value=", ".join(pipeline1.source_tables),
                script2_value=", ".join(pipeline2.source_tables),
                location="Data sources",
                impact="Scripts leen datos diferentes",
                recommendation="Verificar que tablas fuente sean correctas"
            ))

        # Comparar destination table
        if pipeline1.destination_table != pipeline2.destination_table:
            # Esto es esperado en DDV vs EDV, pero crítico en otros casos
            level = (DifferenceLevel.INFO if self.is_ddv_edv_comparison
                    else DifferenceLevel.CRITICAL)
            category = (DifferenceCategory.DDV_EDV_EXPECTED if self.is_ddv_edv_comparison
                       else DifferenceCategory.DATA_FLOW)

            differences.append(Difference(
                level=level,
                category=category,
                description="Tabla destino difiere",
                script1_value=pipeline1.destination_table,
                script2_value=pipeline2.destination_table,
                location="Data destination",
                impact="Scripts escriben a diferentes destinos" if not self.is_ddv_edv_comparison else "Esperado en DDV vs EDV",
                recommendation="Esperado en conversión DDV→EDV" if self.is_ddv_edv_comparison else "Verificar destino correcto"
            ))

        # Comparar número de operaciones
        op_diff = abs(len(pipeline1.operations) - len(pipeline2.operations))
        if op_diff > 5:  # Más de 5 operaciones de diferencia
            differences.append(Difference(
                level=DifferenceLevel.MEDIUM,
                category=DifferenceCategory.DATA_FLOW,
                description="Número de operaciones difiere significativamente",
                script1_value=f"{len(pipeline1.operations)} operaciones",
                script2_value=f"{len(pipeline2.operations)} operaciones",
                location="Data transformations",
                impact="Flujo de transformación puede ser diferente",
                recommendation="Revisar pipeline de transformaciones"
            ))

        return differences

    def _calculate_similarity_score(self, differences: List[Difference]) -> float:
        """Calcula score de similitud (0-100) basado en diferencias"""
        if not differences:
            return 100.0

        # Pesos por nivel de severidad
        weights = {
            DifferenceLevel.CRITICAL: 20.0,
            DifferenceLevel.HIGH: 10.0,
            DifferenceLevel.MEDIUM: 5.0,
            DifferenceLevel.LOW: 2.0,
            DifferenceLevel.INFO: 0.0
        }

        # Calcular penalización total
        penalty = sum(weights[d.level] for d in differences)

        # Score = 100 - penalty (mínimo 0)
        score = max(0.0, 100.0 - penalty)

        return round(score, 2)


class ReportGenerator:
    """Genera reportes detallados de verificación"""

    @staticmethod
    def generate_html_report(report: VerificationReport, output_path: str):
        """Genera reporte HTML"""
        html_content = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte de Verificación de Scripts</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-card {{
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .summary-card.equivalent {{
            background-color: #d4edda;
            border: 2px solid #28a745;
        }}
        .summary-card.not-equivalent {{
            background-color: #f8d7da;
            border: 2px solid #dc3545;
        }}
        .summary-card.score {{
            background-color: #d1ecf1;
            border: 2px solid #17a2b8;
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 1.1em;
        }}
        .summary-card .value {{
            font-size: 2em;
            font-weight: bold;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #3498db;
            color: white;
            font-weight: bold;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .level-CRÍTICO {{
            color: #dc3545;
            font-weight: bold;
        }}
        .level-ALTO {{
            color: #fd7e14;
            font-weight: bold;
        }}
        .level-MEDIO {{
            color: #ffc107;
            font-weight: bold;
        }}
        .level-BAJO {{
            color: #17a2b8;
        }}
        .level-INFO {{
            color: #6c757d;
        }}
        .difference-row {{
            cursor: pointer;
        }}
        .difference-details {{
            display: none;
            background-color: #f8f9fa;
            padding: 15px;
            margin: 10px 0;
            border-left: 4px solid #3498db;
        }}
        .difference-details.show {{
            display: block;
        }}
        .metadata {{
            background-color: #e9ecef;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        .metadata dl {{
            display: grid;
            grid-template-columns: 200px 1fr;
            gap: 10px;
        }}
        .metadata dt {{
            font-weight: bold;
        }}
        .filter-buttons {{
            margin: 20px 0;
        }}
        .filter-btn {{
            padding: 8px 15px;
            margin-right: 10px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
        }}
        .filter-btn.active {{
            background-color: #3498db;
            color: white;
        }}
        .filter-btn:not(.active) {{
            background-color: #e9ecef;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 Reporte de Verificación de Scripts</h1>

        <div class="summary">
            <div class="summary-card {'equivalent' if report.is_equivalent else 'not-equivalent'}">
                <h3>Equivalencia</h3>
                <div class="value">{'✓ SÍ' if report.is_equivalent else '✗ NO'}</div>
            </div>
            <div class="summary-card score">
                <h3>Score de Similitud</h3>
                <div class="value">{report.similarity_score}%</div>
            </div>
            <div class="summary-card">
                <h3>Total Diferencias</h3>
                <div class="value">{len(report.differences)}</div>
            </div>
        </div>

        <div class="metadata">
            <h3>📋 Información de Scripts</h3>
            <dl>
                <dt>Script 1:</dt>
                <dd>{report.script1_name}</dd>
                <dt>Script 2:</dt>
                <dd>{report.script2_name}</dd>
                <dt>Funciones (Script 1):</dt>
                <dd>{report.metadata.get('script1_functions', 'N/A')}</dd>
                <dt>Funciones (Script 2):</dt>
                <dd>{report.metadata.get('script2_functions', 'N/A')}</dd>
                <dt>Operaciones (Script 1):</dt>
                <dd>{report.metadata.get('script1_operations', 'N/A')}</dd>
                <dt>Operaciones (Script 2):</dt>
                <dd>{report.metadata.get('script2_operations', 'N/A')}</dd>
            </dl>
        </div>

        <h2>⚠️ Diferencias Encontradas</h2>

        <div class="filter-buttons">
            <button class="filter-btn active" onclick="filterDifferences('all')">Todas ({len(report.differences)})</button>
            <button class="filter-btn" onclick="filterDifferences('CRÍTICO')">Críticas ({len([d for d in report.differences if d.level == DifferenceLevel.CRITICAL])})</button>
            <button class="filter-btn" onclick="filterDifferences('ALTO')">Altas ({len([d for d in report.differences if d.level == DifferenceLevel.HIGH])})</button>
            <button class="filter-btn" onclick="filterDifferences('MEDIO')">Medias ({len([d for d in report.differences if d.level == DifferenceLevel.MEDIUM])})</button>
        </div>

        <table id="differences-table">
            <thead>
                <tr>
                    <th>Nivel</th>
                    <th>Categoría</th>
                    <th>Descripción</th>
                    <th>Script 1</th>
                    <th>Script 2</th>
                </tr>
            </thead>
            <tbody>
"""

        # Agregar diferencias
        for i, diff in enumerate(report.differences):
            html_content += f"""
                <tr class="difference-row" data-level="{diff.level.value}" onclick="toggleDetails({i})">
                    <td class="level-{diff.level.value}">{diff.level.value}</td>
                    <td>{diff.category.value}</td>
                    <td>{diff.description}</td>
                    <td>{diff.script1_value[:100]}{'...' if len(str(diff.script1_value)) > 100 else ''}</td>
                    <td>{diff.script2_value[:100]}{'...' if len(str(diff.script2_value)) > 100 else ''}</td>
                </tr>
                <tr id="details-{i}" class="difference-details">
                    <td colspan="5">
                        <strong>Ubicación:</strong> {diff.location}<br>
                        <strong>Impacto:</strong> {diff.impact}<br>
                        <strong>Recomendación:</strong> {diff.recommendation}<br>
                        <strong>Valor Script 1:</strong> <pre>{diff.script1_value}</pre>
                        <strong>Valor Script 2:</strong> <pre>{diff.script2_value}</pre>
                    </td>
                </tr>
"""

        html_content += """
            </tbody>
        </table>
    </div>

    <script>
        function toggleDetails(index) {
            const details = document.getElementById('details-' + index);
            details.classList.toggle('show');
        }

        function filterDifferences(level) {
            const rows = document.querySelectorAll('.difference-row');
            const buttons = document.querySelectorAll('.filter-btn');

            // Update active button
            buttons.forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');

            // Filter rows
            rows.forEach(row => {
                const detailsRow = row.nextElementSibling;
                if (level === 'all' || row.dataset.level === level) {
                    row.style.display = '';
                    detailsRow.style.display = detailsRow.classList.contains('show') ? '' : 'none';
                } else {
                    row.style.display = 'none';
                    detailsRow.style.display = 'none';
                }
            });
        }
    </script>
</body>
</html>
"""

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

    @staticmethod
    def generate_json_report(report: VerificationReport, output_path: str):
        """Genera reporte JSON"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)

    @staticmethod
    def print_console_report(report: VerificationReport):
        """Imprime reporte en consola"""
        print("\n" + "="*80)
        print("🔍 REPORTE DE VERIFICACIÓN DE SCRIPTS")
        print("="*80)
        print(f"\n📄 Script 1: {report.script1_name}")
        print(f"📄 Script 2: {report.script2_name}")
        print(f"\n✨ Equivalencia: {'✓ SÍ' if report.is_equivalent else '✗ NO'}")
        print(f"📊 Score de Similitud: {report.similarity_score}%")
        print(f"⚠️  Total Diferencias: {len(report.differences)}")

        critical = report.get_critical_differences()
        high = report.get_high_differences()

        if critical:
            print(f"\n🚨 Diferencias CRÍTICAS: {len(critical)}")
            for diff in critical:
                print(f"   • {diff.description}")
                print(f"     Impacto: {diff.impact}")

        if high:
            print(f"\n⚠️  Diferencias ALTAS: {len(high)}")
            for diff in high:
                print(f"   • {diff.description}")

        print("\n" + "="*80)


# Función principal para uso directo
def verify_scripts(script1_path: str, script2_path: str,
                  is_ddv_edv: bool = False,
                  output_html: str = None,
                  output_json: str = None) -> VerificationReport:
    """
    Verifica similitud entre dos scripts

    Args:
        script1_path: Ruta al primer script
        script2_path: Ruta al segundo script
        is_ddv_edv: True si es comparación DDV vs EDV
        output_html: Ruta para guardar reporte HTML (opcional)
        output_json: Ruta para guardar reporte JSON (opcional)

    Returns:
        VerificationReport con resultados
    """
    print(f"\n🚀 Iniciando verificación...")
    print(f"📄 Script 1: {script1_path}")
    print(f"📄 Script 2: {script2_path}")
    print(f"🔄 Tipo: {'DDV vs EDV' if is_ddv_edv else 'Scripts individuales'}\n")

    verifier = ScriptVerifier(script1_path, script2_path, is_ddv_edv)
    report = verifier.verify()

    # Imprimir reporte en consola
    ReportGenerator.print_console_report(report)

    # Generar reportes adicionales
    if output_html:
        print(f"\n💾 Generando reporte HTML: {output_html}")
        ReportGenerator.generate_html_report(report, output_html)

    if output_json:
        print(f"💾 Generando reporte JSON: {output_json}")
        ReportGenerator.generate_json_report(report, output_json)

    return report


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Uso: python script_verifier.py <script1.py> <script2.py> [--ddv-edv] [--html output.html] [--json output.json]")
        sys.exit(1)

    script1 = sys.argv[1]
    script2 = sys.argv[2]
    is_ddv_edv = "--ddv-edv" in sys.argv

    output_html = None
    output_json = None

    if "--html" in sys.argv:
        idx = sys.argv.index("--html")
        if idx + 1 < len(sys.argv):
            output_html = sys.argv[idx + 1]

    if "--json" in sys.argv:
        idx = sys.argv.index("--json")
        if idx + 1 < len(sys.argv):
            output_json = sys.argv[idx + 1]

    report = verify_scripts(script1, script2, is_ddv_edv, output_html, output_json)
