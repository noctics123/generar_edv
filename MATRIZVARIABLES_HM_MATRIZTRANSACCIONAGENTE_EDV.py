# Databricks notebook source
# coding: utf-8

# ||***********************************************************************************************************************************************
# || PROYECTO   		  : MATRIZ DE VARIABLES - VARIABLES 
# || NOMBRE     		  : MATRIZVARIABLES_HM_MATRIZTRANSACCIONAGENTE_EDV.py
# || TABLA DESTINO	      : BCP_DDV_MATRIZVARIABLES.HM_MATRIZTRANSACCIONAGENTE
# || TABLA FUENTE		  : BCP_DDV_MATRIZVARIABLES.HM_CONCEPTOTRANSACCIONAGENTE
# ||            		  : BCP_DDV_MATRIZVARIABLES.HM_DETCONCEPTOCLIENTE
# ||            		  : BCP_DDV_MATRIZVARIABLES.MM_PARAMETROMATRIZVARIABLES
# || OBJETIVO   		  : Creacion de Segunda Transpuesta hm_matrizvartransaccionagente
# || TIPO       		  : pyspark
# || REPROCESABLE	    : SI
# || OBSERVACION        : Version EDV con esquemas separados (lectura DDV, escritura EDV)
# || SCHEDULER		    : NA
# || JOB  		        : @P4LKKA6
# || VERSION	DESARROLLADOR			  PROVEEDOR	     AT RESPONSABLE	     FECHA		    DESCRIPCION
# || 1        DIEGO GUERRA CRUZADO       	Everis    	  ARTURO ROJAS       06/02/19     Creacion del proceso
# ||          GIULIANA PABLO ALEJANDRO
# || 2        DIEGO UCHARIMA                 Everis       RODRIGO ROJAS     04/05/20     Modificacion del proceso
# ||          MARCOS IRVING MERA SANCHEZ
# || 3        KEYLA NALVARTE                 INDRA        RODRIGO ROJAS     15/06/23     ModificaciÓn de la cabecera
# || 4        ROMARIO CORONEL                INDRA        RODRIGO ROJAS     24/01/23     MIGRACION LHCL HM_MATRIZTRANSACCIONAGENTE
# || 5        EDWIN ROQUE                    INDRA        RODRIGO ROJAS     13/09/24     REDUCCION DEUDA TECNICA Q3
# || 6   	  ESTEFANI MELGAR                INDRA     	  RODRIGO ROJAS     12/06/25     [DAOPBCP – 59432] Corrección Linaje [Excelencia Ingeniería]
# ************************************************************************************************************************************************

# COMMAND ----------

###
 # @section Import
 ##
 
from pyspark.sql.functions import col, concat, when, lit, sha2,trim
from pyspark.sql import functions as F 
import pyspark.sql.functions as func
import itertools
import funciones
from pyspark.sql import DataFrame
        

# COMMAND ----------

CONS_WRITE_FORMAT_NAME = "delta"
CONS_COMPRESSION_MODE_NAME = "snappy"
CONS_WRITE_TEXT_MODE_NAME = "overwrite"
CONS_DFS_NAME = ".dfs.core.windows.net/"
CONS_CONTAINER_NAME = "abfss://bcp-edv-trdata-012@"
CONS_PARTITION_DELTA_NAME = "codmes"
CONS_PARTITION_DELTA_TMP_NAME = "CODMES"
CONS_COMPRESSION_NAME = "compression"
CONS_PARTITION_OVERWRITE_MODE = 'partitionOverwriteMode'
CONS_DYNAMIC = 'dynamic'

# COMMAND ----------

spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss",False) 
spark.conf.set("spark.databricks.io.cache.enabled", False)
spark.conf.set("spark.sql.shuffle.partitions","500")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)


dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables_v')
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')
dbutils.widgets.text(name="PRM_TABLA_PRIMERATRANSPUESTA", defaultValue='HM_CONCEPTOTRANSACCIONAGENTE')
dbutils.widgets.text(name="PRM_TABLA_SEGUNDATRANSPUESTA", defaultValue='HM_MATRIZTRANSACCIONAGENTE_RUBEN')
dbutils.widgets.text(name="PRM_TABLA_SEGUNDATRANSPUESTA_TMP", defaultValue='hm_matriztransaccionagente_tmp_ruben')
dbutils.widgets.text(name="PRM_TABLA_PARAMETROMATRIZVARIABLES", defaultValue='mm_parametromatrizvariables')
dbutils.widgets.text(name="PRM_FECHA_RUTINA", defaultValue='2025-09-01')
dbutils.widgets.text(name="PRM_STORAGE_ACCOUNT_DDV", defaultValue='adlscu1lhclbackp05')
dbutils.widgets.text(name="PRM_CARPETA_OUTPUT", defaultValue='data/RUBEN/DEUDA_TECNICA/out')
dbutils.widgets.text(name="PRM_RUTA_ADLS_TABLES", defaultValue='data/RUBEN/DEUDA_TECNICA/matrizvariables')
dbutils.widgets.text(name="PRM_CATALOG_NAME", defaultValue='catalog_lhcl_prod_bcp')
dbutils.widgets.text(name="PRM_TABLA_HM_DETCONCEPTOCLIENTE", defaultValue='hm_detconceptocliente')

# COMMAND ----------

PRM_ESQUEMA_TABLA_DDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_DDV")
PRM_TABLA_PRIMERATRANSPUESTA = dbutils.widgets.get("PRM_TABLA_PRIMERATRANSPUESTA")
PRM_TABLA_SEGUNDATRANSPUESTA = dbutils.widgets.get("PRM_TABLA_SEGUNDATRANSPUESTA")
PRM_TABLA_SEGUNDATRANSPUESTA_TMP = dbutils.widgets.get("PRM_TABLA_SEGUNDATRANSPUESTA_TMP")
PRM_TABLA_PARAMETROMATRIZVARIABLES = dbutils.widgets.get("PRM_TABLA_PARAMETROMATRIZVARIABLES")
PRM_FECHA_RUTINA = dbutils.widgets.get("PRM_FECHA_RUTINA")
PRM_STORAGE_ACCOUNT_DDV = dbutils.widgets.get("PRM_STORAGE_ACCOUNT_DDV")
PRM_CARPETA_OUTPUT = dbutils.widgets.get("PRM_CARPETA_OUTPUT")
PRM_RUTA_ADLS_TABLES = dbutils.widgets.get("PRM_RUTA_ADLS_TABLES")
PRM_CATALOG_NAME = dbutils.widgets.get("PRM_CATALOG_NAME")
PRM_TABLA_HM_DETCONCEPTOCLIENTE = dbutils.widgets.get("PRM_TABLA_HM_DETCONCEPTOCLIENTE")

PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")

PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME+"."+PRM_ESQUEMA_TABLA_DDV
PRM_CARPETA_RAIZ_DE_PROYECTO = CONS_CONTAINER_NAME+PRM_STORAGE_ACCOUNT_DDV+CONS_DFS_NAME+PRM_RUTA_ADLS_TABLES

PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLA_SEGUNDATRANSPUESTA

# COMMAND ----------

def write_delta(df, output, partition):
    df.write \
    .format(CONS_WRITE_FORMAT_NAME) \
    .option(CONS_COMPRESSION_NAME,CONS_COMPRESSION_MODE_NAME) \
    .option(CONS_PARTITION_OVERWRITE_MODE,CONS_DYNAMIC) \
    .partitionBy(partition).mode(CONS_WRITE_TEXT_MODE_NAME) \
    .saveAsTable(output)

def write_temp_table(df, tmp_table_name, ruta_salida):
    df.write \
        .format(CONS_WRITE_FORMAT_NAME) \
        .mode(CONS_WRITE_TEXT_MODE_NAME) \
        .partitionBy(CONS_PARTITION_DELTA_TMP_NAME) \
        .saveAsTable(tmp_table_name)

# COMMAND ----------
###
 # Obtiene la información de la tabla Base clientes
 #
 # @param nCodMes {string}
 # @param esquemaTabla {string} Esquema al que pertence la tabla
 # @param rutaCarpetaProyecto {string} Ruta principal del proyecto en dataLake
 # @param carpetaDetConceptoClienteSegundaTranspuesta {string} Nombre de la carpeta donde se localiza temporalmente información de la tabla HM_DETCONCEPTOCLIENTE.
 # @param spark {Spark Object}
 # @return {void}
 ##
def extraccion_info_base_cliente(nCodMes: str,
                                  esquemaTabla: str,
                                  rutaCarpetaProyecto: str,
                                  carpetaDetConceptoClienteSegundaTranspuesta: str
                                  ) -> None:
    """
    Extrae información de la tabla HM_DETCONCEPTOCLIENTE y la guarda en una tabla temporal en el Data Lake.

    Parámetros:
    - nCodMes: Código del mes de análisis (formato string).
    - esquemaTabla: Esquema de la tabla en la base de datos.
    - rutaCarpetaProyecto: Ruta principal del proyecto en el Data Lake.
    - carpetaDetConceptoClienteSegundaTranspuesta: Carpeta temporal para guardar los datos extraídos.

    Retorna:
    - None
    """
    
    # Construcción de la consulta SQL
    consulta_sql = f"""
        SELECT
            CODMES,
            CODUNICOCLI AS CODUNICOCLI_LAST,
            MAX(CODINTERNOCOMPUTACIONALCONCEPTO) AS CODINTERNOCOMPUTACIONAL_LAST
        FROM
            {esquemaTabla}.HM_DETCONCEPTOCLIENTE
        WHERE
            CODMES = {nCodMes}
        GROUP BY
            CODMES, CODUNICOCLI
    """
    
    # Ejecución de la consulta
    df_cliente_cuc = spark.sql(consulta_sql)

    # Definición de la ruta de salida
    ruta_salida_1 = f"{rutaCarpetaProyecto}/temp/{carpetaDetConceptoClienteSegundaTranspuesta}"
    tmp_table_1 = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpetaDetConceptoClienteSegundaTranspuesta}_tmp'.lower()

    # Escritura del DataFrame en disco
    (
        df_cliente_cuc
        .write
        .format(CONS_WRITE_FORMAT_NAME)
        .partitionBy('CODMES')
        .mode(CONS_WRITE_TEXT_MODE_NAME)
        .saveAsTable(tmp_table_1)
    )

###
 # Extraemos informacion de 12 meses.
 #
 # @param codMes {string} Mes superior.
 # @param codMes12Atras {string} Mes inferior.
 # @param carpetaDetConceptoClienteSegundaTranspuesta {string} Nombre de la carpeta donde se localiza temporalmente informaci�n de la tabla HM_DETCONCEPTOCLIENTE.
 # @param carpetaClientePrimeraTranspuesta {string} Nombre de la carpeta donde se localiza temporalmente la informaci�n de la tabla HM_CONCEPTOTRANSACCIONAGENTE.
 # @return {Dataframe Spark} Datos de los �ltimos 12 meses.
 ##
 
def extraccionInformacion12Meses(codMes, codMes12Atras, carpetaDetConceptoClienteSegundaTranspuesta, carpetaClientePrimeraTranspuesta):
    
    #Leemos el dataframe de la tabla temporal   
    dfClienteCuc = spark.table(f'{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpetaDetConceptoClienteSegundaTranspuesta}_tmp') 

    #Dataframe con informaci�n de los codinternocomputacional de los clientes de la tabla HM_DETCONCEPTOCLIENTE
    dfClienteCic = spark.sql("""
                             SELECT
                                CODMES,
                                TRIM(CODINTERNOCOMPUTACIONAL) AS CODINTERNOCOMPUTACIONAL,
                                CODINTERNOCOMPUTACIONALCONCEPTO CODINTERNOCOMPUTACIONAL_LAST,
                                CODUNICOCLI CODUNICOCLI_LAST
                             FROM
                                {var:esquemaTabla}.{var:tabladetconceptocliente}
                             WHERE
                                CODMES = {var:codMes}
                             """.\
                             replace("{var:codMes}", codMes).\
                             replace("{var:esquemaTabla}", PRM_ESQUEMA_TABLA) .\
                             replace("{var:tabladetconceptocliente}", PRM_TABLA_HM_DETCONCEPTOCLIENTE)
                             )
                             
    #Nos quedamos con las columnas necesarias para el procesamiento y construcci�n de la segunda transpuesta
    dfInfo12MesesStep0 = spark.sql("""
                                   SELECT
                                     CODUNICOCLI,
                                     TRIM(CODINTERNOCOMPUTACIONAL) AS CODINTERNOCOMPUTACIONAL,
                                     CAN_MTO_TMO_AGT,
                                     CAN_CTD_TMO_AGT,
                                     CAN_CTD_TNM_AGT,
                                     CAN_MTO_TMO_AGT_SOL,
                                     CAN_CTD_TMO_AGT_SOL,
                                     CAN_MTO_TMO_AGT_DOL,
                                     CAN_CTD_TMO_AGT_DOL,
                                     CAN_MTO_TMO_AGT_RET,
                                     CAN_CTD_TMO_AGT_RET,
                                     CAN_MTO_TMO_AGT_DEP,
                                     CAN_CTD_TMO_AGT_DEP,
                                     CAN_MTO_TMO_AGT_TRF,
                                     CAN_CTD_TMO_AGT_TRF,
                                     CAN_MTO_TMO_AGT_ADS,
                                     CAN_CTD_TMO_AGT_ADS,
                                     CAN_MTO_TMO_AGT_DIS,
                                     CAN_CTD_TMO_AGT_DIS,
                                     CAN_MTO_TMO_AGT_PAG_TCR,
                                     CAN_CTD_TMO_AGT_PAG_TCR,
                                     CAN_MTO_TMO_AGT_PAG_BCP,
                                     CAN_CTD_TMO_AGT_PAG_BCP,
                                     CAN_MTO_TMO_AGT_PAG_SRV,
                                     CAN_CTD_TMO_AGT_PAG_SRV,
                                     CAN_MTO_TMO_AGT_EMIS,
                                     CAN_CTD_TMO_AGT_EMIS,
                                     CAN_MTO_TMO_AGT_DGIR,
                                     CAN_CTD_TMO_AGT_DGIR,
                                     CAN_CTD_TNM_AGT_CSLT,
                                     CAN_CTD_TNM_AGT_ADMN,
                                     CAN_FLG_TMO_AGT,
                                     CAN_TKT_TMO_AGT,
                                     CAN_FLG_TNM_AGT,
                                     CAN_FLG_TMO_AGT_SOL,
                                     CAN_TKT_TMO_AGT_SOL,
                                     CAN_FLG_TMO_AGT_DOL,
                                     CAN_TKT_TMO_AGT_DOL,
                                     CAN_FLG_TMO_AGT_RET,
                                     CAN_TKT_TMO_AGT_RET,
                                     CAN_FLG_TMO_AGT_DEP,
                                     CAN_TKT_TMO_AGT_DEP,
                                     CAN_FLG_TMO_AGT_TRF,
                                     CAN_TKT_TMO_AGT_TRF,
                                     CAN_FLG_TMO_AGT_ADS,
                                     CAN_TKT_TMO_AGT_ADS,
                                     CAN_FLG_TMO_AGT_DIS,
                                     CAN_TKT_TMO_AGT_DIS,
                                     CAN_FLG_TMO_AGT_PAG_TCR,
                                     CAN_TKT_TMO_AGT_PAG_TCR,
                                     CAN_FLG_TMO_AGT_PAG_BCP,
                                     CAN_TKT_TMO_AGT_PAG_BCP,
                                     CAN_FLG_TMO_AGT_PAG_SRV,
                                     CAN_TKT_TMO_AGT_PAG_SRV,
                                     CAN_FLG_TMO_AGT_EMIS,
                                     CAN_TKT_TMO_AGT_EMIS,
                                     CAN_FLG_TMO_AGT_DGIR,
                                     CAN_TKT_TMO_AGT_DGIR,
                                     CAN_FLG_TNM_AGT_CSLT,
                                     CAN_FLG_TNM_AGT_ADMN,
                                     CODMES   
                                   FROM {var:esquemaTabla}.{var:tablaPrimeraTranspuesta}
                                   WHERE 
                                      CODMES <= {var:codMes} AND CODMES >= {var:codMes12Atras}
                                   """.\
                                   replace("{var:codMes}", codMes).\
                                   replace("{var:codMes12Atras}", codMes12Atras).\
                                   replace("{var:esquemaTabla}", PRM_ESQUEMA_TABLA).\
                                   replace("{var:tablaPrimeraTranspuesta}", PRM_TABLA_PRIMERATRANSPUESTA)
                                   )
                                   
    #Obtenemos las columnas de la primera transpuesta
    dfInfo12MesesStep0Columns = dfInfo12MesesStep0.columns
    
    #Removemos del dataframe las columnas COCUNICOCLI,CODINTERNOCOMPUTACIONAL Y CODMES
    dfInfo12MesesStep0Columns.remove("CODUNICOCLI")
    dfInfo12MesesStep0Columns.remove("CODINTERNOCOMPUTACIONAL")
    dfInfo12MesesStep0Columns.remove("CODMES")
    
    dfInfo12MesesStep1Cic = dfInfo12MesesStep0.join(dfClienteCic.drop('CODMES'), on = 'CODINTERNOCOMPUTACIONAL', how = 'left')
    
    #Renombramos la columna CODUNICOCLI_LAST,CODINTERNOCOMPUTACIONAL_LAST del dataframe y lo almacenamos en un nuevo dataframe
    columnas = dfInfo12MesesStep1Cic.columns
    dfInfo12MesesStep1Cic1 = dfInfo12MesesStep1Cic.select(*columnas + 
                                                        [F.col("CODUNICOCLI_LAST").alias("CODUNICOCLI_LAST_CIC")] +
                                                        [F.col("CODINTERNOCOMPUTACIONAL_LAST").alias("CODINTERNOCOMPUTACIONAL_LAST_CIC")]
                                                    ).drop("CODUNICOCLI_LAST","CODINTERNOCOMPUTACIONAL_LAST")
    
    #El resultado lo colocamos en una vista
    dfInfo12MesesStep1Cic1.createOrReplaceTempView('temp1_cic')

    dfInfo12MesesStep1Cic2 = spark.sql("""
                                      SELECT 
                                         CODMES, 
                                         CODUNICOCLI, 
                                         CODINTERNOCOMPUTACIONAL,
                                         CASE WHEN TRIM(CODINTERNOCOMPUTACIONAL)<>'.' THEN CODUNICOCLI_LAST_CIC ELSE CODUNICOCLI END CODUNICOCLI_LAST,
                                         CASE WHEN TRIM(CODINTERNOCOMPUTACIONAL)<>'.' THEN CODINTERNOCOMPUTACIONAL_LAST_CIC ELSE CODINTERNOCOMPUTACIONAL END CODINTERNOCOMPUTACIONAL_LAST_CIC,
                                         """+','.join(dfInfo12MesesStep0Columns)+' '+"""
                                      FROM 
                                         temp1_cic
                                      """)
    
    dfInfo12MesesStep2Cuc = dfInfo12MesesStep1Cic2.join(dfClienteCuc.drop('CODMES'), on = 'CODUNICOCLI_LAST', how = 'left')
    
    #Renombramos la columna CODINTERNOCOMPUTACIONAL_LAST dataframe y lo almacenamos en un nuevo dataframe
    columnas = dfInfo12MesesStep2Cuc.columns
    dfInfo12MesesStep2Cuc1 = dfInfo12MesesStep2Cuc.select(*columnas + [F.col("CODINTERNOCOMPUTACIONAL_LAST").alias("CODINTERNOCOMPUTACIONAL_LAST_CUC")]).drop("CODINTERNOCOMPUTACIONAL_LAST")
    
    #El resultado lo colocamos en una vista
    dfInfo12MesesStep2Cuc1.createOrReplaceTempView('temp2_cuc')
    
    #Formamos una cadenas separada por comas(,) con los nombres de las columnas del dataframe
    resultOperadorColumnasDf = ','.join(funciones.calcularOperadoresColumnasDf(dfInfo12MesesStep0Columns))+' '
    
    dfInfo12Meses = spark.sql("""
                               SELECT 
                                  """+codMes+""" CODMESANALISIS, 
                                  CODMES, 
                                  CODUNICOCLI_LAST CODUNICOCLI,
                                  CASE WHEN TRIM(CODUNICOCLI_LAST)<> '"""+ funciones.CONS_VALOR_HASH_PUNTO +"""' THEN NVL(CODINTERNOCOMPUTACIONAL_LAST_CUC, RPAD('.', 12, ' ')) ELSE CODINTERNOCOMPUTACIONAL_LAST_CIC END CODINTERNOCOMPUTACIONAL , 
                                  """+resultOperadorColumnasDf+"""
                               FROM 
                                  temp2_cuc
                               GROUP BY 
                                  CODMES, 
                                  CODUNICOCLI_LAST, 
                                  CASE WHEN TRIM(CODUNICOCLI_LAST)<> '"""+ funciones.CONS_VALOR_HASH_PUNTO +"""' THEN NVL(CODINTERNOCOMPUTACIONAL_LAST_CUC, RPAD('.', 12, ' ')) ELSE CODINTERNOCOMPUTACIONAL_LAST_CIC END 
                               """
                               )

    
    #Escribimos los nombres de las columnas del dataframe en mayúsculas	                        
    dfInfo12Meses1 = dfInfo12Meses.select([F.col(x).alias(x.upper()) for x in dfInfo12Meses.columns])

    #Escribimos el resultado en una tabla temporal de Databricks
    tmp_table_2 = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpetaClientePrimeraTranspuesta}_tmp'.lower()
    ruta_salida_2 = f'{PRM_CARPETA_RAIZ_DE_PROYECTO}/temp/{carpetaClientePrimeraTranspuesta}'
    write_temp_table(dfInfo12Meses1, tmp_table_2, ruta_salida_2)
    #Leemos el resultado calculado
    dfInfo12Meses2 = spark.table(tmp_table_2)

    return dfInfo12Meses2


# COMMAND ----------

  
###
 # Se genera la 2da transpuesta
 # 
 # @param dfInfo12Meses {Dataframe Spark} Listado de columnas de la primera transpuesta y segunda transpuesta
 # @param codMes {List} Listado de columnas de la primera transpuesta
 # @param PRM_TABLA_SEGUNDATRANSPUESTA_TMP {string} Nombre de la carpeta temporal donde se almacenar� informaci�n de la tabla de la segunda transpuesta
 #
 # @return {Dataframe Spark} Listado de columnas generados dinamicamente en el proceso windowAggregate().
 ##
def agruparInformacionMesAnalisis(dfInfo12Meses):

    #Creamos un dataframe temportal en memoria
    dfInfo12Meses.createOrReplaceTempView('dfViewInfoMesesSql')
    
    #Almacena los nombres de las columnas de la tabla dfInfo12Meses
    nameColumn = []
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo MONTO
    columMto = []
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo CANTIDAD
    columCtd = []
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo FLAG
    columFlg = []
   
    #Leemos las columnas del dataframe dfInfo12Meses y los asignamos a una lista
    nameColumn = dfInfo12Meses.schema.names
    
    #Nos quedamos solo con las columnas de tipo Monto, Cantidad y Flag
    for nColumns in nameColumn:
            if 'CAN' in nColumns:
                if  'MTO' in nColumns or 'TKT' in nColumns:
                    columMto.append(nColumns)
                if  'CTD' in nColumns:
                    columCtd.append(nColumns)        
                if  'FLG' in nColumns:
                    columFlg.append(nColumns)
                       
    #Almacena las columnas tipo MONTO de la tabla dfInfo12Meses 
    colsToExpandMonto = columMto
    
    #Almacena las columnas tipo CTD de la tabla dfInfo12Meses
    colsToExpandCantidad = columCtd
    
    #Almacena las columnas tipo FLG de la tabla dfInfo12Meses
    colsToExpandFlag = columFlg
   
    #La _Parte1 sera para la ejecucion de la plantilla tipo MONTO
    aggsIter_Part1Avg = itertools.product(colsToExpandMonto,
                                         [F.avg],
                                         [0,-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part1Avg_PRM_P6M = itertools.product(colsToExpandMonto,
                                                 [F.avg],
                                                 [-11],
                                                 [-6])
                                                 
    aggsIter_Part1Avg_PRM_P3M = itertools.product(colsToExpandMonto,
                                                 [F.avg],
                                                 [-5],
                                                 [-3])
                                                 
    aggsIter_Part1Avg_Prev_Mes = itertools.product(colsToExpandMonto,
                                                   [F.avg],
                                                   [-1],
                                                   [-1])
    
    aggsIter_Part1Max = itertools.product(colsToExpandMonto,
                                         [F.max],
                                         [-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part1Min = itertools.product(colsToExpandMonto,
                                         [F.min],
                                         [-2,-5,-8,-11],
                                         [0])
    
    #La _Parte2 sera para la ejecucion de la plantilla tipo CTD
    aggsIter_Part2Avg = itertools.product(colsToExpandCantidad,
                                         [F.avg],
                                         [0,-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part2Max = itertools.product(colsToExpandCantidad,
                                         [F.max],
                                         [-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part2Min = itertools.product(colsToExpandCantidad,
                                         [F.min],
                                         [-2,-5,-8,-11],
                                         [0])
        
    #La _Parte3 sera para la ejecucion de la plantilla tipo FLG
    aggsIter_Part3Avg = itertools.product(colsToExpandFlag,
                                         [F.avg],
                                         [0],
                                         [0])
    
    aggsIter_Part3Max = itertools.product(colsToExpandFlag,
                                         [F.max],
                                         [-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part3Sum = itertools.product(colsToExpandFlag,
                                         [F.sum],
                                         [-2,-5,-8,-11],
                                         [0])
    
    #Lista de los meses a retroceder para las variables a las cuales se les calculara su frecuencia
    mesFrecuenciasMesesCtd =  [-2,-5,-8,-11]
   
    #La funcion chain() solo es para trasformner las columnas en horizontal el resultados de cada una de las funciones ,diaria q es como concatenar
    aggsIterFinal = itertools.chain(aggsIter_Part1Avg, aggsIter_Part1Avg_PRM_P6M, aggsIter_Part1Avg_PRM_P3M, aggsIter_Part1Avg_Prev_Mes,
                                   aggsIter_Part1Max, aggsIter_Part1Min, aggsIter_Part2Avg, aggsIter_Part2Max, aggsIter_Part2Min,
                                   aggsIter_Part3Avg, aggsIter_Part3Max, aggsIter_Part3Sum)
    
    #Funcion core que realiza el procesamiento del calculo de las nuevas columnas generadas dinamicamente segun sea el tipo de variable y plantilla    
    dfMatrizVarTransaccionAgente = funciones.windowAggregateOpt(dfInfo12Meses,
                              partitionCols = funciones.CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP,
                              orderCol = 'CODMES',
                              aggregations = aggsIterFinal,
                              flag = 1
                              )
    
    #Leemos las columnas del dataframe dfMatrizVarTransaccionAgente y los asignamos a una lista
    columnNamesDf = dfMatrizVarTransaccionAgente.schema.names
    
    #Extraemos las columnas de tipo Flag
    colsFlg = funciones.extraccionColumnasFlg(columnNamesDf,"_FLG_")
    
    #Separar las columnas por comas
    columnFlag = colsFlg.replace("'","").split(',')
    
    #Se concatena una cadena con las columnas tipo flag extraidas previamente
    colsFlagFin = "'CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL','"+colsFlg+"'"
    
    #Se crea el nuevo dataframe dfFlag con los nuevos campos tipo flag
    dfFlag = dfMatrizVarTransaccionAgente.select(eval("[{templatebody}]".replace("{templatebody}", colsFlagFin)))
    
    #Leemos las columnas del dataframe dfFlag y los asignamos a una lista
    dfFlag = dfFlag.schema.names
    
    #Casteamos de string a int las columnas de la lista dfFlag
    columnCastFlag = funciones.casteoColumns(dfFlag,'int')
    
    #Se crea el nuevo dataframe dfFlagFin con los campos tipo flag casteados
    dfFlagFin = dfMatrizVarTransaccionAgente.select(eval("[{templatebody}]".replace("{templatebody}", columnCastFlag)))
    
    #Se crea el nuevo dataframe dfColumnSinFlg con los campos excepto los tipo flag 
    dfColumnSinFlg = dfMatrizVarTransaccionAgente.select([c for c in dfMatrizVarTransaccionAgente.columns if c not in columnFlag])
    
    #Leemos las columnas del dataframe dfColumnSinFlg y los asignamos a una lista
    columnSinFlag =  dfColumnSinFlg.columns
    
    #Casteamos de string a double las columnas de la lista columnSinFlag
    columnCastSinFlag = funciones.casteoColumns(columnSinFlag,'double')
    
    #Se crea el nuevo dataframe dfSinFLagFin con todos los campos excepto los tipo flag 
    dfSinFLagFin = dfMatrizVarTransaccionAgente.select(eval("[{templatebody}]".replace("{templatebody}", columnCastSinFlag)))
    
    #Cruzamos los dataframes dfFlagFin y dfSinFLagFin por las columnas CODMESANALISIS,CODMES,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfMatrizVarTransaccionAgente = dfFlagFin.join(dfSinFLagFin,['CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL'])
    
    #Se calcula las frecuencias para las variables de tipo cantidad con un retroceso de 3,6,9,12 meses
    dfFrecuencia = funciones.calcularFrecuencias(mesFrecuenciasMesesCtd, colsToExpandCantidad,spark)
    
    #Se calcula la recencia para las variables de tipo cantidad
    dfRecencia = funciones.calculoRecencia(colsToExpandCantidad,spark)
    
    #Cruzamos los dataframes dfRecencia y dfFrecuencia por las columnas CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfRecenciaFrecuencia = dfRecencia.join(dfFrecuencia, funciones.CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP)
    
    #Cruzamos los dataframes dfMatrizVarTransaccionAgente y dfRecenciaFrecuencia por las columnas CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfMatrizVarTransaccionAgente = dfMatrizVarTransaccionAgente.join(dfRecenciaFrecuencia, ['CODMESANALISIS','CODUNICOCLI','CODINTERNOCOMPUTACIONAL'])
    
    #Leemos las columnas del dataframe dfMatrizVarTransaccionAgente y los asignamos a una lista    
    dfMatrizVarTransaccionAgenteColumns = dfMatrizVarTransaccionAgente.schema.names
    
    #Extraemos las columnas promedio ultimos 6 meses
    colsMont_PRM_U6M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteColumns,"_PRM_U6M")
    
    #Extraemos las columnas promedio primeros 6 meses
    colsMont_PRM_P6M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteColumns,"_PRM_P6M")
    
    #Extraemos las columnas promedio primeros 3 meses
    colsMont_PRM_P3M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteColumns,"_PRM_P3M")
    
    #Extraemos las columnas promedio ultimos 3 meses
    colsMont_PRM_U3M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteColumns,"_PRM_U3M")
    
    #Extraemos la columna promedio del penultimo mes
    colsMont_P1M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteColumns,"_P1M")
    
    #Extraemos la columna promedio del ultimo mes
    colsMont_U1M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteColumns,"_U1M")
    
    #Se concatena una cadena con las columnas tipo promedio extraidas previamente
    colsMont = "CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS,"+ colsMont_PRM_U6M +","+ colsMont_PRM_P6M +","+ colsMont_PRM_P3M +","+ colsMont_PRM_U3M +","+ colsMont_P1M +","+ colsMont_U1M
    
    #El dataframe dfMatrizVarTransaccionAgente solo con las columnas promedios 
    dfMatrizVarTransaccionAgenteMont = dfMatrizVarTransaccionAgente.select(colsMont.split(','))
    
    #Se calcula el crecimiento 6,3 y 1 para las variables tipo Monto
    original_cols = dfMatrizVarTransaccionAgenteMont.columns
    
    for colName in colsToExpandMonto:
        original_cols.extend([
            (func.round(col(colName + "_PRM_U6M")/col(colName + "_PRM_P6M"),8)).alias(colName + "_G6M"),                  
            (func.round(col(colName + "_PRM_U3M")/col(colName + "_PRM_P3M"),8)).alias(colName + "_G3M"),
            (func.round(col(colName + "_U1M")/col(colName + "_P1M"),8)).alias(colName + "_G1M"),
        ])
        
    dfMatrizVarTransaccionAgenteMont = dfMatrizVarTransaccionAgenteMont.select(*original_cols)
      
    #Leemos las columnas del dataframe dfMatrizVarTransaccionAgenteMont y las asignamos a una lista
    dfMatrizVarTransaccionAgenteCrecimiento = dfMatrizVarTransaccionAgenteMont.schema.names
    
    #Extraemos las columnas crecimiento ultimos 6 meses
    colsMont_PRM_G6M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteCrecimiento, "_G6M")
    
    #Extraemos las columnas crecimiento ultimos 3 meses
    colsMont_PRM_G3M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteCrecimiento, "_G3M")
    
    #Extraemos las columnas crecimiento ultimos 1 meses
    colsMont_PRM_G1M = funciones.extraccionColumnas(dfMatrizVarTransaccionAgenteCrecimiento, "_G1M")
    
    #Se concatena una cadena con las columnas tipo crecimiento extraidas previamente
    colsMontFin = "CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS,"+colsMont_PRM_G6M+","+colsMont_PRM_G3M+","+colsMont_PRM_G1M
    
    #El dataframe dfMatrizVarTransaccionAgenteMont solo con las columnas crecimiento
    dfMatrizVarTransaccionAgenteCrecimiento = dfMatrizVarTransaccionAgenteMont.select(colsMontFin.split(','))
    
    #Cruzamos los dataframes dfMatrizVarTransaccionAgente y dfMatrizVarTransaccionAgenteCrecimiento por las columnas CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS
    dfMatrizVarTransaccionAgente = dfMatrizVarTransaccionAgente.join(dfMatrizVarTransaccionAgenteCrecimiento, ['CODUNICOCLI','CODINTERNOCOMPUTACIONAL','CODMESANALISIS'])
    
    # Casteamos columnas a tipo entero
    columnas_a_castear = ['CODMESANALISIS', 'CODMES']
  
    columnas_casteadas = [F.col(x).cast("int").alias(x) for x in columnas_a_castear]
    dfMatrizVarTransaccionAgente = dfMatrizVarTransaccionAgente.select(
                                *(column_name for column_name in dfMatrizVarTransaccionAgente.columns if column_name not in columnas_a_castear),
                                *(columnas_casteadas)
                            )
    
    #Retorna el dataframe final dfMatrizVarTransaccionAgente luego de pasar por el metodo logicaPostAgrupacionInformacionMesAnalisis
    dfMatrizVarTransaccionAgente = logicaPostAgrupacionInformacionMesAnalisis(dfMatrizVarTransaccionAgente) 
    
    #Retorna el dataframe final dfMatrizVarTransaccionAgente al main del proceso
    return dfMatrizVarTransaccionAgente


# COMMAND ----------


###
 # Actualizaci�n de las variables tipo MINIMO previamente calculadas
 # 
 # @param dfMatrizVarTransaccionAgente {Dataframe Spark} Dataframe sin los campos Minimos actualizados.
 # @return {Dataframe Spark} Dataframe con los campos Minimos actualizados.
 ##
def logicaPostAgrupacionInformacionMesAnalisis(dfMatrizVarTransaccionAgente): 
    
    #Almacena los nombres de las columnas de la tabla df_st_rcc
    nameColumn = []
    
    #declaramos la lista que almacenara las columnas generadas en la 2da transpuesta tipo MONTO (FREQ Y MIN)
    columFreq = []
    
    #declaramos la lista que almacenara las columnas generadas en la 2da transpuesta tipo FLG (SUM)
    columFlg = []
    
    #variables temporales
    cVariableSufijo = ''
    cVariableSufijoInt = 0
    cVariableSufijoFin = ''
    cVariableSufijoInicio = ''
    cVariableFinal = ''
    
    #Leemos las columnas del dataframe dfMatrizVarTransaccionAgente y las asignamos a una lista
    nameColumn = dfMatrizVarTransaccionAgente.schema.names
    
    #Lee las columnas generadas en la 2da transpuesta tipo CTD (FREQ Y MIN)
    for nColumns in nameColumn:
        if 'FR' in nColumns:
            columFreq.append(nColumns)
        if 'CTD' in nColumns:
            if 'MIN' in nColumns:
                columFreq.append(nColumns)
    
    #Se asigna el valor de la lista columFreq a la lista colsToExpandCantidad2daT
    colsToExpandCantidad2daT = columFreq
  
    #Se actualizan los minimos para las variables tipo CTD calculas en la 2da transpuesta
    columna_totalExpand = []
    
    for colName in colsToExpandCantidad2daT:
        if 'FR' in colName:
            if '12' not in colName:
                cVariableSufijo = colName[-2:-1]
            else:
                cVariableSufijo = colName[-2:]
            cVariableSufijoInicio = colName[:-7]
            cVariableSufijoFin = colName[-7:]
            sufijoFinalVariable = cVariableSufijoFin[-4:]
            cVariableSufijoInt = int(cVariableSufijo)
            cVariableFinal = cVariableSufijoInicio+'MIN'+sufijoFinalVariable
            
        variableFinal = cVariableSufijoInicio+cVariableSufijoFin
        columna_totalExpand = [cVariableFinal]
        
        if not (variableFinal == ''): 
            dfMatrizVarTransaccionAgente = dfMatrizVarTransaccionAgente.select(
                                                    *(column_name for column_name in dfMatrizVarTransaccionAgente.columns if column_name not in columna_totalExpand),
                                                    (F.when(F.col(variableFinal) < cVariableSufijoInt,0).otherwise(F.col(cVariableFinal)).alias(cVariableFinal))
                                                )
    
    #Lee las columnas generadas en la 2da transpuesta tipo FLG (SUM)
    for nColumns in nameColumn:
            if 'CAN' in nColumns:      
                if  'FLG' in nColumns:
                    if  'MAX' not in nColumns:
                        if  'X' in nColumns:
                            columFlg.append(nColumns)
    
    #Se asigna el valor de la lista columFlg a la lista colsToExpandFlag              
    colsToExpandFlag = columFlg
    
    #Se actualizan los sum_uXm para las variables tipo FLG calculas en la 2da transpuesta
    columna_totalFlagExpand = []
    
    for colName in colsToExpandFlag:
        if 'X' in colName:
            if '12' not in colName:
                cVariableSufijo = colName[-2:-1]
            else:
                cVariableSufijo = colName[-2:]
            cVariableSufijoInicio = colName[:-7]
            cVariableSufijoFin = colName[-7:]
            cVariableSufijoInt = int(cVariableSufijo)

        variableFinal = cVariableSufijoInicio+cVariableSufijoFin
        columna_totalFlagExpand = [variableFinal]        
        
        if not (variableFinal == ''):
            dfMatrizVarTransaccionAgente = dfMatrizVarTransaccionAgente.select(
                                                    *(column_name for column_name in dfMatrizVarTransaccionAgente.columns if column_name not in columna_totalFlagExpand),
                                                    (F.when(F.col(variableFinal) == cVariableSufijoInt, 1).otherwise(0).alias(variableFinal))
                                                )
    
    #Se crea la columna FECACTUALIZACIONREGISTRO al dataframe dfMatrizVarTransaccionAgente
    fechaActualizacionRegistro = funciones.obtenerFechaActual()
    
    columnas = dfMatrizVarTransaccionAgente.columns
    dfMatrizVarTransaccionAgente = dfMatrizVarTransaccionAgente.select(*columnas + 
                                                                       [lit(PRM_FECHA_RUTINA).alias('FECRUTINA').cast('DATE')] + 
                                                                       [lit(fechaActualizacionRegistro).alias('FECACTUALIZACIONREGISTRO')])

    #Retorna el dataframe final
    return dfMatrizVarTransaccionAgente

# COMMAND ----------

# Guardo en tabla delta el dataframe con las variables calculadas
# @param matriz_df {Dataframe Spark} Segunda transpuesta con la informacion de los clientes homologados
# @return {void}

def save_matriz(dfMatrizVarTransaccionAgente: DataFrame): 
    
    input_df = dfMatrizVarTransaccionAgente.select(
                col('codunicocli').cast('STRING').alias('codunicocli'),
                col('codunicocli').cast('VARCHAR(128)').alias('codclaveunicocli'),
                sha2(concat(lit('0001'), trim(col('codinternocomputacional'))),512).cast('varchar(128)').alias('codclavepartycli'),
                col('codinternocomputacional').cast('STRING').alias('codinternocomputacional'),
                col('codmesanalisis').cast('INT').alias('codmesanalisis'),
                col('can_flg_tmo_agt_u1m').cast('INT').alias('can_flg_tmo_agt_u1m'),
                col('can_flg_tnm_agt_u1m').cast('INT').alias('can_flg_tnm_agt_u1m'),
                col('can_flg_tmo_agt_sol_u1m').cast('INT').alias('can_flg_tmo_agt_sol_u1m'),
                col('can_flg_tmo_agt_dol_u1m').cast('INT').alias('can_flg_tmo_agt_dol_u1m'),
                col('can_flg_tmo_agt_ret_u1m').cast('INT').alias('can_flg_tmo_agt_ret_u1m'),
                col('can_flg_tmo_agt_dep_u1m').cast('INT').alias('can_flg_tmo_agt_dep_u1m'),
                col('can_flg_tmo_agt_trf_u1m').cast('INT').alias('can_flg_tmo_agt_trf_u1m'),
                col('can_flg_tmo_agt_ads_u1m').cast('INT').alias('can_flg_tmo_agt_ads_u1m'),
                col('can_flg_tmo_agt_dis_u1m').cast('INT').alias('can_flg_tmo_agt_dis_u1m'),
                col('can_flg_tmo_agt_pag_tcr_u1m').cast('INT').alias('can_flg_tmo_agt_pag_tcr_u1m'),
                col('can_flg_tmo_agt_pag_bcp_u1m').cast('INT').alias('can_flg_tmo_agt_pag_bcp_u1m'),
                col('can_flg_tmo_agt_pag_srv_u1m').cast('INT').alias('can_flg_tmo_agt_pag_srv_u1m'),
                col('can_flg_tmo_agt_emis_u1m').cast('INT').alias('can_flg_tmo_agt_emis_u1m'),
                col('can_flg_tmo_agt_dgir_u1m').cast('INT').alias('can_flg_tmo_agt_dgir_u1m'),
                col('can_flg_tnm_agt_cslt_u1m').cast('INT').alias('can_flg_tnm_agt_cslt_u1m'),
                col('can_flg_tnm_agt_admn_u1m').cast('INT').alias('can_flg_tnm_agt_admn_u1m'),
                col('can_flg_tmo_agt_max_u3m').cast('INT').alias('can_flg_tmo_agt_max_u3m'),
                col('can_flg_tmo_agt_max_u6m').cast('INT').alias('can_flg_tmo_agt_max_u6m'),
                col('can_flg_tmo_agt_max_u9m').cast('INT').alias('can_flg_tmo_agt_max_u9m'),
                col('can_flg_tmo_agt_max_u12').cast('INT').alias('can_flg_tmo_agt_max_u12'),
                col('can_flg_tnm_agt_max_u3m').cast('INT').alias('can_flg_tnm_agt_max_u3m'),
                col('can_flg_tnm_agt_max_u6m').cast('INT').alias('can_flg_tnm_agt_max_u6m'),
                col('can_flg_tnm_agt_max_u9m').cast('INT').alias('can_flg_tnm_agt_max_u9m'),
                col('can_flg_tnm_agt_max_u12').cast('INT').alias('can_flg_tnm_agt_max_u12'),
                col('can_flg_tmo_agt_sol_max_u3m').cast('INT').alias('can_flg_tmo_agt_sol_max_u3m'),
                col('can_flg_tmo_agt_sol_max_u6m').cast('INT').alias('can_flg_tmo_agt_sol_max_u6m'),
                col('can_flg_tmo_agt_sol_max_u9m').cast('INT').alias('can_flg_tmo_agt_sol_max_u9m'),
                col('can_flg_tmo_agt_sol_max_u12').cast('INT').alias('can_flg_tmo_agt_sol_max_u12'),
                col('can_flg_tmo_agt_dol_max_u3m').cast('INT').alias('can_flg_tmo_agt_dol_max_u3m'),
                col('can_flg_tmo_agt_dol_max_u6m').cast('INT').alias('can_flg_tmo_agt_dol_max_u6m'),
                col('can_flg_tmo_agt_dol_max_u9m').cast('INT').alias('can_flg_tmo_agt_dol_max_u9m'),
                col('can_flg_tmo_agt_dol_max_u12').cast('INT').alias('can_flg_tmo_agt_dol_max_u12'),
                col('can_flg_tmo_agt_ret_max_u3m').cast('INT').alias('can_flg_tmo_agt_ret_max_u3m'),
                col('can_flg_tmo_agt_ret_max_u6m').cast('INT').alias('can_flg_tmo_agt_ret_max_u6m'),
                col('can_flg_tmo_agt_ret_max_u9m').cast('INT').alias('can_flg_tmo_agt_ret_max_u9m'),
                col('can_flg_tmo_agt_ret_max_u12').cast('INT').alias('can_flg_tmo_agt_ret_max_u12'),
                col('can_flg_tmo_agt_dep_max_u3m').cast('INT').alias('can_flg_tmo_agt_dep_max_u3m'),
                col('can_flg_tmo_agt_dep_max_u6m').cast('INT').alias('can_flg_tmo_agt_dep_max_u6m'),
                col('can_flg_tmo_agt_dep_max_u9m').cast('INT').alias('can_flg_tmo_agt_dep_max_u9m'),
                col('can_flg_tmo_agt_dep_max_u12').cast('INT').alias('can_flg_tmo_agt_dep_max_u12'),
                col('can_flg_tmo_agt_trf_max_u3m').cast('INT').alias('can_flg_tmo_agt_trf_max_u3m'),
                col('can_flg_tmo_agt_trf_max_u6m').cast('INT').alias('can_flg_tmo_agt_trf_max_u6m'),
                col('can_flg_tmo_agt_trf_max_u9m').cast('INT').alias('can_flg_tmo_agt_trf_max_u9m'),
                col('can_flg_tmo_agt_trf_max_u12').cast('INT').alias('can_flg_tmo_agt_trf_max_u12'),
                col('can_flg_tmo_agt_ads_max_u3m').cast('INT').alias('can_flg_tmo_agt_ads_max_u3m'),
                col('can_flg_tmo_agt_ads_max_u6m').cast('INT').alias('can_flg_tmo_agt_ads_max_u6m'),
                col('can_flg_tmo_agt_ads_max_u9m').cast('INT').alias('can_flg_tmo_agt_ads_max_u9m'),
                col('can_flg_tmo_agt_ads_max_u12').cast('INT').alias('can_flg_tmo_agt_ads_max_u12'),
                col('can_flg_tmo_agt_dis_max_u3m').cast('INT').alias('can_flg_tmo_agt_dis_max_u3m'),
                col('can_flg_tmo_agt_dis_max_u6m').cast('INT').alias('can_flg_tmo_agt_dis_max_u6m'),
                col('can_flg_tmo_agt_dis_max_u9m').cast('INT').alias('can_flg_tmo_agt_dis_max_u9m'),
                col('can_flg_tmo_agt_dis_max_u12').cast('INT').alias('can_flg_tmo_agt_dis_max_u12'),
                col('can_flg_tmo_agt_pag_tcr_max_u3m').cast('INT').alias('can_flg_tmo_agt_pag_tcr_max_u3m'),
                col('can_flg_tmo_agt_pag_tcr_max_u6m').cast('INT').alias('can_flg_tmo_agt_pag_tcr_max_u6m'),
                col('can_flg_tmo_agt_pag_tcr_max_u9m').cast('INT').alias('can_flg_tmo_agt_pag_tcr_max_u9m'),
                col('can_flg_tmo_agt_pag_tcr_max_u12').cast('INT').alias('can_flg_tmo_agt_pag_tcr_max_u12'),
                col('can_flg_tmo_agt_pag_bcp_max_u3m').cast('INT').alias('can_flg_tmo_agt_pag_bcp_max_u3m'),
                col('can_flg_tmo_agt_pag_bcp_max_u6m').cast('INT').alias('can_flg_tmo_agt_pag_bcp_max_u6m'),
                col('can_flg_tmo_agt_pag_bcp_max_u9m').cast('INT').alias('can_flg_tmo_agt_pag_bcp_max_u9m'),
                col('can_flg_tmo_agt_pag_bcp_max_u12').cast('INT').alias('can_flg_tmo_agt_pag_bcp_max_u12'),
                col('can_flg_tmo_agt_pag_srv_max_u3m').cast('INT').alias('can_flg_tmo_agt_pag_srv_max_u3m'),
                col('can_flg_tmo_agt_pag_srv_max_u6m').cast('INT').alias('can_flg_tmo_agt_pag_srv_max_u6m'),
                col('can_flg_tmo_agt_pag_srv_max_u9m').cast('INT').alias('can_flg_tmo_agt_pag_srv_max_u9m'),
                col('can_flg_tmo_agt_pag_srv_max_u12').cast('INT').alias('can_flg_tmo_agt_pag_srv_max_u12'),
                col('can_flg_tmo_agt_emis_max_u3m').cast('INT').alias('can_flg_tmo_agt_emis_max_u3m'),
                col('can_flg_tmo_agt_emis_max_u6m').cast('INT').alias('can_flg_tmo_agt_emis_max_u6m'),
                col('can_flg_tmo_agt_emis_max_u9m').cast('INT').alias('can_flg_tmo_agt_emis_max_u9m'),
                col('can_flg_tmo_agt_emis_max_u12').cast('INT').alias('can_flg_tmo_agt_emis_max_u12'),
                col('can_flg_tmo_agt_dgir_max_u3m').cast('INT').alias('can_flg_tmo_agt_dgir_max_u3m'),
                col('can_flg_tmo_agt_dgir_max_u6m').cast('INT').alias('can_flg_tmo_agt_dgir_max_u6m'),
                col('can_flg_tmo_agt_dgir_max_u9m').cast('INT').alias('can_flg_tmo_agt_dgir_max_u9m'),
                col('can_flg_tmo_agt_dgir_max_u12').cast('INT').alias('can_flg_tmo_agt_dgir_max_u12'),
                col('can_flg_tnm_agt_cslt_max_u3m').cast('INT').alias('can_flg_tnm_agt_cslt_max_u3m'),
                col('can_flg_tnm_agt_cslt_max_u6m').cast('INT').alias('can_flg_tnm_agt_cslt_max_u6m'),
                col('can_flg_tnm_agt_cslt_max_u9m').cast('INT').alias('can_flg_tnm_agt_cslt_max_u9m'),
                col('can_flg_tnm_agt_cslt_max_u12').cast('INT').alias('can_flg_tnm_agt_cslt_max_u12'),
                col('can_flg_tnm_agt_admn_max_u3m').cast('INT').alias('can_flg_tnm_agt_admn_max_u3m'),
                col('can_flg_tnm_agt_admn_max_u6m').cast('INT').alias('can_flg_tnm_agt_admn_max_u6m'),
                col('can_flg_tnm_agt_admn_max_u9m').cast('INT').alias('can_flg_tnm_agt_admn_max_u9m'),
                col('can_flg_tnm_agt_admn_max_u12').cast('INT').alias('can_flg_tnm_agt_admn_max_u12'),
                col('can_flg_tmo_agt_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_x3m_u3m'),
                col('can_flg_tmo_agt_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_x6m_u6m'),
                col('can_flg_tmo_agt_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_x9m_u9m'),
                col('can_flg_tmo_agt_x12_u12').cast('INT').alias('can_flg_tmo_agt_x12_u12'),
                col('can_flg_tnm_agt_x3m_u3m').cast('INT').alias('can_flg_tnm_agt_x3m_u3m'),
                col('can_flg_tnm_agt_x6m_u6m').cast('INT').alias('can_flg_tnm_agt_x6m_u6m'),
                col('can_flg_tnm_agt_x9m_u9m').cast('INT').alias('can_flg_tnm_agt_x9m_u9m'),
                col('can_flg_tnm_agt_x12_u12').cast('INT').alias('can_flg_tnm_agt_x12_u12'),
                col('can_flg_tmo_agt_sol_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_sol_x3m_u3m'),
                col('can_flg_tmo_agt_sol_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_sol_x6m_u6m'),
                col('can_flg_tmo_agt_sol_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_sol_x9m_u9m'),
                col('can_flg_tmo_agt_sol_x12_u12').cast('INT').alias('can_flg_tmo_agt_sol_x12_u12'),
                col('can_flg_tmo_agt_dol_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_dol_x3m_u3m'),
                col('can_flg_tmo_agt_dol_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_dol_x6m_u6m'),
                col('can_flg_tmo_agt_dol_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_dol_x9m_u9m'),
                col('can_flg_tmo_agt_dol_x12_u12').cast('INT').alias('can_flg_tmo_agt_dol_x12_u12'),
                col('can_flg_tmo_agt_ret_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_ret_x3m_u3m'),
                col('can_flg_tmo_agt_ret_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_ret_x6m_u6m'),
                col('can_flg_tmo_agt_ret_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_ret_x9m_u9m'),
                col('can_flg_tmo_agt_ret_x12_u12').cast('INT').alias('can_flg_tmo_agt_ret_x12_u12'),
                col('can_flg_tmo_agt_dep_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_dep_x3m_u3m'),
                col('can_flg_tmo_agt_dep_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_dep_x6m_u6m'),
                col('can_flg_tmo_agt_dep_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_dep_x9m_u9m'),
                col('can_flg_tmo_agt_dep_x12_u12').cast('INT').alias('can_flg_tmo_agt_dep_x12_u12'),
                col('can_flg_tmo_agt_trf_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_trf_x3m_u3m'),
                col('can_flg_tmo_agt_trf_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_trf_x6m_u6m'),
                col('can_flg_tmo_agt_trf_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_trf_x9m_u9m'),
                col('can_flg_tmo_agt_trf_x12_u12').cast('INT').alias('can_flg_tmo_agt_trf_x12_u12'),
                col('can_flg_tmo_agt_ads_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_ads_x3m_u3m'),
                col('can_flg_tmo_agt_ads_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_ads_x6m_u6m'),
                col('can_flg_tmo_agt_ads_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_ads_x9m_u9m'),
                col('can_flg_tmo_agt_ads_x12_u12').cast('INT').alias('can_flg_tmo_agt_ads_x12_u12'),
                col('can_flg_tmo_agt_dis_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_dis_x3m_u3m'),
                col('can_flg_tmo_agt_dis_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_dis_x6m_u6m'),
                col('can_flg_tmo_agt_dis_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_dis_x9m_u9m'),
                col('can_flg_tmo_agt_dis_x12_u12').cast('INT').alias('can_flg_tmo_agt_dis_x12_u12'),
                col('can_flg_tmo_agt_pag_tcr_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_pag_tcr_x3m_u3m'),
                col('can_flg_tmo_agt_pag_tcr_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_pag_tcr_x6m_u6m'),
                col('can_flg_tmo_agt_pag_tcr_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_pag_tcr_x9m_u9m'),
                col('can_flg_tmo_agt_pag_tcr_x12_u12').cast('INT').alias('can_flg_tmo_agt_pag_tcr_x12_u12'),
                col('can_flg_tmo_agt_pag_bcp_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_pag_bcp_x3m_u3m'),
                col('can_flg_tmo_agt_pag_bcp_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_pag_bcp_x6m_u6m'),
                col('can_flg_tmo_agt_pag_bcp_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_pag_bcp_x9m_u9m'),
                col('can_flg_tmo_agt_pag_bcp_x12_u12').cast('INT').alias('can_flg_tmo_agt_pag_bcp_x12_u12'),
                col('can_flg_tmo_agt_pag_srv_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_pag_srv_x3m_u3m'),
                col('can_flg_tmo_agt_pag_srv_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_pag_srv_x6m_u6m'),
                col('can_flg_tmo_agt_pag_srv_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_pag_srv_x9m_u9m'),
                col('can_flg_tmo_agt_pag_srv_x12_u12').cast('INT').alias('can_flg_tmo_agt_pag_srv_x12_u12'),
                col('can_flg_tmo_agt_emis_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_emis_x3m_u3m'),
                col('can_flg_tmo_agt_emis_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_emis_x6m_u6m'),
                col('can_flg_tmo_agt_emis_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_emis_x9m_u9m'),
                col('can_flg_tmo_agt_emis_x12_u12').cast('INT').alias('can_flg_tmo_agt_emis_x12_u12'),
                col('can_flg_tmo_agt_dgir_x3m_u3m').cast('INT').alias('can_flg_tmo_agt_dgir_x3m_u3m'),
                col('can_flg_tmo_agt_dgir_x6m_u6m').cast('INT').alias('can_flg_tmo_agt_dgir_x6m_u6m'),
                col('can_flg_tmo_agt_dgir_x9m_u9m').cast('INT').alias('can_flg_tmo_agt_dgir_x9m_u9m'),
                col('can_flg_tmo_agt_dgir_x12_u12').cast('INT').alias('can_flg_tmo_agt_dgir_x12_u12'),
                col('can_flg_tnm_agt_cslt_x3m_u3m').cast('INT').alias('can_flg_tnm_agt_cslt_x3m_u3m'),
                col('can_flg_tnm_agt_cslt_x6m_u6m').cast('INT').alias('can_flg_tnm_agt_cslt_x6m_u6m'),
                col('can_flg_tnm_agt_cslt_x9m_u9m').cast('INT').alias('can_flg_tnm_agt_cslt_x9m_u9m'),
                col('can_flg_tnm_agt_cslt_x12_u12').cast('INT').alias('can_flg_tnm_agt_cslt_x12_u12'),
                col('can_flg_tnm_agt_admn_x3m_u3m').cast('INT').alias('can_flg_tnm_agt_admn_x3m_u3m'),
                col('can_flg_tnm_agt_admn_x6m_u6m').cast('INT').alias('can_flg_tnm_agt_admn_x6m_u6m'),
                col('can_flg_tnm_agt_admn_x9m_u9m').cast('INT').alias('can_flg_tnm_agt_admn_x9m_u9m'),
                col('can_flg_tnm_agt_admn_x12_u12').cast('INT').alias('can_flg_tnm_agt_admn_x12_u12'),
                col('can_mto_tmo_agt_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_u1m'),
                col('can_mto_tmo_agt_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_prm_u3m'),
                col('can_mto_tmo_agt_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_prm_u6m'),
                col('can_mto_tmo_agt_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_prm_u9m'),
                col('can_mto_tmo_agt_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_prm_u12'),
                col('can_mto_tmo_agt_sol_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_u1m'),
                col('can_mto_tmo_agt_sol_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_prm_u3m'),
                col('can_mto_tmo_agt_sol_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_prm_u6m'),
                col('can_mto_tmo_agt_sol_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_prm_u9m'),
                col('can_mto_tmo_agt_sol_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_sol_prm_u12'),
                col('can_mto_tmo_agt_dol_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_u1m'),
                col('can_mto_tmo_agt_dol_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_prm_u3m'),
                col('can_mto_tmo_agt_dol_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_prm_u6m'),
                col('can_mto_tmo_agt_dol_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_prm_u9m'),
                col('can_mto_tmo_agt_dol_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dol_prm_u12'),
                col('can_mto_tmo_agt_ret_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_u1m'),
                col('can_mto_tmo_agt_ret_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_prm_u3m'),
                col('can_mto_tmo_agt_ret_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_prm_u6m'),
                col('can_mto_tmo_agt_ret_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_prm_u9m'),
                col('can_mto_tmo_agt_ret_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_ret_prm_u12'),
                col('can_mto_tmo_agt_dep_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_u1m'),
                col('can_mto_tmo_agt_dep_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_prm_u3m'),
                col('can_mto_tmo_agt_dep_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_prm_u6m'),
                col('can_mto_tmo_agt_dep_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_prm_u9m'),
                col('can_mto_tmo_agt_dep_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dep_prm_u12'),
                col('can_mto_tmo_agt_trf_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_u1m'),
                col('can_mto_tmo_agt_trf_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_prm_u3m'),
                col('can_mto_tmo_agt_trf_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_prm_u6m'),
                col('can_mto_tmo_agt_trf_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_prm_u9m'),
                col('can_mto_tmo_agt_trf_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_trf_prm_u12'),
                col('can_mto_tmo_agt_ads_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_u1m'),
                col('can_mto_tmo_agt_ads_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_prm_u3m'),
                col('can_mto_tmo_agt_ads_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_prm_u6m'),
                col('can_mto_tmo_agt_ads_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_prm_u9m'),
                col('can_mto_tmo_agt_ads_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_ads_prm_u12'),
                col('can_mto_tmo_agt_dis_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_u1m'),
                col('can_mto_tmo_agt_dis_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_prm_u3m'),
                col('can_mto_tmo_agt_dis_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_prm_u6m'),
                col('can_mto_tmo_agt_dis_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_prm_u9m'),
                col('can_mto_tmo_agt_dis_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dis_prm_u12'),
                col('can_mto_tmo_agt_pag_tcr_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_u1m'),
                col('can_mto_tmo_agt_pag_tcr_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_prm_u3m'),
                col('can_mto_tmo_agt_pag_tcr_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_prm_u6m'),
                col('can_mto_tmo_agt_pag_tcr_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_prm_u9m'),
                col('can_mto_tmo_agt_pag_tcr_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_prm_u12'),
                col('can_mto_tmo_agt_pag_bcp_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_u1m'),
                col('can_mto_tmo_agt_pag_bcp_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_prm_u3m'),
                col('can_mto_tmo_agt_pag_bcp_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_prm_u6m'),
                col('can_mto_tmo_agt_pag_bcp_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_prm_u9m'),
                col('can_mto_tmo_agt_pag_bcp_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_prm_u12'),
                col('can_mto_tmo_agt_pag_srv_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_u1m'),
                col('can_mto_tmo_agt_pag_srv_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_prm_u3m'),
                col('can_mto_tmo_agt_pag_srv_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_prm_u6m'),
                col('can_mto_tmo_agt_pag_srv_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_prm_u9m'),
                col('can_mto_tmo_agt_pag_srv_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_prm_u12'),
                col('can_mto_tmo_agt_emis_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_u1m'),
                col('can_mto_tmo_agt_emis_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_prm_u3m'),
                col('can_mto_tmo_agt_emis_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_prm_u6m'),
                col('can_mto_tmo_agt_emis_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_prm_u9m'),
                col('can_mto_tmo_agt_emis_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_emis_prm_u12'),
                col('can_mto_tmo_agt_dgir_u1m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_u1m'),
                col('can_mto_tmo_agt_dgir_prm_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_prm_u3m'),
                col('can_mto_tmo_agt_dgir_prm_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_prm_u6m'),
                col('can_mto_tmo_agt_dgir_prm_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_prm_u9m'),
                col('can_mto_tmo_agt_dgir_prm_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_prm_u12'),
                col('can_tkt_tmo_agt_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_u1m'),
                col('can_tkt_tmo_agt_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_prm_u3m'),
                col('can_tkt_tmo_agt_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_prm_u6m'),
                col('can_tkt_tmo_agt_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_prm_u9m'),
                col('can_tkt_tmo_agt_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_prm_u12'),
                col('can_tkt_tmo_agt_sol_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_u1m'),
                col('can_tkt_tmo_agt_sol_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_prm_u3m'),
                col('can_tkt_tmo_agt_sol_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_prm_u6m'),
                col('can_tkt_tmo_agt_sol_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_prm_u9m'),
                col('can_tkt_tmo_agt_sol_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_prm_u12'),
                col('can_tkt_tmo_agt_dol_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_u1m'),
                col('can_tkt_tmo_agt_dol_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_prm_u3m'),
                col('can_tkt_tmo_agt_dol_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_prm_u6m'),
                col('can_tkt_tmo_agt_dol_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_prm_u9m'),
                col('can_tkt_tmo_agt_dol_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_prm_u12'),
                col('can_tkt_tmo_agt_ret_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_u1m'),
                col('can_tkt_tmo_agt_ret_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_prm_u3m'),
                col('can_tkt_tmo_agt_ret_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_prm_u6m'),
                col('can_tkt_tmo_agt_ret_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_prm_u9m'),
                col('can_tkt_tmo_agt_ret_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_prm_u12'),
                col('can_tkt_tmo_agt_dep_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_u1m'),
                col('can_tkt_tmo_agt_dep_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_prm_u3m'),
                col('can_tkt_tmo_agt_dep_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_prm_u6m'),
                col('can_tkt_tmo_agt_dep_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_prm_u9m'),
                col('can_tkt_tmo_agt_dep_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_prm_u12'),
                col('can_tkt_tmo_agt_trf_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_u1m'),
                col('can_tkt_tmo_agt_trf_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_prm_u3m'),
                col('can_tkt_tmo_agt_trf_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_prm_u6m'),
                col('can_tkt_tmo_agt_trf_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_prm_u9m'),
                col('can_tkt_tmo_agt_trf_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_prm_u12'),
                col('can_tkt_tmo_agt_ads_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_u1m'),
                col('can_tkt_tmo_agt_ads_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_prm_u3m'),
                col('can_tkt_tmo_agt_ads_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_prm_u6m'),
                col('can_tkt_tmo_agt_ads_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_prm_u9m'),
                col('can_tkt_tmo_agt_ads_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_prm_u12'),
                col('can_tkt_tmo_agt_dis_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_u1m'),
                col('can_tkt_tmo_agt_dis_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_prm_u3m'),
                col('can_tkt_tmo_agt_dis_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_prm_u6m'),
                col('can_tkt_tmo_agt_dis_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_prm_u9m'),
                col('can_tkt_tmo_agt_dis_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_prm_u12'),
                col('can_tkt_tmo_agt_pag_tcr_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_u1m'),
                col('can_tkt_tmo_agt_pag_tcr_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_prm_u3m'),
                col('can_tkt_tmo_agt_pag_tcr_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_prm_u6m'),
                col('can_tkt_tmo_agt_pag_tcr_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_prm_u9m'),
                col('can_tkt_tmo_agt_pag_tcr_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_prm_u12'),
                col('can_tkt_tmo_agt_pag_bcp_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_u1m'),
                col('can_tkt_tmo_agt_pag_bcp_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_prm_u3m'),
                col('can_tkt_tmo_agt_pag_bcp_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_prm_u6m'),
                col('can_tkt_tmo_agt_pag_bcp_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_prm_u9m'),
                col('can_tkt_tmo_agt_pag_bcp_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_prm_u12'),
                col('can_tkt_tmo_agt_pag_srv_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_u1m'),
                col('can_tkt_tmo_agt_pag_srv_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_prm_u3m'),
                col('can_tkt_tmo_agt_pag_srv_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_prm_u6m'),
                col('can_tkt_tmo_agt_pag_srv_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_prm_u9m'),
                col('can_tkt_tmo_agt_pag_srv_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_prm_u12'),
                col('can_tkt_tmo_agt_emis_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_u1m'),
                col('can_tkt_tmo_agt_emis_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_prm_u3m'),
                col('can_tkt_tmo_agt_emis_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_prm_u6m'),
                col('can_tkt_tmo_agt_emis_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_prm_u9m'),
                col('can_tkt_tmo_agt_emis_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_prm_u12'),
                col('can_tkt_tmo_agt_dgir_u1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_u1m'),
                col('can_tkt_tmo_agt_dgir_prm_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_prm_u3m'),
                col('can_tkt_tmo_agt_dgir_prm_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_prm_u6m'),
                col('can_tkt_tmo_agt_dgir_prm_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_prm_u9m'),
                col('can_tkt_tmo_agt_dgir_prm_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_prm_u12'),
                col('can_mto_tmo_agt_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_prm_p6m'),
                col('can_mto_tmo_agt_sol_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_prm_p6m'),
                col('can_mto_tmo_agt_dol_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_prm_p6m'),
                col('can_mto_tmo_agt_ret_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_prm_p6m'),
                col('can_mto_tmo_agt_dep_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_prm_p6m'),
                col('can_mto_tmo_agt_trf_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_prm_p6m'),
                col('can_mto_tmo_agt_ads_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_prm_p6m'),
                col('can_mto_tmo_agt_dis_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_prm_p6m'),
                col('can_mto_tmo_agt_pag_tcr_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_prm_p6m'),
                col('can_mto_tmo_agt_pag_bcp_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_prm_p6m'),
                col('can_mto_tmo_agt_pag_srv_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_prm_p6m'),
                col('can_mto_tmo_agt_emis_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_prm_p6m'),
                col('can_mto_tmo_agt_dgir_prm_p6m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_prm_p6m'),
                col('can_tkt_tmo_agt_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_prm_p6m'),
                col('can_tkt_tmo_agt_sol_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_prm_p6m'),
                col('can_tkt_tmo_agt_dol_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_prm_p6m'),
                col('can_tkt_tmo_agt_ret_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_prm_p6m'),
                col('can_tkt_tmo_agt_dep_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_prm_p6m'),
                col('can_tkt_tmo_agt_trf_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_prm_p6m'),
                col('can_tkt_tmo_agt_ads_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_prm_p6m'),
                col('can_tkt_tmo_agt_dis_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_prm_p6m'),
                col('can_tkt_tmo_agt_pag_tcr_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_prm_p6m'),
                col('can_tkt_tmo_agt_pag_bcp_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_prm_p6m'),
                col('can_tkt_tmo_agt_pag_srv_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_prm_p6m'),
                col('can_tkt_tmo_agt_emis_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_prm_p6m'),
                col('can_tkt_tmo_agt_dgir_prm_p6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_prm_p6m'),
                col('can_mto_tmo_agt_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_prm_p3m'),
                col('can_mto_tmo_agt_sol_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_prm_p3m'),
                col('can_mto_tmo_agt_dol_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_prm_p3m'),
                col('can_mto_tmo_agt_ret_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_prm_p3m'),
                col('can_mto_tmo_agt_dep_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_prm_p3m'),
                col('can_mto_tmo_agt_trf_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_prm_p3m'),
                col('can_mto_tmo_agt_ads_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_prm_p3m'),
                col('can_mto_tmo_agt_dis_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_prm_p3m'),
                col('can_mto_tmo_agt_pag_tcr_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_prm_p3m'),
                col('can_mto_tmo_agt_pag_bcp_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_prm_p3m'),
                col('can_mto_tmo_agt_pag_srv_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_prm_p3m'),
                col('can_mto_tmo_agt_emis_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_prm_p3m'),
                col('can_mto_tmo_agt_dgir_prm_p3m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_prm_p3m'),
                col('can_tkt_tmo_agt_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_prm_p3m'),
                col('can_tkt_tmo_agt_sol_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_prm_p3m'),
                col('can_tkt_tmo_agt_dol_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_prm_p3m'),
                col('can_tkt_tmo_agt_ret_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_prm_p3m'),
                col('can_tkt_tmo_agt_dep_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_prm_p3m'),
                col('can_tkt_tmo_agt_trf_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_prm_p3m'),
                col('can_tkt_tmo_agt_ads_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_prm_p3m'),
                col('can_tkt_tmo_agt_dis_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_prm_p3m'),
                col('can_tkt_tmo_agt_pag_tcr_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_prm_p3m'),
                col('can_tkt_tmo_agt_pag_bcp_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_prm_p3m'),
                col('can_tkt_tmo_agt_pag_srv_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_prm_p3m'),
                col('can_tkt_tmo_agt_emis_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_prm_p3m'),
                col('can_tkt_tmo_agt_dgir_prm_p3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_prm_p3m'),
                col('can_mto_tmo_agt_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_p1m'),
                col('can_mto_tmo_agt_sol_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_p1m'),
                col('can_mto_tmo_agt_dol_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_p1m'),
                col('can_mto_tmo_agt_ret_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_p1m'),
                col('can_mto_tmo_agt_dep_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_p1m'),
                col('can_mto_tmo_agt_trf_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_p1m'),
                col('can_mto_tmo_agt_ads_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_p1m'),
                col('can_mto_tmo_agt_dis_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_p1m'),
                col('can_mto_tmo_agt_pag_tcr_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_p1m'),
                col('can_mto_tmo_agt_pag_bcp_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_p1m'),
                col('can_mto_tmo_agt_pag_srv_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_p1m'),
                col('can_mto_tmo_agt_emis_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_p1m'),
                col('can_mto_tmo_agt_dgir_p1m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_p1m'),
                col('can_tkt_tmo_agt_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_p1m'),
                col('can_tkt_tmo_agt_sol_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_p1m'),
                col('can_tkt_tmo_agt_dol_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_p1m'),
                col('can_tkt_tmo_agt_ret_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_p1m'),
                col('can_tkt_tmo_agt_dep_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_p1m'),
                col('can_tkt_tmo_agt_trf_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_p1m'),
                col('can_tkt_tmo_agt_ads_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_p1m'),
                col('can_tkt_tmo_agt_dis_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_p1m'),
                col('can_tkt_tmo_agt_pag_tcr_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_p1m'),
                col('can_tkt_tmo_agt_pag_bcp_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_p1m'),
                col('can_tkt_tmo_agt_pag_srv_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_p1m'),
                col('can_tkt_tmo_agt_emis_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_p1m'),
                col('can_tkt_tmo_agt_dgir_p1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_p1m'),
                col('can_mto_tmo_agt_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_max_u3m'),
                col('can_mto_tmo_agt_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_max_u6m'),
                col('can_mto_tmo_agt_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_max_u9m'),
                col('can_mto_tmo_agt_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_max_u12'),
                col('can_mto_tmo_agt_sol_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_max_u3m'),
                col('can_mto_tmo_agt_sol_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_max_u6m'),
                col('can_mto_tmo_agt_sol_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_max_u9m'),
                col('can_mto_tmo_agt_sol_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_sol_max_u12'),
                col('can_mto_tmo_agt_dol_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_max_u3m'),
                col('can_mto_tmo_agt_dol_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_max_u6m'),
                col('can_mto_tmo_agt_dol_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_max_u9m'),
                col('can_mto_tmo_agt_dol_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dol_max_u12'),
                col('can_mto_tmo_agt_ret_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_max_u3m'),
                col('can_mto_tmo_agt_ret_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_max_u6m'),
                col('can_mto_tmo_agt_ret_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_max_u9m'),
                col('can_mto_tmo_agt_ret_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_ret_max_u12'),
                col('can_mto_tmo_agt_dep_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_max_u3m'),
                col('can_mto_tmo_agt_dep_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_max_u6m'),
                col('can_mto_tmo_agt_dep_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_max_u9m'),
                col('can_mto_tmo_agt_dep_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dep_max_u12'),
                col('can_mto_tmo_agt_trf_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_max_u3m'),
                col('can_mto_tmo_agt_trf_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_max_u6m'),
                col('can_mto_tmo_agt_trf_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_max_u9m'),
                col('can_mto_tmo_agt_trf_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_trf_max_u12'),
                col('can_mto_tmo_agt_ads_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_max_u3m'),
                col('can_mto_tmo_agt_ads_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_max_u6m'),
                col('can_mto_tmo_agt_ads_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_max_u9m'),
                col('can_mto_tmo_agt_ads_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_ads_max_u12'),
                col('can_mto_tmo_agt_dis_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_max_u3m'),
                col('can_mto_tmo_agt_dis_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_max_u6m'),
                col('can_mto_tmo_agt_dis_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_max_u9m'),
                col('can_mto_tmo_agt_dis_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dis_max_u12'),
                col('can_mto_tmo_agt_pag_tcr_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_max_u3m'),
                col('can_mto_tmo_agt_pag_tcr_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_max_u6m'),
                col('can_mto_tmo_agt_pag_tcr_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_max_u9m'),
                col('can_mto_tmo_agt_pag_tcr_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_max_u12'),
                col('can_mto_tmo_agt_pag_bcp_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_max_u3m'),
                col('can_mto_tmo_agt_pag_bcp_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_max_u6m'),
                col('can_mto_tmo_agt_pag_bcp_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_max_u9m'),
                col('can_mto_tmo_agt_pag_bcp_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_max_u12'),
                col('can_mto_tmo_agt_pag_srv_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_max_u3m'),
                col('can_mto_tmo_agt_pag_srv_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_max_u6m'),
                col('can_mto_tmo_agt_pag_srv_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_max_u9m'),
                col('can_mto_tmo_agt_pag_srv_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_max_u12'),
                col('can_mto_tmo_agt_emis_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_max_u3m'),
                col('can_mto_tmo_agt_emis_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_max_u6m'),
                col('can_mto_tmo_agt_emis_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_max_u9m'),
                col('can_mto_tmo_agt_emis_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_emis_max_u12'),
                col('can_mto_tmo_agt_dgir_max_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_max_u3m'),
                col('can_mto_tmo_agt_dgir_max_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_max_u6m'),
                col('can_mto_tmo_agt_dgir_max_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_max_u9m'),
                col('can_mto_tmo_agt_dgir_max_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_max_u12'),
                col('can_tkt_tmo_agt_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_max_u3m'),
                col('can_tkt_tmo_agt_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_max_u6m'),
                col('can_tkt_tmo_agt_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_max_u9m'),
                col('can_tkt_tmo_agt_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_max_u12'),
                col('can_tkt_tmo_agt_sol_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_max_u3m'),
                col('can_tkt_tmo_agt_sol_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_max_u6m'),
                col('can_tkt_tmo_agt_sol_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_max_u9m'),
                col('can_tkt_tmo_agt_sol_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_max_u12'),
                col('can_tkt_tmo_agt_dol_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_max_u3m'),
                col('can_tkt_tmo_agt_dol_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_max_u6m'),
                col('can_tkt_tmo_agt_dol_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_max_u9m'),
                col('can_tkt_tmo_agt_dol_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_max_u12'),
                col('can_tkt_tmo_agt_ret_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_max_u3m'),
                col('can_tkt_tmo_agt_ret_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_max_u6m'),
                col('can_tkt_tmo_agt_ret_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_max_u9m'),
                col('can_tkt_tmo_agt_ret_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_max_u12'),
                col('can_tkt_tmo_agt_dep_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_max_u3m'),
                col('can_tkt_tmo_agt_dep_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_max_u6m'),
                col('can_tkt_tmo_agt_dep_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_max_u9m'),
                col('can_tkt_tmo_agt_dep_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_max_u12'),
                col('can_tkt_tmo_agt_trf_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_max_u3m'),
                col('can_tkt_tmo_agt_trf_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_max_u6m'),
                col('can_tkt_tmo_agt_trf_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_max_u9m'),
                col('can_tkt_tmo_agt_trf_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_max_u12'),
                col('can_tkt_tmo_agt_ads_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_max_u3m'),
                col('can_tkt_tmo_agt_ads_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_max_u6m'),
                col('can_tkt_tmo_agt_ads_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_max_u9m'),
                col('can_tkt_tmo_agt_ads_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_max_u12'),
                col('can_tkt_tmo_agt_dis_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_max_u3m'),
                col('can_tkt_tmo_agt_dis_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_max_u6m'),
                col('can_tkt_tmo_agt_dis_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_max_u9m'),
                col('can_tkt_tmo_agt_dis_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_max_u12'),
                col('can_tkt_tmo_agt_pag_tcr_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_max_u3m'),
                col('can_tkt_tmo_agt_pag_tcr_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_max_u6m'),
                col('can_tkt_tmo_agt_pag_tcr_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_max_u9m'),
                col('can_tkt_tmo_agt_pag_tcr_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_max_u12'),
                col('can_tkt_tmo_agt_pag_bcp_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_max_u3m'),
                col('can_tkt_tmo_agt_pag_bcp_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_max_u6m'),
                col('can_tkt_tmo_agt_pag_bcp_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_max_u9m'),
                col('can_tkt_tmo_agt_pag_bcp_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_max_u12'),
                col('can_tkt_tmo_agt_pag_srv_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_max_u3m'),
                col('can_tkt_tmo_agt_pag_srv_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_max_u6m'),
                col('can_tkt_tmo_agt_pag_srv_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_max_u9m'),
                col('can_tkt_tmo_agt_pag_srv_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_max_u12'),
                col('can_tkt_tmo_agt_emis_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_max_u3m'),
                col('can_tkt_tmo_agt_emis_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_max_u6m'),
                col('can_tkt_tmo_agt_emis_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_max_u9m'),
                col('can_tkt_tmo_agt_emis_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_max_u12'),
                col('can_tkt_tmo_agt_dgir_max_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_max_u3m'),
                col('can_tkt_tmo_agt_dgir_max_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_max_u6m'),
                col('can_tkt_tmo_agt_dgir_max_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_max_u9m'),
                col('can_tkt_tmo_agt_dgir_max_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_max_u12'),
                col('can_mto_tmo_agt_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_min_u3m'),
                col('can_mto_tmo_agt_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_min_u6m'),
                col('can_mto_tmo_agt_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_min_u9m'),
                col('can_mto_tmo_agt_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_min_u12'),
                col('can_mto_tmo_agt_sol_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_min_u3m'),
                col('can_mto_tmo_agt_sol_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_min_u6m'),
                col('can_mto_tmo_agt_sol_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_min_u9m'),
                col('can_mto_tmo_agt_sol_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_sol_min_u12'),
                col('can_mto_tmo_agt_dol_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_min_u3m'),
                col('can_mto_tmo_agt_dol_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_min_u6m'),
                col('can_mto_tmo_agt_dol_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_min_u9m'),
                col('can_mto_tmo_agt_dol_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dol_min_u12'),
                col('can_mto_tmo_agt_ret_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_min_u3m'),
                col('can_mto_tmo_agt_ret_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_min_u6m'),
                col('can_mto_tmo_agt_ret_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_min_u9m'),
                col('can_mto_tmo_agt_ret_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_ret_min_u12'),
                col('can_mto_tmo_agt_dep_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_min_u3m'),
                col('can_mto_tmo_agt_dep_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_min_u6m'),
                col('can_mto_tmo_agt_dep_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_min_u9m'),
                col('can_mto_tmo_agt_dep_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dep_min_u12'),
                col('can_mto_tmo_agt_trf_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_min_u3m'),
                col('can_mto_tmo_agt_trf_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_min_u6m'),
                col('can_mto_tmo_agt_trf_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_min_u9m'),
                col('can_mto_tmo_agt_trf_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_trf_min_u12'),
                col('can_mto_tmo_agt_ads_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_min_u3m'),
                col('can_mto_tmo_agt_ads_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_min_u6m'),
                col('can_mto_tmo_agt_ads_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_min_u9m'),
                col('can_mto_tmo_agt_ads_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_ads_min_u12'),
                col('can_mto_tmo_agt_dis_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_min_u3m'),
                col('can_mto_tmo_agt_dis_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_min_u6m'),
                col('can_mto_tmo_agt_dis_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_min_u9m'),
                col('can_mto_tmo_agt_dis_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dis_min_u12'),
                col('can_mto_tmo_agt_pag_tcr_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_min_u3m'),
                col('can_mto_tmo_agt_pag_tcr_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_min_u6m'),
                col('can_mto_tmo_agt_pag_tcr_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_min_u9m'),
                col('can_mto_tmo_agt_pag_tcr_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_min_u12'),
                col('can_mto_tmo_agt_pag_bcp_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_min_u3m'),
                col('can_mto_tmo_agt_pag_bcp_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_min_u6m'),
                col('can_mto_tmo_agt_pag_bcp_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_min_u9m'),
                col('can_mto_tmo_agt_pag_bcp_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_min_u12'),
                col('can_mto_tmo_agt_pag_srv_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_min_u3m'),
                col('can_mto_tmo_agt_pag_srv_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_min_u6m'),
                col('can_mto_tmo_agt_pag_srv_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_min_u9m'),
                col('can_mto_tmo_agt_pag_srv_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_min_u12'),
                col('can_mto_tmo_agt_emis_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_min_u3m'),
                col('can_mto_tmo_agt_emis_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_min_u6m'),
                col('can_mto_tmo_agt_emis_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_min_u9m'),
                col('can_mto_tmo_agt_emis_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_emis_min_u12'),
                col('can_mto_tmo_agt_dgir_min_u3m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_min_u3m'),
                col('can_mto_tmo_agt_dgir_min_u6m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_min_u6m'),
                col('can_mto_tmo_agt_dgir_min_u9m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_min_u9m'),
                col('can_mto_tmo_agt_dgir_min_u12').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_min_u12'),
                col('can_tkt_tmo_agt_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_min_u3m'),
                col('can_tkt_tmo_agt_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_min_u6m'),
                col('can_tkt_tmo_agt_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_min_u9m'),
                col('can_tkt_tmo_agt_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_min_u12'),
                col('can_tkt_tmo_agt_sol_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_min_u3m'),
                col('can_tkt_tmo_agt_sol_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_min_u6m'),
                col('can_tkt_tmo_agt_sol_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_min_u9m'),
                col('can_tkt_tmo_agt_sol_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_min_u12'),
                col('can_tkt_tmo_agt_dol_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_min_u3m'),
                col('can_tkt_tmo_agt_dol_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_min_u6m'),
                col('can_tkt_tmo_agt_dol_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_min_u9m'),
                col('can_tkt_tmo_agt_dol_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_min_u12'),
                col('can_tkt_tmo_agt_ret_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_min_u3m'),
                col('can_tkt_tmo_agt_ret_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_min_u6m'),
                col('can_tkt_tmo_agt_ret_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_min_u9m'),
                col('can_tkt_tmo_agt_ret_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_min_u12'),
                col('can_tkt_tmo_agt_dep_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_min_u3m'),
                col('can_tkt_tmo_agt_dep_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_min_u6m'),
                col('can_tkt_tmo_agt_dep_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_min_u9m'),
                col('can_tkt_tmo_agt_dep_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_min_u12'),
                col('can_tkt_tmo_agt_trf_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_min_u3m'),
                col('can_tkt_tmo_agt_trf_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_min_u6m'),
                col('can_tkt_tmo_agt_trf_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_min_u9m'),
                col('can_tkt_tmo_agt_trf_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_min_u12'),
                col('can_tkt_tmo_agt_ads_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_min_u3m'),
                col('can_tkt_tmo_agt_ads_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_min_u6m'),
                col('can_tkt_tmo_agt_ads_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_min_u9m'),
                col('can_tkt_tmo_agt_ads_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_min_u12'),
                col('can_tkt_tmo_agt_dis_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_min_u3m'),
                col('can_tkt_tmo_agt_dis_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_min_u6m'),
                col('can_tkt_tmo_agt_dis_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_min_u9m'),
                col('can_tkt_tmo_agt_dis_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_min_u12'),
                col('can_tkt_tmo_agt_pag_tcr_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_min_u3m'),
                col('can_tkt_tmo_agt_pag_tcr_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_min_u6m'),
                col('can_tkt_tmo_agt_pag_tcr_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_min_u9m'),
                col('can_tkt_tmo_agt_pag_tcr_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_min_u12'),
                col('can_tkt_tmo_agt_pag_bcp_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_min_u3m'),
                col('can_tkt_tmo_agt_pag_bcp_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_min_u6m'),
                col('can_tkt_tmo_agt_pag_bcp_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_min_u9m'),
                col('can_tkt_tmo_agt_pag_bcp_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_min_u12'),
                col('can_tkt_tmo_agt_pag_srv_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_min_u3m'),
                col('can_tkt_tmo_agt_pag_srv_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_min_u6m'),
                col('can_tkt_tmo_agt_pag_srv_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_min_u9m'),
                col('can_tkt_tmo_agt_pag_srv_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_min_u12'),
                col('can_tkt_tmo_agt_emis_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_min_u3m'),
                col('can_tkt_tmo_agt_emis_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_min_u6m'),
                col('can_tkt_tmo_agt_emis_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_min_u9m'),
                col('can_tkt_tmo_agt_emis_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_min_u12'),
                col('can_tkt_tmo_agt_dgir_min_u3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_min_u3m'),
                col('can_tkt_tmo_agt_dgir_min_u6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_min_u6m'),
                col('can_tkt_tmo_agt_dgir_min_u9m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_min_u9m'),
                col('can_tkt_tmo_agt_dgir_min_u12').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_min_u12'),
                col('can_ctd_tmo_agt_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_u1m'),
                col('can_ctd_tmo_agt_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_prm_u3m'),
                col('can_ctd_tmo_agt_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_prm_u6m'),
                col('can_ctd_tmo_agt_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_prm_u9m'),
                col('can_ctd_tmo_agt_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_prm_u12'),
                col('can_ctd_tnm_agt_u1m').cast('DOUBLE').alias('can_ctd_tnm_agt_u1m'),
                col('can_ctd_tnm_agt_prm_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_prm_u3m'),
                col('can_ctd_tnm_agt_prm_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_prm_u6m'),
                col('can_ctd_tnm_agt_prm_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_prm_u9m'),
                col('can_ctd_tnm_agt_prm_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_prm_u12'),
                col('can_ctd_tmo_agt_sol_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_u1m'),
                col('can_ctd_tmo_agt_sol_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_prm_u3m'),
                col('can_ctd_tmo_agt_sol_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_prm_u6m'),
                col('can_ctd_tmo_agt_sol_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_prm_u9m'),
                col('can_ctd_tmo_agt_sol_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_prm_u12'),
                col('can_ctd_tmo_agt_dol_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_u1m'),
                col('can_ctd_tmo_agt_dol_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_prm_u3m'),
                col('can_ctd_tmo_agt_dol_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_prm_u6m'),
                col('can_ctd_tmo_agt_dol_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_prm_u9m'),
                col('can_ctd_tmo_agt_dol_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_prm_u12'),
                col('can_ctd_tmo_agt_ret_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_u1m'),
                col('can_ctd_tmo_agt_ret_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_prm_u3m'),
                col('can_ctd_tmo_agt_ret_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_prm_u6m'),
                col('can_ctd_tmo_agt_ret_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_prm_u9m'),
                col('can_ctd_tmo_agt_ret_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_prm_u12'),
                col('can_ctd_tmo_agt_dep_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_u1m'),
                col('can_ctd_tmo_agt_dep_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_prm_u3m'),
                col('can_ctd_tmo_agt_dep_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_prm_u6m'),
                col('can_ctd_tmo_agt_dep_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_prm_u9m'),
                col('can_ctd_tmo_agt_dep_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_prm_u12'),
                col('can_ctd_tmo_agt_trf_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_u1m'),
                col('can_ctd_tmo_agt_trf_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_prm_u3m'),
                col('can_ctd_tmo_agt_trf_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_prm_u6m'),
                col('can_ctd_tmo_agt_trf_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_prm_u9m'),
                col('can_ctd_tmo_agt_trf_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_prm_u12'),
                col('can_ctd_tmo_agt_ads_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_u1m'),
                col('can_ctd_tmo_agt_ads_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_prm_u3m'),
                col('can_ctd_tmo_agt_ads_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_prm_u6m'),
                col('can_ctd_tmo_agt_ads_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_prm_u9m'),
                col('can_ctd_tmo_agt_ads_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_prm_u12'),
                col('can_ctd_tmo_agt_dis_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_u1m'),
                col('can_ctd_tmo_agt_dis_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_prm_u3m'),
                col('can_ctd_tmo_agt_dis_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_prm_u6m'),
                col('can_ctd_tmo_agt_dis_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_prm_u9m'),
                col('can_ctd_tmo_agt_dis_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_prm_u12'),
                col('can_ctd_tmo_agt_pag_tcr_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_u1m'),
                col('can_ctd_tmo_agt_pag_tcr_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_prm_u3m'),
                col('can_ctd_tmo_agt_pag_tcr_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_prm_u6m'),
                col('can_ctd_tmo_agt_pag_tcr_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_prm_u9m'),
                col('can_ctd_tmo_agt_pag_tcr_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_prm_u12'),
                col('can_ctd_tmo_agt_pag_bcp_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_u1m'),
                col('can_ctd_tmo_agt_pag_bcp_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_prm_u3m'),
                col('can_ctd_tmo_agt_pag_bcp_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_prm_u6m'),
                col('can_ctd_tmo_agt_pag_bcp_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_prm_u9m'),
                col('can_ctd_tmo_agt_pag_bcp_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_prm_u12'),
                col('can_ctd_tmo_agt_pag_srv_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_u1m'),
                col('can_ctd_tmo_agt_pag_srv_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_prm_u3m'),
                col('can_ctd_tmo_agt_pag_srv_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_prm_u6m'),
                col('can_ctd_tmo_agt_pag_srv_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_prm_u9m'),
                col('can_ctd_tmo_agt_pag_srv_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_prm_u12'),
                col('can_ctd_tmo_agt_emis_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_u1m'),
                col('can_ctd_tmo_agt_emis_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_prm_u3m'),
                col('can_ctd_tmo_agt_emis_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_prm_u6m'),
                col('can_ctd_tmo_agt_emis_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_prm_u9m'),
                col('can_ctd_tmo_agt_emis_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_prm_u12'),
                col('can_ctd_tmo_agt_dgir_u1m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_u1m'),
                col('can_ctd_tmo_agt_dgir_prm_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_prm_u3m'),
                col('can_ctd_tmo_agt_dgir_prm_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_prm_u6m'),
                col('can_ctd_tmo_agt_dgir_prm_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_prm_u9m'),
                col('can_ctd_tmo_agt_dgir_prm_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_prm_u12'),
                col('can_ctd_tnm_agt_cslt_u1m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_u1m'),
                col('can_ctd_tnm_agt_cslt_prm_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_prm_u3m'),
                col('can_ctd_tnm_agt_cslt_prm_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_prm_u6m'),
                col('can_ctd_tnm_agt_cslt_prm_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_prm_u9m'),
                col('can_ctd_tnm_agt_cslt_prm_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_prm_u12'),
                col('can_ctd_tnm_agt_admn_u1m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_u1m'),
                col('can_ctd_tnm_agt_admn_prm_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_prm_u3m'),
                col('can_ctd_tnm_agt_admn_prm_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_prm_u6m'),
                col('can_ctd_tnm_agt_admn_prm_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_prm_u9m'),
                col('can_ctd_tnm_agt_admn_prm_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_prm_u12'),
                col('can_ctd_tmo_agt_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_max_u3m'),
                col('can_ctd_tmo_agt_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_max_u6m'),
                col('can_ctd_tmo_agt_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_max_u9m'),
                col('can_ctd_tmo_agt_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_max_u12'),
                col('can_ctd_tnm_agt_max_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_max_u3m'),
                col('can_ctd_tnm_agt_max_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_max_u6m'),
                col('can_ctd_tnm_agt_max_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_max_u9m'),
                col('can_ctd_tnm_agt_max_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_max_u12'),
                col('can_ctd_tmo_agt_sol_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_max_u3m'),
                col('can_ctd_tmo_agt_sol_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_max_u6m'),
                col('can_ctd_tmo_agt_sol_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_max_u9m'),
                col('can_ctd_tmo_agt_sol_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_max_u12'),
                col('can_ctd_tmo_agt_dol_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_max_u3m'),
                col('can_ctd_tmo_agt_dol_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_max_u6m'),
                col('can_ctd_tmo_agt_dol_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_max_u9m'),
                col('can_ctd_tmo_agt_dol_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_max_u12'),
                col('can_ctd_tmo_agt_ret_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_max_u3m'),
                col('can_ctd_tmo_agt_ret_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_max_u6m'),
                col('can_ctd_tmo_agt_ret_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_max_u9m'),
                col('can_ctd_tmo_agt_ret_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_max_u12'),
                col('can_ctd_tmo_agt_dep_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_max_u3m'),
                col('can_ctd_tmo_agt_dep_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_max_u6m'),
                col('can_ctd_tmo_agt_dep_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_max_u9m'),
                col('can_ctd_tmo_agt_dep_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_max_u12'),
                col('can_ctd_tmo_agt_trf_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_max_u3m'),
                col('can_ctd_tmo_agt_trf_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_max_u6m'),
                col('can_ctd_tmo_agt_trf_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_max_u9m'),
                col('can_ctd_tmo_agt_trf_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_max_u12'),
                col('can_ctd_tmo_agt_ads_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_max_u3m'),
                col('can_ctd_tmo_agt_ads_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_max_u6m'),
                col('can_ctd_tmo_agt_ads_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_max_u9m'),
                col('can_ctd_tmo_agt_ads_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_max_u12'),
                col('can_ctd_tmo_agt_dis_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_max_u3m'),
                col('can_ctd_tmo_agt_dis_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_max_u6m'),
                col('can_ctd_tmo_agt_dis_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_max_u9m'),
                col('can_ctd_tmo_agt_dis_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_max_u12'),
                col('can_ctd_tmo_agt_pag_tcr_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_max_u3m'),
                col('can_ctd_tmo_agt_pag_tcr_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_max_u6m'),
                col('can_ctd_tmo_agt_pag_tcr_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_max_u9m'),
                col('can_ctd_tmo_agt_pag_tcr_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_max_u12'),
                col('can_ctd_tmo_agt_pag_bcp_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_max_u3m'),
                col('can_ctd_tmo_agt_pag_bcp_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_max_u6m'),
                col('can_ctd_tmo_agt_pag_bcp_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_max_u9m'),
                col('can_ctd_tmo_agt_pag_bcp_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_max_u12'),
                col('can_ctd_tmo_agt_pag_srv_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_max_u3m'),
                col('can_ctd_tmo_agt_pag_srv_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_max_u6m'),
                col('can_ctd_tmo_agt_pag_srv_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_max_u9m'),
                col('can_ctd_tmo_agt_pag_srv_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_max_u12'),
                col('can_ctd_tmo_agt_emis_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_max_u3m'),
                col('can_ctd_tmo_agt_emis_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_max_u6m'),
                col('can_ctd_tmo_agt_emis_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_max_u9m'),
                col('can_ctd_tmo_agt_emis_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_max_u12'),
                col('can_ctd_tmo_agt_dgir_max_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_max_u3m'),
                col('can_ctd_tmo_agt_dgir_max_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_max_u6m'),
                col('can_ctd_tmo_agt_dgir_max_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_max_u9m'),
                col('can_ctd_tmo_agt_dgir_max_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_max_u12'),
                col('can_ctd_tnm_agt_cslt_max_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_max_u3m'),
                col('can_ctd_tnm_agt_cslt_max_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_max_u6m'),
                col('can_ctd_tnm_agt_cslt_max_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_max_u9m'),
                col('can_ctd_tnm_agt_cslt_max_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_max_u12'),
                col('can_ctd_tnm_agt_admn_max_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_max_u3m'),
                col('can_ctd_tnm_agt_admn_max_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_max_u6m'),
                col('can_ctd_tnm_agt_admn_max_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_max_u9m'),
                col('can_ctd_tnm_agt_admn_max_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_max_u12'),
                col('can_ctd_tmo_agt_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_min_u3m'),
                col('can_ctd_tmo_agt_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_min_u6m'),
                col('can_ctd_tmo_agt_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_min_u9m'),
                col('can_ctd_tmo_agt_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_min_u12'),
                col('can_ctd_tnm_agt_min_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_min_u3m'),
                col('can_ctd_tnm_agt_min_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_min_u6m'),
                col('can_ctd_tnm_agt_min_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_min_u9m'),
                col('can_ctd_tnm_agt_min_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_min_u12'),
                col('can_ctd_tmo_agt_sol_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_min_u3m'),
                col('can_ctd_tmo_agt_sol_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_min_u6m'),
                col('can_ctd_tmo_agt_sol_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_min_u9m'),
                col('can_ctd_tmo_agt_sol_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_min_u12'),
                col('can_ctd_tmo_agt_dol_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_min_u3m'),
                col('can_ctd_tmo_agt_dol_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_min_u6m'),
                col('can_ctd_tmo_agt_dol_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_min_u9m'),
                col('can_ctd_tmo_agt_dol_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_min_u12'),
                col('can_ctd_tmo_agt_ret_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_min_u3m'),
                col('can_ctd_tmo_agt_ret_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_min_u6m'),
                col('can_ctd_tmo_agt_ret_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_min_u9m'),
                col('can_ctd_tmo_agt_ret_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_min_u12'),
                col('can_ctd_tmo_agt_dep_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_min_u3m'),
                col('can_ctd_tmo_agt_dep_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_min_u6m'),
                col('can_ctd_tmo_agt_dep_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_min_u9m'),
                col('can_ctd_tmo_agt_dep_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_min_u12'),
                col('can_ctd_tmo_agt_trf_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_min_u3m'),
                col('can_ctd_tmo_agt_trf_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_min_u6m'),
                col('can_ctd_tmo_agt_trf_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_min_u9m'),
                col('can_ctd_tmo_agt_trf_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_min_u12'),
                col('can_ctd_tmo_agt_ads_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_min_u3m'),
                col('can_ctd_tmo_agt_ads_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_min_u6m'),
                col('can_ctd_tmo_agt_ads_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_min_u9m'),
                col('can_ctd_tmo_agt_ads_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_min_u12'),
                col('can_ctd_tmo_agt_dis_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_min_u3m'),
                col('can_ctd_tmo_agt_dis_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_min_u6m'),
                col('can_ctd_tmo_agt_dis_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_min_u9m'),
                col('can_ctd_tmo_agt_dis_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_min_u12'),
                col('can_ctd_tmo_agt_pag_tcr_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_min_u3m'),
                col('can_ctd_tmo_agt_pag_tcr_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_min_u6m'),
                col('can_ctd_tmo_agt_pag_tcr_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_min_u9m'),
                col('can_ctd_tmo_agt_pag_tcr_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_min_u12'),
                col('can_ctd_tmo_agt_pag_bcp_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_min_u3m'),
                col('can_ctd_tmo_agt_pag_bcp_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_min_u6m'),
                col('can_ctd_tmo_agt_pag_bcp_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_min_u9m'),
                col('can_ctd_tmo_agt_pag_bcp_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_min_u12'),
                col('can_ctd_tmo_agt_pag_srv_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_min_u3m'),
                col('can_ctd_tmo_agt_pag_srv_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_min_u6m'),
                col('can_ctd_tmo_agt_pag_srv_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_min_u9m'),
                col('can_ctd_tmo_agt_pag_srv_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_min_u12'),
                col('can_ctd_tmo_agt_emis_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_min_u3m'),
                col('can_ctd_tmo_agt_emis_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_min_u6m'),
                col('can_ctd_tmo_agt_emis_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_min_u9m'),
                col('can_ctd_tmo_agt_emis_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_min_u12'),
                col('can_ctd_tmo_agt_dgir_min_u3m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_min_u3m'),
                col('can_ctd_tmo_agt_dgir_min_u6m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_min_u6m'),
                col('can_ctd_tmo_agt_dgir_min_u9m').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_min_u9m'),
                col('can_ctd_tmo_agt_dgir_min_u12').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_min_u12'),
                col('can_ctd_tnm_agt_cslt_min_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_min_u3m'),
                col('can_ctd_tnm_agt_cslt_min_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_min_u6m'),
                col('can_ctd_tnm_agt_cslt_min_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_min_u9m'),
                col('can_ctd_tnm_agt_cslt_min_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_min_u12'),
                col('can_ctd_tnm_agt_admn_min_u3m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_min_u3m'),
                col('can_ctd_tnm_agt_admn_min_u6m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_min_u6m'),
                col('can_ctd_tnm_agt_admn_min_u9m').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_min_u9m'),
                col('can_ctd_tnm_agt_admn_min_u12').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_min_u12'),
                col('can_ctd_tmo_agt_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_rec'),
                col('can_ctd_tnm_agt_rec').cast('DOUBLE').alias('can_ctd_tnm_agt_rec'),
                col('can_ctd_tmo_agt_sol_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_sol_rec'),
                col('can_ctd_tmo_agt_dol_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_dol_rec'),
                col('can_ctd_tmo_agt_ret_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_ret_rec'),
                col('can_ctd_tmo_agt_dep_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_dep_rec'),
                col('can_ctd_tmo_agt_trf_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_trf_rec'),
                col('can_ctd_tmo_agt_ads_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_ads_rec'),
                col('can_ctd_tmo_agt_dis_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_dis_rec'),
                col('can_ctd_tmo_agt_pag_tcr_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_tcr_rec'),
                col('can_ctd_tmo_agt_pag_bcp_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_bcp_rec'),
                col('can_ctd_tmo_agt_pag_srv_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_pag_srv_rec'),
                col('can_ctd_tmo_agt_emis_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_emis_rec'),
                col('can_ctd_tmo_agt_dgir_rec').cast('DOUBLE').alias('can_ctd_tmo_agt_dgir_rec'),
                col('can_ctd_tnm_agt_cslt_rec').cast('DOUBLE').alias('can_ctd_tnm_agt_cslt_rec'),
                col('can_ctd_tnm_agt_admn_rec').cast('DOUBLE').alias('can_ctd_tnm_agt_admn_rec'),
                col('can_ctd_tmo_agt_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_frq_u3m'),
                col('can_ctd_tnm_agt_frq_u3m').cast('BIGINT').alias('can_ctd_tnm_agt_frq_u3m'),
                col('can_ctd_tmo_agt_sol_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_sol_frq_u3m'),
                col('can_ctd_tmo_agt_dol_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_dol_frq_u3m'),
                col('can_ctd_tmo_agt_ret_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_ret_frq_u3m'),
                col('can_ctd_tmo_agt_dep_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_dep_frq_u3m'),
                col('can_ctd_tmo_agt_trf_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_trf_frq_u3m'),
                col('can_ctd_tmo_agt_ads_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_ads_frq_u3m'),
                col('can_ctd_tmo_agt_dis_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_dis_frq_u3m'),
                col('can_ctd_tmo_agt_pag_tcr_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_tcr_frq_u3m'),
                col('can_ctd_tmo_agt_pag_bcp_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_bcp_frq_u3m'),
                col('can_ctd_tmo_agt_pag_srv_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_srv_frq_u3m'),
                col('can_ctd_tmo_agt_emis_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_emis_frq_u3m'),
                col('can_ctd_tmo_agt_dgir_frq_u3m').cast('BIGINT').alias('can_ctd_tmo_agt_dgir_frq_u3m'),
                col('can_ctd_tnm_agt_cslt_frq_u3m').cast('BIGINT').alias('can_ctd_tnm_agt_cslt_frq_u3m'),
                col('can_ctd_tnm_agt_admn_frq_u3m').cast('BIGINT').alias('can_ctd_tnm_agt_admn_frq_u3m'),
                col('can_ctd_tmo_agt_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_frq_u6m'),
                col('can_ctd_tnm_agt_frq_u6m').cast('BIGINT').alias('can_ctd_tnm_agt_frq_u6m'),
                col('can_ctd_tmo_agt_sol_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_sol_frq_u6m'),
                col('can_ctd_tmo_agt_dol_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_dol_frq_u6m'),
                col('can_ctd_tmo_agt_ret_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_ret_frq_u6m'),
                col('can_ctd_tmo_agt_dep_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_dep_frq_u6m'),
                col('can_ctd_tmo_agt_trf_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_trf_frq_u6m'),
                col('can_ctd_tmo_agt_ads_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_ads_frq_u6m'),
                col('can_ctd_tmo_agt_dis_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_dis_frq_u6m'),
                col('can_ctd_tmo_agt_pag_tcr_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_tcr_frq_u6m'),
                col('can_ctd_tmo_agt_pag_bcp_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_bcp_frq_u6m'),
                col('can_ctd_tmo_agt_pag_srv_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_srv_frq_u6m'),
                col('can_ctd_tmo_agt_emis_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_emis_frq_u6m'),
                col('can_ctd_tmo_agt_dgir_frq_u6m').cast('BIGINT').alias('can_ctd_tmo_agt_dgir_frq_u6m'),
                col('can_ctd_tnm_agt_cslt_frq_u6m').cast('BIGINT').alias('can_ctd_tnm_agt_cslt_frq_u6m'),
                col('can_ctd_tnm_agt_admn_frq_u6m').cast('BIGINT').alias('can_ctd_tnm_agt_admn_frq_u6m'),
                col('can_ctd_tmo_agt_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_frq_u9m'),
                col('can_ctd_tnm_agt_frq_u9m').cast('BIGINT').alias('can_ctd_tnm_agt_frq_u9m'),
                col('can_ctd_tmo_agt_sol_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_sol_frq_u9m'),
                col('can_ctd_tmo_agt_dol_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_dol_frq_u9m'),
                col('can_ctd_tmo_agt_ret_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_ret_frq_u9m'),
                col('can_ctd_tmo_agt_dep_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_dep_frq_u9m'),
                col('can_ctd_tmo_agt_trf_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_trf_frq_u9m'),
                col('can_ctd_tmo_agt_ads_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_ads_frq_u9m'),
                col('can_ctd_tmo_agt_dis_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_dis_frq_u9m'),
                col('can_ctd_tmo_agt_pag_tcr_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_tcr_frq_u9m'),
                col('can_ctd_tmo_agt_pag_bcp_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_bcp_frq_u9m'),
                col('can_ctd_tmo_agt_pag_srv_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_pag_srv_frq_u9m'),
                col('can_ctd_tmo_agt_emis_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_emis_frq_u9m'),
                col('can_ctd_tmo_agt_dgir_frq_u9m').cast('BIGINT').alias('can_ctd_tmo_agt_dgir_frq_u9m'),
                col('can_ctd_tnm_agt_cslt_frq_u9m').cast('BIGINT').alias('can_ctd_tnm_agt_cslt_frq_u9m'),
                col('can_ctd_tnm_agt_admn_frq_u9m').cast('BIGINT').alias('can_ctd_tnm_agt_admn_frq_u9m'),
                col('can_ctd_tmo_agt_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_frq_u12'),
                col('can_ctd_tnm_agt_frq_u12').cast('BIGINT').alias('can_ctd_tnm_agt_frq_u12'),
                col('can_ctd_tmo_agt_sol_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_sol_frq_u12'),
                col('can_ctd_tmo_agt_dol_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_dol_frq_u12'),
                col('can_ctd_tmo_agt_ret_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_ret_frq_u12'),
                col('can_ctd_tmo_agt_dep_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_dep_frq_u12'),
                col('can_ctd_tmo_agt_trf_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_trf_frq_u12'),
                col('can_ctd_tmo_agt_ads_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_ads_frq_u12'),
                col('can_ctd_tmo_agt_dis_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_dis_frq_u12'),
                col('can_ctd_tmo_agt_pag_tcr_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_pag_tcr_frq_u12'),
                col('can_ctd_tmo_agt_pag_bcp_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_pag_bcp_frq_u12'),
                col('can_ctd_tmo_agt_pag_srv_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_pag_srv_frq_u12'),
                col('can_ctd_tmo_agt_emis_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_emis_frq_u12'),
                col('can_ctd_tmo_agt_dgir_frq_u12').cast('BIGINT').alias('can_ctd_tmo_agt_dgir_frq_u12'),
                col('can_ctd_tnm_agt_cslt_frq_u12').cast('BIGINT').alias('can_ctd_tnm_agt_cslt_frq_u12'),
                col('can_ctd_tnm_agt_admn_frq_u12').cast('BIGINT').alias('can_ctd_tnm_agt_admn_frq_u12'),
                col('can_mto_tmo_agt_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_g6m'),
                col('can_mto_tmo_agt_sol_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_g6m'),
                col('can_mto_tmo_agt_dol_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_g6m'),
                col('can_mto_tmo_agt_ret_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_g6m'),
                col('can_mto_tmo_agt_dep_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_g6m'),
                col('can_mto_tmo_agt_trf_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_g6m'),
                col('can_mto_tmo_agt_ads_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_g6m'),
                col('can_mto_tmo_agt_dis_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_g6m'),
                col('can_mto_tmo_agt_pag_tcr_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_g6m'),
                col('can_mto_tmo_agt_pag_bcp_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_g6m'),
                col('can_mto_tmo_agt_pag_srv_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_g6m'),
                col('can_mto_tmo_agt_emis_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_g6m'),
                col('can_mto_tmo_agt_dgir_g6m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_g6m'),
                col('can_tkt_tmo_agt_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_g6m'),
                col('can_tkt_tmo_agt_sol_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_g6m'),
                col('can_tkt_tmo_agt_dol_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_g6m'),
                col('can_tkt_tmo_agt_ret_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_g6m'),
                col('can_tkt_tmo_agt_dep_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_g6m'),
                col('can_tkt_tmo_agt_trf_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_g6m'),
                col('can_tkt_tmo_agt_ads_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_g6m'),
                col('can_tkt_tmo_agt_dis_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_g6m'),
                col('can_tkt_tmo_agt_pag_tcr_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_g6m'),
                col('can_tkt_tmo_agt_pag_bcp_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_g6m'),
                col('can_tkt_tmo_agt_pag_srv_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_g6m'),
                col('can_tkt_tmo_agt_emis_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_g6m'),
                col('can_tkt_tmo_agt_dgir_g6m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_g6m'),
                col('can_mto_tmo_agt_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_g3m'),
                col('can_mto_tmo_agt_sol_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_g3m'),
                col('can_mto_tmo_agt_dol_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_g3m'),
                col('can_mto_tmo_agt_ret_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_g3m'),
                col('can_mto_tmo_agt_dep_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_g3m'),
                col('can_mto_tmo_agt_trf_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_g3m'),
                col('can_mto_tmo_agt_ads_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_g3m'),
                col('can_mto_tmo_agt_dis_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_g3m'),
                col('can_mto_tmo_agt_pag_tcr_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_g3m'),
                col('can_mto_tmo_agt_pag_bcp_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_g3m'),
                col('can_mto_tmo_agt_pag_srv_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_g3m'),
                col('can_mto_tmo_agt_emis_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_g3m'),
                col('can_mto_tmo_agt_dgir_g3m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_g3m'),
                col('can_tkt_tmo_agt_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_g3m'),
                col('can_tkt_tmo_agt_sol_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_g3m'),
                col('can_tkt_tmo_agt_dol_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_g3m'),
                col('can_tkt_tmo_agt_ret_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_g3m'),
                col('can_tkt_tmo_agt_dep_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_g3m'),
                col('can_tkt_tmo_agt_trf_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_g3m'),
                col('can_tkt_tmo_agt_ads_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_g3m'),
                col('can_tkt_tmo_agt_dis_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_g3m'),
                col('can_tkt_tmo_agt_pag_tcr_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_g3m'),
                col('can_tkt_tmo_agt_pag_bcp_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_g3m'),
                col('can_tkt_tmo_agt_pag_srv_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_g3m'),
                col('can_tkt_tmo_agt_emis_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_g3m'),
                col('can_tkt_tmo_agt_dgir_g3m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_g3m'),
                col('can_mto_tmo_agt_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_g1m'),
                col('can_mto_tmo_agt_sol_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_sol_g1m'),
                col('can_mto_tmo_agt_dol_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_dol_g1m'),
                col('can_mto_tmo_agt_ret_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_ret_g1m'),
                col('can_mto_tmo_agt_dep_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_dep_g1m'),
                col('can_mto_tmo_agt_trf_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_trf_g1m'),
                col('can_mto_tmo_agt_ads_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_ads_g1m'),
                col('can_mto_tmo_agt_dis_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_dis_g1m'),
                col('can_mto_tmo_agt_pag_tcr_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_tcr_g1m'),
                col('can_mto_tmo_agt_pag_bcp_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_bcp_g1m'),
                col('can_mto_tmo_agt_pag_srv_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_pag_srv_g1m'),
                col('can_mto_tmo_agt_emis_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_emis_g1m'),
                col('can_mto_tmo_agt_dgir_g1m').cast('DOUBLE').alias('can_mto_tmo_agt_dgir_g1m'),
                col('can_tkt_tmo_agt_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_g1m'),
                col('can_tkt_tmo_agt_sol_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_sol_g1m'),
                col('can_tkt_tmo_agt_dol_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dol_g1m'),
                col('can_tkt_tmo_agt_ret_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_ret_g1m'),
                col('can_tkt_tmo_agt_dep_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dep_g1m'),
                col('can_tkt_tmo_agt_trf_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_trf_g1m'),
                col('can_tkt_tmo_agt_ads_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_ads_g1m'),
                col('can_tkt_tmo_agt_dis_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dis_g1m'),
                col('can_tkt_tmo_agt_pag_tcr_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_tcr_g1m'),
                col('can_tkt_tmo_agt_pag_bcp_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_bcp_g1m'),
                col('can_tkt_tmo_agt_pag_srv_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_pag_srv_g1m'),
                col('can_tkt_tmo_agt_emis_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_emis_g1m'),
                col('can_tkt_tmo_agt_dgir_g1m').cast('DOUBLE').alias('can_tkt_tmo_agt_dgir_g1m'),
                col('fecrutina').cast('DATE').alias('fecrutina'),
                col('fecactualizacionregistro').cast('TIMESTAMP').alias('fecactualizacionregistro'),
                col('codmes').cast('INT').alias('codmes'))

    write_delta(input_df,VAL_DESTINO_NAME, CONS_PARTITION_DELTA_NAME)

# COMMAND ----------

#
 # M�todo principal de ejecuci�n
 #
 # @return {void}
 ##
def main():

    #Leemos la tabla de parametros de fecha inicio y fecha fin para el proceso
    mesInicio, mesFin = (spark.sql(f"""
                                    SELECT CAST(CAST(CODMESINICIO AS INT) AS STRING), CAST(CAST(CODMESFIN AS INT) AS STRING)
                                    FROM {PRM_ESQUEMA_TABLA}.{PRM_TABLA_PARAMETROMATRIZVARIABLES}
                                    """).take(1)[0][0:2])
                                 
    #Retorna un Dataframe Pandas iterable con una distancia entre fecha inicio y fecha fin de 12 meses para cada registro del dataframe 
    totalMeses = funciones.calculoDfAnios(mesInicio, mesFin)

    if mesInicio == mesFin:
        codMesProceso = funciones.calcularCodmes(PRM_FECHA_RUTINA)
        totalMeses = funciones.calculoDfAnios(codMesProceso, codMesProceso)

    carpetaDetConceptoClienteSegundaTranspuesta = "detconceptocliente_"+PRM_TABLA_SEGUNDATRANSPUESTA.lower()
    carpetaClientePrimeraTranspuesta = "cli_"+PRM_TABLA_PRIMERATRANSPUESTA.lower()
    tmp_table_1 = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpetaDetConceptoClienteSegundaTranspuesta}_tmp'.lower()
    tmp_table_2 = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpetaClientePrimeraTranspuesta}_tmp'.lower()


    #Recorremos el dataframe pandas totalMeses segun sea la cantidad de registros 
    for index, row in  totalMeses.iterrows():
    
        #Leemos el valor de la fila codMes
        codMes = str(row['codMes'])
        
        #Leemos el valor de la fila mes11atras
        codMes12Atras = str(row['mes11atras'])
        
        extraccion_info_base_cliente(codMes, PRM_ESQUEMA_TABLA, PRM_CARPETA_RAIZ_DE_PROYECTO, carpetaDetConceptoClienteSegundaTranspuesta)
    
        #Se extrae informaci�n de doce meses de la primera transpuesta
        dfInfo12Meses = extraccionInformacion12Meses(codMes, codMes12Atras, carpetaDetConceptoClienteSegundaTranspuesta, carpetaClientePrimeraTranspuesta)
        
        #Se procesa la logica de la segunda transpuesta
        dfMatrizVarTransaccionAgente = agruparInformacionMesAnalisis(dfInfo12Meses)
        
        save_matriz(dfMatrizVarTransaccionAgente)
                
        spark.sql(f'TRUNCATE TABLE {tmp_table_1};')
        spark.sql(f'TRUNCATE TABLE {tmp_table_2};')
        pass
    spark.sql(f'VACUUM {tmp_table_1} RETAIN 0 HOURS;')    
    spark.sql(f'VACUUM {tmp_table_2} RETAIN 0 HOURS;')

# COMMAND ----------

###
# @section Ejecucion
##

# Inicio Proceso main()
if __name__ == "__main__":
    main()
#retorna el valor en 1 si el proceso termina de manera satisfactoria.
dbutils.notebook.exit(1)
