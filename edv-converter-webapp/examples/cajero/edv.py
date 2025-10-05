# Databricks notebook source
# Created by COE_Data_Scaffold
# coding: utf-8
# ||**********************************************************************************************************************
# || PROYECTO       : BCP - SQUAD FOR ANALYTICS - MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO
# || NOMBRE         : MATRIZVARIABLES_HM_MATRIZTRANSACCIONCAJERO.py
# || TABLA DESTINO  : BCP_DDV_MATRIZVARIABLES.HM_MATRIZTRANSACCIONCAJERO
# || TABLA FUENTE   : BCP_DDV_MATRIZVARIABLES.HM_CONCEPTOTRANSACCIONCAJERO
# ||                  BCP_DDV_MATRIZVARIABLES.HM_DETCONCEPTOCLIENTE
# ||                  BCP_DDV_MATRIZVARIABLES.MM_PARAMETROMATRIZVARIABLES
# || OBJETIVO       : Poblar la tabla BCP_DDV_MATRIZVARIABLES.HM_MATRIZTRANSACCIONCAJERO
# || TIPO           : PY
# || REPROCESABLE   : SI
# || OBSERVACION    : NA
# || SCHEDULER      : SI
# || JOB            : @P4LKKA9
# || VERSION      DESARROLLADOR             PROVEEDOR            PO                 FECHA        DESCRIPCION
# || -------------------------------------------------------------------------------------------------------------------
# || 1.1      DIEGO GUERRA CRUZADO            Everis    	  ARTURO ROJAS        06/02/19     Creaci�n del proceso
# ||          GIULIANA PABLO ALEJANDRO
# || 1.2      DIEGO UCHARIMA                  Everis        RODRIGO ROJAS       04/05/20     Modificaci�n del proceso
# ||          MARCOS IRVING MERA SANCHEZ
# || 1.3      KEYLA NALVARTE                  INDRA         RODRIGO ROJAS       15/06/23     ModificaciÓn de la cabecera
# || 1.4      KEYLA NALVARTE                  INDRA         RODRIGO ROJAS       10/01/23     Migración a Cloud
# || 1.5      ELMER ACERO                     INDRA         RODRIGO ROJAS       31/07/25     Deuda Técnica
# ********************************************************************************************************************

# COMMAND ----------
# Libraries imports
import itertools
from pyspark.sql.functions import col, concat, when,trim,lit
from pyspark.sql import functions as F 
import pyspark.sql.functions as func
import funciones
import bcp_coe_data_cleaner
from bcp_coe_data_cleaner import coe_data_cleaner
bcp_coe_data_cleaner.__version__


# COMMAND ----------
###
 # @section Parámetros de proceso
 ##
#Aplicar configuración de proceso
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss",False)

CONS_WRITE_FORMAT_NAME = "delta"
CONS_COMPRESSION_MODE_NAME = "snappy"
CONS_WRITE_TEXT_MODE_NAME = "overwrite"
CONS_DFS_NAME = ".dfs.core.windows.net/"
CONS_CONTAINER_NAME = "abfss://bcp-edv-trdata-012@"
CONS_PARTITION_DELTA_NAME = "codmes"


# COMMAND ----------
dbutils.widgets.text(name="PRM_STORAGE_ACCOUNT_DDV", defaultValue='adlscu1lhclbackp05')
dbutils.widgets.text(name="PRM_CARPETA_OUTPUT", defaultValue='data/RUBEN/DEUDA_TECNICA/out')
dbutils.widgets.text(name="PRM_RUTA_ADLS_TABLES", defaultValue='data/RUBEN/DEUDA_TECNICA/matrizvariables')
dbutils.widgets.text(name="PRM_TABLE_NAME", defaultValue='HM_MATRIZTRANSACCIONCAJERO_RUBEN')
dbutils.widgets.text(name="PRM_CATALOG_NAME", defaultValue='catalog_lhcl_prod_bcp')
dbutils.widgets.text(name="PRM_FECHA_RUTINA", defaultValue='2025-09-01')
dbutils.widgets.text(name="PRM_TABLA_PRIMERATRANSPUESTA", defaultValue='HM_CONCEPTOTRANSACCIONCAJERO')
dbutils.widgets.text(name="PRM_TABLA_SEGUNDATRANSPUESTA", defaultValue='HM_MATRIZTRANSACCIONCAJERO_RUBEN')
dbutils.widgets.text(name="PRM_TABLA_SEGUNDATRANSPUESTA_TMP", defaultValue='hm_matriztransaccioncajero_tmp_ruben')
dbutils.widgets.text(name="PRM_TABLA_PARAMETROMATRIZVARIABLES", defaultValue='mm_parametromatrizvariables')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_X", defaultValue='bcp_ddv_matrizvariables')
dbutils.widgets.text(name="PRM_CARPETA_RAIZ_DE_PROYECTO", defaultValue='/desa/bcp/ddv/analytics/matrizvariables')

dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables_v')
dbutils.widgets.text(name="PRM_CATALOG_NAME_EDV", defaultValue='catalog_lhcl_prod_bcp_expl')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_EDV", defaultValue='bcp_edv_trdata_012')

# COMMAND ----------
PRM_STORAGE_ACCOUNT_DDV = dbutils.widgets.get("PRM_STORAGE_ACCOUNT_DDV")
PRM_CARPETA_OUTPUT = dbutils.widgets.get("PRM_CARPETA_OUTPUT")
PRM_RUTA_ADLS_TABLES = dbutils.widgets.get("PRM_RUTA_ADLS_TABLES")
PRM_TABLE_NAME = dbutils.widgets.get("PRM_TABLE_NAME")
PRM_CATALOG_NAME = dbutils.widgets.get("PRM_CATALOG_NAME")
PRM_FECHA_RUTINA = dbutils.widgets.get("PRM_FECHA_RUTINA")
PRM_TABLA_PRIMERATRANSPUESTA = dbutils.widgets.get("PRM_TABLA_PRIMERATRANSPUESTA")
PRM_TABLA_PARAMETROMATRIZVARIABLES = dbutils.widgets.get("PRM_TABLA_PARAMETROMATRIZVARIABLES")
PRM_TABLA_SEGUNDATRANSPUESTA = dbutils.widgets.get("PRM_TABLA_SEGUNDATRANSPUESTA")
PRM_TABLA_SEGUNDATRANSPUESTA_TMP = dbutils.widgets.get("PRM_TABLA_SEGUNDATRANSPUESTA_TMP")
PRM_ESQUEMA_TABLA_X = dbutils.widgets.get("PRM_ESQUEMA_TABLA_X")
PRM_CARPETA_RAIZ_DE_PROYECTO_X = dbutils.widgets.get("PRM_CARPETA_RAIZ_DE_PROYECTO")

PRM_ESQUEMA_TABLA_DDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_DDV")
PRM_CATALOG_NAME_EDV = dbutils.widgets.get("PRM_CATALOG_NAME_EDV")
PRM_ESQUEMA_TABLA_EDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_EDV")

PRM_CARPETA_RAIZ_DE_PROYECTO = CONS_CONTAINER_NAME+PRM_STORAGE_ACCOUNT_DDV+CONS_DFS_NAME+PRM_RUTA_ADLS_TABLES
PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME+"."+PRM_ESQUEMA_TABLA_DDV

PRM_ESQUEMA_TABLA_ESCRITURA = PRM_CATALOG_NAME_EDV + "." + PRM_ESQUEMA_TABLA_EDV
VAL_DESTINO_NAME = PRM_ESQUEMA_TABLA_ESCRITURA + "." + PRM_TABLE_NAME
objeto = coe_data_cleaner.DataCleanerLHCL()

# COMMAND ----------
## Funcion de escritura
def write_delta(df, output, partition):
    df.write \
          .format(CONS_WRITE_FORMAT_NAME) \
          .option("compression",CONS_COMPRESSION_MODE_NAME) \
          .option("partitionOverwritemode","dynamic")  \
          .partitionBy(partition).mode(CONS_WRITE_TEXT_MODE_NAME) \
          .saveAsTable(output)

# COMMAND ----------
# M�todo principal de ejecuci�n
 #
 # @return {void}
 ##
###
 # Extraemos informacion de 12 meses.
 #
 # @param codMes {string} Mes superior.
 # @param codMes12Atras {string} Mes inferior.
 # @param carpetaDetConceptoClienteSegundaTranspuesta {string} Nombre de la carpeta donde se localiza temporalmente informaci�n de la tabla HM_DETCONCEPTOCLIENTE.
 # @param carpetaClientePrimeraTranspuesta {string} Nombre de la carpeta donde se localiza temporalmente la informaci�n de la tabla HM_CONCEPTOTRANSACCIONCAJERO.
 # @return {Dataframe Spark} Datos de los �ltimos 12 meses.
 ## 
def extraccionInformacion12Meses(codMes, codMes12Atras, carpetaDetConceptoClienteSegundaTranspuesta, carpetaClientePrimeraTranspuesta):
    
    #Leemos el dataframe de disco a memoria
    dfClienteCuc = spark.read.format(funciones.CONS_FORMATO_DE_ESCRITURA_EN_DISCO).load(PRM_CARPETA_RAIZ_DE_PROYECTO+"/temp/"+carpetaDetConceptoClienteSegundaTranspuesta+"/CODMES="+str(codMes))
    
    #Dataframe con informaci�n de los codinternocomputacional de los clientes de la tabla HM_DETCONCEPTOCLIENTE
    dfClienteCic = spark.sql("""
                             SELECT 
                                CODMES, 
                                TRIM(CODINTERNOCOMPUTACIONAL) AS CODINTERNOCOMPUTACIONAL, 
                                CODINTERNOCOMPUTACIONALCONCEPTO CODINTERNOCOMPUTACIONAL_LAST, 
                                CODUNICOCLI CODUNICOCLI_LAST 
                             FROM 
                                {var:esquemaTabla}.HM_DETCONCEPTOCLIENTE 
                             WHERE 
                                CODMES = {var:codMes}
                             """.\
                             replace("{var:codMes}", codMes).\
                             replace("{var:esquemaTabla}", PRM_ESQUEMA_TABLA)
                             )
    
    #Nos quedamos con las columnas necesarias para el procesamiento y construcci�n de la segunda transpuesta
    dfInfo12MesesStep0 = spark.sql("""
                                   SELECT
                                      CODUNICOCLI,
                                      TRIM(CODINTERNOCOMPUTACIONAL) AS CODINTERNOCOMPUTACIONAL,
                                      CAN_MTO_TMO_ATM,
                                      CAN_CTD_TMO_ATM,
                                      CAN_CTD_TNM_ATM,
                                      CAN_MTO_TMO_ATM_SOL,
                                      CAN_CTD_TMO_ATM_SOL,
                                      CAN_MTO_TMO_ATM_DOL,
                                      CAN_CTD_TMO_ATM_DOL,
                                      CAN_MTO_TMO_ATM_RET,
                                      CAN_CTD_TMO_ATM_RET,
                                      CAN_MTO_TMO_ATM_RET_SOL,
                                      CAN_CTD_TMO_ATM_RET_SOL,
                                      CAN_MTO_TMO_ATM_RET_DOL,
                                      CAN_CTD_TMO_ATM_RET_DOL,
                                      CAN_MTO_TMO_ATM_DEP,
                                      CAN_CTD_TMO_ATM_DEP,
                                      CAN_MTO_TMO_ATM_DEP_SOL,
                                      CAN_CTD_TMO_ATM_DEP_SOL,
                                      CAN_MTO_TMO_ATM_DEP_DOL,
                                      CAN_CTD_TMO_ATM_DEP_DOL,
                                      CAN_MTO_TMO_ATM_TRF,
                                      CAN_CTD_TMO_ATM_TRF,
                                      CAN_MTO_TMO_ATM_TRF_SOL,
                                      CAN_CTD_TMO_ATM_TRF_SOL,
                                      CAN_MTO_TMO_ATM_TRF_DOL,
                                      CAN_CTD_TMO_ATM_TRF_DOL,
                                      CAN_MTO_TMO_ATM_ADS,
                                      CAN_CTD_TMO_ATM_ADS,
                                      CAN_MTO_TMO_ATM_DIS,
                                      CAN_CTD_TMO_ATM_DIS,
                                      CAN_MTO_TMO_ATM_PAG_TCR,
                                      CAN_CTD_TMO_ATM_PAG_TCR,
                                      CAN_MTO_TMO_ATM_PAG_SRV,
                                      CAN_CTD_TMO_ATM_PAG_SRV,
                                      CAN_CTD_TNM_ATM_CSLT,
                                      CAN_CTD_TNM_ATM_ADMN,
                                      CAN_FLG_TMO_ATM,
                                      CAN_TKT_TMO_ATM,
                                      CAN_FLG_TNM_ATM,
                                      CAN_FLG_TMO_ATM_SOL,
                                      CAN_TKT_TMO_ATM_SOL,
                                      CAN_FLG_TMO_ATM_DOL,
                                      CAN_TKT_TMO_ATM_DOL,
                                      CAN_FLG_TMO_ATM_RET,
                                      CAN_TKT_TMO_ATM_RET,
                                      CAN_FLG_TMO_ATM_RET_SOL,
                                      CAN_TKT_TMO_ATM_RET_SOL,
                                      CAN_FLG_TMO_ATM_RET_DOL,
                                      CAN_TKT_TMO_ATM_RET_DOL,
                                      CAN_FLG_TMO_ATM_DEP,
                                      CAN_TKT_TMO_ATM_DEP,
                                      CAN_FLG_TMO_ATM_DEP_SOL,
                                      CAN_TKT_TMO_ATM_DEP_SOL,
                                      CAN_FLG_TMO_ATM_DEP_DOL,
                                      CAN_TKT_TMO_ATM_DEP_DOL,
                                      CAN_FLG_TMO_ATM_TRF,
                                      CAN_TKT_TMO_ATM_TRF,
                                      CAN_FLG_TMO_ATM_TRF_SOL,
                                      CAN_TKT_TMO_ATM_TRF_SOL,
                                      CAN_FLG_TMO_ATM_TRF_DOL,
                                      CAN_TKT_TMO_ATM_TRF_DOL,
                                      CAN_FLG_TMO_ATM_ADS,
                                      CAN_TKT_TMO_ATM_ADS,
                                      CAN_FLG_TMO_ATM_DIS,
                                      CAN_TKT_TMO_ATM_DIS,
                                      CAN_FLG_TMO_ATM_PAG_TCR,
                                      CAN_TKT_TMO_ATM_PAG_TCR,
                                      CAN_FLG_TMO_ATM_PAG_SRV,
                                      CAN_TKT_TMO_ATM_PAG_SRV,
                                      CAN_FLG_TNM_ATM_CSLT,
                                      CAN_FLG_TNM_ATM_ADMN,
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
    columnas_a_renombrar = {'CODUNICOCLI_LAST':'CODUNICOCLI_LAST_CIC','CODINTERNOCOMPUTACIONAL_LAST':'CODINTERNOCOMPUTACIONAL_LAST_CIC'}
    dfInfo12MesesStep2Cic = funciones.rename_columns(dfInfo12MesesStep1Cic,columnas_a_renombrar)
    #El resultado lo colocamos en una vista
    dfInfo12MesesStep2Cic.createOrReplaceTempView('temp1_cic')

    dfInfo12MesesStep3Cic = spark.sql("""
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
    
    dfInfo12MesesStep2Cuc = dfInfo12MesesStep3Cic.join(dfClienteCuc.drop('CODMES'), on = 'CODUNICOCLI_LAST', how = 'left')
    
    #Renombramos la columna CODINTERNOCOMPUTACIONAL_LAST dataframe y lo almacenamos en un nuevo dataframe
    columnas_a_renombrar = {'CODINTERNOCOMPUTACIONAL_LAST':'CODINTERNOCOMPUTACIONAL_LAST_CUC'}
    dfInfo12MesesStep3Cuc = funciones.rename_columns(dfInfo12MesesStep2Cuc,columnas_a_renombrar)
    
    #El resultado lo colocamos en una vista
    dfInfo12MesesStep3Cuc.createOrReplaceTempView('temp2_cuc')
    
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

    
    #Escribimos los nombres de las columnas del dataframe en may�sculas	                           
    dfInfo12MesesStep01 = dfInfo12Meses.select([F.col(x).alias(x.upper()) for x in dfInfo12Meses.columns])
    
    #Escribimos el resultado en disco duro para forzar la ejecuci�n del DAG SPARK
    tmp_table = f"{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpetaClientePrimeraTranspuesta}_tmp"
    ruta_salida = PRM_CARPETA_RAIZ_DE_PROYECTO+"/temp/"+carpetaClientePrimeraTranspuesta
    dfInfo12MesesStep01.write.format(funciones.CONS_FORMATO_DE_ESCRITURA_EN_DISCO).mode(funciones.CONS_MODO_DE_ESCRITURA).saveAsTable(tmp_table)
    
    #Leemos el resultado calculado
    dfInfo12MesesFinal = spark.read.table(f"{tmp_table}")

    return dfInfo12MesesFinal

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
def agruparInformacionMesAnalisis(dfInfo12Meses,codMes,PRM_TABLA_SEGUNDATRANSPUESTA_TMP):
    
    #Creamos un dataframe temportal en memoria
    dfInfo12Meses.createOrReplaceTempView('dfViewInfoMesesSql')

    #Almacena los nombres de las columnas de la tabla dfInfo12Meses
    nameColumn = []
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo MONTO
    colum_mto = []
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo CANTIDAD
    colum_ctd = []
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo FLAG
    columFlg = []
    
    #Leemos las columnas del dataframe dfInfo12Meses y los asignamos a una lista
    nameColumn = dfInfo12Meses.schema.names
    
    #Nos quedamos solo con las columnas de tipo Monto, Cantidad y Flag
    for nColumns in nameColumn:
            if 'CAN' in nColumns:
                if  'MTO' in nColumns or 'TKT' in nColumns:
                    colum_mto.append(nColumns)
                if  'CTD' in nColumns:
                    colum_ctd.append(nColumns)        
                if  'FLG' in nColumns:
                    columFlg.append(nColumns) 
                    
    #Almacena las columnas tipo MONTO de la tabla dfInfo12Meses 
    colsToExpandMonto = colum_mto
    
    #Almacena las columnas tipo CTD de la tabla dfInfo12Meses
    colsToExpandCantidad = colum_ctd
    
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
    mesFrecuenciasMesesCTD = [-2,-5,-8,-11]
   
    #La funcion chain() solo es para trasformner las columnas en horizontal el resultados de cada una de las funciones ,diaria q es como concatenar
    aggsIterFinal = itertools.chain(aggsIter_Part1Avg, aggsIter_Part1Avg_PRM_P6M, aggsIter_Part1Avg_PRM_P3M, aggsIter_Part1Avg_Prev_Mes,
                                   aggsIter_Part1Max, aggsIter_Part1Min, aggsIter_Part2Avg, aggsIter_Part2Max, aggsIter_Part2Min,
                                   aggsIter_Part3Avg, aggsIter_Part3Max, aggsIter_Part3Sum)
    
    #Funcion core que realiza el procesamiento del calculo de las nuevas columnas generadas dinamicamente segun sea el tipo de variable y plantilla
    funciones.windowAggregate(dfInfo12Meses,
                              partitionCols = funciones.CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP,
                              orderCol = 'CODMES',
                              aggregations = aggsIterFinal,
                              codMes = codMes,
                              tablaTmp = PRM_TABLA_SEGUNDATRANSPUESTA_TMP,
                              flag = 1,
                              PRM_CARPETA_RAIZ_DE_PROYECTO = PRM_CARPETA_RAIZ_DE_PROYECTO
                              )
    
    #Leemos el dataframe de disco a memoria
    dfMatrizVarTransaccionCajero = spark.read.format(funciones.CONS_FORMATO_DE_ESCRITURA_EN_DISCO).load(PRM_CARPETA_RAIZ_DE_PROYECTO+"/temp/"+PRM_TABLA_SEGUNDATRANSPUESTA_TMP+"/CODMES="+str(codMes))
    
    #Leemos las columnas del dataframe dfMatrizVarTransaccionCajero y los asignamos a una lista
    columnNamesDf = dfMatrizVarTransaccionCajero.schema.names
    
    #Extraemos las columnas de tipo Flag
    colsFlg = funciones.extraccionColumnasFlg(columnNamesDf,"_FLG_")
    
    #Separar las columnas por comas
    columnFlag = colsFlg.replace("'","").split(',')
    
    #Se concatena una cadena con las columnas tipo flag extraidas previamente
    colsFlagFin = "'CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL','"+colsFlg+"'"
    
    #Se crea el nuevo dataframe dfFlag con los nuevos campos tipo flag
    dfFlag = dfMatrizVarTransaccionCajero.select(eval("[{templatebody}]".replace("{templatebody}", colsFlagFin)))
    
    #Leemos las columnas del dataframe dfFlag y los asignamos a una lista
    dfFlagSchema = dfFlag.schema.names
    
    #Casteamos de string a int las columnas de la lista dfFlag
    columnCastFlag = funciones.casteoColumns(dfFlagSchema,'int')
    
    #Se crea el nuevo dataframe dfFlagFin con los campos tipo flag casteados
    dfFlagFin = dfMatrizVarTransaccionCajero.select(eval("[{templatebody}]".replace("{templatebody}", columnCastFlag)))
    
    #Se crea el nuevo dataframe dfColumnSinFlg sin los campos de la cadena ColumnsFLag
    dfColumnSinFlg = dfMatrizVarTransaccionCajero.select([c for c in dfMatrizVarTransaccionCajero.columns if c not in columnFlag])
    
    #Leemos las columnas del dataframe dfColumnSinFlg y las asignamos a una lista
    columnSinFlag = dfColumnSinFlg.columns
    
    #Casteamos de string a double las columnas de la lista columnSinFlag
    columnCastSinFlag = funciones.casteoColumns(columnSinFlag, 'double')
    
    #Se crea el nuevo dataframe dfSinFLagFin con los campos tipo excepto los tipo flag casteados
    dfSinFLagFin = dfMatrizVarTransaccionCajero.select(eval("[{templatebody}]".replace("{templatebody}", columnCastSinFlag)))
    
    #Cruzamos los dataframes dfFlagFin y dfSinFLagFin por las columnas CODMESANALISIS,CODMES,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfMatrizVarTransaccionCajeroJoin = dfFlagFin.join(dfSinFLagFin,['CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL'])
    
    #Se calcula las frecuencias para las variables de tipo cantidad con un retroceso de 3,6,9,12 meses
    dfFrecuencia = funciones.calcularFrecuencias(mesFrecuenciasMesesCTD, colsToExpandCantidad,spark)
    
    #Se calcula la recencia para las variables de tipo cantidad
    dfRecencia = funciones.calculoRecencia(colsToExpandCantidad,spark)
    
    #Cruzamos los dataframes dfRecencia y dfFrecuencia por las columnas CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfRecenciaFrecuencia = dfRecencia.join(dfFrecuencia,funciones.CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP)
    
    #Cruzamos los dataframes dfMatrizVarTransaccionCajeroJoin y dfRecenciaFrecuencia por las columnas CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfMatrizVarTransaccionCajero2 = dfMatrizVarTransaccionCajeroJoin.join(dfRecenciaFrecuencia, funciones.CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP)
    
    #Leemos las columnas del dataframe dfMatrizVarTransaccionCajero2 y los asignamos a una lista   
    dfMatrizVarTransaccionCajeroColumns = dfMatrizVarTransaccionCajero2.schema.names
    
    #Extraemos las columnas promedio ultimos 6 meses
    colsMont_PRM_U6M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroColumns, "_PRM_U6M")
    
    #Extraemos las columnas promedio primeros 6 meses
    colsMont_PRM_P6M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroColumns, "_PRM_P6M")
    
    #Extraemos las columnas promedio primeros 3 meses
    colsMont_PRM_P3M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroColumns, "_PRM_P3M")
    
    #Extraemos las columnas promedio ultimos 3 meses
    colsMont_PRM_U3M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroColumns, "_PRM_U3M")
    
    #Extraemos la columna promedio del penultimo mes
    colsMont_P1M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroColumns, "_P1M")
    
    #Extraemos la columna promedio del ultimo mes
    colsMont_U1M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroColumns, "_U1M")
    
    #Se concatena una cadena con las columnas tipo promedio extraidas previamente
    colsMont = "CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS,"+ colsMont_PRM_U6M +","+ colsMont_PRM_P6M +","+ colsMont_PRM_P3M +","+ colsMont_PRM_U3M +","+ colsMont_P1M +","+ colsMont_U1M
    
    #El dataframe dfMatrizVarTransaccionCajero2 solo con las columnas promedios 
    dfMatrizVarTransaccionCajeroMont = dfMatrizVarTransaccionCajero2.select(colsMont.split(','))
    
    #Se calcula el crecimiento 6,3 y 1 para las variables tipo Monto
    colsCrecimiento = {}
    for colName in colsToExpandMonto:
      colsCrecimiento[f"{colName}_G6M"] = func.round(col(colName + "_PRM_U6M")/col(colName + "_PRM_P6M"),8).alias(f"{colName}_G6M")
      colsCrecimiento[f"{colName}_G3M"] = func.round(col(colName + "_PRM_U3M")/col(colName + "_PRM_P3M"),8).alias(f"{colName}_G3M")
      colsCrecimiento[f"{colName}_G1M"] = func.round(col(colName + "_U1M")/col(colName + "_P1M"),8).alias(f"{colName}_G1M")
      
    dfMatrizVarTransaccionCajeroMontStep01 = dfMatrizVarTransaccionCajeroMont.select([colsCrecimiento.get(col_name, col_name) for col_name in list(set(dfMatrizVarTransaccionCajeroMont.columns+list(colsCrecimiento.keys())))])
      
    #Leemos las columnas del dataframe dfMatrizVarTransaccionCajeroMontStep01 y las asignamos a una lista
    dfMatrizVarTransaccionCajeroMontColumns = dfMatrizVarTransaccionCajeroMontStep01.schema.names
    
    #Extraemos las columnas crecimiento ultimos 6 meses
    colsMont_PRM_G6M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroMontColumns, "_G6M")
    
    #Extraemos las columnas crecimiento ultimos 3 meses
    colsMont_PRM_G3M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroMontColumns,  "_G3M")
    
    #Extraemos las columnas crecimiento ultimos 1 meses
    colsMont_PRM_G1M = funciones.extraccionColumnas(dfMatrizVarTransaccionCajeroMontColumns, "_G1M")
    
    #Se concatena una cadena con las columnas tipo crecimiento extraidas previamente
    colsMontFin = "CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS,"+ colsMont_PRM_G6M +","+ colsMont_PRM_G3M +","+ colsMont_PRM_G1M
    
    #El dataframe dfMatrizVarTransaccionCajeroMontStep01 solo con las columnas crecimiento
    dfMatrizVarTransaccionCajeroCrecimiento = dfMatrizVarTransaccionCajeroMontStep01.select(colsMontFin.split(','))
    
    #Cruzamos los dataframes dfMatrizVarTransaccionCajero2 y dfMatrizVarTransaccionCajeroCrecimiento por las columnas CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS
    dfMatrizVarTransaccionCajero3 = dfMatrizVarTransaccionCajero2.join(dfMatrizVarTransaccionCajeroCrecimiento, ['CODUNICOCLI','CODINTERNOCOMPUTACIONAL','CODMESANALISIS'])
    
    #Casteamos la columna CODMESANALISIS a integer del dataframe dfMatrizVarTransaccionCajero3
    columnas_a_castear =  ["CODMESANALISIS", "CODMES"]
    dfMatrizVarTransaccionCajero4 = dfMatrizVarTransaccionCajero3.select(*[col(column_name).cast("int").alias(column_name) if column_name in columnas_a_castear else F.col(column_name) for column_name in dfMatrizVarTransaccionCajero3.columns])
    
    #Retorna el dataframe final dfMatrizVarTransaccionCajero4 luego de pasar por el metodo logicaPostAgrupacionInformacionMesAnalisis
    dfMatrizVarTransaccionCajero5 = logicaPostAgrupacionInformacionMesAnalisis(dfMatrizVarTransaccionCajero4)
    
    #Retorna el dataframe final dfMatrizVarTransaccionCajero6 al main del proceso
    return dfMatrizVarTransaccionCajero5

# COMMAND ---------- 
###
 # Actualizaci�n de las variables tipo MINIMO previamente calculadas
 # 
 # @param dfMatrizVarTransaccionCajero {Dataframe Spark} Dataframe sin los campos Minimos actualizados.
 # @return {Dataframe Spark} Dataframe con los campos Minimos actualizados.
 ##
def logicaPostAgrupacionInformacionMesAnalisis(dfMatrizVarTransaccionCajero): 
    
    #Almacena los nombres de las columnas de la tabla dfMatrizVarTransaccionCajero
    nameColumn = []
    #Declaramos la lista que almacenara las columnas generadas en la 2da transpuesta tipo MONTO (FREQ Y MIN)
    columFreq = []
    #Declaramos la lista que almacenara las columnas generadas en la 2da transpuesta tipo FLG (SUM)
    columFlg = []
    
    #Variables temporales
    cVariableSufijo = ''
    cVariableSufijoInt = 0
    cVariableSufijoFin = ''
    cVariableSufijoInicio = ''
    cVariableFinal = ''
    
    #Leemos las columnas del dataframe dfMatrizVarTransaccionCajero y las asignamos a una lista
    nameColumn = dfMatrizVarTransaccionCajero.schema.names
    
    #Lee las columnas generadas en la 2da transpuesta tipo CTD (FREQ Y MIN)
    for nColumns in nameColumn:
        if 'FR' in nColumns:
            columFreq.append(nColumns)
        if 'CTD' in nColumns:
            if 'MIN' in nColumns:
                columFreq.append(nColumns)
    
    #Se asigna el valor de la lista columFreq a la lista colsToExpandCantidad2daT
    colsToExpandCantidad2daT = columFreq
  

    colsCTD = {}
    #Se actualizan los minimos para las variables tipo CTD calculas en la 2da transpuesta
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
        if not (variableFinal == ''): 
            colsCTD[cVariableFinal] = when(col(variableFinal) < cVariableSufijoInt,0).otherwise(col(cVariableFinal)).alias(cVariableFinal)

    dfMatrizVarTransaccionCajeroStep01 = dfMatrizVarTransaccionCajero.select([colsCTD.get(col_name, col_name) for col_name in list(set(dfMatrizVarTransaccionCajero.columns + list(colsCTD.keys())))])
        
    #Lee las columnas generadas en la 2da transpuesta tipo FLG (SUM)
    for nColumns in nameColumn:
            if 'CAN' in nColumns:      
                if  'FLG' in nColumns:
                    if  'MAX' not in nColumns:
                        if  'X' in nColumns:
                            columFlg.append(nColumns)
    
    #Se asigna el valor de la lista columFlg a la lista colsToExpandFlag               
    colsToExpandFlag = columFlg
    
    colsSum_FLG = {}
    #Se actualizan los sum_uXm para las variables tipo FLG calculas en la 2da transpuesta
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
        if not (variableFinal == ''): 
            colsSum_FLG[variableFinal] = when(col(variableFinal) == cVariableSufijoInt,1).otherwise(0).alias(variableFinal)

    dfMatrizVarTransaccionCajeroStep02 = dfMatrizVarTransaccionCajeroStep01.select([colsSum_FLG.get(col_name,col_name) for col_name in list(set(dfMatrizVarTransaccionCajeroStep01.columns +list(colsSum_FLG.keys())))])
    
    #Se crea la columna FECACTUALIZACIONREGISTRO y FECRUTINA al dataframe dfMatrizVarTransaccionCajeroStep02
    
    fechaActualizacionRegistro = funciones.obtenerFechaActual()
    
    dfMatrizVarTransaccionCajeroStep03 = dfMatrizVarTransaccionCajeroStep02.select([col(x) for x in dfMatrizVarTransaccionCajeroStep02.columns] + 
                                              [lit(fechaActualizacionRegistro).alias("FECACTUALIZACIONREGISTRO"),
                                              lit(PRM_FECHA_RUTINA).alias("FECRUTINA").cast("date")
                                              ]
                                              )
    
    #Se escribe el dataframe final dfMatrizVarTransaccionCajeroStep03 en HDFS, el cual se refleja en una tabla external particionada con el nombre del valor de la variable PRM_TABLA_SEGUNDATRANSPUESTA
    #funciones.writeToHivePartitioned(spark, dfMatrizVarTransaccionCajeroStep03, tableName = PRM_TABLA_SEGUNDATRANSPUESTA, partitionField = 'CODMES', dbName = PRM_ESQUEMA_TABLA)
    columnas = dfMatrizVarTransaccionCajeroStep03.columns
    dfMatrizVarTransaccionCajeroStep04 = dfMatrizVarTransaccionCajeroStep03.select(*columnas + [F.col('CODUNICOCLI').alias('CODCLAVEUNICOCLI')] + [F.sha2(concat(lit('0001'),trim(col('CODINTERNOCOMPUTACIONAL'))),512).alias('CODCLAVEPARTYCLI')])
    
    input_df = dfMatrizVarTransaccionCajeroStep04.select(
        col('codunicocli').cast('string').alias('codunicocli'),
        col('codclaveunicocli').cast('varchar(128)').alias('codclaveunicocli'),
        col('codclavepartycli').cast('varchar(128)').alias('codclavepartycli'),
        col('codinternocomputacional').cast('string').alias('codinternocomputacional'),
        col('codmesanalisis').cast('int').alias('codmesanalisis'),
        col('can_flg_tmo_atm_u1m').cast('int').alias('can_flg_tmo_atm_u1m'),
        col('can_flg_tnm_atm_u1m').cast('int').alias('can_flg_tnm_atm_u1m'),
        col('can_flg_tmo_atm_sol_u1m').cast('int').alias('can_flg_tmo_atm_sol_u1m'),
        col('can_flg_tmo_atm_dol_u1m').cast('int').alias('can_flg_tmo_atm_dol_u1m'),
        col('can_flg_tmo_atm_ret_u1m').cast('int').alias('can_flg_tmo_atm_ret_u1m'),
        col('can_flg_tmo_atm_ret_sol_u1m').cast('int').alias('can_flg_tmo_atm_ret_sol_u1m'),
        col('can_flg_tmo_atm_ret_dol_u1m').cast('int').alias('can_flg_tmo_atm_ret_dol_u1m'),
        col('can_flg_tmo_atm_dep_u1m').cast('int').alias('can_flg_tmo_atm_dep_u1m'),
        col('can_flg_tmo_atm_dep_sol_u1m').cast('int').alias('can_flg_tmo_atm_dep_sol_u1m'),
        col('can_flg_tmo_atm_dep_dol_u1m').cast('int').alias('can_flg_tmo_atm_dep_dol_u1m'),
        col('can_flg_tmo_atm_trf_u1m').cast('int').alias('can_flg_tmo_atm_trf_u1m'),
        col('can_flg_tmo_atm_trf_sol_u1m').cast('int').alias('can_flg_tmo_atm_trf_sol_u1m'),
        col('can_flg_tmo_atm_trf_dol_u1m').cast('int').alias('can_flg_tmo_atm_trf_dol_u1m'),
        col('can_flg_tmo_atm_ads_u1m').cast('int').alias('can_flg_tmo_atm_ads_u1m'),
        col('can_flg_tmo_atm_dis_u1m').cast('int').alias('can_flg_tmo_atm_dis_u1m'),
        col('can_flg_tmo_atm_pag_tcr_u1m').cast('int').alias('can_flg_tmo_atm_pag_tcr_u1m'),
        col('can_flg_tmo_atm_pag_srv_u1m').cast('int').alias('can_flg_tmo_atm_pag_srv_u1m'),
        col('can_flg_tnm_atm_cslt_u1m').cast('int').alias('can_flg_tnm_atm_cslt_u1m'),
        col('can_flg_tnm_atm_admn_u1m').cast('int').alias('can_flg_tnm_atm_admn_u1m'),
        col('can_flg_tmo_atm_max_u3m').cast('int').alias('can_flg_tmo_atm_max_u3m'),
        col('can_flg_tmo_atm_max_u6m').cast('int').alias('can_flg_tmo_atm_max_u6m'),
        col('can_flg_tmo_atm_max_u9m').cast('int').alias('can_flg_tmo_atm_max_u9m'),
        col('can_flg_tmo_atm_max_u12').cast('int').alias('can_flg_tmo_atm_max_u12'),
        col('can_flg_tnm_atm_max_u3m').cast('int').alias('can_flg_tnm_atm_max_u3m'),
        col('can_flg_tnm_atm_max_u6m').cast('int').alias('can_flg_tnm_atm_max_u6m'),
        col('can_flg_tnm_atm_max_u9m').cast('int').alias('can_flg_tnm_atm_max_u9m'),
        col('can_flg_tnm_atm_max_u12').cast('int').alias('can_flg_tnm_atm_max_u12'),
        col('can_flg_tmo_atm_sol_max_u3m').cast('int').alias('can_flg_tmo_atm_sol_max_u3m'),
        col('can_flg_tmo_atm_sol_max_u6m').cast('int').alias('can_flg_tmo_atm_sol_max_u6m'),
        col('can_flg_tmo_atm_sol_max_u9m').cast('int').alias('can_flg_tmo_atm_sol_max_u9m'),
        col('can_flg_tmo_atm_sol_max_u12').cast('int').alias('can_flg_tmo_atm_sol_max_u12'),
        col('can_flg_tmo_atm_dol_max_u3m').cast('int').alias('can_flg_tmo_atm_dol_max_u3m'),
        col('can_flg_tmo_atm_dol_max_u6m').cast('int').alias('can_flg_tmo_atm_dol_max_u6m'),
        col('can_flg_tmo_atm_dol_max_u9m').cast('int').alias('can_flg_tmo_atm_dol_max_u9m'),
        col('can_flg_tmo_atm_dol_max_u12').cast('int').alias('can_flg_tmo_atm_dol_max_u12'),
        col('can_flg_tmo_atm_ret_max_u3m').cast('int').alias('can_flg_tmo_atm_ret_max_u3m'),
        col('can_flg_tmo_atm_ret_max_u6m').cast('int').alias('can_flg_tmo_atm_ret_max_u6m'),
        col('can_flg_tmo_atm_ret_max_u9m').cast('int').alias('can_flg_tmo_atm_ret_max_u9m'),
        col('can_flg_tmo_atm_ret_max_u12').cast('int').alias('can_flg_tmo_atm_ret_max_u12'),
        col('can_flg_tmo_atm_ret_sol_max_u3m').cast('int').alias('can_flg_tmo_atm_ret_sol_max_u3m'),
        col('can_flg_tmo_atm_ret_sol_max_u6m').cast('int').alias('can_flg_tmo_atm_ret_sol_max_u6m'),
        col('can_flg_tmo_atm_ret_sol_max_u9m').cast('int').alias('can_flg_tmo_atm_ret_sol_max_u9m'),
        col('can_flg_tmo_atm_ret_sol_max_u12').cast('int').alias('can_flg_tmo_atm_ret_sol_max_u12'),
        col('can_flg_tmo_atm_ret_dol_max_u3m').cast('int').alias('can_flg_tmo_atm_ret_dol_max_u3m'),
        col('can_flg_tmo_atm_ret_dol_max_u6m').cast('int').alias('can_flg_tmo_atm_ret_dol_max_u6m'),
        col('can_flg_tmo_atm_ret_dol_max_u9m').cast('int').alias('can_flg_tmo_atm_ret_dol_max_u9m'),
        col('can_flg_tmo_atm_ret_dol_max_u12').cast('int').alias('can_flg_tmo_atm_ret_dol_max_u12'),
        col('can_flg_tmo_atm_dep_max_u3m').cast('int').alias('can_flg_tmo_atm_dep_max_u3m'),
        col('can_flg_tmo_atm_dep_max_u6m').cast('int').alias('can_flg_tmo_atm_dep_max_u6m'),
        col('can_flg_tmo_atm_dep_max_u9m').cast('int').alias('can_flg_tmo_atm_dep_max_u9m'),
        col('can_flg_tmo_atm_dep_max_u12').cast('int').alias('can_flg_tmo_atm_dep_max_u12'),
        col('can_flg_tmo_atm_dep_sol_max_u3m').cast('int').alias('can_flg_tmo_atm_dep_sol_max_u3m'),
        col('can_flg_tmo_atm_dep_sol_max_u6m').cast('int').alias('can_flg_tmo_atm_dep_sol_max_u6m'),
        col('can_flg_tmo_atm_dep_sol_max_u9m').cast('int').alias('can_flg_tmo_atm_dep_sol_max_u9m'),
        col('can_flg_tmo_atm_dep_sol_max_u12').cast('int').alias('can_flg_tmo_atm_dep_sol_max_u12'),
        col('can_flg_tmo_atm_dep_dol_max_u3m').cast('int').alias('can_flg_tmo_atm_dep_dol_max_u3m'),
        col('can_flg_tmo_atm_dep_dol_max_u6m').cast('int').alias('can_flg_tmo_atm_dep_dol_max_u6m'),
        col('can_flg_tmo_atm_dep_dol_max_u9m').cast('int').alias('can_flg_tmo_atm_dep_dol_max_u9m'),
        col('can_flg_tmo_atm_dep_dol_max_u12').cast('int').alias('can_flg_tmo_atm_dep_dol_max_u12'),
        col('can_flg_tmo_atm_trf_max_u3m').cast('int').alias('can_flg_tmo_atm_trf_max_u3m'),
        col('can_flg_tmo_atm_trf_max_u6m').cast('int').alias('can_flg_tmo_atm_trf_max_u6m'),
        col('can_flg_tmo_atm_trf_max_u9m').cast('int').alias('can_flg_tmo_atm_trf_max_u9m'),
        col('can_flg_tmo_atm_trf_max_u12').cast('int').alias('can_flg_tmo_atm_trf_max_u12'),
        col('can_flg_tmo_atm_trf_sol_max_u3m').cast('int').alias('can_flg_tmo_atm_trf_sol_max_u3m'),
        col('can_flg_tmo_atm_trf_sol_max_u6m').cast('int').alias('can_flg_tmo_atm_trf_sol_max_u6m'),
        col('can_flg_tmo_atm_trf_sol_max_u9m').cast('int').alias('can_flg_tmo_atm_trf_sol_max_u9m'),
        col('can_flg_tmo_atm_trf_sol_max_u12').cast('int').alias('can_flg_tmo_atm_trf_sol_max_u12'),
        col('can_flg_tmo_atm_trf_dol_max_u3m').cast('int').alias('can_flg_tmo_atm_trf_dol_max_u3m'),
        col('can_flg_tmo_atm_trf_dol_max_u6m').cast('int').alias('can_flg_tmo_atm_trf_dol_max_u6m'),
        col('can_flg_tmo_atm_trf_dol_max_u9m').cast('int').alias('can_flg_tmo_atm_trf_dol_max_u9m'),
        col('can_flg_tmo_atm_trf_dol_max_u12').cast('int').alias('can_flg_tmo_atm_trf_dol_max_u12'),
        col('can_flg_tmo_atm_ads_max_u3m').cast('int').alias('can_flg_tmo_atm_ads_max_u3m'),
        col('can_flg_tmo_atm_ads_max_u6m').cast('int').alias('can_flg_tmo_atm_ads_max_u6m'),
        col('can_flg_tmo_atm_ads_max_u9m').cast('int').alias('can_flg_tmo_atm_ads_max_u9m'),
        col('can_flg_tmo_atm_ads_max_u12').cast('int').alias('can_flg_tmo_atm_ads_max_u12'),
        col('can_flg_tmo_atm_dis_max_u3m').cast('int').alias('can_flg_tmo_atm_dis_max_u3m'),
        col('can_flg_tmo_atm_dis_max_u6m').cast('int').alias('can_flg_tmo_atm_dis_max_u6m'),
        col('can_flg_tmo_atm_dis_max_u9m').cast('int').alias('can_flg_tmo_atm_dis_max_u9m'),
        col('can_flg_tmo_atm_dis_max_u12').cast('int').alias('can_flg_tmo_atm_dis_max_u12'),
        col('can_flg_tmo_atm_pag_tcr_max_u3m').cast('int').alias('can_flg_tmo_atm_pag_tcr_max_u3m'),
        col('can_flg_tmo_atm_pag_tcr_max_u6m').cast('int').alias('can_flg_tmo_atm_pag_tcr_max_u6m'),
        col('can_flg_tmo_atm_pag_tcr_max_u9m').cast('int').alias('can_flg_tmo_atm_pag_tcr_max_u9m'),
        col('can_flg_tmo_atm_pag_tcr_max_u12').cast('int').alias('can_flg_tmo_atm_pag_tcr_max_u12'),
        col('can_flg_tmo_atm_pag_srv_max_u3m').cast('int').alias('can_flg_tmo_atm_pag_srv_max_u3m'),
        col('can_flg_tmo_atm_pag_srv_max_u6m').cast('int').alias('can_flg_tmo_atm_pag_srv_max_u6m'),
        col('can_flg_tmo_atm_pag_srv_max_u9m').cast('int').alias('can_flg_tmo_atm_pag_srv_max_u9m'),
        col('can_flg_tmo_atm_pag_srv_max_u12').cast('int').alias('can_flg_tmo_atm_pag_srv_max_u12'),
        col('can_flg_tnm_atm_cslt_max_u3m').cast('int').alias('can_flg_tnm_atm_cslt_max_u3m'),
        col('can_flg_tnm_atm_cslt_max_u6m').cast('int').alias('can_flg_tnm_atm_cslt_max_u6m'),
        col('can_flg_tnm_atm_cslt_max_u9m').cast('int').alias('can_flg_tnm_atm_cslt_max_u9m'),
        col('can_flg_tnm_atm_cslt_max_u12').cast('int').alias('can_flg_tnm_atm_cslt_max_u12'),
        col('can_flg_tnm_atm_admn_max_u3m').cast('int').alias('can_flg_tnm_atm_admn_max_u3m'),
        col('can_flg_tnm_atm_admn_max_u6m').cast('int').alias('can_flg_tnm_atm_admn_max_u6m'),
        col('can_flg_tnm_atm_admn_max_u9m').cast('int').alias('can_flg_tnm_atm_admn_max_u9m'),
        col('can_flg_tnm_atm_admn_max_u12').cast('int').alias('can_flg_tnm_atm_admn_max_u12'),
        col('can_flg_tmo_atm_x3m_u3m').cast('int').alias('can_flg_tmo_atm_x3m_u3m'),
        col('can_flg_tmo_atm_x6m_u6m').cast('int').alias('can_flg_tmo_atm_x6m_u6m'),
        col('can_flg_tmo_atm_x9m_u9m').cast('int').alias('can_flg_tmo_atm_x9m_u9m'),
        col('can_flg_tmo_atm_x12_u12').cast('int').alias('can_flg_tmo_atm_x12_u12'),
        col('can_flg_tnm_atm_x3m_u3m').cast('int').alias('can_flg_tnm_atm_x3m_u3m'),
        col('can_flg_tnm_atm_x6m_u6m').cast('int').alias('can_flg_tnm_atm_x6m_u6m'),
        col('can_flg_tnm_atm_x9m_u9m').cast('int').alias('can_flg_tnm_atm_x9m_u9m'),
        col('can_flg_tnm_atm_x12_u12').cast('int').alias('can_flg_tnm_atm_x12_u12'),
        col('can_flg_tmo_atm_sol_x3m_u3m').cast('int').alias('can_flg_tmo_atm_sol_x3m_u3m'),
        col('can_flg_tmo_atm_sol_x6m_u6m').cast('int').alias('can_flg_tmo_atm_sol_x6m_u6m'),
        col('can_flg_tmo_atm_sol_x9m_u9m').cast('int').alias('can_flg_tmo_atm_sol_x9m_u9m'),
        col('can_flg_tmo_atm_sol_x12_u12').cast('int').alias('can_flg_tmo_atm_sol_x12_u12'),
        col('can_flg_tmo_atm_dol_x3m_u3m').cast('int').alias('can_flg_tmo_atm_dol_x3m_u3m'),
        col('can_flg_tmo_atm_dol_x6m_u6m').cast('int').alias('can_flg_tmo_atm_dol_x6m_u6m'),
        col('can_flg_tmo_atm_dol_x9m_u9m').cast('int').alias('can_flg_tmo_atm_dol_x9m_u9m'),
        col('can_flg_tmo_atm_dol_x12_u12').cast('int').alias('can_flg_tmo_atm_dol_x12_u12'),
        col('can_flg_tmo_atm_ret_x3m_u3m').cast('int').alias('can_flg_tmo_atm_ret_x3m_u3m'),
        col('can_flg_tmo_atm_ret_x6m_u6m').cast('int').alias('can_flg_tmo_atm_ret_x6m_u6m'),
        col('can_flg_tmo_atm_ret_x9m_u9m').cast('int').alias('can_flg_tmo_atm_ret_x9m_u9m'),
        col('can_flg_tmo_atm_ret_x12_u12').cast('int').alias('can_flg_tmo_atm_ret_x12_u12'),
        col('can_flg_tmo_atm_ret_sol_x3m_u3m').cast('int').alias('can_flg_tmo_atm_ret_sol_x3m_u3m'),
        col('can_flg_tmo_atm_ret_sol_x6m_u6m').cast('int').alias('can_flg_tmo_atm_ret_sol_x6m_u6m'),
        col('can_flg_tmo_atm_ret_sol_x9m_u9m').cast('int').alias('can_flg_tmo_atm_ret_sol_x9m_u9m'),
        col('can_flg_tmo_atm_ret_sol_x12_u12').cast('int').alias('can_flg_tmo_atm_ret_sol_x12_u12'),
        col('can_flg_tmo_atm_ret_dol_x3m_u3m').cast('int').alias('can_flg_tmo_atm_ret_dol_x3m_u3m'),
        col('can_flg_tmo_atm_ret_dol_x6m_u6m').cast('int').alias('can_flg_tmo_atm_ret_dol_x6m_u6m'),
        col('can_flg_tmo_atm_ret_dol_x9m_u9m').cast('int').alias('can_flg_tmo_atm_ret_dol_x9m_u9m'),
        col('can_flg_tmo_atm_ret_dol_x12_u12').cast('int').alias('can_flg_tmo_atm_ret_dol_x12_u12'),
        col('can_flg_tmo_atm_dep_x3m_u3m').cast('int').alias('can_flg_tmo_atm_dep_x3m_u3m'),
        col('can_flg_tmo_atm_dep_x6m_u6m').cast('int').alias('can_flg_tmo_atm_dep_x6m_u6m'),
        col('can_flg_tmo_atm_dep_x9m_u9m').cast('int').alias('can_flg_tmo_atm_dep_x9m_u9m'),
        col('can_flg_tmo_atm_dep_x12_u12').cast('int').alias('can_flg_tmo_atm_dep_x12_u12'),
        col('can_flg_tmo_atm_dep_sol_x3m_u3m').cast('int').alias('can_flg_tmo_atm_dep_sol_x3m_u3m'),
        col('can_flg_tmo_atm_dep_sol_x6m_u6m').cast('int').alias('can_flg_tmo_atm_dep_sol_x6m_u6m'),
        col('can_flg_tmo_atm_dep_sol_x9m_u9m').cast('int').alias('can_flg_tmo_atm_dep_sol_x9m_u9m'),
        col('can_flg_tmo_atm_dep_sol_x12_u12').cast('int').alias('can_flg_tmo_atm_dep_sol_x12_u12'),
        col('can_flg_tmo_atm_dep_dol_x3m_u3m').cast('int').alias('can_flg_tmo_atm_dep_dol_x3m_u3m'),
        col('can_flg_tmo_atm_dep_dol_x6m_u6m').cast('int').alias('can_flg_tmo_atm_dep_dol_x6m_u6m'),
        col('can_flg_tmo_atm_dep_dol_x9m_u9m').cast('int').alias('can_flg_tmo_atm_dep_dol_x9m_u9m'),
        col('can_flg_tmo_atm_dep_dol_x12_u12').cast('int').alias('can_flg_tmo_atm_dep_dol_x12_u12'),
        col('can_flg_tmo_atm_trf_x3m_u3m').cast('int').alias('can_flg_tmo_atm_trf_x3m_u3m'),
        col('can_flg_tmo_atm_trf_x6m_u6m').cast('int').alias('can_flg_tmo_atm_trf_x6m_u6m'),
        col('can_flg_tmo_atm_trf_x9m_u9m').cast('int').alias('can_flg_tmo_atm_trf_x9m_u9m'),
        col('can_flg_tmo_atm_trf_x12_u12').cast('int').alias('can_flg_tmo_atm_trf_x12_u12'),
        col('can_flg_tmo_atm_trf_sol_x3m_u3m').cast('int').alias('can_flg_tmo_atm_trf_sol_x3m_u3m'),
        col('can_flg_tmo_atm_trf_sol_x6m_u6m').cast('int').alias('can_flg_tmo_atm_trf_sol_x6m_u6m'),
        col('can_flg_tmo_atm_trf_sol_x9m_u9m').cast('int').alias('can_flg_tmo_atm_trf_sol_x9m_u9m'),
        col('can_flg_tmo_atm_trf_sol_x12_u12').cast('int').alias('can_flg_tmo_atm_trf_sol_x12_u12'),
        col('can_flg_tmo_atm_trf_dol_x3m_u3m').cast('int').alias('can_flg_tmo_atm_trf_dol_x3m_u3m'),
        col('can_flg_tmo_atm_trf_dol_x6m_u6m').cast('int').alias('can_flg_tmo_atm_trf_dol_x6m_u6m'),
        col('can_flg_tmo_atm_trf_dol_x9m_u9m').cast('int').alias('can_flg_tmo_atm_trf_dol_x9m_u9m'),
        col('can_flg_tmo_atm_trf_dol_x12_u12').cast('int').alias('can_flg_tmo_atm_trf_dol_x12_u12'),
        col('can_flg_tmo_atm_ads_x3m_u3m').cast('int').alias('can_flg_tmo_atm_ads_x3m_u3m'),
        col('can_flg_tmo_atm_ads_x6m_u6m').cast('int').alias('can_flg_tmo_atm_ads_x6m_u6m'),
        col('can_flg_tmo_atm_ads_x9m_u9m').cast('int').alias('can_flg_tmo_atm_ads_x9m_u9m'),
        col('can_flg_tmo_atm_ads_x12_u12').cast('int').alias('can_flg_tmo_atm_ads_x12_u12'),
        col('can_flg_tmo_atm_dis_x3m_u3m').cast('int').alias('can_flg_tmo_atm_dis_x3m_u3m'),
        col('can_flg_tmo_atm_dis_x6m_u6m').cast('int').alias('can_flg_tmo_atm_dis_x6m_u6m'),
        col('can_flg_tmo_atm_dis_x9m_u9m').cast('int').alias('can_flg_tmo_atm_dis_x9m_u9m'),
        col('can_flg_tmo_atm_dis_x12_u12').cast('int').alias('can_flg_tmo_atm_dis_x12_u12'),
        col('can_flg_tmo_atm_pag_tcr_x3m_u3m').cast('int').alias('can_flg_tmo_atm_pag_tcr_x3m_u3m'),
        col('can_flg_tmo_atm_pag_tcr_x6m_u6m').cast('int').alias('can_flg_tmo_atm_pag_tcr_x6m_u6m'),
        col('can_flg_tmo_atm_pag_tcr_x9m_u9m').cast('int').alias('can_flg_tmo_atm_pag_tcr_x9m_u9m'),
        col('can_flg_tmo_atm_pag_tcr_x12_u12').cast('int').alias('can_flg_tmo_atm_pag_tcr_x12_u12'),
        col('can_flg_tmo_atm_pag_srv_x3m_u3m').cast('int').alias('can_flg_tmo_atm_pag_srv_x3m_u3m'),
        col('can_flg_tmo_atm_pag_srv_x6m_u6m').cast('int').alias('can_flg_tmo_atm_pag_srv_x6m_u6m'),
        col('can_flg_tmo_atm_pag_srv_x9m_u9m').cast('int').alias('can_flg_tmo_atm_pag_srv_x9m_u9m'),
        col('can_flg_tmo_atm_pag_srv_x12_u12').cast('int').alias('can_flg_tmo_atm_pag_srv_x12_u12'),
        col('can_flg_tnm_atm_cslt_x3m_u3m').cast('int').alias('can_flg_tnm_atm_cslt_x3m_u3m'),
        col('can_flg_tnm_atm_cslt_x6m_u6m').cast('int').alias('can_flg_tnm_atm_cslt_x6m_u6m'),
        col('can_flg_tnm_atm_cslt_x9m_u9m').cast('int').alias('can_flg_tnm_atm_cslt_x9m_u9m'),
        col('can_flg_tnm_atm_cslt_x12_u12').cast('int').alias('can_flg_tnm_atm_cslt_x12_u12'),
        col('can_flg_tnm_atm_admn_x3m_u3m').cast('int').alias('can_flg_tnm_atm_admn_x3m_u3m'),
        col('can_flg_tnm_atm_admn_x6m_u6m').cast('int').alias('can_flg_tnm_atm_admn_x6m_u6m'),
        col('can_flg_tnm_atm_admn_x9m_u9m').cast('int').alias('can_flg_tnm_atm_admn_x9m_u9m'),
        col('can_flg_tnm_atm_admn_x12_u12').cast('int').alias('can_flg_tnm_atm_admn_x12_u12'),
        col('can_mto_tmo_atm_u1m').cast('double').alias('can_mto_tmo_atm_u1m'),
        col('can_mto_tmo_atm_prm_u3m').cast('double').alias('can_mto_tmo_atm_prm_u3m'),
        col('can_mto_tmo_atm_prm_u6m').cast('double').alias('can_mto_tmo_atm_prm_u6m'),
        col('can_mto_tmo_atm_prm_u9m').cast('double').alias('can_mto_tmo_atm_prm_u9m'),
        col('can_mto_tmo_atm_prm_u12').cast('double').alias('can_mto_tmo_atm_prm_u12'),
        col('can_mto_tmo_atm_sol_u1m').cast('double').alias('can_mto_tmo_atm_sol_u1m'),
        col('can_mto_tmo_atm_sol_prm_u3m').cast('double').alias('can_mto_tmo_atm_sol_prm_u3m'),
        col('can_mto_tmo_atm_sol_prm_u6m').cast('double').alias('can_mto_tmo_atm_sol_prm_u6m'),
        col('can_mto_tmo_atm_sol_prm_u9m').cast('double').alias('can_mto_tmo_atm_sol_prm_u9m'),
        col('can_mto_tmo_atm_sol_prm_u12').cast('double').alias('can_mto_tmo_atm_sol_prm_u12'),
        col('can_mto_tmo_atm_dol_u1m').cast('double').alias('can_mto_tmo_atm_dol_u1m'),
        col('can_mto_tmo_atm_dol_prm_u3m').cast('double').alias('can_mto_tmo_atm_dol_prm_u3m'),
        col('can_mto_tmo_atm_dol_prm_u6m').cast('double').alias('can_mto_tmo_atm_dol_prm_u6m'),
        col('can_mto_tmo_atm_dol_prm_u9m').cast('double').alias('can_mto_tmo_atm_dol_prm_u9m'),
        col('can_mto_tmo_atm_dol_prm_u12').cast('double').alias('can_mto_tmo_atm_dol_prm_u12'),
        col('can_mto_tmo_atm_ret_u1m').cast('double').alias('can_mto_tmo_atm_ret_u1m'),
        col('can_mto_tmo_atm_ret_prm_u3m').cast('double').alias('can_mto_tmo_atm_ret_prm_u3m'),
        col('can_mto_tmo_atm_ret_prm_u6m').cast('double').alias('can_mto_tmo_atm_ret_prm_u6m'),
        col('can_mto_tmo_atm_ret_prm_u9m').cast('double').alias('can_mto_tmo_atm_ret_prm_u9m'),
        col('can_mto_tmo_atm_ret_prm_u12').cast('double').alias('can_mto_tmo_atm_ret_prm_u12'),
        col('can_mto_tmo_atm_ret_sol_u1m').cast('double').alias('can_mto_tmo_atm_ret_sol_u1m'),
        col('can_mto_tmo_atm_ret_sol_prm_u3m').cast('double').alias('can_mto_tmo_atm_ret_sol_prm_u3m'),
        col('can_mto_tmo_atm_ret_sol_prm_u6m').cast('double').alias('can_mto_tmo_atm_ret_sol_prm_u6m'),
        col('can_mto_tmo_atm_ret_sol_prm_u9m').cast('double').alias('can_mto_tmo_atm_ret_sol_prm_u9m'),
        col('can_mto_tmo_atm_ret_sol_prm_u12').cast('double').alias('can_mto_tmo_atm_ret_sol_prm_u12'),
        col('can_mto_tmo_atm_ret_dol_u1m').cast('double').alias('can_mto_tmo_atm_ret_dol_u1m'),
        col('can_mto_tmo_atm_ret_dol_prm_u3m').cast('double').alias('can_mto_tmo_atm_ret_dol_prm_u3m'),
        col('can_mto_tmo_atm_ret_dol_prm_u6m').cast('double').alias('can_mto_tmo_atm_ret_dol_prm_u6m'),
        col('can_mto_tmo_atm_ret_dol_prm_u9m').cast('double').alias('can_mto_tmo_atm_ret_dol_prm_u9m'),
        col('can_mto_tmo_atm_ret_dol_prm_u12').cast('double').alias('can_mto_tmo_atm_ret_dol_prm_u12'),
        col('can_mto_tmo_atm_dep_u1m').cast('double').alias('can_mto_tmo_atm_dep_u1m'),
        col('can_mto_tmo_atm_dep_prm_u3m').cast('double').alias('can_mto_tmo_atm_dep_prm_u3m'),
        col('can_mto_tmo_atm_dep_prm_u6m').cast('double').alias('can_mto_tmo_atm_dep_prm_u6m'),
        col('can_mto_tmo_atm_dep_prm_u9m').cast('double').alias('can_mto_tmo_atm_dep_prm_u9m'),
        col('can_mto_tmo_atm_dep_prm_u12').cast('double').alias('can_mto_tmo_atm_dep_prm_u12'),
        col('can_mto_tmo_atm_dep_sol_u1m').cast('double').alias('can_mto_tmo_atm_dep_sol_u1m'),
        col('can_mto_tmo_atm_dep_sol_prm_u3m').cast('double').alias('can_mto_tmo_atm_dep_sol_prm_u3m'),
        col('can_mto_tmo_atm_dep_sol_prm_u6m').cast('double').alias('can_mto_tmo_atm_dep_sol_prm_u6m'),
        col('can_mto_tmo_atm_dep_sol_prm_u9m').cast('double').alias('can_mto_tmo_atm_dep_sol_prm_u9m'),
        col('can_mto_tmo_atm_dep_sol_prm_u12').cast('double').alias('can_mto_tmo_atm_dep_sol_prm_u12'),
        col('can_mto_tmo_atm_dep_dol_u1m').cast('double').alias('can_mto_tmo_atm_dep_dol_u1m'),
        col('can_mto_tmo_atm_dep_dol_prm_u3m').cast('double').alias('can_mto_tmo_atm_dep_dol_prm_u3m'),
        col('can_mto_tmo_atm_dep_dol_prm_u6m').cast('double').alias('can_mto_tmo_atm_dep_dol_prm_u6m'),
        col('can_mto_tmo_atm_dep_dol_prm_u9m').cast('double').alias('can_mto_tmo_atm_dep_dol_prm_u9m'),
        col('can_mto_tmo_atm_dep_dol_prm_u12').cast('double').alias('can_mto_tmo_atm_dep_dol_prm_u12'),
        col('can_mto_tmo_atm_trf_u1m').cast('double').alias('can_mto_tmo_atm_trf_u1m'),
        col('can_mto_tmo_atm_trf_prm_u3m').cast('double').alias('can_mto_tmo_atm_trf_prm_u3m'),
        col('can_mto_tmo_atm_trf_prm_u6m').cast('double').alias('can_mto_tmo_atm_trf_prm_u6m'),
        col('can_mto_tmo_atm_trf_prm_u9m').cast('double').alias('can_mto_tmo_atm_trf_prm_u9m'),
        col('can_mto_tmo_atm_trf_prm_u12').cast('double').alias('can_mto_tmo_atm_trf_prm_u12'),
        col('can_mto_tmo_atm_trf_sol_u1m').cast('double').alias('can_mto_tmo_atm_trf_sol_u1m'),
        col('can_mto_tmo_atm_trf_sol_prm_u3m').cast('double').alias('can_mto_tmo_atm_trf_sol_prm_u3m'),
        col('can_mto_tmo_atm_trf_sol_prm_u6m').cast('double').alias('can_mto_tmo_atm_trf_sol_prm_u6m'),
        col('can_mto_tmo_atm_trf_sol_prm_u9m').cast('double').alias('can_mto_tmo_atm_trf_sol_prm_u9m'),
        col('can_mto_tmo_atm_trf_sol_prm_u12').cast('double').alias('can_mto_tmo_atm_trf_sol_prm_u12'),
        col('can_mto_tmo_atm_trf_dol_u1m').cast('double').alias('can_mto_tmo_atm_trf_dol_u1m'),
        col('can_mto_tmo_atm_trf_dol_prm_u3m').cast('double').alias('can_mto_tmo_atm_trf_dol_prm_u3m'),
        col('can_mto_tmo_atm_trf_dol_prm_u6m').cast('double').alias('can_mto_tmo_atm_trf_dol_prm_u6m'),
        col('can_mto_tmo_atm_trf_dol_prm_u9m').cast('double').alias('can_mto_tmo_atm_trf_dol_prm_u9m'),
        col('can_mto_tmo_atm_trf_dol_prm_u12').cast('double').alias('can_mto_tmo_atm_trf_dol_prm_u12'),
        col('can_mto_tmo_atm_ads_u1m').cast('double').alias('can_mto_tmo_atm_ads_u1m'),
        col('can_mto_tmo_atm_ads_prm_u3m').cast('double').alias('can_mto_tmo_atm_ads_prm_u3m'),
        col('can_mto_tmo_atm_ads_prm_u6m').cast('double').alias('can_mto_tmo_atm_ads_prm_u6m'),
        col('can_mto_tmo_atm_ads_prm_u9m').cast('double').alias('can_mto_tmo_atm_ads_prm_u9m'),
        col('can_mto_tmo_atm_ads_prm_u12').cast('double').alias('can_mto_tmo_atm_ads_prm_u12'),
        col('can_mto_tmo_atm_dis_u1m').cast('double').alias('can_mto_tmo_atm_dis_u1m'),
        col('can_mto_tmo_atm_dis_prm_u3m').cast('double').alias('can_mto_tmo_atm_dis_prm_u3m'),
        col('can_mto_tmo_atm_dis_prm_u6m').cast('double').alias('can_mto_tmo_atm_dis_prm_u6m'),
        col('can_mto_tmo_atm_dis_prm_u9m').cast('double').alias('can_mto_tmo_atm_dis_prm_u9m'),
        col('can_mto_tmo_atm_dis_prm_u12').cast('double').alias('can_mto_tmo_atm_dis_prm_u12'),
        col('can_mto_tmo_atm_pag_tcr_u1m').cast('double').alias('can_mto_tmo_atm_pag_tcr_u1m'),
        col('can_mto_tmo_atm_pag_tcr_prm_u3m').cast('double').alias('can_mto_tmo_atm_pag_tcr_prm_u3m'),
        col('can_mto_tmo_atm_pag_tcr_prm_u6m').cast('double').alias('can_mto_tmo_atm_pag_tcr_prm_u6m'),
        col('can_mto_tmo_atm_pag_tcr_prm_u9m').cast('double').alias('can_mto_tmo_atm_pag_tcr_prm_u9m'),
        col('can_mto_tmo_atm_pag_tcr_prm_u12').cast('double').alias('can_mto_tmo_atm_pag_tcr_prm_u12'),
        col('can_mto_tmo_atm_pag_srv_u1m').cast('double').alias('can_mto_tmo_atm_pag_srv_u1m'),
        col('can_mto_tmo_atm_pag_srv_prm_u3m').cast('double').alias('can_mto_tmo_atm_pag_srv_prm_u3m'),
        col('can_mto_tmo_atm_pag_srv_prm_u6m').cast('double').alias('can_mto_tmo_atm_pag_srv_prm_u6m'),
        col('can_mto_tmo_atm_pag_srv_prm_u9m').cast('double').alias('can_mto_tmo_atm_pag_srv_prm_u9m'),
        col('can_mto_tmo_atm_pag_srv_prm_u12').cast('double').alias('can_mto_tmo_atm_pag_srv_prm_u12'),
        col('can_tkt_tmo_atm_u1m').cast('double').alias('can_tkt_tmo_atm_u1m'),
        col('can_tkt_tmo_atm_prm_u3m').cast('double').alias('can_tkt_tmo_atm_prm_u3m'),
        col('can_tkt_tmo_atm_prm_u6m').cast('double').alias('can_tkt_tmo_atm_prm_u6m'),
        col('can_tkt_tmo_atm_prm_u9m').cast('double').alias('can_tkt_tmo_atm_prm_u9m'),
        col('can_tkt_tmo_atm_prm_u12').cast('double').alias('can_tkt_tmo_atm_prm_u12'),
        col('can_tkt_tmo_atm_sol_u1m').cast('double').alias('can_tkt_tmo_atm_sol_u1m'),
        col('can_tkt_tmo_atm_sol_prm_u3m').cast('double').alias('can_tkt_tmo_atm_sol_prm_u3m'),
        col('can_tkt_tmo_atm_sol_prm_u6m').cast('double').alias('can_tkt_tmo_atm_sol_prm_u6m'),
        col('can_tkt_tmo_atm_sol_prm_u9m').cast('double').alias('can_tkt_tmo_atm_sol_prm_u9m'),
        col('can_tkt_tmo_atm_sol_prm_u12').cast('double').alias('can_tkt_tmo_atm_sol_prm_u12'),
        col('can_tkt_tmo_atm_dol_u1m').cast('double').alias('can_tkt_tmo_atm_dol_u1m'),
        col('can_tkt_tmo_atm_dol_prm_u3m').cast('double').alias('can_tkt_tmo_atm_dol_prm_u3m'),
        col('can_tkt_tmo_atm_dol_prm_u6m').cast('double').alias('can_tkt_tmo_atm_dol_prm_u6m'),
        col('can_tkt_tmo_atm_dol_prm_u9m').cast('double').alias('can_tkt_tmo_atm_dol_prm_u9m'),
        col('can_tkt_tmo_atm_dol_prm_u12').cast('double').alias('can_tkt_tmo_atm_dol_prm_u12'),
        col('can_tkt_tmo_atm_ret_u1m').cast('double').alias('can_tkt_tmo_atm_ret_u1m'),
        col('can_tkt_tmo_atm_ret_prm_u3m').cast('double').alias('can_tkt_tmo_atm_ret_prm_u3m'),
        col('can_tkt_tmo_atm_ret_prm_u6m').cast('double').alias('can_tkt_tmo_atm_ret_prm_u6m'),
        col('can_tkt_tmo_atm_ret_prm_u9m').cast('double').alias('can_tkt_tmo_atm_ret_prm_u9m'),
        col('can_tkt_tmo_atm_ret_prm_u12').cast('double').alias('can_tkt_tmo_atm_ret_prm_u12'),
        col('can_tkt_tmo_atm_ret_sol_u1m').cast('double').alias('can_tkt_tmo_atm_ret_sol_u1m'),
        col('can_tkt_tmo_atm_ret_sol_prm_u3m').cast('double').alias('can_tkt_tmo_atm_ret_sol_prm_u3m'),
        col('can_tkt_tmo_atm_ret_sol_prm_u6m').cast('double').alias('can_tkt_tmo_atm_ret_sol_prm_u6m'),
        col('can_tkt_tmo_atm_ret_sol_prm_u9m').cast('double').alias('can_tkt_tmo_atm_ret_sol_prm_u9m'),
        col('can_tkt_tmo_atm_ret_sol_prm_u12').cast('double').alias('can_tkt_tmo_atm_ret_sol_prm_u12'),
        col('can_tkt_tmo_atm_ret_dol_u1m').cast('double').alias('can_tkt_tmo_atm_ret_dol_u1m'),
        col('can_tkt_tmo_atm_ret_dol_prm_u3m').cast('double').alias('can_tkt_tmo_atm_ret_dol_prm_u3m'),
        col('can_tkt_tmo_atm_ret_dol_prm_u6m').cast('double').alias('can_tkt_tmo_atm_ret_dol_prm_u6m'),
        col('can_tkt_tmo_atm_ret_dol_prm_u9m').cast('double').alias('can_tkt_tmo_atm_ret_dol_prm_u9m'),
        col('can_tkt_tmo_atm_ret_dol_prm_u12').cast('double').alias('can_tkt_tmo_atm_ret_dol_prm_u12'),
        col('can_tkt_tmo_atm_dep_u1m').cast('double').alias('can_tkt_tmo_atm_dep_u1m'),
        col('can_tkt_tmo_atm_dep_prm_u3m').cast('double').alias('can_tkt_tmo_atm_dep_prm_u3m'),
        col('can_tkt_tmo_atm_dep_prm_u6m').cast('double').alias('can_tkt_tmo_atm_dep_prm_u6m'),
        col('can_tkt_tmo_atm_dep_prm_u9m').cast('double').alias('can_tkt_tmo_atm_dep_prm_u9m'),
        col('can_tkt_tmo_atm_dep_prm_u12').cast('double').alias('can_tkt_tmo_atm_dep_prm_u12'),
        col('can_tkt_tmo_atm_dep_sol_u1m').cast('double').alias('can_tkt_tmo_atm_dep_sol_u1m'),
        col('can_tkt_tmo_atm_dep_sol_prm_u3m').cast('double').alias('can_tkt_tmo_atm_dep_sol_prm_u3m'),
        col('can_tkt_tmo_atm_dep_sol_prm_u6m').cast('double').alias('can_tkt_tmo_atm_dep_sol_prm_u6m'),
        col('can_tkt_tmo_atm_dep_sol_prm_u9m').cast('double').alias('can_tkt_tmo_atm_dep_sol_prm_u9m'),
        col('can_tkt_tmo_atm_dep_sol_prm_u12').cast('double').alias('can_tkt_tmo_atm_dep_sol_prm_u12'),
        col('can_tkt_tmo_atm_dep_dol_u1m').cast('double').alias('can_tkt_tmo_atm_dep_dol_u1m'),
        col('can_tkt_tmo_atm_dep_dol_prm_u3m').cast('double').alias('can_tkt_tmo_atm_dep_dol_prm_u3m'),
        col('can_tkt_tmo_atm_dep_dol_prm_u6m').cast('double').alias('can_tkt_tmo_atm_dep_dol_prm_u6m'),
        col('can_tkt_tmo_atm_dep_dol_prm_u9m').cast('double').alias('can_tkt_tmo_atm_dep_dol_prm_u9m'),
        col('can_tkt_tmo_atm_dep_dol_prm_u12').cast('double').alias('can_tkt_tmo_atm_dep_dol_prm_u12'),
        col('can_tkt_tmo_atm_trf_u1m').cast('double').alias('can_tkt_tmo_atm_trf_u1m'),
        col('can_tkt_tmo_atm_trf_prm_u3m').cast('double').alias('can_tkt_tmo_atm_trf_prm_u3m'),
        col('can_tkt_tmo_atm_trf_prm_u6m').cast('double').alias('can_tkt_tmo_atm_trf_prm_u6m'),
        col('can_tkt_tmo_atm_trf_prm_u9m').cast('double').alias('can_tkt_tmo_atm_trf_prm_u9m'),
        col('can_tkt_tmo_atm_trf_prm_u12').cast('double').alias('can_tkt_tmo_atm_trf_prm_u12'),
        col('can_tkt_tmo_atm_trf_sol_u1m').cast('double').alias('can_tkt_tmo_atm_trf_sol_u1m'),
        col('can_tkt_tmo_atm_trf_sol_prm_u3m').cast('double').alias('can_tkt_tmo_atm_trf_sol_prm_u3m'),
        col('can_tkt_tmo_atm_trf_sol_prm_u6m').cast('double').alias('can_tkt_tmo_atm_trf_sol_prm_u6m'),
        col('can_tkt_tmo_atm_trf_sol_prm_u9m').cast('double').alias('can_tkt_tmo_atm_trf_sol_prm_u9m'),
        col('can_tkt_tmo_atm_trf_sol_prm_u12').cast('double').alias('can_tkt_tmo_atm_trf_sol_prm_u12'),
        col('can_tkt_tmo_atm_trf_dol_u1m').cast('double').alias('can_tkt_tmo_atm_trf_dol_u1m'),
        col('can_tkt_tmo_atm_trf_dol_prm_u3m').cast('double').alias('can_tkt_tmo_atm_trf_dol_prm_u3m'),
        col('can_tkt_tmo_atm_trf_dol_prm_u6m').cast('double').alias('can_tkt_tmo_atm_trf_dol_prm_u6m'),
        col('can_tkt_tmo_atm_trf_dol_prm_u9m').cast('double').alias('can_tkt_tmo_atm_trf_dol_prm_u9m'),
        col('can_tkt_tmo_atm_trf_dol_prm_u12').cast('double').alias('can_tkt_tmo_atm_trf_dol_prm_u12'),
        col('can_tkt_tmo_atm_ads_u1m').cast('double').alias('can_tkt_tmo_atm_ads_u1m'),
        col('can_tkt_tmo_atm_ads_prm_u3m').cast('double').alias('can_tkt_tmo_atm_ads_prm_u3m'),
        col('can_tkt_tmo_atm_ads_prm_u6m').cast('double').alias('can_tkt_tmo_atm_ads_prm_u6m'),
        col('can_tkt_tmo_atm_ads_prm_u9m').cast('double').alias('can_tkt_tmo_atm_ads_prm_u9m'),
        col('can_tkt_tmo_atm_ads_prm_u12').cast('double').alias('can_tkt_tmo_atm_ads_prm_u12'),
        col('can_tkt_tmo_atm_dis_u1m').cast('double').alias('can_tkt_tmo_atm_dis_u1m'),
        col('can_tkt_tmo_atm_dis_prm_u3m').cast('double').alias('can_tkt_tmo_atm_dis_prm_u3m'),
        col('can_tkt_tmo_atm_dis_prm_u6m').cast('double').alias('can_tkt_tmo_atm_dis_prm_u6m'),
        col('can_tkt_tmo_atm_dis_prm_u9m').cast('double').alias('can_tkt_tmo_atm_dis_prm_u9m'),
        col('can_tkt_tmo_atm_dis_prm_u12').cast('double').alias('can_tkt_tmo_atm_dis_prm_u12'),
        col('can_tkt_tmo_atm_pag_tcr_u1m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_u1m'),
        col('can_tkt_tmo_atm_pag_tcr_prm_u3m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_prm_u3m'),
        col('can_tkt_tmo_atm_pag_tcr_prm_u6m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_prm_u6m'),
        col('can_tkt_tmo_atm_pag_tcr_prm_u9m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_prm_u9m'),
        col('can_tkt_tmo_atm_pag_tcr_prm_u12').cast('double').alias('can_tkt_tmo_atm_pag_tcr_prm_u12'),
        col('can_tkt_tmo_atm_pag_srv_u1m').cast('double').alias('can_tkt_tmo_atm_pag_srv_u1m'),
        col('can_tkt_tmo_atm_pag_srv_prm_u3m').cast('double').alias('can_tkt_tmo_atm_pag_srv_prm_u3m'),
        col('can_tkt_tmo_atm_pag_srv_prm_u6m').cast('double').alias('can_tkt_tmo_atm_pag_srv_prm_u6m'),
        col('can_tkt_tmo_atm_pag_srv_prm_u9m').cast('double').alias('can_tkt_tmo_atm_pag_srv_prm_u9m'),
        col('can_tkt_tmo_atm_pag_srv_prm_u12').cast('double').alias('can_tkt_tmo_atm_pag_srv_prm_u12'),
        col('can_mto_tmo_atm_prm_p6m').cast('double').alias('can_mto_tmo_atm_prm_p6m'),
        col('can_mto_tmo_atm_sol_prm_p6m').cast('double').alias('can_mto_tmo_atm_sol_prm_p6m'),
        col('can_mto_tmo_atm_dol_prm_p6m').cast('double').alias('can_mto_tmo_atm_dol_prm_p6m'),
        col('can_mto_tmo_atm_ret_prm_p6m').cast('double').alias('can_mto_tmo_atm_ret_prm_p6m'),
        col('can_mto_tmo_atm_ret_sol_prm_p6m').cast('double').alias('can_mto_tmo_atm_ret_sol_prm_p6m'),
        col('can_mto_tmo_atm_ret_dol_prm_p6m').cast('double').alias('can_mto_tmo_atm_ret_dol_prm_p6m'),
        col('can_mto_tmo_atm_dep_prm_p6m').cast('double').alias('can_mto_tmo_atm_dep_prm_p6m'),
        col('can_mto_tmo_atm_dep_sol_prm_p6m').cast('double').alias('can_mto_tmo_atm_dep_sol_prm_p6m'),
        col('can_mto_tmo_atm_dep_dol_prm_p6m').cast('double').alias('can_mto_tmo_atm_dep_dol_prm_p6m'),
        col('can_mto_tmo_atm_trf_prm_p6m').cast('double').alias('can_mto_tmo_atm_trf_prm_p6m'),
        col('can_mto_tmo_atm_trf_sol_prm_p6m').cast('double').alias('can_mto_tmo_atm_trf_sol_prm_p6m'),
        col('can_mto_tmo_atm_trf_dol_prm_p6m').cast('double').alias('can_mto_tmo_atm_trf_dol_prm_p6m'),
        col('can_mto_tmo_atm_ads_prm_p6m').cast('double').alias('can_mto_tmo_atm_ads_prm_p6m'),
        col('can_mto_tmo_atm_dis_prm_p6m').cast('double').alias('can_mto_tmo_atm_dis_prm_p6m'),
        col('can_mto_tmo_atm_pag_tcr_prm_p6m').cast('double').alias('can_mto_tmo_atm_pag_tcr_prm_p6m'),
        col('can_mto_tmo_atm_pag_srv_prm_p6m').cast('double').alias('can_mto_tmo_atm_pag_srv_prm_p6m'),
        col('can_tkt_tmo_atm_prm_p6m').cast('double').alias('can_tkt_tmo_atm_prm_p6m'),
        col('can_tkt_tmo_atm_sol_prm_p6m').cast('double').alias('can_tkt_tmo_atm_sol_prm_p6m'),
        col('can_tkt_tmo_atm_dol_prm_p6m').cast('double').alias('can_tkt_tmo_atm_dol_prm_p6m'),
        col('can_tkt_tmo_atm_ret_prm_p6m').cast('double').alias('can_tkt_tmo_atm_ret_prm_p6m'),
        col('can_tkt_tmo_atm_ret_sol_prm_p6m').cast('double').alias('can_tkt_tmo_atm_ret_sol_prm_p6m'),
        col('can_tkt_tmo_atm_ret_dol_prm_p6m').cast('double').alias('can_tkt_tmo_atm_ret_dol_prm_p6m'),
        col('can_tkt_tmo_atm_dep_prm_p6m').cast('double').alias('can_tkt_tmo_atm_dep_prm_p6m'),
        col('can_tkt_tmo_atm_dep_sol_prm_p6m').cast('double').alias('can_tkt_tmo_atm_dep_sol_prm_p6m'),
        col('can_tkt_tmo_atm_dep_dol_prm_p6m').cast('double').alias('can_tkt_tmo_atm_dep_dol_prm_p6m'),
        col('can_tkt_tmo_atm_trf_prm_p6m').cast('double').alias('can_tkt_tmo_atm_trf_prm_p6m'),
        col('can_tkt_tmo_atm_trf_sol_prm_p6m').cast('double').alias('can_tkt_tmo_atm_trf_sol_prm_p6m'),
        col('can_tkt_tmo_atm_trf_dol_prm_p6m').cast('double').alias('can_tkt_tmo_atm_trf_dol_prm_p6m'),
        col('can_tkt_tmo_atm_ads_prm_p6m').cast('double').alias('can_tkt_tmo_atm_ads_prm_p6m'),
        col('can_tkt_tmo_atm_dis_prm_p6m').cast('double').alias('can_tkt_tmo_atm_dis_prm_p6m'),
        col('can_tkt_tmo_atm_pag_tcr_prm_p6m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_prm_p6m'),
        col('can_tkt_tmo_atm_pag_srv_prm_p6m').cast('double').alias('can_tkt_tmo_atm_pag_srv_prm_p6m'),
        col('can_mto_tmo_atm_prm_p3m').cast('double').alias('can_mto_tmo_atm_prm_p3m'),
        col('can_mto_tmo_atm_sol_prm_p3m').cast('double').alias('can_mto_tmo_atm_sol_prm_p3m'),
        col('can_mto_tmo_atm_dol_prm_p3m').cast('double').alias('can_mto_tmo_atm_dol_prm_p3m'),
        col('can_mto_tmo_atm_ret_prm_p3m').cast('double').alias('can_mto_tmo_atm_ret_prm_p3m'),
        col('can_mto_tmo_atm_ret_sol_prm_p3m').cast('double').alias('can_mto_tmo_atm_ret_sol_prm_p3m'),
        col('can_mto_tmo_atm_ret_dol_prm_p3m').cast('double').alias('can_mto_tmo_atm_ret_dol_prm_p3m'),
        col('can_mto_tmo_atm_dep_prm_p3m').cast('double').alias('can_mto_tmo_atm_dep_prm_p3m'),
        col('can_mto_tmo_atm_dep_sol_prm_p3m').cast('double').alias('can_mto_tmo_atm_dep_sol_prm_p3m'),
        col('can_mto_tmo_atm_dep_dol_prm_p3m').cast('double').alias('can_mto_tmo_atm_dep_dol_prm_p3m'),
        col('can_mto_tmo_atm_trf_prm_p3m').cast('double').alias('can_mto_tmo_atm_trf_prm_p3m'),
        col('can_mto_tmo_atm_trf_sol_prm_p3m').cast('double').alias('can_mto_tmo_atm_trf_sol_prm_p3m'),
        col('can_mto_tmo_atm_trf_dol_prm_p3m').cast('double').alias('can_mto_tmo_atm_trf_dol_prm_p3m'),
        col('can_mto_tmo_atm_ads_prm_p3m').cast('double').alias('can_mto_tmo_atm_ads_prm_p3m'),
        col('can_mto_tmo_atm_dis_prm_p3m').cast('double').alias('can_mto_tmo_atm_dis_prm_p3m'),
        col('can_mto_tmo_atm_pag_tcr_prm_p3m').cast('double').alias('can_mto_tmo_atm_pag_tcr_prm_p3m'),
        col('can_mto_tmo_atm_pag_srv_prm_p3m').cast('double').alias('can_mto_tmo_atm_pag_srv_prm_p3m'),
        col('can_tkt_tmo_atm_prm_p3m').cast('double').alias('can_tkt_tmo_atm_prm_p3m'),
        col('can_tkt_tmo_atm_sol_prm_p3m').cast('double').alias('can_tkt_tmo_atm_sol_prm_p3m'),
        col('can_tkt_tmo_atm_dol_prm_p3m').cast('double').alias('can_tkt_tmo_atm_dol_prm_p3m'),
        col('can_tkt_tmo_atm_ret_prm_p3m').cast('double').alias('can_tkt_tmo_atm_ret_prm_p3m'),
        col('can_tkt_tmo_atm_ret_sol_prm_p3m').cast('double').alias('can_tkt_tmo_atm_ret_sol_prm_p3m'),
        col('can_tkt_tmo_atm_ret_dol_prm_p3m').cast('double').alias('can_tkt_tmo_atm_ret_dol_prm_p3m'),
        col('can_tkt_tmo_atm_dep_prm_p3m').cast('double').alias('can_tkt_tmo_atm_dep_prm_p3m'),
        col('can_tkt_tmo_atm_dep_sol_prm_p3m').cast('double').alias('can_tkt_tmo_atm_dep_sol_prm_p3m'),
        col('can_tkt_tmo_atm_dep_dol_prm_p3m').cast('double').alias('can_tkt_tmo_atm_dep_dol_prm_p3m'),
        col('can_tkt_tmo_atm_trf_prm_p3m').cast('double').alias('can_tkt_tmo_atm_trf_prm_p3m'),
        col('can_tkt_tmo_atm_trf_sol_prm_p3m').cast('double').alias('can_tkt_tmo_atm_trf_sol_prm_p3m'),
        col('can_tkt_tmo_atm_trf_dol_prm_p3m').cast('double').alias('can_tkt_tmo_atm_trf_dol_prm_p3m'),
        col('can_tkt_tmo_atm_ads_prm_p3m').cast('double').alias('can_tkt_tmo_atm_ads_prm_p3m'),
        col('can_tkt_tmo_atm_dis_prm_p3m').cast('double').alias('can_tkt_tmo_atm_dis_prm_p3m'),
        col('can_tkt_tmo_atm_pag_tcr_prm_p3m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_prm_p3m'),
        col('can_tkt_tmo_atm_pag_srv_prm_p3m').cast('double').alias('can_tkt_tmo_atm_pag_srv_prm_p3m'),
        col('can_mto_tmo_atm_p1m').cast('double').alias('can_mto_tmo_atm_p1m'),
        col('can_mto_tmo_atm_sol_p1m').cast('double').alias('can_mto_tmo_atm_sol_p1m'),
        col('can_mto_tmo_atm_dol_p1m').cast('double').alias('can_mto_tmo_atm_dol_p1m'),
        col('can_mto_tmo_atm_ret_p1m').cast('double').alias('can_mto_tmo_atm_ret_p1m'),
        col('can_mto_tmo_atm_ret_sol_p1m').cast('double').alias('can_mto_tmo_atm_ret_sol_p1m'),
        col('can_mto_tmo_atm_ret_dol_p1m').cast('double').alias('can_mto_tmo_atm_ret_dol_p1m'),
        col('can_mto_tmo_atm_dep_p1m').cast('double').alias('can_mto_tmo_atm_dep_p1m'),
        col('can_mto_tmo_atm_dep_sol_p1m').cast('double').alias('can_mto_tmo_atm_dep_sol_p1m'),
        col('can_mto_tmo_atm_dep_dol_p1m').cast('double').alias('can_mto_tmo_atm_dep_dol_p1m'),
        col('can_mto_tmo_atm_trf_p1m').cast('double').alias('can_mto_tmo_atm_trf_p1m'),
        col('can_mto_tmo_atm_trf_sol_p1m').cast('double').alias('can_mto_tmo_atm_trf_sol_p1m'),
        col('can_mto_tmo_atm_trf_dol_p1m').cast('double').alias('can_mto_tmo_atm_trf_dol_p1m'),
        col('can_mto_tmo_atm_ads_p1m').cast('double').alias('can_mto_tmo_atm_ads_p1m'),
        col('can_mto_tmo_atm_dis_p1m').cast('double').alias('can_mto_tmo_atm_dis_p1m'),
        col('can_mto_tmo_atm_pag_tcr_p1m').cast('double').alias('can_mto_tmo_atm_pag_tcr_p1m'),
        col('can_mto_tmo_atm_pag_srv_p1m').cast('double').alias('can_mto_tmo_atm_pag_srv_p1m'),
        col('can_tkt_tmo_atm_p1m').cast('double').alias('can_tkt_tmo_atm_p1m'),
        col('can_tkt_tmo_atm_sol_p1m').cast('double').alias('can_tkt_tmo_atm_sol_p1m'),
        col('can_tkt_tmo_atm_dol_p1m').cast('double').alias('can_tkt_tmo_atm_dol_p1m'),
        col('can_tkt_tmo_atm_ret_p1m').cast('double').alias('can_tkt_tmo_atm_ret_p1m'),
        col('can_tkt_tmo_atm_ret_sol_p1m').cast('double').alias('can_tkt_tmo_atm_ret_sol_p1m'),
        col('can_tkt_tmo_atm_ret_dol_p1m').cast('double').alias('can_tkt_tmo_atm_ret_dol_p1m'),
        col('can_tkt_tmo_atm_dep_p1m').cast('double').alias('can_tkt_tmo_atm_dep_p1m'),
        col('can_tkt_tmo_atm_dep_sol_p1m').cast('double').alias('can_tkt_tmo_atm_dep_sol_p1m'),
        col('can_tkt_tmo_atm_dep_dol_p1m').cast('double').alias('can_tkt_tmo_atm_dep_dol_p1m'),
        col('can_tkt_tmo_atm_trf_p1m').cast('double').alias('can_tkt_tmo_atm_trf_p1m'),
        col('can_tkt_tmo_atm_trf_sol_p1m').cast('double').alias('can_tkt_tmo_atm_trf_sol_p1m'),
        col('can_tkt_tmo_atm_trf_dol_p1m').cast('double').alias('can_tkt_tmo_atm_trf_dol_p1m'),
        col('can_tkt_tmo_atm_ads_p1m').cast('double').alias('can_tkt_tmo_atm_ads_p1m'),
        col('can_tkt_tmo_atm_dis_p1m').cast('double').alias('can_tkt_tmo_atm_dis_p1m'),
        col('can_tkt_tmo_atm_pag_tcr_p1m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_p1m'),
        col('can_tkt_tmo_atm_pag_srv_p1m').cast('double').alias('can_tkt_tmo_atm_pag_srv_p1m'),
        col('can_mto_tmo_atm_max_u3m').cast('double').alias('can_mto_tmo_atm_max_u3m'),
        col('can_mto_tmo_atm_max_u6m').cast('double').alias('can_mto_tmo_atm_max_u6m'),
        col('can_mto_tmo_atm_max_u9m').cast('double').alias('can_mto_tmo_atm_max_u9m'),
        col('can_mto_tmo_atm_max_u12').cast('double').alias('can_mto_tmo_atm_max_u12'),
        col('can_mto_tmo_atm_sol_max_u3m').cast('double').alias('can_mto_tmo_atm_sol_max_u3m'),
        col('can_mto_tmo_atm_sol_max_u6m').cast('double').alias('can_mto_tmo_atm_sol_max_u6m'),
        col('can_mto_tmo_atm_sol_max_u9m').cast('double').alias('can_mto_tmo_atm_sol_max_u9m'),
        col('can_mto_tmo_atm_sol_max_u12').cast('double').alias('can_mto_tmo_atm_sol_max_u12'),
        col('can_mto_tmo_atm_dol_max_u3m').cast('double').alias('can_mto_tmo_atm_dol_max_u3m'),
        col('can_mto_tmo_atm_dol_max_u6m').cast('double').alias('can_mto_tmo_atm_dol_max_u6m'),
        col('can_mto_tmo_atm_dol_max_u9m').cast('double').alias('can_mto_tmo_atm_dol_max_u9m'),
        col('can_mto_tmo_atm_dol_max_u12').cast('double').alias('can_mto_tmo_atm_dol_max_u12'),
        col('can_mto_tmo_atm_ret_max_u3m').cast('double').alias('can_mto_tmo_atm_ret_max_u3m'),
        col('can_mto_tmo_atm_ret_max_u6m').cast('double').alias('can_mto_tmo_atm_ret_max_u6m'),
        col('can_mto_tmo_atm_ret_max_u9m').cast('double').alias('can_mto_tmo_atm_ret_max_u9m'),
        col('can_mto_tmo_atm_ret_max_u12').cast('double').alias('can_mto_tmo_atm_ret_max_u12'),
        col('can_mto_tmo_atm_ret_sol_max_u3m').cast('double').alias('can_mto_tmo_atm_ret_sol_max_u3m'),
        col('can_mto_tmo_atm_ret_sol_max_u6m').cast('double').alias('can_mto_tmo_atm_ret_sol_max_u6m'),
        col('can_mto_tmo_atm_ret_sol_max_u9m').cast('double').alias('can_mto_tmo_atm_ret_sol_max_u9m'),
        col('can_mto_tmo_atm_ret_sol_max_u12').cast('double').alias('can_mto_tmo_atm_ret_sol_max_u12'),
        col('can_mto_tmo_atm_ret_dol_max_u3m').cast('double').alias('can_mto_tmo_atm_ret_dol_max_u3m'),
        col('can_mto_tmo_atm_ret_dol_max_u6m').cast('double').alias('can_mto_tmo_atm_ret_dol_max_u6m'),
        col('can_mto_tmo_atm_ret_dol_max_u9m').cast('double').alias('can_mto_tmo_atm_ret_dol_max_u9m'),
        col('can_mto_tmo_atm_ret_dol_max_u12').cast('double').alias('can_mto_tmo_atm_ret_dol_max_u12'),
        col('can_mto_tmo_atm_dep_max_u3m').cast('double').alias('can_mto_tmo_atm_dep_max_u3m'),
        col('can_mto_tmo_atm_dep_max_u6m').cast('double').alias('can_mto_tmo_atm_dep_max_u6m'),
        col('can_mto_tmo_atm_dep_max_u9m').cast('double').alias('can_mto_tmo_atm_dep_max_u9m'),
        col('can_mto_tmo_atm_dep_max_u12').cast('double').alias('can_mto_tmo_atm_dep_max_u12'),
        col('can_mto_tmo_atm_dep_sol_max_u3m').cast('double').alias('can_mto_tmo_atm_dep_sol_max_u3m'),
        col('can_mto_tmo_atm_dep_sol_max_u6m').cast('double').alias('can_mto_tmo_atm_dep_sol_max_u6m'),
        col('can_mto_tmo_atm_dep_sol_max_u9m').cast('double').alias('can_mto_tmo_atm_dep_sol_max_u9m'),
        col('can_mto_tmo_atm_dep_sol_max_u12').cast('double').alias('can_mto_tmo_atm_dep_sol_max_u12'),
        col('can_mto_tmo_atm_dep_dol_max_u3m').cast('double').alias('can_mto_tmo_atm_dep_dol_max_u3m'),
        col('can_mto_tmo_atm_dep_dol_max_u6m').cast('double').alias('can_mto_tmo_atm_dep_dol_max_u6m'),
        col('can_mto_tmo_atm_dep_dol_max_u9m').cast('double').alias('can_mto_tmo_atm_dep_dol_max_u9m'),
        col('can_mto_tmo_atm_dep_dol_max_u12').cast('double').alias('can_mto_tmo_atm_dep_dol_max_u12'),
        col('can_mto_tmo_atm_trf_max_u3m').cast('double').alias('can_mto_tmo_atm_trf_max_u3m'),
        col('can_mto_tmo_atm_trf_max_u6m').cast('double').alias('can_mto_tmo_atm_trf_max_u6m'),
        col('can_mto_tmo_atm_trf_max_u9m').cast('double').alias('can_mto_tmo_atm_trf_max_u9m'),
        col('can_mto_tmo_atm_trf_max_u12').cast('double').alias('can_mto_tmo_atm_trf_max_u12'),
        col('can_mto_tmo_atm_trf_sol_max_u3m').cast('double').alias('can_mto_tmo_atm_trf_sol_max_u3m'),
        col('can_mto_tmo_atm_trf_sol_max_u6m').cast('double').alias('can_mto_tmo_atm_trf_sol_max_u6m'),
        col('can_mto_tmo_atm_trf_sol_max_u9m').cast('double').alias('can_mto_tmo_atm_trf_sol_max_u9m'),
        col('can_mto_tmo_atm_trf_sol_max_u12').cast('double').alias('can_mto_tmo_atm_trf_sol_max_u12'),
        col('can_mto_tmo_atm_trf_dol_max_u3m').cast('double').alias('can_mto_tmo_atm_trf_dol_max_u3m'),
        col('can_mto_tmo_atm_trf_dol_max_u6m').cast('double').alias('can_mto_tmo_atm_trf_dol_max_u6m'),
        col('can_mto_tmo_atm_trf_dol_max_u9m').cast('double').alias('can_mto_tmo_atm_trf_dol_max_u9m'),
        col('can_mto_tmo_atm_trf_dol_max_u12').cast('double').alias('can_mto_tmo_atm_trf_dol_max_u12'),
        col('can_mto_tmo_atm_ads_max_u3m').cast('double').alias('can_mto_tmo_atm_ads_max_u3m'),
        col('can_mto_tmo_atm_ads_max_u6m').cast('double').alias('can_mto_tmo_atm_ads_max_u6m'),
        col('can_mto_tmo_atm_ads_max_u9m').cast('double').alias('can_mto_tmo_atm_ads_max_u9m'),
        col('can_mto_tmo_atm_ads_max_u12').cast('double').alias('can_mto_tmo_atm_ads_max_u12'),
        col('can_mto_tmo_atm_dis_max_u3m').cast('double').alias('can_mto_tmo_atm_dis_max_u3m'),
        col('can_mto_tmo_atm_dis_max_u6m').cast('double').alias('can_mto_tmo_atm_dis_max_u6m'),
        col('can_mto_tmo_atm_dis_max_u9m').cast('double').alias('can_mto_tmo_atm_dis_max_u9m'),
        col('can_mto_tmo_atm_dis_max_u12').cast('double').alias('can_mto_tmo_atm_dis_max_u12'),
        col('can_mto_tmo_atm_pag_tcr_max_u3m').cast('double').alias('can_mto_tmo_atm_pag_tcr_max_u3m'),
        col('can_mto_tmo_atm_pag_tcr_max_u6m').cast('double').alias('can_mto_tmo_atm_pag_tcr_max_u6m'),
        col('can_mto_tmo_atm_pag_tcr_max_u9m').cast('double').alias('can_mto_tmo_atm_pag_tcr_max_u9m'),
        col('can_mto_tmo_atm_pag_tcr_max_u12').cast('double').alias('can_mto_tmo_atm_pag_tcr_max_u12'),
        col('can_mto_tmo_atm_pag_srv_max_u3m').cast('double').alias('can_mto_tmo_atm_pag_srv_max_u3m'),
        col('can_mto_tmo_atm_pag_srv_max_u6m').cast('double').alias('can_mto_tmo_atm_pag_srv_max_u6m'),
        col('can_mto_tmo_atm_pag_srv_max_u9m').cast('double').alias('can_mto_tmo_atm_pag_srv_max_u9m'),
        col('can_mto_tmo_atm_pag_srv_max_u12').cast('double').alias('can_mto_tmo_atm_pag_srv_max_u12'),
        col('can_tkt_tmo_atm_max_u3m').cast('double').alias('can_tkt_tmo_atm_max_u3m'),
        col('can_tkt_tmo_atm_max_u6m').cast('double').alias('can_tkt_tmo_atm_max_u6m'),
        col('can_tkt_tmo_atm_max_u9m').cast('double').alias('can_tkt_tmo_atm_max_u9m'),
        col('can_tkt_tmo_atm_max_u12').cast('double').alias('can_tkt_tmo_atm_max_u12'),
        col('can_tkt_tmo_atm_sol_max_u3m').cast('double').alias('can_tkt_tmo_atm_sol_max_u3m'),
        col('can_tkt_tmo_atm_sol_max_u6m').cast('double').alias('can_tkt_tmo_atm_sol_max_u6m'),
        col('can_tkt_tmo_atm_sol_max_u9m').cast('double').alias('can_tkt_tmo_atm_sol_max_u9m'),
        col('can_tkt_tmo_atm_sol_max_u12').cast('double').alias('can_tkt_tmo_atm_sol_max_u12'),
        col('can_tkt_tmo_atm_dol_max_u3m').cast('double').alias('can_tkt_tmo_atm_dol_max_u3m'),
        col('can_tkt_tmo_atm_dol_max_u6m').cast('double').alias('can_tkt_tmo_atm_dol_max_u6m'),
        col('can_tkt_tmo_atm_dol_max_u9m').cast('double').alias('can_tkt_tmo_atm_dol_max_u9m'),
        col('can_tkt_tmo_atm_dol_max_u12').cast('double').alias('can_tkt_tmo_atm_dol_max_u12'),
        col('can_tkt_tmo_atm_ret_max_u3m').cast('double').alias('can_tkt_tmo_atm_ret_max_u3m'),
        col('can_tkt_tmo_atm_ret_max_u6m').cast('double').alias('can_tkt_tmo_atm_ret_max_u6m'),
        col('can_tkt_tmo_atm_ret_max_u9m').cast('double').alias('can_tkt_tmo_atm_ret_max_u9m'),
        col('can_tkt_tmo_atm_ret_max_u12').cast('double').alias('can_tkt_tmo_atm_ret_max_u12'),
        col('can_tkt_tmo_atm_ret_sol_max_u3m').cast('double').alias('can_tkt_tmo_atm_ret_sol_max_u3m'),
        col('can_tkt_tmo_atm_ret_sol_max_u6m').cast('double').alias('can_tkt_tmo_atm_ret_sol_max_u6m'),
        col('can_tkt_tmo_atm_ret_sol_max_u9m').cast('double').alias('can_tkt_tmo_atm_ret_sol_max_u9m'),
        col('can_tkt_tmo_atm_ret_sol_max_u12').cast('double').alias('can_tkt_tmo_atm_ret_sol_max_u12'),
        col('can_tkt_tmo_atm_ret_dol_max_u3m').cast('double').alias('can_tkt_tmo_atm_ret_dol_max_u3m'),
        col('can_tkt_tmo_atm_ret_dol_max_u6m').cast('double').alias('can_tkt_tmo_atm_ret_dol_max_u6m'),
        col('can_tkt_tmo_atm_ret_dol_max_u9m').cast('double').alias('can_tkt_tmo_atm_ret_dol_max_u9m'),
        col('can_tkt_tmo_atm_ret_dol_max_u12').cast('double').alias('can_tkt_tmo_atm_ret_dol_max_u12'),
        col('can_tkt_tmo_atm_dep_max_u3m').cast('double').alias('can_tkt_tmo_atm_dep_max_u3m'),
        col('can_tkt_tmo_atm_dep_max_u6m').cast('double').alias('can_tkt_tmo_atm_dep_max_u6m'),
        col('can_tkt_tmo_atm_dep_max_u9m').cast('double').alias('can_tkt_tmo_atm_dep_max_u9m'),
        col('can_tkt_tmo_atm_dep_max_u12').cast('double').alias('can_tkt_tmo_atm_dep_max_u12'),
        col('can_tkt_tmo_atm_dep_sol_max_u3m').cast('double').alias('can_tkt_tmo_atm_dep_sol_max_u3m'),
        col('can_tkt_tmo_atm_dep_sol_max_u6m').cast('double').alias('can_tkt_tmo_atm_dep_sol_max_u6m'),
        col('can_tkt_tmo_atm_dep_sol_max_u9m').cast('double').alias('can_tkt_tmo_atm_dep_sol_max_u9m'),
        col('can_tkt_tmo_atm_dep_sol_max_u12').cast('double').alias('can_tkt_tmo_atm_dep_sol_max_u12'),
        col('can_tkt_tmo_atm_dep_dol_max_u3m').cast('double').alias('can_tkt_tmo_atm_dep_dol_max_u3m'),
        col('can_tkt_tmo_atm_dep_dol_max_u6m').cast('double').alias('can_tkt_tmo_atm_dep_dol_max_u6m'),
        col('can_tkt_tmo_atm_dep_dol_max_u9m').cast('double').alias('can_tkt_tmo_atm_dep_dol_max_u9m'),
        col('can_tkt_tmo_atm_dep_dol_max_u12').cast('double').alias('can_tkt_tmo_atm_dep_dol_max_u12'),
        col('can_tkt_tmo_atm_trf_max_u3m').cast('double').alias('can_tkt_tmo_atm_trf_max_u3m'),
        col('can_tkt_tmo_atm_trf_max_u6m').cast('double').alias('can_tkt_tmo_atm_trf_max_u6m'),
        col('can_tkt_tmo_atm_trf_max_u9m').cast('double').alias('can_tkt_tmo_atm_trf_max_u9m'),
        col('can_tkt_tmo_atm_trf_max_u12').cast('double').alias('can_tkt_tmo_atm_trf_max_u12'),
        col('can_tkt_tmo_atm_trf_sol_max_u3m').cast('double').alias('can_tkt_tmo_atm_trf_sol_max_u3m'),
        col('can_tkt_tmo_atm_trf_sol_max_u6m').cast('double').alias('can_tkt_tmo_atm_trf_sol_max_u6m'),
        col('can_tkt_tmo_atm_trf_sol_max_u9m').cast('double').alias('can_tkt_tmo_atm_trf_sol_max_u9m'),
        col('can_tkt_tmo_atm_trf_sol_max_u12').cast('double').alias('can_tkt_tmo_atm_trf_sol_max_u12'),
        col('can_tkt_tmo_atm_trf_dol_max_u3m').cast('double').alias('can_tkt_tmo_atm_trf_dol_max_u3m'),
        col('can_tkt_tmo_atm_trf_dol_max_u6m').cast('double').alias('can_tkt_tmo_atm_trf_dol_max_u6m'),
        col('can_tkt_tmo_atm_trf_dol_max_u9m').cast('double').alias('can_tkt_tmo_atm_trf_dol_max_u9m'),
        col('can_tkt_tmo_atm_trf_dol_max_u12').cast('double').alias('can_tkt_tmo_atm_trf_dol_max_u12'),
        col('can_tkt_tmo_atm_ads_max_u3m').cast('double').alias('can_tkt_tmo_atm_ads_max_u3m'),
        col('can_tkt_tmo_atm_ads_max_u6m').cast('double').alias('can_tkt_tmo_atm_ads_max_u6m'),
        col('can_tkt_tmo_atm_ads_max_u9m').cast('double').alias('can_tkt_tmo_atm_ads_max_u9m'),
        col('can_tkt_tmo_atm_ads_max_u12').cast('double').alias('can_tkt_tmo_atm_ads_max_u12'),
        col('can_tkt_tmo_atm_dis_max_u3m').cast('double').alias('can_tkt_tmo_atm_dis_max_u3m'),
        col('can_tkt_tmo_atm_dis_max_u6m').cast('double').alias('can_tkt_tmo_atm_dis_max_u6m'),
        col('can_tkt_tmo_atm_dis_max_u9m').cast('double').alias('can_tkt_tmo_atm_dis_max_u9m'),
        col('can_tkt_tmo_atm_dis_max_u12').cast('double').alias('can_tkt_tmo_atm_dis_max_u12'),
        col('can_tkt_tmo_atm_pag_tcr_max_u3m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_max_u3m'),
        col('can_tkt_tmo_atm_pag_tcr_max_u6m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_max_u6m'),
        col('can_tkt_tmo_atm_pag_tcr_max_u9m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_max_u9m'),
        col('can_tkt_tmo_atm_pag_tcr_max_u12').cast('double').alias('can_tkt_tmo_atm_pag_tcr_max_u12'),
        col('can_tkt_tmo_atm_pag_srv_max_u3m').cast('double').alias('can_tkt_tmo_atm_pag_srv_max_u3m'),
        col('can_tkt_tmo_atm_pag_srv_max_u6m').cast('double').alias('can_tkt_tmo_atm_pag_srv_max_u6m'),
        col('can_tkt_tmo_atm_pag_srv_max_u9m').cast('double').alias('can_tkt_tmo_atm_pag_srv_max_u9m'),
        col('can_tkt_tmo_atm_pag_srv_max_u12').cast('double').alias('can_tkt_tmo_atm_pag_srv_max_u12'),
        col('can_mto_tmo_atm_min_u3m').cast('double').alias('can_mto_tmo_atm_min_u3m'),
        col('can_mto_tmo_atm_min_u6m').cast('double').alias('can_mto_tmo_atm_min_u6m'),
        col('can_mto_tmo_atm_min_u9m').cast('double').alias('can_mto_tmo_atm_min_u9m'),
        col('can_mto_tmo_atm_min_u12').cast('double').alias('can_mto_tmo_atm_min_u12'),
        col('can_mto_tmo_atm_sol_min_u3m').cast('double').alias('can_mto_tmo_atm_sol_min_u3m'),
        col('can_mto_tmo_atm_sol_min_u6m').cast('double').alias('can_mto_tmo_atm_sol_min_u6m'),
        col('can_mto_tmo_atm_sol_min_u9m').cast('double').alias('can_mto_tmo_atm_sol_min_u9m'),
        col('can_mto_tmo_atm_sol_min_u12').cast('double').alias('can_mto_tmo_atm_sol_min_u12'),
        col('can_mto_tmo_atm_dol_min_u3m').cast('double').alias('can_mto_tmo_atm_dol_min_u3m'),
        col('can_mto_tmo_atm_dol_min_u6m').cast('double').alias('can_mto_tmo_atm_dol_min_u6m'),
        col('can_mto_tmo_atm_dol_min_u9m').cast('double').alias('can_mto_tmo_atm_dol_min_u9m'),
        col('can_mto_tmo_atm_dol_min_u12').cast('double').alias('can_mto_tmo_atm_dol_min_u12'),
        col('can_mto_tmo_atm_ret_min_u3m').cast('double').alias('can_mto_tmo_atm_ret_min_u3m'),
        col('can_mto_tmo_atm_ret_min_u6m').cast('double').alias('can_mto_tmo_atm_ret_min_u6m'),
        col('can_mto_tmo_atm_ret_min_u9m').cast('double').alias('can_mto_tmo_atm_ret_min_u9m'),
        col('can_mto_tmo_atm_ret_min_u12').cast('double').alias('can_mto_tmo_atm_ret_min_u12'),
        col('can_mto_tmo_atm_ret_sol_min_u3m').cast('double').alias('can_mto_tmo_atm_ret_sol_min_u3m'),
        col('can_mto_tmo_atm_ret_sol_min_u6m').cast('double').alias('can_mto_tmo_atm_ret_sol_min_u6m'),
        col('can_mto_tmo_atm_ret_sol_min_u9m').cast('double').alias('can_mto_tmo_atm_ret_sol_min_u9m'),
        col('can_mto_tmo_atm_ret_sol_min_u12').cast('double').alias('can_mto_tmo_atm_ret_sol_min_u12'),
        col('can_mto_tmo_atm_ret_dol_min_u3m').cast('double').alias('can_mto_tmo_atm_ret_dol_min_u3m'),
        col('can_mto_tmo_atm_ret_dol_min_u6m').cast('double').alias('can_mto_tmo_atm_ret_dol_min_u6m'),
        col('can_mto_tmo_atm_ret_dol_min_u9m').cast('double').alias('can_mto_tmo_atm_ret_dol_min_u9m'),
        col('can_mto_tmo_atm_ret_dol_min_u12').cast('double').alias('can_mto_tmo_atm_ret_dol_min_u12'),
        col('can_mto_tmo_atm_dep_min_u3m').cast('double').alias('can_mto_tmo_atm_dep_min_u3m'),
        col('can_mto_tmo_atm_dep_min_u6m').cast('double').alias('can_mto_tmo_atm_dep_min_u6m'),
        col('can_mto_tmo_atm_dep_min_u9m').cast('double').alias('can_mto_tmo_atm_dep_min_u9m'),
        col('can_mto_tmo_atm_dep_min_u12').cast('double').alias('can_mto_tmo_atm_dep_min_u12'),
        col('can_mto_tmo_atm_dep_sol_min_u3m').cast('double').alias('can_mto_tmo_atm_dep_sol_min_u3m'),
        col('can_mto_tmo_atm_dep_sol_min_u6m').cast('double').alias('can_mto_tmo_atm_dep_sol_min_u6m'),
        col('can_mto_tmo_atm_dep_sol_min_u9m').cast('double').alias('can_mto_tmo_atm_dep_sol_min_u9m'),
        col('can_mto_tmo_atm_dep_sol_min_u12').cast('double').alias('can_mto_tmo_atm_dep_sol_min_u12'),
        col('can_mto_tmo_atm_dep_dol_min_u3m').cast('double').alias('can_mto_tmo_atm_dep_dol_min_u3m'),
        col('can_mto_tmo_atm_dep_dol_min_u6m').cast('double').alias('can_mto_tmo_atm_dep_dol_min_u6m'),
        col('can_mto_tmo_atm_dep_dol_min_u9m').cast('double').alias('can_mto_tmo_atm_dep_dol_min_u9m'),
        col('can_mto_tmo_atm_dep_dol_min_u12').cast('double').alias('can_mto_tmo_atm_dep_dol_min_u12'),
        col('can_mto_tmo_atm_trf_min_u3m').cast('double').alias('can_mto_tmo_atm_trf_min_u3m'),
        col('can_mto_tmo_atm_trf_min_u6m').cast('double').alias('can_mto_tmo_atm_trf_min_u6m'),
        col('can_mto_tmo_atm_trf_min_u9m').cast('double').alias('can_mto_tmo_atm_trf_min_u9m'),
        col('can_mto_tmo_atm_trf_min_u12').cast('double').alias('can_mto_tmo_atm_trf_min_u12'),
        col('can_mto_tmo_atm_trf_sol_min_u3m').cast('double').alias('can_mto_tmo_atm_trf_sol_min_u3m'),
        col('can_mto_tmo_atm_trf_sol_min_u6m').cast('double').alias('can_mto_tmo_atm_trf_sol_min_u6m'),
        col('can_mto_tmo_atm_trf_sol_min_u9m').cast('double').alias('can_mto_tmo_atm_trf_sol_min_u9m'),
        col('can_mto_tmo_atm_trf_sol_min_u12').cast('double').alias('can_mto_tmo_atm_trf_sol_min_u12'),
        col('can_mto_tmo_atm_trf_dol_min_u3m').cast('double').alias('can_mto_tmo_atm_trf_dol_min_u3m'),
        col('can_mto_tmo_atm_trf_dol_min_u6m').cast('double').alias('can_mto_tmo_atm_trf_dol_min_u6m'),
        col('can_mto_tmo_atm_trf_dol_min_u9m').cast('double').alias('can_mto_tmo_atm_trf_dol_min_u9m'),
        col('can_mto_tmo_atm_trf_dol_min_u12').cast('double').alias('can_mto_tmo_atm_trf_dol_min_u12'),
        col('can_mto_tmo_atm_ads_min_u3m').cast('double').alias('can_mto_tmo_atm_ads_min_u3m'),
        col('can_mto_tmo_atm_ads_min_u6m').cast('double').alias('can_mto_tmo_atm_ads_min_u6m'),
        col('can_mto_tmo_atm_ads_min_u9m').cast('double').alias('can_mto_tmo_atm_ads_min_u9m'),
        col('can_mto_tmo_atm_ads_min_u12').cast('double').alias('can_mto_tmo_atm_ads_min_u12'),
        col('can_mto_tmo_atm_dis_min_u3m').cast('double').alias('can_mto_tmo_atm_dis_min_u3m'),
        col('can_mto_tmo_atm_dis_min_u6m').cast('double').alias('can_mto_tmo_atm_dis_min_u6m'),
        col('can_mto_tmo_atm_dis_min_u9m').cast('double').alias('can_mto_tmo_atm_dis_min_u9m'),
        col('can_mto_tmo_atm_dis_min_u12').cast('double').alias('can_mto_tmo_atm_dis_min_u12'),
        col('can_mto_tmo_atm_pag_tcr_min_u3m').cast('double').alias('can_mto_tmo_atm_pag_tcr_min_u3m'),
        col('can_mto_tmo_atm_pag_tcr_min_u6m').cast('double').alias('can_mto_tmo_atm_pag_tcr_min_u6m'),
        col('can_mto_tmo_atm_pag_tcr_min_u9m').cast('double').alias('can_mto_tmo_atm_pag_tcr_min_u9m'),
        col('can_mto_tmo_atm_pag_tcr_min_u12').cast('double').alias('can_mto_tmo_atm_pag_tcr_min_u12'),
        col('can_mto_tmo_atm_pag_srv_min_u3m').cast('double').alias('can_mto_tmo_atm_pag_srv_min_u3m'),
        col('can_mto_tmo_atm_pag_srv_min_u6m').cast('double').alias('can_mto_tmo_atm_pag_srv_min_u6m'),
        col('can_mto_tmo_atm_pag_srv_min_u9m').cast('double').alias('can_mto_tmo_atm_pag_srv_min_u9m'),
        col('can_mto_tmo_atm_pag_srv_min_u12').cast('double').alias('can_mto_tmo_atm_pag_srv_min_u12'),
        col('can_tkt_tmo_atm_min_u3m').cast('double').alias('can_tkt_tmo_atm_min_u3m'),
        col('can_tkt_tmo_atm_min_u6m').cast('double').alias('can_tkt_tmo_atm_min_u6m'),
        col('can_tkt_tmo_atm_min_u9m').cast('double').alias('can_tkt_tmo_atm_min_u9m'),
        col('can_tkt_tmo_atm_min_u12').cast('double').alias('can_tkt_tmo_atm_min_u12'),
        col('can_tkt_tmo_atm_sol_min_u3m').cast('double').alias('can_tkt_tmo_atm_sol_min_u3m'),
        col('can_tkt_tmo_atm_sol_min_u6m').cast('double').alias('can_tkt_tmo_atm_sol_min_u6m'),
        col('can_tkt_tmo_atm_sol_min_u9m').cast('double').alias('can_tkt_tmo_atm_sol_min_u9m'),
        col('can_tkt_tmo_atm_sol_min_u12').cast('double').alias('can_tkt_tmo_atm_sol_min_u12'),
        col('can_tkt_tmo_atm_dol_min_u3m').cast('double').alias('can_tkt_tmo_atm_dol_min_u3m'),
        col('can_tkt_tmo_atm_dol_min_u6m').cast('double').alias('can_tkt_tmo_atm_dol_min_u6m'),
        col('can_tkt_tmo_atm_dol_min_u9m').cast('double').alias('can_tkt_tmo_atm_dol_min_u9m'),
        col('can_tkt_tmo_atm_dol_min_u12').cast('double').alias('can_tkt_tmo_atm_dol_min_u12'),
        col('can_tkt_tmo_atm_ret_min_u3m').cast('double').alias('can_tkt_tmo_atm_ret_min_u3m'),
        col('can_tkt_tmo_atm_ret_min_u6m').cast('double').alias('can_tkt_tmo_atm_ret_min_u6m'),
        col('can_tkt_tmo_atm_ret_min_u9m').cast('double').alias('can_tkt_tmo_atm_ret_min_u9m'),
        col('can_tkt_tmo_atm_ret_min_u12').cast('double').alias('can_tkt_tmo_atm_ret_min_u12'),
        col('can_tkt_tmo_atm_ret_sol_min_u3m').cast('double').alias('can_tkt_tmo_atm_ret_sol_min_u3m'),
        col('can_tkt_tmo_atm_ret_sol_min_u6m').cast('double').alias('can_tkt_tmo_atm_ret_sol_min_u6m'),
        col('can_tkt_tmo_atm_ret_sol_min_u9m').cast('double').alias('can_tkt_tmo_atm_ret_sol_min_u9m'),
        col('can_tkt_tmo_atm_ret_sol_min_u12').cast('double').alias('can_tkt_tmo_atm_ret_sol_min_u12'),
        col('can_tkt_tmo_atm_ret_dol_min_u3m').cast('double').alias('can_tkt_tmo_atm_ret_dol_min_u3m'),
        col('can_tkt_tmo_atm_ret_dol_min_u6m').cast('double').alias('can_tkt_tmo_atm_ret_dol_min_u6m'),
        col('can_tkt_tmo_atm_ret_dol_min_u9m').cast('double').alias('can_tkt_tmo_atm_ret_dol_min_u9m'),
        col('can_tkt_tmo_atm_ret_dol_min_u12').cast('double').alias('can_tkt_tmo_atm_ret_dol_min_u12'),
        col('can_tkt_tmo_atm_dep_min_u3m').cast('double').alias('can_tkt_tmo_atm_dep_min_u3m'),
        col('can_tkt_tmo_atm_dep_min_u6m').cast('double').alias('can_tkt_tmo_atm_dep_min_u6m'),
        col('can_tkt_tmo_atm_dep_min_u9m').cast('double').alias('can_tkt_tmo_atm_dep_min_u9m'),
        col('can_tkt_tmo_atm_dep_min_u12').cast('double').alias('can_tkt_tmo_atm_dep_min_u12'),
        col('can_tkt_tmo_atm_dep_sol_min_u3m').cast('double').alias('can_tkt_tmo_atm_dep_sol_min_u3m'),
        col('can_tkt_tmo_atm_dep_sol_min_u6m').cast('double').alias('can_tkt_tmo_atm_dep_sol_min_u6m'),
        col('can_tkt_tmo_atm_dep_sol_min_u9m').cast('double').alias('can_tkt_tmo_atm_dep_sol_min_u9m'),
        col('can_tkt_tmo_atm_dep_sol_min_u12').cast('double').alias('can_tkt_tmo_atm_dep_sol_min_u12'),
        col('can_tkt_tmo_atm_dep_dol_min_u3m').cast('double').alias('can_tkt_tmo_atm_dep_dol_min_u3m'),
        col('can_tkt_tmo_atm_dep_dol_min_u6m').cast('double').alias('can_tkt_tmo_atm_dep_dol_min_u6m'),
        col('can_tkt_tmo_atm_dep_dol_min_u9m').cast('double').alias('can_tkt_tmo_atm_dep_dol_min_u9m'),
        col('can_tkt_tmo_atm_dep_dol_min_u12').cast('double').alias('can_tkt_tmo_atm_dep_dol_min_u12'),
        col('can_tkt_tmo_atm_trf_min_u3m').cast('double').alias('can_tkt_tmo_atm_trf_min_u3m'),
        col('can_tkt_tmo_atm_trf_min_u6m').cast('double').alias('can_tkt_tmo_atm_trf_min_u6m'),
        col('can_tkt_tmo_atm_trf_min_u9m').cast('double').alias('can_tkt_tmo_atm_trf_min_u9m'),
        col('can_tkt_tmo_atm_trf_min_u12').cast('double').alias('can_tkt_tmo_atm_trf_min_u12'),
        col('can_tkt_tmo_atm_trf_sol_min_u3m').cast('double').alias('can_tkt_tmo_atm_trf_sol_min_u3m'),
        col('can_tkt_tmo_atm_trf_sol_min_u6m').cast('double').alias('can_tkt_tmo_atm_trf_sol_min_u6m'),
        col('can_tkt_tmo_atm_trf_sol_min_u9m').cast('double').alias('can_tkt_tmo_atm_trf_sol_min_u9m'),
        col('can_tkt_tmo_atm_trf_sol_min_u12').cast('double').alias('can_tkt_tmo_atm_trf_sol_min_u12'),
        col('can_tkt_tmo_atm_trf_dol_min_u3m').cast('double').alias('can_tkt_tmo_atm_trf_dol_min_u3m'),
        col('can_tkt_tmo_atm_trf_dol_min_u6m').cast('double').alias('can_tkt_tmo_atm_trf_dol_min_u6m'),
        col('can_tkt_tmo_atm_trf_dol_min_u9m').cast('double').alias('can_tkt_tmo_atm_trf_dol_min_u9m'),
        col('can_tkt_tmo_atm_trf_dol_min_u12').cast('double').alias('can_tkt_tmo_atm_trf_dol_min_u12'),
        col('can_tkt_tmo_atm_ads_min_u3m').cast('double').alias('can_tkt_tmo_atm_ads_min_u3m'),
        col('can_tkt_tmo_atm_ads_min_u6m').cast('double').alias('can_tkt_tmo_atm_ads_min_u6m'),
        col('can_tkt_tmo_atm_ads_min_u9m').cast('double').alias('can_tkt_tmo_atm_ads_min_u9m'),
        col('can_tkt_tmo_atm_ads_min_u12').cast('double').alias('can_tkt_tmo_atm_ads_min_u12'),
        col('can_tkt_tmo_atm_dis_min_u3m').cast('double').alias('can_tkt_tmo_atm_dis_min_u3m'),
        col('can_tkt_tmo_atm_dis_min_u6m').cast('double').alias('can_tkt_tmo_atm_dis_min_u6m'),
        col('can_tkt_tmo_atm_dis_min_u9m').cast('double').alias('can_tkt_tmo_atm_dis_min_u9m'),
        col('can_tkt_tmo_atm_dis_min_u12').cast('double').alias('can_tkt_tmo_atm_dis_min_u12'),
        col('can_tkt_tmo_atm_pag_tcr_min_u3m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_min_u3m'),
        col('can_tkt_tmo_atm_pag_tcr_min_u6m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_min_u6m'),
        col('can_tkt_tmo_atm_pag_tcr_min_u9m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_min_u9m'),
        col('can_tkt_tmo_atm_pag_tcr_min_u12').cast('double').alias('can_tkt_tmo_atm_pag_tcr_min_u12'),
        col('can_tkt_tmo_atm_pag_srv_min_u3m').cast('double').alias('can_tkt_tmo_atm_pag_srv_min_u3m'),
        col('can_tkt_tmo_atm_pag_srv_min_u6m').cast('double').alias('can_tkt_tmo_atm_pag_srv_min_u6m'),
        col('can_tkt_tmo_atm_pag_srv_min_u9m').cast('double').alias('can_tkt_tmo_atm_pag_srv_min_u9m'),
        col('can_tkt_tmo_atm_pag_srv_min_u12').cast('double').alias('can_tkt_tmo_atm_pag_srv_min_u12'),
        col('can_ctd_tmo_atm_u1m').cast('double').alias('can_ctd_tmo_atm_u1m'),
        col('can_ctd_tmo_atm_prm_u3m').cast('double').alias('can_ctd_tmo_atm_prm_u3m'),
        col('can_ctd_tmo_atm_prm_u6m').cast('double').alias('can_ctd_tmo_atm_prm_u6m'),
        col('can_ctd_tmo_atm_prm_u9m').cast('double').alias('can_ctd_tmo_atm_prm_u9m'),
        col('can_ctd_tmo_atm_prm_u12').cast('double').alias('can_ctd_tmo_atm_prm_u12'),
        col('can_ctd_tnm_atm_u1m').cast('double').alias('can_ctd_tnm_atm_u1m'),
        col('can_ctd_tnm_atm_prm_u3m').cast('double').alias('can_ctd_tnm_atm_prm_u3m'),
        col('can_ctd_tnm_atm_prm_u6m').cast('double').alias('can_ctd_tnm_atm_prm_u6m'),
        col('can_ctd_tnm_atm_prm_u9m').cast('double').alias('can_ctd_tnm_atm_prm_u9m'),
        col('can_ctd_tnm_atm_prm_u12').cast('double').alias('can_ctd_tnm_atm_prm_u12'),
        col('can_ctd_tmo_atm_sol_u1m').cast('double').alias('can_ctd_tmo_atm_sol_u1m'),
        col('can_ctd_tmo_atm_sol_prm_u3m').cast('double').alias('can_ctd_tmo_atm_sol_prm_u3m'),
        col('can_ctd_tmo_atm_sol_prm_u6m').cast('double').alias('can_ctd_tmo_atm_sol_prm_u6m'),
        col('can_ctd_tmo_atm_sol_prm_u9m').cast('double').alias('can_ctd_tmo_atm_sol_prm_u9m'),
        col('can_ctd_tmo_atm_sol_prm_u12').cast('double').alias('can_ctd_tmo_atm_sol_prm_u12'),
        col('can_ctd_tmo_atm_dol_u1m').cast('double').alias('can_ctd_tmo_atm_dol_u1m'),
        col('can_ctd_tmo_atm_dol_prm_u3m').cast('double').alias('can_ctd_tmo_atm_dol_prm_u3m'),
        col('can_ctd_tmo_atm_dol_prm_u6m').cast('double').alias('can_ctd_tmo_atm_dol_prm_u6m'),
        col('can_ctd_tmo_atm_dol_prm_u9m').cast('double').alias('can_ctd_tmo_atm_dol_prm_u9m'),
        col('can_ctd_tmo_atm_dol_prm_u12').cast('double').alias('can_ctd_tmo_atm_dol_prm_u12'),
        col('can_ctd_tmo_atm_ret_u1m').cast('double').alias('can_ctd_tmo_atm_ret_u1m'),
        col('can_ctd_tmo_atm_ret_prm_u3m').cast('double').alias('can_ctd_tmo_atm_ret_prm_u3m'),
        col('can_ctd_tmo_atm_ret_prm_u6m').cast('double').alias('can_ctd_tmo_atm_ret_prm_u6m'),
        col('can_ctd_tmo_atm_ret_prm_u9m').cast('double').alias('can_ctd_tmo_atm_ret_prm_u9m'),
        col('can_ctd_tmo_atm_ret_prm_u12').cast('double').alias('can_ctd_tmo_atm_ret_prm_u12'),
        col('can_ctd_tmo_atm_ret_sol_u1m').cast('double').alias('can_ctd_tmo_atm_ret_sol_u1m'),
        col('can_ctd_tmo_atm_ret_sol_prm_u3m').cast('double').alias('can_ctd_tmo_atm_ret_sol_prm_u3m'),
        col('can_ctd_tmo_atm_ret_sol_prm_u6m').cast('double').alias('can_ctd_tmo_atm_ret_sol_prm_u6m'),
        col('can_ctd_tmo_atm_ret_sol_prm_u9m').cast('double').alias('can_ctd_tmo_atm_ret_sol_prm_u9m'),
        col('can_ctd_tmo_atm_ret_sol_prm_u12').cast('double').alias('can_ctd_tmo_atm_ret_sol_prm_u12'),
        col('can_ctd_tmo_atm_ret_dol_u1m').cast('double').alias('can_ctd_tmo_atm_ret_dol_u1m'),
        col('can_ctd_tmo_atm_ret_dol_prm_u3m').cast('double').alias('can_ctd_tmo_atm_ret_dol_prm_u3m'),
        col('can_ctd_tmo_atm_ret_dol_prm_u6m').cast('double').alias('can_ctd_tmo_atm_ret_dol_prm_u6m'),
        col('can_ctd_tmo_atm_ret_dol_prm_u9m').cast('double').alias('can_ctd_tmo_atm_ret_dol_prm_u9m'),
        col('can_ctd_tmo_atm_ret_dol_prm_u12').cast('double').alias('can_ctd_tmo_atm_ret_dol_prm_u12'),
        col('can_ctd_tmo_atm_dep_u1m').cast('double').alias('can_ctd_tmo_atm_dep_u1m'),
        col('can_ctd_tmo_atm_dep_prm_u3m').cast('double').alias('can_ctd_tmo_atm_dep_prm_u3m'),
        col('can_ctd_tmo_atm_dep_prm_u6m').cast('double').alias('can_ctd_tmo_atm_dep_prm_u6m'),
        col('can_ctd_tmo_atm_dep_prm_u9m').cast('double').alias('can_ctd_tmo_atm_dep_prm_u9m'),
        col('can_ctd_tmo_atm_dep_prm_u12').cast('double').alias('can_ctd_tmo_atm_dep_prm_u12'),
        col('can_ctd_tmo_atm_dep_sol_u1m').cast('double').alias('can_ctd_tmo_atm_dep_sol_u1m'),
        col('can_ctd_tmo_atm_dep_sol_prm_u3m').cast('double').alias('can_ctd_tmo_atm_dep_sol_prm_u3m'),
        col('can_ctd_tmo_atm_dep_sol_prm_u6m').cast('double').alias('can_ctd_tmo_atm_dep_sol_prm_u6m'),
        col('can_ctd_tmo_atm_dep_sol_prm_u9m').cast('double').alias('can_ctd_tmo_atm_dep_sol_prm_u9m'),
        col('can_ctd_tmo_atm_dep_sol_prm_u12').cast('double').alias('can_ctd_tmo_atm_dep_sol_prm_u12'),
        col('can_ctd_tmo_atm_dep_dol_u1m').cast('double').alias('can_ctd_tmo_atm_dep_dol_u1m'),
        col('can_ctd_tmo_atm_dep_dol_prm_u3m').cast('double').alias('can_ctd_tmo_atm_dep_dol_prm_u3m'),
        col('can_ctd_tmo_atm_dep_dol_prm_u6m').cast('double').alias('can_ctd_tmo_atm_dep_dol_prm_u6m'),
        col('can_ctd_tmo_atm_dep_dol_prm_u9m').cast('double').alias('can_ctd_tmo_atm_dep_dol_prm_u9m'),
        col('can_ctd_tmo_atm_dep_dol_prm_u12').cast('double').alias('can_ctd_tmo_atm_dep_dol_prm_u12'),
        col('can_ctd_tmo_atm_trf_u1m').cast('double').alias('can_ctd_tmo_atm_trf_u1m'),
        col('can_ctd_tmo_atm_trf_prm_u3m').cast('double').alias('can_ctd_tmo_atm_trf_prm_u3m'),
        col('can_ctd_tmo_atm_trf_prm_u6m').cast('double').alias('can_ctd_tmo_atm_trf_prm_u6m'),
        col('can_ctd_tmo_atm_trf_prm_u9m').cast('double').alias('can_ctd_tmo_atm_trf_prm_u9m'),
        col('can_ctd_tmo_atm_trf_prm_u12').cast('double').alias('can_ctd_tmo_atm_trf_prm_u12'),
        col('can_ctd_tmo_atm_trf_sol_u1m').cast('double').alias('can_ctd_tmo_atm_trf_sol_u1m'),
        col('can_ctd_tmo_atm_trf_sol_prm_u3m').cast('double').alias('can_ctd_tmo_atm_trf_sol_prm_u3m'),
        col('can_ctd_tmo_atm_trf_sol_prm_u6m').cast('double').alias('can_ctd_tmo_atm_trf_sol_prm_u6m'),
        col('can_ctd_tmo_atm_trf_sol_prm_u9m').cast('double').alias('can_ctd_tmo_atm_trf_sol_prm_u9m'),
        col('can_ctd_tmo_atm_trf_sol_prm_u12').cast('double').alias('can_ctd_tmo_atm_trf_sol_prm_u12'),
        col('can_ctd_tmo_atm_trf_dol_u1m').cast('double').alias('can_ctd_tmo_atm_trf_dol_u1m'),
        col('can_ctd_tmo_atm_trf_dol_prm_u3m').cast('double').alias('can_ctd_tmo_atm_trf_dol_prm_u3m'),
        col('can_ctd_tmo_atm_trf_dol_prm_u6m').cast('double').alias('can_ctd_tmo_atm_trf_dol_prm_u6m'),
        col('can_ctd_tmo_atm_trf_dol_prm_u9m').cast('double').alias('can_ctd_tmo_atm_trf_dol_prm_u9m'),
        col('can_ctd_tmo_atm_trf_dol_prm_u12').cast('double').alias('can_ctd_tmo_atm_trf_dol_prm_u12'),
        col('can_ctd_tmo_atm_ads_u1m').cast('double').alias('can_ctd_tmo_atm_ads_u1m'),
        col('can_ctd_tmo_atm_ads_prm_u3m').cast('double').alias('can_ctd_tmo_atm_ads_prm_u3m'),
        col('can_ctd_tmo_atm_ads_prm_u6m').cast('double').alias('can_ctd_tmo_atm_ads_prm_u6m'),
        col('can_ctd_tmo_atm_ads_prm_u9m').cast('double').alias('can_ctd_tmo_atm_ads_prm_u9m'),
        col('can_ctd_tmo_atm_ads_prm_u12').cast('double').alias('can_ctd_tmo_atm_ads_prm_u12'),
        col('can_ctd_tmo_atm_dis_u1m').cast('double').alias('can_ctd_tmo_atm_dis_u1m'),
        col('can_ctd_tmo_atm_dis_prm_u3m').cast('double').alias('can_ctd_tmo_atm_dis_prm_u3m'),
        col('can_ctd_tmo_atm_dis_prm_u6m').cast('double').alias('can_ctd_tmo_atm_dis_prm_u6m'),
        col('can_ctd_tmo_atm_dis_prm_u9m').cast('double').alias('can_ctd_tmo_atm_dis_prm_u9m'),
        col('can_ctd_tmo_atm_dis_prm_u12').cast('double').alias('can_ctd_tmo_atm_dis_prm_u12'),
        col('can_ctd_tmo_atm_pag_tcr_u1m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_u1m'),
        col('can_ctd_tmo_atm_pag_tcr_prm_u3m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_prm_u3m'),
        col('can_ctd_tmo_atm_pag_tcr_prm_u6m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_prm_u6m'),
        col('can_ctd_tmo_atm_pag_tcr_prm_u9m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_prm_u9m'),
        col('can_ctd_tmo_atm_pag_tcr_prm_u12').cast('double').alias('can_ctd_tmo_atm_pag_tcr_prm_u12'),
        col('can_ctd_tmo_atm_pag_srv_u1m').cast('double').alias('can_ctd_tmo_atm_pag_srv_u1m'),
        col('can_ctd_tmo_atm_pag_srv_prm_u3m').cast('double').alias('can_ctd_tmo_atm_pag_srv_prm_u3m'),
        col('can_ctd_tmo_atm_pag_srv_prm_u6m').cast('double').alias('can_ctd_tmo_atm_pag_srv_prm_u6m'),
        col('can_ctd_tmo_atm_pag_srv_prm_u9m').cast('double').alias('can_ctd_tmo_atm_pag_srv_prm_u9m'),
        col('can_ctd_tmo_atm_pag_srv_prm_u12').cast('double').alias('can_ctd_tmo_atm_pag_srv_prm_u12'),
        col('can_ctd_tnm_atm_cslt_u1m').cast('double').alias('can_ctd_tnm_atm_cslt_u1m'),
        col('can_ctd_tnm_atm_cslt_prm_u3m').cast('double').alias('can_ctd_tnm_atm_cslt_prm_u3m'),
        col('can_ctd_tnm_atm_cslt_prm_u6m').cast('double').alias('can_ctd_tnm_atm_cslt_prm_u6m'),
        col('can_ctd_tnm_atm_cslt_prm_u9m').cast('double').alias('can_ctd_tnm_atm_cslt_prm_u9m'),
        col('can_ctd_tnm_atm_cslt_prm_u12').cast('double').alias('can_ctd_tnm_atm_cslt_prm_u12'),
        col('can_ctd_tnm_atm_admn_u1m').cast('double').alias('can_ctd_tnm_atm_admn_u1m'),
        col('can_ctd_tnm_atm_admn_prm_u3m').cast('double').alias('can_ctd_tnm_atm_admn_prm_u3m'),
        col('can_ctd_tnm_atm_admn_prm_u6m').cast('double').alias('can_ctd_tnm_atm_admn_prm_u6m'),
        col('can_ctd_tnm_atm_admn_prm_u9m').cast('double').alias('can_ctd_tnm_atm_admn_prm_u9m'),
        col('can_ctd_tnm_atm_admn_prm_u12').cast('double').alias('can_ctd_tnm_atm_admn_prm_u12'),
        col('can_ctd_tmo_atm_max_u3m').cast('double').alias('can_ctd_tmo_atm_max_u3m'),
        col('can_ctd_tmo_atm_max_u6m').cast('double').alias('can_ctd_tmo_atm_max_u6m'),
        col('can_ctd_tmo_atm_max_u9m').cast('double').alias('can_ctd_tmo_atm_max_u9m'),
        col('can_ctd_tmo_atm_max_u12').cast('double').alias('can_ctd_tmo_atm_max_u12'),
        col('can_ctd_tnm_atm_max_u3m').cast('double').alias('can_ctd_tnm_atm_max_u3m'),
        col('can_ctd_tnm_atm_max_u6m').cast('double').alias('can_ctd_tnm_atm_max_u6m'),
        col('can_ctd_tnm_atm_max_u9m').cast('double').alias('can_ctd_tnm_atm_max_u9m'),
        col('can_ctd_tnm_atm_max_u12').cast('double').alias('can_ctd_tnm_atm_max_u12'),
        col('can_ctd_tmo_atm_sol_max_u3m').cast('double').alias('can_ctd_tmo_atm_sol_max_u3m'),
        col('can_ctd_tmo_atm_sol_max_u6m').cast('double').alias('can_ctd_tmo_atm_sol_max_u6m'),
        col('can_ctd_tmo_atm_sol_max_u9m').cast('double').alias('can_ctd_tmo_atm_sol_max_u9m'),
        col('can_ctd_tmo_atm_sol_max_u12').cast('double').alias('can_ctd_tmo_atm_sol_max_u12'),
        col('can_ctd_tmo_atm_dol_max_u3m').cast('double').alias('can_ctd_tmo_atm_dol_max_u3m'),
        col('can_ctd_tmo_atm_dol_max_u6m').cast('double').alias('can_ctd_tmo_atm_dol_max_u6m'),
        col('can_ctd_tmo_atm_dol_max_u9m').cast('double').alias('can_ctd_tmo_atm_dol_max_u9m'),
        col('can_ctd_tmo_atm_dol_max_u12').cast('double').alias('can_ctd_tmo_atm_dol_max_u12'),
        col('can_ctd_tmo_atm_ret_max_u3m').cast('double').alias('can_ctd_tmo_atm_ret_max_u3m'),
        col('can_ctd_tmo_atm_ret_max_u6m').cast('double').alias('can_ctd_tmo_atm_ret_max_u6m'),
        col('can_ctd_tmo_atm_ret_max_u9m').cast('double').alias('can_ctd_tmo_atm_ret_max_u9m'),
        col('can_ctd_tmo_atm_ret_max_u12').cast('double').alias('can_ctd_tmo_atm_ret_max_u12'),
        col('can_ctd_tmo_atm_ret_sol_max_u3m').cast('double').alias('can_ctd_tmo_atm_ret_sol_max_u3m'),
        col('can_ctd_tmo_atm_ret_sol_max_u6m').cast('double').alias('can_ctd_tmo_atm_ret_sol_max_u6m'),
        col('can_ctd_tmo_atm_ret_sol_max_u9m').cast('double').alias('can_ctd_tmo_atm_ret_sol_max_u9m'),
        col('can_ctd_tmo_atm_ret_sol_max_u12').cast('double').alias('can_ctd_tmo_atm_ret_sol_max_u12'),
        col('can_ctd_tmo_atm_ret_dol_max_u3m').cast('double').alias('can_ctd_tmo_atm_ret_dol_max_u3m'),
        col('can_ctd_tmo_atm_ret_dol_max_u6m').cast('double').alias('can_ctd_tmo_atm_ret_dol_max_u6m'),
        col('can_ctd_tmo_atm_ret_dol_max_u9m').cast('double').alias('can_ctd_tmo_atm_ret_dol_max_u9m'),
        col('can_ctd_tmo_atm_ret_dol_max_u12').cast('double').alias('can_ctd_tmo_atm_ret_dol_max_u12'),
        col('can_ctd_tmo_atm_dep_max_u3m').cast('double').alias('can_ctd_tmo_atm_dep_max_u3m'),
        col('can_ctd_tmo_atm_dep_max_u6m').cast('double').alias('can_ctd_tmo_atm_dep_max_u6m'),
        col('can_ctd_tmo_atm_dep_max_u9m').cast('double').alias('can_ctd_tmo_atm_dep_max_u9m'),
        col('can_ctd_tmo_atm_dep_max_u12').cast('double').alias('can_ctd_tmo_atm_dep_max_u12'),
        col('can_ctd_tmo_atm_dep_sol_max_u3m').cast('double').alias('can_ctd_tmo_atm_dep_sol_max_u3m'),
        col('can_ctd_tmo_atm_dep_sol_max_u6m').cast('double').alias('can_ctd_tmo_atm_dep_sol_max_u6m'),
        col('can_ctd_tmo_atm_dep_sol_max_u9m').cast('double').alias('can_ctd_tmo_atm_dep_sol_max_u9m'),
        col('can_ctd_tmo_atm_dep_sol_max_u12').cast('double').alias('can_ctd_tmo_atm_dep_sol_max_u12'),
        col('can_ctd_tmo_atm_dep_dol_max_u3m').cast('double').alias('can_ctd_tmo_atm_dep_dol_max_u3m'),
        col('can_ctd_tmo_atm_dep_dol_max_u6m').cast('double').alias('can_ctd_tmo_atm_dep_dol_max_u6m'),
        col('can_ctd_tmo_atm_dep_dol_max_u9m').cast('double').alias('can_ctd_tmo_atm_dep_dol_max_u9m'),
        col('can_ctd_tmo_atm_dep_dol_max_u12').cast('double').alias('can_ctd_tmo_atm_dep_dol_max_u12'),
        col('can_ctd_tmo_atm_trf_max_u3m').cast('double').alias('can_ctd_tmo_atm_trf_max_u3m'),
        col('can_ctd_tmo_atm_trf_max_u6m').cast('double').alias('can_ctd_tmo_atm_trf_max_u6m'),
        col('can_ctd_tmo_atm_trf_max_u9m').cast('double').alias('can_ctd_tmo_atm_trf_max_u9m'),
        col('can_ctd_tmo_atm_trf_max_u12').cast('double').alias('can_ctd_tmo_atm_trf_max_u12'),
        col('can_ctd_tmo_atm_trf_sol_max_u3m').cast('double').alias('can_ctd_tmo_atm_trf_sol_max_u3m'),
        col('can_ctd_tmo_atm_trf_sol_max_u6m').cast('double').alias('can_ctd_tmo_atm_trf_sol_max_u6m'),
        col('can_ctd_tmo_atm_trf_sol_max_u9m').cast('double').alias('can_ctd_tmo_atm_trf_sol_max_u9m'),
        col('can_ctd_tmo_atm_trf_sol_max_u12').cast('double').alias('can_ctd_tmo_atm_trf_sol_max_u12'),
        col('can_ctd_tmo_atm_trf_dol_max_u3m').cast('double').alias('can_ctd_tmo_atm_trf_dol_max_u3m'),
        col('can_ctd_tmo_atm_trf_dol_max_u6m').cast('double').alias('can_ctd_tmo_atm_trf_dol_max_u6m'),
        col('can_ctd_tmo_atm_trf_dol_max_u9m').cast('double').alias('can_ctd_tmo_atm_trf_dol_max_u9m'),
        col('can_ctd_tmo_atm_trf_dol_max_u12').cast('double').alias('can_ctd_tmo_atm_trf_dol_max_u12'),
        col('can_ctd_tmo_atm_ads_max_u3m').cast('double').alias('can_ctd_tmo_atm_ads_max_u3m'),
        col('can_ctd_tmo_atm_ads_max_u6m').cast('double').alias('can_ctd_tmo_atm_ads_max_u6m'),
        col('can_ctd_tmo_atm_ads_max_u9m').cast('double').alias('can_ctd_tmo_atm_ads_max_u9m'),
        col('can_ctd_tmo_atm_ads_max_u12').cast('double').alias('can_ctd_tmo_atm_ads_max_u12'),
        col('can_ctd_tmo_atm_dis_max_u3m').cast('double').alias('can_ctd_tmo_atm_dis_max_u3m'),
        col('can_ctd_tmo_atm_dis_max_u6m').cast('double').alias('can_ctd_tmo_atm_dis_max_u6m'),
        col('can_ctd_tmo_atm_dis_max_u9m').cast('double').alias('can_ctd_tmo_atm_dis_max_u9m'),
        col('can_ctd_tmo_atm_dis_max_u12').cast('double').alias('can_ctd_tmo_atm_dis_max_u12'),
        col('can_ctd_tmo_atm_pag_tcr_max_u3m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_max_u3m'),
        col('can_ctd_tmo_atm_pag_tcr_max_u6m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_max_u6m'),
        col('can_ctd_tmo_atm_pag_tcr_max_u9m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_max_u9m'),
        col('can_ctd_tmo_atm_pag_tcr_max_u12').cast('double').alias('can_ctd_tmo_atm_pag_tcr_max_u12'),
        col('can_ctd_tmo_atm_pag_srv_max_u3m').cast('double').alias('can_ctd_tmo_atm_pag_srv_max_u3m'),
        col('can_ctd_tmo_atm_pag_srv_max_u6m').cast('double').alias('can_ctd_tmo_atm_pag_srv_max_u6m'),
        col('can_ctd_tmo_atm_pag_srv_max_u9m').cast('double').alias('can_ctd_tmo_atm_pag_srv_max_u9m'),
        col('can_ctd_tmo_atm_pag_srv_max_u12').cast('double').alias('can_ctd_tmo_atm_pag_srv_max_u12'),
        col('can_ctd_tnm_atm_cslt_max_u3m').cast('double').alias('can_ctd_tnm_atm_cslt_max_u3m'),
        col('can_ctd_tnm_atm_cslt_max_u6m').cast('double').alias('can_ctd_tnm_atm_cslt_max_u6m'),
        col('can_ctd_tnm_atm_cslt_max_u9m').cast('double').alias('can_ctd_tnm_atm_cslt_max_u9m'),
        col('can_ctd_tnm_atm_cslt_max_u12').cast('double').alias('can_ctd_tnm_atm_cslt_max_u12'),
        col('can_ctd_tnm_atm_admn_max_u3m').cast('double').alias('can_ctd_tnm_atm_admn_max_u3m'),
        col('can_ctd_tnm_atm_admn_max_u6m').cast('double').alias('can_ctd_tnm_atm_admn_max_u6m'),
        col('can_ctd_tnm_atm_admn_max_u9m').cast('double').alias('can_ctd_tnm_atm_admn_max_u9m'),
        col('can_ctd_tnm_atm_admn_max_u12').cast('double').alias('can_ctd_tnm_atm_admn_max_u12'),
        col('can_ctd_tmo_atm_min_u3m').cast('double').alias('can_ctd_tmo_atm_min_u3m'),
        col('can_ctd_tmo_atm_min_u6m').cast('double').alias('can_ctd_tmo_atm_min_u6m'),
        col('can_ctd_tmo_atm_min_u9m').cast('double').alias('can_ctd_tmo_atm_min_u9m'),
        col('can_ctd_tmo_atm_min_u12').cast('double').alias('can_ctd_tmo_atm_min_u12'),
        col('can_ctd_tnm_atm_min_u3m').cast('double').alias('can_ctd_tnm_atm_min_u3m'),
        col('can_ctd_tnm_atm_min_u6m').cast('double').alias('can_ctd_tnm_atm_min_u6m'),
        col('can_ctd_tnm_atm_min_u9m').cast('double').alias('can_ctd_tnm_atm_min_u9m'),
        col('can_ctd_tnm_atm_min_u12').cast('double').alias('can_ctd_tnm_atm_min_u12'),
        col('can_ctd_tmo_atm_sol_min_u3m').cast('double').alias('can_ctd_tmo_atm_sol_min_u3m'),
        col('can_ctd_tmo_atm_sol_min_u6m').cast('double').alias('can_ctd_tmo_atm_sol_min_u6m'),
        col('can_ctd_tmo_atm_sol_min_u9m').cast('double').alias('can_ctd_tmo_atm_sol_min_u9m'),
        col('can_ctd_tmo_atm_sol_min_u12').cast('double').alias('can_ctd_tmo_atm_sol_min_u12'),
        col('can_ctd_tmo_atm_dol_min_u3m').cast('double').alias('can_ctd_tmo_atm_dol_min_u3m'),
        col('can_ctd_tmo_atm_dol_min_u6m').cast('double').alias('can_ctd_tmo_atm_dol_min_u6m'),
        col('can_ctd_tmo_atm_dol_min_u9m').cast('double').alias('can_ctd_tmo_atm_dol_min_u9m'),
        col('can_ctd_tmo_atm_dol_min_u12').cast('double').alias('can_ctd_tmo_atm_dol_min_u12'),
        col('can_ctd_tmo_atm_ret_min_u3m').cast('double').alias('can_ctd_tmo_atm_ret_min_u3m'),
        col('can_ctd_tmo_atm_ret_min_u6m').cast('double').alias('can_ctd_tmo_atm_ret_min_u6m'),
        col('can_ctd_tmo_atm_ret_min_u9m').cast('double').alias('can_ctd_tmo_atm_ret_min_u9m'),
        col('can_ctd_tmo_atm_ret_min_u12').cast('double').alias('can_ctd_tmo_atm_ret_min_u12'),
        col('can_ctd_tmo_atm_ret_sol_min_u3m').cast('double').alias('can_ctd_tmo_atm_ret_sol_min_u3m'),
        col('can_ctd_tmo_atm_ret_sol_min_u6m').cast('double').alias('can_ctd_tmo_atm_ret_sol_min_u6m'),
        col('can_ctd_tmo_atm_ret_sol_min_u9m').cast('double').alias('can_ctd_tmo_atm_ret_sol_min_u9m'),
        col('can_ctd_tmo_atm_ret_sol_min_u12').cast('double').alias('can_ctd_tmo_atm_ret_sol_min_u12'),
        col('can_ctd_tmo_atm_ret_dol_min_u3m').cast('double').alias('can_ctd_tmo_atm_ret_dol_min_u3m'),
        col('can_ctd_tmo_atm_ret_dol_min_u6m').cast('double').alias('can_ctd_tmo_atm_ret_dol_min_u6m'),
        col('can_ctd_tmo_atm_ret_dol_min_u9m').cast('double').alias('can_ctd_tmo_atm_ret_dol_min_u9m'),
        col('can_ctd_tmo_atm_ret_dol_min_u12').cast('double').alias('can_ctd_tmo_atm_ret_dol_min_u12'),
        col('can_ctd_tmo_atm_dep_min_u3m').cast('double').alias('can_ctd_tmo_atm_dep_min_u3m'),
        col('can_ctd_tmo_atm_dep_min_u6m').cast('double').alias('can_ctd_tmo_atm_dep_min_u6m'),
        col('can_ctd_tmo_atm_dep_min_u9m').cast('double').alias('can_ctd_tmo_atm_dep_min_u9m'),
        col('can_ctd_tmo_atm_dep_min_u12').cast('double').alias('can_ctd_tmo_atm_dep_min_u12'),
        col('can_ctd_tmo_atm_dep_sol_min_u3m').cast('double').alias('can_ctd_tmo_atm_dep_sol_min_u3m'),
        col('can_ctd_tmo_atm_dep_sol_min_u6m').cast('double').alias('can_ctd_tmo_atm_dep_sol_min_u6m'),
        col('can_ctd_tmo_atm_dep_sol_min_u9m').cast('double').alias('can_ctd_tmo_atm_dep_sol_min_u9m'),
        col('can_ctd_tmo_atm_dep_sol_min_u12').cast('double').alias('can_ctd_tmo_atm_dep_sol_min_u12'),
        col('can_ctd_tmo_atm_dep_dol_min_u3m').cast('double').alias('can_ctd_tmo_atm_dep_dol_min_u3m'),
        col('can_ctd_tmo_atm_dep_dol_min_u6m').cast('double').alias('can_ctd_tmo_atm_dep_dol_min_u6m'),
        col('can_ctd_tmo_atm_dep_dol_min_u9m').cast('double').alias('can_ctd_tmo_atm_dep_dol_min_u9m'),
        col('can_ctd_tmo_atm_dep_dol_min_u12').cast('double').alias('can_ctd_tmo_atm_dep_dol_min_u12'),
        col('can_ctd_tmo_atm_trf_min_u3m').cast('double').alias('can_ctd_tmo_atm_trf_min_u3m'),
        col('can_ctd_tmo_atm_trf_min_u6m').cast('double').alias('can_ctd_tmo_atm_trf_min_u6m'),
        col('can_ctd_tmo_atm_trf_min_u9m').cast('double').alias('can_ctd_tmo_atm_trf_min_u9m'),
        col('can_ctd_tmo_atm_trf_min_u12').cast('double').alias('can_ctd_tmo_atm_trf_min_u12'),
        col('can_ctd_tmo_atm_trf_sol_min_u3m').cast('double').alias('can_ctd_tmo_atm_trf_sol_min_u3m'),
        col('can_ctd_tmo_atm_trf_sol_min_u6m').cast('double').alias('can_ctd_tmo_atm_trf_sol_min_u6m'),
        col('can_ctd_tmo_atm_trf_sol_min_u9m').cast('double').alias('can_ctd_tmo_atm_trf_sol_min_u9m'),
        col('can_ctd_tmo_atm_trf_sol_min_u12').cast('double').alias('can_ctd_tmo_atm_trf_sol_min_u12'),
        col('can_ctd_tmo_atm_trf_dol_min_u3m').cast('double').alias('can_ctd_tmo_atm_trf_dol_min_u3m'),
        col('can_ctd_tmo_atm_trf_dol_min_u6m').cast('double').alias('can_ctd_tmo_atm_trf_dol_min_u6m'),
        col('can_ctd_tmo_atm_trf_dol_min_u9m').cast('double').alias('can_ctd_tmo_atm_trf_dol_min_u9m'),
        col('can_ctd_tmo_atm_trf_dol_min_u12').cast('double').alias('can_ctd_tmo_atm_trf_dol_min_u12'),
        col('can_ctd_tmo_atm_ads_min_u3m').cast('double').alias('can_ctd_tmo_atm_ads_min_u3m'),
        col('can_ctd_tmo_atm_ads_min_u6m').cast('double').alias('can_ctd_tmo_atm_ads_min_u6m'),
        col('can_ctd_tmo_atm_ads_min_u9m').cast('double').alias('can_ctd_tmo_atm_ads_min_u9m'),
        col('can_ctd_tmo_atm_ads_min_u12').cast('double').alias('can_ctd_tmo_atm_ads_min_u12'),
        col('can_ctd_tmo_atm_dis_min_u3m').cast('double').alias('can_ctd_tmo_atm_dis_min_u3m'),
        col('can_ctd_tmo_atm_dis_min_u6m').cast('double').alias('can_ctd_tmo_atm_dis_min_u6m'),
        col('can_ctd_tmo_atm_dis_min_u9m').cast('double').alias('can_ctd_tmo_atm_dis_min_u9m'),
        col('can_ctd_tmo_atm_dis_min_u12').cast('double').alias('can_ctd_tmo_atm_dis_min_u12'),
        col('can_ctd_tmo_atm_pag_tcr_min_u3m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_min_u3m'),
        col('can_ctd_tmo_atm_pag_tcr_min_u6m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_min_u6m'),
        col('can_ctd_tmo_atm_pag_tcr_min_u9m').cast('double').alias('can_ctd_tmo_atm_pag_tcr_min_u9m'),
        col('can_ctd_tmo_atm_pag_tcr_min_u12').cast('double').alias('can_ctd_tmo_atm_pag_tcr_min_u12'),
        col('can_ctd_tmo_atm_pag_srv_min_u3m').cast('double').alias('can_ctd_tmo_atm_pag_srv_min_u3m'),
        col('can_ctd_tmo_atm_pag_srv_min_u6m').cast('double').alias('can_ctd_tmo_atm_pag_srv_min_u6m'),
        col('can_ctd_tmo_atm_pag_srv_min_u9m').cast('double').alias('can_ctd_tmo_atm_pag_srv_min_u9m'),
        col('can_ctd_tmo_atm_pag_srv_min_u12').cast('double').alias('can_ctd_tmo_atm_pag_srv_min_u12'),
        col('can_ctd_tnm_atm_cslt_min_u3m').cast('double').alias('can_ctd_tnm_atm_cslt_min_u3m'),
        col('can_ctd_tnm_atm_cslt_min_u6m').cast('double').alias('can_ctd_tnm_atm_cslt_min_u6m'),
        col('can_ctd_tnm_atm_cslt_min_u9m').cast('double').alias('can_ctd_tnm_atm_cslt_min_u9m'),
        col('can_ctd_tnm_atm_cslt_min_u12').cast('double').alias('can_ctd_tnm_atm_cslt_min_u12'),
        col('can_ctd_tnm_atm_admn_min_u3m').cast('double').alias('can_ctd_tnm_atm_admn_min_u3m'),
        col('can_ctd_tnm_atm_admn_min_u6m').cast('double').alias('can_ctd_tnm_atm_admn_min_u6m'),
        col('can_ctd_tnm_atm_admn_min_u9m').cast('double').alias('can_ctd_tnm_atm_admn_min_u9m'),
        col('can_ctd_tnm_atm_admn_min_u12').cast('double').alias('can_ctd_tnm_atm_admn_min_u12'),
        col('can_ctd_tmo_atm_rec').cast('double').alias('can_ctd_tmo_atm_rec'),
        col('can_ctd_tnm_atm_rec').cast('double').alias('can_ctd_tnm_atm_rec'),
        col('can_ctd_tmo_atm_sol_rec').cast('double').alias('can_ctd_tmo_atm_sol_rec'),
        col('can_ctd_tmo_atm_dol_rec').cast('double').alias('can_ctd_tmo_atm_dol_rec'),
        col('can_ctd_tmo_atm_ret_rec').cast('double').alias('can_ctd_tmo_atm_ret_rec'),
        col('can_ctd_tmo_atm_ret_sol_rec').cast('double').alias('can_ctd_tmo_atm_ret_sol_rec'),
        col('can_ctd_tmo_atm_ret_dol_rec').cast('double').alias('can_ctd_tmo_atm_ret_dol_rec'),
        col('can_ctd_tmo_atm_dep_rec').cast('double').alias('can_ctd_tmo_atm_dep_rec'),
        col('can_ctd_tmo_atm_dep_sol_rec').cast('double').alias('can_ctd_tmo_atm_dep_sol_rec'),
        col('can_ctd_tmo_atm_dep_dol_rec').cast('double').alias('can_ctd_tmo_atm_dep_dol_rec'),
        col('can_ctd_tmo_atm_trf_rec').cast('double').alias('can_ctd_tmo_atm_trf_rec'),
        col('can_ctd_tmo_atm_trf_sol_rec').cast('double').alias('can_ctd_tmo_atm_trf_sol_rec'),
        col('can_ctd_tmo_atm_trf_dol_rec').cast('double').alias('can_ctd_tmo_atm_trf_dol_rec'),
        col('can_ctd_tmo_atm_ads_rec').cast('double').alias('can_ctd_tmo_atm_ads_rec'),
        col('can_ctd_tmo_atm_dis_rec').cast('double').alias('can_ctd_tmo_atm_dis_rec'),
        col('can_ctd_tmo_atm_pag_tcr_rec').cast('double').alias('can_ctd_tmo_atm_pag_tcr_rec'),
        col('can_ctd_tmo_atm_pag_srv_rec').cast('double').alias('can_ctd_tmo_atm_pag_srv_rec'),
        col('can_ctd_tnm_atm_cslt_rec').cast('double').alias('can_ctd_tnm_atm_cslt_rec'),
        col('can_ctd_tnm_atm_admn_rec').cast('double').alias('can_ctd_tnm_atm_admn_rec'),
        col('can_ctd_tmo_atm_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_frq_u3m'),
        col('can_ctd_tnm_atm_frq_u3m').cast('bigint').alias('can_ctd_tnm_atm_frq_u3m'),
        col('can_ctd_tmo_atm_sol_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_sol_frq_u3m'),
        col('can_ctd_tmo_atm_dol_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_dol_frq_u3m'),
        col('can_ctd_tmo_atm_ret_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_ret_frq_u3m'),
        col('can_ctd_tmo_atm_ret_sol_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_ret_sol_frq_u3m'),
        col('can_ctd_tmo_atm_ret_dol_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_ret_dol_frq_u3m'),
        col('can_ctd_tmo_atm_dep_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_dep_frq_u3m'),
        col('can_ctd_tmo_atm_dep_sol_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_dep_sol_frq_u3m'),
        col('can_ctd_tmo_atm_dep_dol_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_dep_dol_frq_u3m'),
        col('can_ctd_tmo_atm_trf_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_trf_frq_u3m'),
        col('can_ctd_tmo_atm_trf_sol_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_trf_sol_frq_u3m'),
        col('can_ctd_tmo_atm_trf_dol_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_trf_dol_frq_u3m'),
        col('can_ctd_tmo_atm_ads_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_ads_frq_u3m'),
        col('can_ctd_tmo_atm_dis_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_dis_frq_u3m'),
        col('can_ctd_tmo_atm_pag_tcr_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_pag_tcr_frq_u3m'),
        col('can_ctd_tmo_atm_pag_srv_frq_u3m').cast('bigint').alias('can_ctd_tmo_atm_pag_srv_frq_u3m'),
        col('can_ctd_tnm_atm_cslt_frq_u3m').cast('bigint').alias('can_ctd_tnm_atm_cslt_frq_u3m'),
        col('can_ctd_tnm_atm_admn_frq_u3m').cast('bigint').alias('can_ctd_tnm_atm_admn_frq_u3m'),
        col('can_ctd_tmo_atm_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_frq_u6m'),
        col('can_ctd_tnm_atm_frq_u6m').cast('bigint').alias('can_ctd_tnm_atm_frq_u6m'),
        col('can_ctd_tmo_atm_sol_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_sol_frq_u6m'),
        col('can_ctd_tmo_atm_dol_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_dol_frq_u6m'),
        col('can_ctd_tmo_atm_ret_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_ret_frq_u6m'),
        col('can_ctd_tmo_atm_ret_sol_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_ret_sol_frq_u6m'),
        col('can_ctd_tmo_atm_ret_dol_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_ret_dol_frq_u6m'),
        col('can_ctd_tmo_atm_dep_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_dep_frq_u6m'),
        col('can_ctd_tmo_atm_dep_sol_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_dep_sol_frq_u6m'),
        col('can_ctd_tmo_atm_dep_dol_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_dep_dol_frq_u6m'),
        col('can_ctd_tmo_atm_trf_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_trf_frq_u6m'),
        col('can_ctd_tmo_atm_trf_sol_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_trf_sol_frq_u6m'),
        col('can_ctd_tmo_atm_trf_dol_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_trf_dol_frq_u6m'),
        col('can_ctd_tmo_atm_ads_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_ads_frq_u6m'),
        col('can_ctd_tmo_atm_dis_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_dis_frq_u6m'),
        col('can_ctd_tmo_atm_pag_tcr_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_pag_tcr_frq_u6m'),
        col('can_ctd_tmo_atm_pag_srv_frq_u6m').cast('bigint').alias('can_ctd_tmo_atm_pag_srv_frq_u6m'),
        col('can_ctd_tnm_atm_cslt_frq_u6m').cast('bigint').alias('can_ctd_tnm_atm_cslt_frq_u6m'),
        col('can_ctd_tnm_atm_admn_frq_u6m').cast('bigint').alias('can_ctd_tnm_atm_admn_frq_u6m'),
        col('can_ctd_tmo_atm_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_frq_u9m'),
        col('can_ctd_tnm_atm_frq_u9m').cast('bigint').alias('can_ctd_tnm_atm_frq_u9m'),
        col('can_ctd_tmo_atm_sol_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_sol_frq_u9m'),
        col('can_ctd_tmo_atm_dol_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_dol_frq_u9m'),
        col('can_ctd_tmo_atm_ret_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_ret_frq_u9m'),
        col('can_ctd_tmo_atm_ret_sol_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_ret_sol_frq_u9m'),
        col('can_ctd_tmo_atm_ret_dol_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_ret_dol_frq_u9m'),
        col('can_ctd_tmo_atm_dep_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_dep_frq_u9m'),
        col('can_ctd_tmo_atm_dep_sol_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_dep_sol_frq_u9m'),
        col('can_ctd_tmo_atm_dep_dol_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_dep_dol_frq_u9m'),
        col('can_ctd_tmo_atm_trf_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_trf_frq_u9m'),
        col('can_ctd_tmo_atm_trf_sol_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_trf_sol_frq_u9m'),
        col('can_ctd_tmo_atm_trf_dol_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_trf_dol_frq_u9m'),
        col('can_ctd_tmo_atm_ads_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_ads_frq_u9m'),
        col('can_ctd_tmo_atm_dis_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_dis_frq_u9m'),
        col('can_ctd_tmo_atm_pag_tcr_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_pag_tcr_frq_u9m'),
        col('can_ctd_tmo_atm_pag_srv_frq_u9m').cast('bigint').alias('can_ctd_tmo_atm_pag_srv_frq_u9m'),
        col('can_ctd_tnm_atm_cslt_frq_u9m').cast('bigint').alias('can_ctd_tnm_atm_cslt_frq_u9m'),
        col('can_ctd_tnm_atm_admn_frq_u9m').cast('bigint').alias('can_ctd_tnm_atm_admn_frq_u9m'),
        col('can_ctd_tmo_atm_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_frq_u12'),
        col('can_ctd_tnm_atm_frq_u12').cast('bigint').alias('can_ctd_tnm_atm_frq_u12'),
        col('can_ctd_tmo_atm_sol_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_sol_frq_u12'),
        col('can_ctd_tmo_atm_dol_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_dol_frq_u12'),
        col('can_ctd_tmo_atm_ret_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_ret_frq_u12'),
        col('can_ctd_tmo_atm_ret_sol_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_ret_sol_frq_u12'),
        col('can_ctd_tmo_atm_ret_dol_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_ret_dol_frq_u12'),
        col('can_ctd_tmo_atm_dep_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_dep_frq_u12'),
        col('can_ctd_tmo_atm_dep_sol_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_dep_sol_frq_u12'),
        col('can_ctd_tmo_atm_dep_dol_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_dep_dol_frq_u12'),
        col('can_ctd_tmo_atm_trf_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_trf_frq_u12'),
        col('can_ctd_tmo_atm_trf_sol_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_trf_sol_frq_u12'),
        col('can_ctd_tmo_atm_trf_dol_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_trf_dol_frq_u12'),
        col('can_ctd_tmo_atm_ads_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_ads_frq_u12'),
        col('can_ctd_tmo_atm_dis_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_dis_frq_u12'),
        col('can_ctd_tmo_atm_pag_tcr_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_pag_tcr_frq_u12'),
        col('can_ctd_tmo_atm_pag_srv_frq_u12').cast('bigint').alias('can_ctd_tmo_atm_pag_srv_frq_u12'),
        col('can_ctd_tnm_atm_cslt_frq_u12').cast('bigint').alias('can_ctd_tnm_atm_cslt_frq_u12'),
        col('can_ctd_tnm_atm_admn_frq_u12').cast('bigint').alias('can_ctd_tnm_atm_admn_frq_u12'),
        col('can_mto_tmo_atm_g6m').cast('double').alias('can_mto_tmo_atm_g6m'),
        col('can_mto_tmo_atm_sol_g6m').cast('double').alias('can_mto_tmo_atm_sol_g6m'),
        col('can_mto_tmo_atm_dol_g6m').cast('double').alias('can_mto_tmo_atm_dol_g6m'),
        col('can_mto_tmo_atm_ret_g6m').cast('double').alias('can_mto_tmo_atm_ret_g6m'),
        col('can_mto_tmo_atm_ret_sol_g6m').cast('double').alias('can_mto_tmo_atm_ret_sol_g6m'),
        col('can_mto_tmo_atm_ret_dol_g6m').cast('double').alias('can_mto_tmo_atm_ret_dol_g6m'),
        col('can_mto_tmo_atm_dep_g6m').cast('double').alias('can_mto_tmo_atm_dep_g6m'),
        col('can_mto_tmo_atm_dep_sol_g6m').cast('double').alias('can_mto_tmo_atm_dep_sol_g6m'),
        col('can_mto_tmo_atm_dep_dol_g6m').cast('double').alias('can_mto_tmo_atm_dep_dol_g6m'),
        col('can_mto_tmo_atm_trf_g6m').cast('double').alias('can_mto_tmo_atm_trf_g6m'),
        col('can_mto_tmo_atm_trf_sol_g6m').cast('double').alias('can_mto_tmo_atm_trf_sol_g6m'),
        col('can_mto_tmo_atm_trf_dol_g6m').cast('double').alias('can_mto_tmo_atm_trf_dol_g6m'),
        col('can_mto_tmo_atm_ads_g6m').cast('double').alias('can_mto_tmo_atm_ads_g6m'),
        col('can_mto_tmo_atm_dis_g6m').cast('double').alias('can_mto_tmo_atm_dis_g6m'),
        col('can_mto_tmo_atm_pag_tcr_g6m').cast('double').alias('can_mto_tmo_atm_pag_tcr_g6m'),
        col('can_mto_tmo_atm_pag_srv_g6m').cast('double').alias('can_mto_tmo_atm_pag_srv_g6m'),
        col('can_tkt_tmo_atm_g6m').cast('double').alias('can_tkt_tmo_atm_g6m'),
        col('can_tkt_tmo_atm_sol_g6m').cast('double').alias('can_tkt_tmo_atm_sol_g6m'),
        col('can_tkt_tmo_atm_dol_g6m').cast('double').alias('can_tkt_tmo_atm_dol_g6m'),
        col('can_tkt_tmo_atm_ret_g6m').cast('double').alias('can_tkt_tmo_atm_ret_g6m'),
        col('can_tkt_tmo_atm_ret_sol_g6m').cast('double').alias('can_tkt_tmo_atm_ret_sol_g6m'),
        col('can_tkt_tmo_atm_ret_dol_g6m').cast('double').alias('can_tkt_tmo_atm_ret_dol_g6m'),
        col('can_tkt_tmo_atm_dep_g6m').cast('double').alias('can_tkt_tmo_atm_dep_g6m'),
        col('can_tkt_tmo_atm_dep_sol_g6m').cast('double').alias('can_tkt_tmo_atm_dep_sol_g6m'),
        col('can_tkt_tmo_atm_dep_dol_g6m').cast('double').alias('can_tkt_tmo_atm_dep_dol_g6m'),
        col('can_tkt_tmo_atm_trf_g6m').cast('double').alias('can_tkt_tmo_atm_trf_g6m'),
        col('can_tkt_tmo_atm_trf_sol_g6m').cast('double').alias('can_tkt_tmo_atm_trf_sol_g6m'),
        col('can_tkt_tmo_atm_trf_dol_g6m').cast('double').alias('can_tkt_tmo_atm_trf_dol_g6m'),
        col('can_tkt_tmo_atm_ads_g6m').cast('double').alias('can_tkt_tmo_atm_ads_g6m'),
        col('can_tkt_tmo_atm_dis_g6m').cast('double').alias('can_tkt_tmo_atm_dis_g6m'),
        col('can_tkt_tmo_atm_pag_tcr_g6m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_g6m'),
        col('can_tkt_tmo_atm_pag_srv_g6m').cast('double').alias('can_tkt_tmo_atm_pag_srv_g6m'),
        col('can_mto_tmo_atm_g3m').cast('double').alias('can_mto_tmo_atm_g3m'),
        col('can_mto_tmo_atm_sol_g3m').cast('double').alias('can_mto_tmo_atm_sol_g3m'),
        col('can_mto_tmo_atm_dol_g3m').cast('double').alias('can_mto_tmo_atm_dol_g3m'),
        col('can_mto_tmo_atm_ret_g3m').cast('double').alias('can_mto_tmo_atm_ret_g3m'),
        col('can_mto_tmo_atm_ret_sol_g3m').cast('double').alias('can_mto_tmo_atm_ret_sol_g3m'),
        col('can_mto_tmo_atm_ret_dol_g3m').cast('double').alias('can_mto_tmo_atm_ret_dol_g3m'),
        col('can_mto_tmo_atm_dep_g3m').cast('double').alias('can_mto_tmo_atm_dep_g3m'),
        col('can_mto_tmo_atm_dep_sol_g3m').cast('double').alias('can_mto_tmo_atm_dep_sol_g3m'),
        col('can_mto_tmo_atm_dep_dol_g3m').cast('double').alias('can_mto_tmo_atm_dep_dol_g3m'),
        col('can_mto_tmo_atm_trf_g3m').cast('double').alias('can_mto_tmo_atm_trf_g3m'),
        col('can_mto_tmo_atm_trf_sol_g3m').cast('double').alias('can_mto_tmo_atm_trf_sol_g3m'),
        col('can_mto_tmo_atm_trf_dol_g3m').cast('double').alias('can_mto_tmo_atm_trf_dol_g3m'),
        col('can_mto_tmo_atm_ads_g3m').cast('double').alias('can_mto_tmo_atm_ads_g3m'),
        col('can_mto_tmo_atm_dis_g3m').cast('double').alias('can_mto_tmo_atm_dis_g3m'),
        col('can_mto_tmo_atm_pag_tcr_g3m').cast('double').alias('can_mto_tmo_atm_pag_tcr_g3m'),
        col('can_mto_tmo_atm_pag_srv_g3m').cast('double').alias('can_mto_tmo_atm_pag_srv_g3m'),
        col('can_tkt_tmo_atm_g3m').cast('double').alias('can_tkt_tmo_atm_g3m'),
        col('can_tkt_tmo_atm_sol_g3m').cast('double').alias('can_tkt_tmo_atm_sol_g3m'),
        col('can_tkt_tmo_atm_dol_g3m').cast('double').alias('can_tkt_tmo_atm_dol_g3m'),
        col('can_tkt_tmo_atm_ret_g3m').cast('double').alias('can_tkt_tmo_atm_ret_g3m'),
        col('can_tkt_tmo_atm_ret_sol_g3m').cast('double').alias('can_tkt_tmo_atm_ret_sol_g3m'),
        col('can_tkt_tmo_atm_ret_dol_g3m').cast('double').alias('can_tkt_tmo_atm_ret_dol_g3m'),
        col('can_tkt_tmo_atm_dep_g3m').cast('double').alias('can_tkt_tmo_atm_dep_g3m'),
        col('can_tkt_tmo_atm_dep_sol_g3m').cast('double').alias('can_tkt_tmo_atm_dep_sol_g3m'),
        col('can_tkt_tmo_atm_dep_dol_g3m').cast('double').alias('can_tkt_tmo_atm_dep_dol_g3m'),
        col('can_tkt_tmo_atm_trf_g3m').cast('double').alias('can_tkt_tmo_atm_trf_g3m'),
        col('can_tkt_tmo_atm_trf_sol_g3m').cast('double').alias('can_tkt_tmo_atm_trf_sol_g3m'),
        col('can_tkt_tmo_atm_trf_dol_g3m').cast('double').alias('can_tkt_tmo_atm_trf_dol_g3m'),
        col('can_tkt_tmo_atm_ads_g3m').cast('double').alias('can_tkt_tmo_atm_ads_g3m'),
        col('can_tkt_tmo_atm_dis_g3m').cast('double').alias('can_tkt_tmo_atm_dis_g3m'),
        col('can_tkt_tmo_atm_pag_tcr_g3m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_g3m'),
        col('can_tkt_tmo_atm_pag_srv_g3m').cast('double').alias('can_tkt_tmo_atm_pag_srv_g3m'),
        col('can_mto_tmo_atm_g1m').cast('double').alias('can_mto_tmo_atm_g1m'),
        col('can_mto_tmo_atm_sol_g1m').cast('double').alias('can_mto_tmo_atm_sol_g1m'),
        col('can_mto_tmo_atm_dol_g1m').cast('double').alias('can_mto_tmo_atm_dol_g1m'),
        col('can_mto_tmo_atm_ret_g1m').cast('double').alias('can_mto_tmo_atm_ret_g1m'),
        col('can_mto_tmo_atm_ret_sol_g1m').cast('double').alias('can_mto_tmo_atm_ret_sol_g1m'),
        col('can_mto_tmo_atm_ret_dol_g1m').cast('double').alias('can_mto_tmo_atm_ret_dol_g1m'),
        col('can_mto_tmo_atm_dep_g1m').cast('double').alias('can_mto_tmo_atm_dep_g1m'),
        col('can_mto_tmo_atm_dep_sol_g1m').cast('double').alias('can_mto_tmo_atm_dep_sol_g1m'),
        col('can_mto_tmo_atm_dep_dol_g1m').cast('double').alias('can_mto_tmo_atm_dep_dol_g1m'),
        col('can_mto_tmo_atm_trf_g1m').cast('double').alias('can_mto_tmo_atm_trf_g1m'),
        col('can_mto_tmo_atm_trf_sol_g1m').cast('double').alias('can_mto_tmo_atm_trf_sol_g1m'),
        col('can_mto_tmo_atm_trf_dol_g1m').cast('double').alias('can_mto_tmo_atm_trf_dol_g1m'),
        col('can_mto_tmo_atm_ads_g1m').cast('double').alias('can_mto_tmo_atm_ads_g1m'),
        col('can_mto_tmo_atm_dis_g1m').cast('double').alias('can_mto_tmo_atm_dis_g1m'),
        col('can_mto_tmo_atm_pag_tcr_g1m').cast('double').alias('can_mto_tmo_atm_pag_tcr_g1m'),
        col('can_mto_tmo_atm_pag_srv_g1m').cast('double').alias('can_mto_tmo_atm_pag_srv_g1m'),
        col('can_tkt_tmo_atm_g1m').cast('double').alias('can_tkt_tmo_atm_g1m'),
        col('can_tkt_tmo_atm_sol_g1m').cast('double').alias('can_tkt_tmo_atm_sol_g1m'),
        col('can_tkt_tmo_atm_dol_g1m').cast('double').alias('can_tkt_tmo_atm_dol_g1m'),
        col('can_tkt_tmo_atm_ret_g1m').cast('double').alias('can_tkt_tmo_atm_ret_g1m'),
        col('can_tkt_tmo_atm_ret_sol_g1m').cast('double').alias('can_tkt_tmo_atm_ret_sol_g1m'),
        col('can_tkt_tmo_atm_ret_dol_g1m').cast('double').alias('can_tkt_tmo_atm_ret_dol_g1m'),
        col('can_tkt_tmo_atm_dep_g1m').cast('double').alias('can_tkt_tmo_atm_dep_g1m'),
        col('can_tkt_tmo_atm_dep_sol_g1m').cast('double').alias('can_tkt_tmo_atm_dep_sol_g1m'),
        col('can_tkt_tmo_atm_dep_dol_g1m').cast('double').alias('can_tkt_tmo_atm_dep_dol_g1m'),
        col('can_tkt_tmo_atm_trf_g1m').cast('double').alias('can_tkt_tmo_atm_trf_g1m'),
        col('can_tkt_tmo_atm_trf_sol_g1m').cast('double').alias('can_tkt_tmo_atm_trf_sol_g1m'),
        col('can_tkt_tmo_atm_trf_dol_g1m').cast('double').alias('can_tkt_tmo_atm_trf_dol_g1m'),
        col('can_tkt_tmo_atm_ads_g1m').cast('double').alias('can_tkt_tmo_atm_ads_g1m'),
        col('can_tkt_tmo_atm_dis_g1m').cast('double').alias('can_tkt_tmo_atm_dis_g1m'),
        col('can_tkt_tmo_atm_pag_tcr_g1m').cast('double').alias('can_tkt_tmo_atm_pag_tcr_g1m'),
        col('can_tkt_tmo_atm_pag_srv_g1m').cast('double').alias('can_tkt_tmo_atm_pag_srv_g1m'),
        col('fecrutina').cast('date').alias('fecrutina'),
        col('fecactualizacionregistro').cast('timestamp').alias('fecactualizacionregistro'),
        col('codmes').cast('int').alias('codmes')
    )
    write_delta(input_df, VAL_DESTINO_NAME, CONS_PARTITION_DELTA_NAME)
    #Retorna el dataframe final
    return dfMatrizVarTransaccionCajeroStep04

# COMMAND ---------- 

def main():
    #Leemos la tabla de parametros de fecha inicio y fecha fin para el proceso
    mesInicio, mesFin = spark.sql("""
                                  SELECT CAST(CAST(CODMESINICIO AS INT) AS STRING), CAST(CAST(CODMESFIN AS INT) AS STRING) FROM {var:esquemaTabla}.{var:parametroVariables}
                                  """.\
                                  replace("{var:esquemaTabla}", PRM_ESQUEMA_TABLA).\
                                  replace("{var:parametroVariables}", PRM_TABLA_PARAMETROMATRIZVARIABLES)
                                 ).take(1)[0][0:2]
                   
    #Retorna un Dataframe Pandas iterable con una distancia entre fecha inicio y fecha fin de 12 meses para cada registro del dataframe 
    totalMeses = funciones.calculoDfAnios(mesInicio, mesFin)

    if mesInicio == mesFin:
        codMesProceso = str(funciones.calcularCodmes(PRM_FECHA_RUTINA))
        totalMeses = funciones.calculoDfAnios(codMesProceso , codMesProceso )

    carpetaDetConceptoClienteSegundaTranspuesta = "detconceptocliente_"+PRM_TABLA_SEGUNDATRANSPUESTA.lower()
    
    carpetaClientePrimeraTranspuesta = "cli_"+PRM_TABLA_PRIMERATRANSPUESTA.lower()
    
    #Recorremos el dataframe pandas totalMeses seg�n sea la cantidad de registros 
    for index, row in  totalMeses.iterrows():
    
        #Leemos el valor de la fila codMes
        codMes = str(row['codMes'])
        
        #Leemos el valor de la fila mes11atras
        codMes12Atras = str(row['mes11atras'])
        
        funciones.extraccionInfoBaseCliente(codMes, PRM_ESQUEMA_TABLA, PRM_CARPETA_RAIZ_DE_PROYECTO, carpetaDetConceptoClienteSegundaTranspuesta, spark)
        
        #Se extrae informaci�n de doce meses de la primera transpuesta
        dfInfo12Meses = extraccionInformacion12Meses(codMes, codMes12Atras, carpetaDetConceptoClienteSegundaTranspuesta, carpetaClientePrimeraTranspuesta)
        
        #Se procesa la logica de la segunda transpuesta
        dfMatrizVarTransaccionCajero = agruparInformacionMesAnalisis(dfInfo12Meses, codMes, PRM_TABLA_SEGUNDATRANSPUESTA_TMP)
        
        #Se borra la carpeta temporal en hdfs 
        ruta_1 = f"{PRM_CARPETA_RAIZ_DE_PROYECTO}/temp/{PRM_TABLA_SEGUNDATRANSPUESTA_TMP}"
        ruta_2 = f"{PRM_CARPETA_RAIZ_DE_PROYECTO}/temp/{carpetaDetConceptoClienteSegundaTranspuesta}"
        rutas_a_eliminar = [ruta_1,ruta_2]
        if len(rutas_a_eliminar) > 0:
            objeto.cleanPaths(rutas_a_eliminar)
        
        tmp_table_1 = f'{PRM_ESQUEMA_TABLA_ESCRITURA}.{carpetaClientePrimeraTranspuesta}_tmp'.lower()
        spark.sql(f'TRUNCATE TABLE {tmp_table_1};')
        spark.sql(f'VACUUM {tmp_table_1} RETAIN 0 HOURS;') 
        


# COMMAND ----------

###
 # @section Ejecución
 ##

# Inicio Proceso main()
if __name__ == "__main__":
    main()
 
#Retorna el valor en 1 si el proceso termina de manera satisfactoria.
dbutils.notebook.exit(1)