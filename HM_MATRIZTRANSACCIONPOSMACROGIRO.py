# Databricks notebook source
# coding: utf-8

# ||********************************************************************************************************
# || PROYECTO   		  : MATRIZ DE VARIABLES - VARIABLES 
# || NOMBRE     		  : HM_MATRIZTRANSACCIONPOSMACROGIRO.py
# || TABLA DESTINO	      : BCP_DDV_MATRIZVARIABLES.HM_MATRIZTRANSACCIONPOSMACROGIRO
# || TABLAS FUENTES		  : BCP_DDV_MATRIZVARIABLES.HM_CONCEPTOTRANSACCIONPOS
# || OBJETIVO   		  : Creación de Segunda Transpuesta hm_matrizvartransaccionposmacrogiro
# || TIPO       		  : pyspark
# || REPROCESABLE	    : SI
# || SCHEDULER		    : NA
# || JOB  		        : @P4LKKBC
# || VERSION	DESARROLLADOR			  PROVEEDOR	           PO	          FECHA		    DESCRIPCION
# || 1.1      DIEGO GUERRA CRUZADO         Everis    	 ARTURO ROJAS       06/02/19     Creación del proceso
# ||          GIULIANA PABLO ALEJANDRO
# || 1.2      DIEGO UCHARIMA  A            Everis        RODRIGO ROJAS      04/05/20     Modificación del proceso
# ||          MARCOS IRVING MERA SANCHEZ
# || 1.3      KEYLA NALVARTE DIONISIO      INDRA         RODRIGO ROJAS      24/04/2023   Modificación de cabecera
# || 2.0      ROY MELENDEZ                 BLUETAB       RODRIGO ROJAS      02/01/2024   Migracion Cloud
# || 2.1      CARLOS AQUINO                BLUETAB       RODRIGO ROJAS      27/06/2024   Optimización de proceso, adecuación función Windowaggregate, adición de Coe Data cleanner 
# || 2.2      EDWIN ROQUE	               INDRA         RODRIGO ROJAS      20/08/2024	 Reducción deuda tecnica Q3, cambio saveAsTable.   
# *************************************************************************************************************

# COMMAND ----------

###
 # @section Import
 ##

from pyspark.sql.functions import col, concat, rpad, from_unixtime, when, months_between,last_day,to_date,format_number,format_string,add_months,unix_timestamp,substring,concat_ws,lit,round, current_timestamp, trim, concat,sha2
from pyspark.sql import functions as F 
import pyspark.sql.functions as func
import itertools
import funciones as funciones
import pytz
from datetime import datetime
from pyspark import StorageLevel
import bcp_coe_data_cleaner
from bcp_coe_data_cleaner import coe_data_cleaner
bcp_coe_data_cleaner.__version__

# COMMAND ----------

###
 # @section Constantes
 ##
CONS_GRUPO = 'P2'
CONS_WRITE_FORMAT_NAME = "delta"
CONS_COMPRESSION_MODE_NAME = "snappy"
CONS_WRITE_TEXT_MODE_NAME = "overwrite"
CONS_DFS_NAME = ".dfs.core.windows.net/"
CONS_CONTAINER_NAME = "abfss://lhcldata@"
CONS_PARTITION_DELTA_NAME = "CODMES"
LIMA_TIMEZONE = pytz.timezone('America/Lima')
objeto = coe_data_cleaner.DataCleanerLHCL()
FLAG_EJECUCION_CUARENTENA = False

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss",False)
spark.conf.set("spark.databricks.io.cache.enabled", False)

# COMMAND ----------

# OPTIMIZACIÓN: Configuración de Spark para mejor rendimiento
# Adaptive Query Execution (AQE) - Optimiza shuffles y maneja skew automáticamente
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64m")

# Ajustar particiones de shuffle para cluster mediano
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Broadcast automático para dimensiones pequeñas
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64*1024*1024))

# Overwrite dinámico de particiones
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Delta Lake - Optimización de escritura y compactación automática
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

#params
dbutils.widgets.text(name="PRM_STORAGE_ACCOUNT_DDV", defaultValue='adlscu1lhclbackd03')
dbutils.widgets.text(name="PRM_CARPETA_OUTPUT", defaultValue='desa/bcp/ddv/analytics/matrizvariables/data/out')
dbutils.widgets.text(name="PRM_RUTA_ADLS_TABLES", defaultValue='desa/bcp/ddv/analytics/matrizvariables')
dbutils.widgets.text(name="PRM_ESQUEMA_TABLA_DDV", defaultValue='bcp_ddv_matrizvariables')
dbutils.widgets.text(name="PRM_TABLE_NAME", defaultValue='HM_MATRIZTRANSACCIONPOSMACROGIRO')
dbutils.widgets.text(name="PRM_CATALOG_NAME", defaultValue='catalog_lhcl_desa_bcp')
dbutils.widgets.text(name="PRM_TABLA_PRIMERATRANSPUESTA", defaultValue='hm_conceptotransaccionpos')
dbutils.widgets.text(name="PRM_TABLA_PARAMETROMATRIZVARIABLES", defaultValue='mm_parametromatrizvariables')
dbutils.widgets.text(name="PRM_FECHA_RUTINA", defaultValue='2024-07-06')
dbutils.widgets.text(name="PRM_TABLA_SEGUNDATRANSPUESTA_TMP", defaultValue='hm_matriztransaccionposmacrogiro_tmp')
dbutils.widgets.text(name="PRM_TABLA_PARAM_GRUPO", defaultValue='mm_parametrogrupoconcepto')

PRM_STORAGE_ACCOUNT_DDV = dbutils.widgets.get("PRM_STORAGE_ACCOUNT_DDV")
PRM_CARPETA_OUTPUT = dbutils.widgets.get("PRM_CARPETA_OUTPUT")
PRM_RUTA_ADLS_TABLES = dbutils.widgets.get("PRM_RUTA_ADLS_TABLES")
PRM_ESQUEMA_TABLA_DDV = dbutils.widgets.get("PRM_ESQUEMA_TABLA_DDV")
PRM_TABLE_NAME = dbutils.widgets.get("PRM_TABLE_NAME")
PRM_CATALOG_NAME = dbutils.widgets.get("PRM_CATALOG_NAME")
PRM_TABLA_PRIMERATRANSPUESTA = dbutils.widgets.get("PRM_TABLA_PRIMERATRANSPUESTA")
PRM_TABLA_PARAMETROMATRIZVARIABLES = dbutils.widgets.get("PRM_TABLA_PARAMETROMATRIZVARIABLES")
PRM_FECHA_RUTINA = dbutils.widgets.get("PRM_FECHA_RUTINA")
PRM_TABLA_SEGUNDATRANSPUESTA_TMP = dbutils.widgets.get("PRM_TABLA_SEGUNDATRANSPUESTA_TMP")
PRM_TABLA_PARAM_GRUPO = dbutils.widgets.get("PRM_TABLA_PARAM_GRUPO")

PRM_ESQUEMA_TABLA = PRM_CATALOG_NAME+"."+PRM_ESQUEMA_TABLA_DDV
PRM_CARPETA_RAIZ_DE_PROYECTO = CONS_CONTAINER_NAME+PRM_STORAGE_ACCOUNT_DDV+CONS_DFS_NAME+PRM_RUTA_ADLS_TABLES
VAL_DESTINO_NAME= PRM_ESQUEMA_TABLA+"."+PRM_TABLE_NAME

# COMMAND ----------

## Funcion de escritura
def write_delta(df, output, partition):
    df.write \
          .format(CONS_WRITE_FORMAT_NAME) \
          .option("compression",CONS_COMPRESSION_MODE_NAME) \
          .option("partitionOverwritemode","dynamic") \
          .partitionBy(partition).mode(CONS_WRITE_TEXT_MODE_NAME) \
          .saveAsTable(output)

# COMMAND ----------

###
 # Obtiene los datos de los últimos 12 meses
 #
 # @param codMes {string} Mes final hasta donde se toman los datos
 # @param codMes12Atras {string} Mes inicial desde donde se toman los datos
 # @param carpetaDetConceptoClienteSegundaTranspuesta {string} Nombre de la carpeta donde se localiza temporalmente información de la tabla HM_DETCONCEPTOCLIENTE.
 # @param carpetaClientePrimeraTranspuesta {string} Nombre de la carpeta donde se localiza temporalmente la información de la tabla HM_DETCONCEPTOTRANSACCIONPOS.
 # @return {Dataframe Pandas}
 ##
def extraccionInformacion12Meses(codMes, codMes12Atras, carpetaDetConceptoClienteSegundaTranspuesta, carpetaClientePrimeraTranspuesta):
    
    #Leemos el dataframe de disco a memoria
    dfClienteCuc = spark.read.format(funciones.CONS_FORMATO_DE_ESCRITURA_EN_DISCO).load(PRM_CARPETA_RAIZ_DE_PROYECTO+"/temp/"+carpetaDetConceptoClienteSegundaTranspuesta+"/CODMES="+str(codMes))
    
    #Dataframe con información de los codinternocomputacional de los clientes de la tabla HM_DETCONCEPTOCLIENTE  
    dfClienteCic = spark.sql("""
                             SELECT 
                                CODMES, 
                                CODINTERNOCOMPUTACIONAL CODINTERNOCOMPUTACIONAL, 
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

    #Devuelve un string con las columnas pertenecientes al grupo transaccion pos
    cadenaGrupoColumns = funciones.crearDfGrupo(CONS_GRUPO, spark, PRM_ESQUEMA_TABLA, PRM_TABLA_PARAM_GRUPO, PRM_TABLA_PRIMERATRANSPUESTA)
    
    #Nos quedamos con las columnas necesarias para el procesamiento y construccion de la segunda transpuesta    
    dfInfo12MesesStep0 = spark.sql("""
                                   SELECT 
                                      CODMES,
                                      CODUNICOCLI, 
                                      CODINTERNOCOMPUTACIONAL, 
                                      {var: cadenaGrupoColumns} 
                                   FROM 
                                      {var:esquemaTabla}.{var:tablaPrimeraTranspuesta} 
                                   WHERE 
                                      CODMES <={var:codMes} AND CODMES>={var:codMes12Atras}
                                   """.\
                                   replace("{var: cadenaGrupoColumns}", cadenaGrupoColumns).\
                                   replace("{var:esquemaTabla}",PRM_ESQUEMA_TABLA ).\
                                   replace("{var:tablaPrimeraTranspuesta}", PRM_TABLA_PRIMERATRANSPUESTA).\
                                   replace("{var:codMes}", codMes).\
                                   replace("{var:codMes12Atras}", codMes12Atras)
                                   )
    
    dfInfo12MesesStep1Cic = dfInfo12MesesStep0.join(dfClienteCic.drop('CODMES'), on = 'CODINTERNOCOMPUTACIONAL', how = 'left')
    dfClienteCic.unpersist()
    dfInfo12MesesStep0.unpersist()
    del dfClienteCic
    del dfInfo12MesesStep0
     
    #Renombramos la columna CODUNICOCLI_LAST y CODINTERNOCOMPUTACIONAL_LAST del dataframe y lo almacenamos en un nuevo dataframe
    columnas = dfInfo12MesesStep1Cic.columns
    dfInfo12MesesStep1Cic = dfInfo12MesesStep1Cic.select(*columnas + 
                                                        [F.col("CODUNICOCLI_LAST").alias("CODUNICOCLI_LAST_CIC")] +
                                                        [F.col("CODINTERNOCOMPUTACIONAL_LAST").alias("CODINTERNOCOMPUTACIONAL_LAST_CIC")]
                                                    ).drop("CODUNICOCLI_LAST","CODINTERNOCOMPUTACIONAL_LAST")

    #El resultado lo colocamos en una vista
    dfInfo12MesesStep1Cic.createOrReplaceTempView('temp1_cic')

    dfInfo12MesesStep1Cic = spark.sql("""
                                      SELECT 
                                         CODMES, 
                                         CODUNICOCLI, 
                                         CODINTERNOCOMPUTACIONAL,
                                         CASE WHEN TRIM(CODINTERNOCOMPUTACIONAL)<>'.' THEN CODUNICOCLI_LAST_CIC ELSE CODUNICOCLI END CODUNICOCLI_LAST,
                                         CASE WHEN TRIM(CODINTERNOCOMPUTACIONAL)<>'.' THEN CODINTERNOCOMPUTACIONAL_LAST_CIC ELSE CODINTERNOCOMPUTACIONAL END CODINTERNOCOMPUTACIONAL_LAST_CIC,
                                         """+cadenaGrupoColumns+' '+"""
                                      FROM 
                                          temp1_cic
                                      """)
                                 
    dfInfo12MesesStep2Cuc = dfInfo12MesesStep1Cic.join(dfClienteCuc.drop('CODMES'), on = 'CODUNICOCLI_LAST', how = 'left')
    dfInfo12MesesStep1Cic.unpersist()
    del dfInfo12MesesStep1Cic
    
    #Renombramos la columna CODINTERNOCOMPUTACIONAL_LAST dataframe y lo almacenamos en un nuevo dataframe
    columnas = dfInfo12MesesStep2Cuc.columns
    dfInfo12MesesStep2Cuc = dfInfo12MesesStep2Cuc.select(*columnas + 
                                                         [F.col("CODINTERNOCOMPUTACIONAL_LAST").alias("CODINTERNOCOMPUTACIONAL_LAST_CUC")]
                                                        ).drop("CODINTERNOCOMPUTACIONAL_LAST")
    
    #El resultado lo colocamos en una vista
    dfInfo12MesesStep2Cuc.createOrReplaceTempView('temp2_cuc')
    del dfInfo12MesesStep2Cuc
    
    #Eliminamos los espacios en blanco de cada uno de los nombres de las columnas del dataframe
    cadenaGrupoColumnsList = [cadena.strip() for cadena in cadenaGrupoColumns.split(',')]
    
    #Formamos una cadenas separada por comas(,) con los nombres de las columnas del dataframe
    resultOperadorColumnasDf = ','.join(funciones.calcularOperadoresColumnasDf(cadenaGrupoColumnsList))+' '

    dfInfo12Meses = spark.sql("""
                               SELECT
                                  """+codMes+""" CODMESANALISIS,
                                  CODMES,
                                  TRIM(CODUNICOCLI_LAST) CODUNICOCLI,
                                  CASE WHEN TRIM(CODUNICOCLI_LAST)<> '"""+ funciones.CONS_VALOR_HASH_PUNTO +"""' THEN NVL(TRIM(CODINTERNOCOMPUTACIONAL_LAST_CUC), RPAD('.', 12, ' ')) ELSE TRIM(CODINTERNOCOMPUTACIONAL_LAST_CIC) END CODINTERNOCOMPUTACIONAL ,
                                  """+resultOperadorColumnasDf+"""
                               FROM
                                  temp2_cuc
                               GROUP BY
                                  CODMES,
                                  TRIM(CODUNICOCLI_LAST),
                                  CASE WHEN TRIM(CODUNICOCLI_LAST)<> '"""+ funciones.CONS_VALOR_HASH_PUNTO +"""' THEN NVL(TRIM(CODINTERNOCOMPUTACIONAL_LAST_CUC), RPAD('.', 12, ' ')) ELSE TRIM(CODINTERNOCOMPUTACIONAL_LAST_CIC) END
                               """
                               )

		
    #Escribimos los nombres de las columnas del dataframe en mayúsculas
    dfInfo12Meses = dfInfo12Meses.select([F.col(x).alias(x.upper()) for x in dfInfo12Meses.columns])

    # OPTIMIZACIÓN: Reemplazar write/read a disco por cache en memoria
    # Esto elimina I/O costoso a disco y es funcionalmente equivalente
    dfInfo12Meses = dfInfo12Meses.cache()
    dfInfo12Meses.count()  # Fuerza la materialización del DataFrame en memoria
   
    return dfInfo12Meses
   
###
 # Se genera la 2da transpuesta
 # 
 # @param dfInfo12Meses {Dataframe Spark} Listado de columnas de la primera transpuesta y segunda transpuesta
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
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo TKT
    columTkt = []
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo CANTIDAD
    columCtd = []
    
    #declaramos la lista que almacenara las columnas generadas en la 1era transpuesta tipo FLAG
    columFlg = []
    
    #Leemos las columnas del dataframe dfInfo12Meses y los asignamos a una lista
    nameColumn = dfInfo12Meses.schema.names
    
    #Nos quedamos solo con las columnas de tipo Monto, TKT, Cantidad y Flag
    for nColumns in nameColumn:
            if 'POS' in nColumns:
                if  'MTO' in nColumns:
                    columMto.append(nColumns)
                if  'TKT' in nColumns:
                    columTkt.append(nColumns)  
                if  'CTD' in nColumns:
                    columCtd.append(nColumns)        
                if  'FLG' in nColumns:
                    columFlg.append(nColumns)
                            
    #Almacena las columnas tipo MONTO de la tabla dfInfo12Meses 
    colsToExpandMonto = columMto
    
    #Almacena las columnas tipo ANT de la tabla dfInfo12Meses
    colsToExpandTkt = columTkt
    
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
    
    #La _Parte2 sera para la ejecucion de la plantilla tipo TKT
    aggsIter_Part2Avg = itertools.product(colsToExpandTkt,
                                         [F.avg],
                                         [0,-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part2Avg_PRM_P6M = itertools.product(colsToExpandTkt,
                                                 [F.avg],
                                                 [-11],
                                                 [-6])
                                                 
    aggsIter_Part2Avg_PRM_P3M = itertools.product(colsToExpandTkt,
                                                 [F.avg],
                                                 [-5],
                                                 [-3])
                                                 
    aggsIter_Part2Avg_Prev_Mes = itertools.product(colsToExpandTkt,
                                                   [F.avg],
                                                   [-1],
                                                   [-1])
    
    aggsIter_Part2Max = itertools.product(colsToExpandTkt,
                                         [F.max],
                                         [-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part2Min = itertools.product(colsToExpandTkt,
                                         [F.min],
                                         [-2,-5,-8,-11],
                                         [0])
    
    #La _Parte3 sera para la ejecucion de la plantilla tipo CTD
    aggsIter_Part3Avg = itertools.product(colsToExpandCantidad,
                                         [F.avg],
                                         [0,-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part3Avg_PRM_P6M = itertools.product(colsToExpandCantidad,
                                                 [F.avg],
                                                 [-11],
                                                 [-6])
                                                 
    aggsIter_Part3Avg_PRM_P3M = itertools.product(colsToExpandCantidad,
                                                 [F.avg],
                                                 [-5],
                                                 [-3])
                                                 
    aggsIter_Part3Avg_Prev_Mes = itertools.product(colsToExpandCantidad,
                                                   [F.avg],
                                                   [-1],
                                                   [-1])
    
    aggsIter_Part3Max = itertools.product(colsToExpandCantidad,
                                         [F.max],
                                         [-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part3Min = itertools.product(colsToExpandCantidad,
                                         [F.min],
                                         [-2,-5,-8,-11],
                                         [0])
        
    #La _Parte4 sera para la ejecucion de la plantilla tipo FLG
    aggsIter_Part4Avg = itertools.product(colsToExpandFlag,
                                         [F.avg],
                                         [0],
                                         [0])
    
    aggsIter_Part4Max = itertools.product(colsToExpandFlag,
                                         [F.max],
                                         [-2,-5,-8,-11],
                                         [0])
    
    aggsIter_Part4Sum = itertools.product(colsToExpandFlag,
                                         [F.sum],
                                         [-2,-5,-8,-11],
                                         [0])
    
	#Lista de los meses a retroceder para las variables a las cuales se les calculara su frecuencia
    mesFrecuenciasMesesCtd =  [-2,-5,-8,-11]
   
    #La funcion chain() solo es para trasformner las columnas en horizontal el resultados de cada una de las funciones ,diaria q es como concatenar
    aggsIterFinal = itertools.chain(aggsIter_Part1Avg, aggsIter_Part1Avg_PRM_P6M, aggsIter_Part1Avg_PRM_P3M, 
                                    aggsIter_Part1Avg_Prev_Mes, aggsIter_Part1Max, aggsIter_Part1Min, aggsIter_Part2Avg, 
                                    aggsIter_Part2Avg_PRM_P6M, aggsIter_Part2Avg_PRM_P3M, aggsIter_Part2Avg_Prev_Mes, 
                                    aggsIter_Part2Max, aggsIter_Part2Min, aggsIter_Part3Avg, aggsIter_Part3Avg_PRM_P6M, 
                                    aggsIter_Part3Avg_PRM_P3M, aggsIter_Part3Avg_Prev_Mes, aggsIter_Part3Max, aggsIter_Part3Min, 
                                    aggsIter_Part4Avg, aggsIter_Part4Max, aggsIter_Part4Sum)
     
    #Funcion core que realiza el procesamiento del calculo de las nuevas columnas generadas dinamicamente segun sea el tipo de variable y plantilla 
    dfMatrizVarTransaccionPosMacrog = funciones.windowAggregateOpt(
        dfInfo12Meses,
        partitionCols=funciones.CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP,
        orderCol='CODMES',
        aggregations=aggsIterFinal,
        flag=1)
    
    # OPTIMIZACIÓN FASE 1: Cambiar MEMORY_ONLY_2 a MEMORY_AND_DISK para evitar OOM
    # MEMORY_ONLY_2 replica en memoria 2 veces (costoso), MEMORY_AND_DISK es más seguro para clusters medianos
    dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrog.persist(StorageLevel.MEMORY_AND_DISK)

    # OPTIMIZACIÓN: Materializar explícitamente el DataFrame para forzar ejecución
    dfMatrizVarTransaccionPosMacrogiro.count()

    dfMatrizVarTransaccionPosMacrog.unpersist()
    del dfMatrizVarTransaccionPosMacrog
    print(f"{datetime.now(LIMA_TIMEZONE):%Y-%m-%d | %H:%M:%S} -- fin: Paraleizacion WindowaAgg")
        
    #Leemos las columnas del dataframe dfMatrizVarTransaccionPosMacrogiro y los asignamos a una lista
    columnNamesDf = dfMatrizVarTransaccionPosMacrogiro.schema.names 
      
    #Extraemos las columnas de tipo Flag
    colsFlg = funciones.extraccionColumnasFlg(columnNamesDf,"_FLG_")
    
    #Se concatena una cadena con las columnas tipo flag extraidas previamente
    colsFlagFin = "'CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL','"+colsFlg+"'"
    
    #Se crea el nuevo dataframe dfFlag con los nuevos campos tipo flag
    dfFlag = dfMatrizVarTransaccionPosMacrogiro.select(eval("[{templatebody1}]".replace("{templatebody1}", colsFlagFin)))
    
    #Leemos las columnas del dataframe dfFlag y los asignamos a una lista
    dfFlag = dfFlag.schema.names
    
    #Casteamos de string a int las columnas de la lista dfFlag
    columnCastFlag = funciones.casteoColumns(dfFlag,'int')
    
    #Se crea el nuevo dataframe dfFlagFin con los campos tipo flag casteados
    dfFlagFin = dfMatrizVarTransaccionPosMacrogiro.select(eval("[{templatebody}]".replace("{templatebody}", columnCastFlag)))
    
    #Leemos las columnas del dataframe dfFlagFin y los asignamos a una lista
    Columns = dfFlagFin.columns     
    
    #Nos quedamos solo con las columnas desde la posicion 4 hasta la ultima columna
    ColumnsFLag = Columns[4:]
    
    #Se crea el nuevo dataframe dfColumnSinFlg sin los campos de la cadena ColumnsFLag
    dfColumnSinFlg = dfMatrizVarTransaccionPosMacrogiro.select([c for c in dfMatrizVarTransaccionPosMacrogiro.columns if c not in ColumnsFLag])
    
    #Leemos las columnas del dataframe dfColumnSinFlg y las asignamos a una lista
    columnSinFlag =  dfColumnSinFlg.columns
    
    #Casteamos de string a double las columnas de la lista columnSinFlag
    columnCastSinFlag = funciones.casteoColumns(columnSinFlag,'double')
    
    #Se crea el nuevo dataframe dfSinFLagFin con los campos tipo excepto los tipo flag casteados
    dfSinFLagFin = dfMatrizVarTransaccionPosMacrogiro.select(eval("[{templatebody}]".replace("{templatebody}", columnCastSinFlag)))
    
    #Cruzamos los dataframes dfFlagFin y dfSinFLagFin por las columnas CODMESANALISIS,CODMES,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfMatrizVarTransaccionPosMacrogiro_2 = dfFlagFin.join(dfSinFLagFin,['CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL'])
    
    dfMatrizVarTransaccionPosMacrogiro.unpersist()
    del dfMatrizVarTransaccionPosMacrogiro
    
    #Se calcula las frecuencias para las variables de tipo cantidad con un retroceso de 3,6,9,12 meses
    dfFrecuencia = funciones.calcularFrecuencias(mesFrecuenciasMesesCtd,colsToExpandCantidad,spark)
    
    #Casteamos la columna CODMESANALISIS a string del dataframe dfFrecuencia
    columna_a_castear = ['CODMESANALISIS']
    columna_casteada = [F.col(x).cast("string").alias(x) for x in columna_a_castear]
    dfFrecuencia = dfFrecuencia.select(
        						*(column_name for column_name in dfFrecuencia.columns if column_name not in columna_a_castear),
                                *(columna_casteada))
    
    #Se calcula la recencia para las variables de tipo cantidad
    dfRecencia = funciones.calculoRecencia(colsToExpandCantidad,spark)
    
    #Casteamos la columna CODMESANALISIS a string del dataframe dfRecencia
    dfRecencia = dfRecencia.select(
        						*(column_name for column_name in dfRecencia.columns if column_name not in columna_a_castear),
                                *(columna_casteada))
    
    #Cruzamos los dataframes dfRecencia y dfFrecuencia por las columnas CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfRecenciaFrecuencia = dfRecencia.join(dfFrecuencia, funciones.CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP)
    
    #Cruzamos los dataframes dfMatrizVarTransaccionPosMacrogiro y dfRecenciaFrecuencia por las columnas CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL
    dfMatrizVarTransaccionPosMacrogiro_3 = dfMatrizVarTransaccionPosMacrogiro_2.join(dfRecenciaFrecuencia, funciones.CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP)
    dfMatrizVarTransaccionPosMacrogiro_2.unpersist()
    del dfMatrizVarTransaccionPosMacrogiro_2
    
    #Leemos las columnas del dataframe dfMatrizVarTransaccionPosMacrogiro y los asignamos a una lista  
    dfMatrizVarTransaccionPosMacrogiroColumns = dfMatrizVarTransaccionPosMacrogiro_3.schema.names
    
    #Extraemos las columnas promedio ultimos 6 meses
    colsMont_PRM_U6M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroColumns, "_PRM_U6M")
    
    #Extraemos las columnas promedio primeros 6 meses
    colsMont_PRM_P6M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroColumns, "_PRM_P6M")
    
    #Extraemos las columnas promedio primeros 3 meses
    colsMont_PRM_P3M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroColumns, "_PRM_P3M")
    
    #Extraemos las columnas promedio ultimos 3 meses
    colsMont_PRM_U3M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroColumns, "_PRM_U3M")
    
    #Extraemos la columna promedio del penultimo mes
    colsMont_P1M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroColumns, "_P1M")
    
    #Extraemos la columna promedio del ultimo mes
    colsMont_U1M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroColumns, "_U1M")
    
    #Se concatena una cadena con las columnas tipo promedio extraidas previamente
    colsMont = "CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS,"+ colsMont_PRM_U6M +","+ colsMont_PRM_P6M +","+ colsMont_PRM_P3M +","+ colsMont_PRM_U3M +","+ colsMont_P1M +","+ colsMont_U1M
    
    #El dataframe dfMatrizVarTransaccionPosMacrogiro solo con las columnas promedios 
    dfMatrizVarTransaccionPosMacrogiroMont = dfMatrizVarTransaccionPosMacrogiro_3.select(colsMont.split(','))
        
    # OPTIMIZACIÓN: Consolidar 3 loops en 1 solo select para reducir overhead
    # Preparar todas las columnas nuevas de una vez
    all_new_cols = []

    # Procesar Monto
    for colName in colsToExpandMonto:
        all_new_cols.extend([
            func.round(col(colName + "_PRM_U6M")/col(colName + "_PRM_P6M"), 8).alias(colName + "_G6M"),
            func.round(col(colName + "_PRM_U3M")/col(colName + "_PRM_P3M"), 8).alias(colName + "_G3M"),
            func.round(col(colName + "_U1M")/col(colName + "_P1M"), 8).alias(colName + "_G1M")
        ])

    # Procesar TKT
    for colName in colsToExpandTkt:
        all_new_cols.extend([
            func.round(col(colName + "_PRM_U6M")/col(colName + "_PRM_P6M"), 8).alias(colName + "_G6M"),
            func.round(col(colName + "_PRM_U3M")/col(colName + "_PRM_P3M"), 8).alias(colName + "_G3M"),
            func.round(col(colName + "_U1M")/col(colName + "_P1M"), 8).alias(colName + "_G1M")
        ])

    # Procesar Cantidad
    for colName in colsToExpandCantidad:
        all_new_cols.extend([
            func.round(col(colName + "_PRM_U6M")/col(colName + "_PRM_P6M"), 8).alias(colName + "_G6M"),
            func.round(col(colName + "_PRM_U3M")/col(colName + "_PRM_P3M"), 8).alias(colName + "_G3M"),
            func.round(col(colName + "_U1M")/col(colName + "_P1M"), 8).alias(colName + "_G1M")
        ])

    # UNA sola transformación en vez de 3
    dfMatrizVarTransaccionPosMacrogiroMont = dfMatrizVarTransaccionPosMacrogiroMont.select("*", *all_new_cols)  
        
    #Leemos las columnas del dataframe dfMatrizVarTransaccionPosMacrogiroMont y las asignamos a una lista		
    dfMatrizVarTransaccionPosMacrogiroMontColumns = dfMatrizVarTransaccionPosMacrogiroMont.schema.names
    
    #Extraemos las columnas crecimiento ultimos 6 meses
    colsMont_PRM_G6M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroMontColumns, "_G6M")
    
    #Extraemos las columnas crecimiento ultimos 3 meses
    colsMont_PRM_G3M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroMontColumns, "_G3M")
    
    #Extraemos las columnas crecimiento ultimos 1 meses
    colsMont_PRM_G1M = funciones.extraccionColumnas(dfMatrizVarTransaccionPosMacrogiroMontColumns, "_G1M")
    
    #Se concatena una cadena con las columnas tipo crecimiento extraidas previamente
    colsMontFin = "CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS,"+ colsMont_PRM_G6M +","+ colsMont_PRM_G3M +","+ colsMont_PRM_G1M
    
    #El dataframe dfMatrizVarTransaccionPosMacrogiroMont solo con las columnas crecimiento
    dfMatrizVarTransaccionPosMacrogiroCrecimiento = dfMatrizVarTransaccionPosMacrogiroMont.select(colsMontFin.split(','))
    
    #Cruzamos los dataframes dfMatrizVarTransaccionPosMacrogiro y dfMatrizVarTransaccionPosMacrogiroCrecimiento por las columnas CODUNICOCLI,CODINTERNOCOMPUTACIONAL,CODMESANALISIS
    dfMatrizVarTransaccionPosMacrogiro_4 = dfMatrizVarTransaccionPosMacrogiro_3.join(dfMatrizVarTransaccionPosMacrogiroCrecimiento, ['CODUNICOCLI','CODINTERNOCOMPUTACIONAL','CODMESANALISIS'])
    dfMatrizVarTransaccionPosMacrogiro_3.unpersist()
    del dfMatrizVarTransaccionPosMacrogiro_3
    
    #Casteamos la columna CODMESANALISIS a integer del dataframe dfMatrizVarTransaccionPosMacrogiro
    columna_a_castear = ['CODMESANALISIS']
    columna_casteada = [F.col(x).cast("int").alias(x) for x in columna_a_castear]
    dfMatrizVarTransaccionPosMacrogiro_4 = dfMatrizVarTransaccionPosMacrogiro_4.select(
        											*(column_name for column_name in dfMatrizVarTransaccionPosMacrogiro_4.columns if column_name not in columna_a_castear),
                                					*(columna_casteada))
    
    #Casteamos la columna CODMES a integer del dataframe dfMatrizVarTransaccionPosMacrogiro
    columna_a_castear = ['CODMES']
    columna_casteada = [F.col(x).cast("int").alias(x) for x in columna_a_castear]
    dfMatrizVarTransaccionPosMacrogiro_4 = dfMatrizVarTransaccionPosMacrogiro_4.select(
        											*(column_name for column_name in dfMatrizVarTransaccionPosMacrogiro_4.columns if column_name not in columna_a_castear),
                                					*(columna_casteada))
        
    #Retorna el dataframe final dfMatrizVarTransaccionPosMacrogiro luego de pasar por el metodo logicaPostAgrupacionInformacionMesAnalisis
    logicaPostAgrupacionInformacionMesAnalisis(dfMatrizVarTransaccionPosMacrogiro_4)
    dfMatrizVarTransaccionPosMacrogiro_4.unpersist()
    del dfMatrizVarTransaccionPosMacrogiro_4
    
###
 # Actualización de las variables tipo MINIMO previamente calculadas
 # 
 # @param dfMatrizVarTransaccionPosMacrogiro {Dataframe Spark} Dataframe sin los campos Minimos actualizados.
 # @return {Dataframe Spark} Dataframe con los campos Minimos actualizados.
 ##
def logicaPostAgrupacionInformacionMesAnalisis(dfMatrizVarTransaccionPosMacrogiro): 
    
    #Almacena los nombres de las columnas de la tabla dfMatrizVarTransaccionPosMacrogiro
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
    
    #Leemos las columnas del dataframe dfMatrizVarTransaccionPosMacrogiro y las asignamos a una lista
    nameColumn = dfMatrizVarTransaccionPosMacrogiro.schema.names
    
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
    columna_cantidadExpand = []
    
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
        columna_cantidadExpand = [cVariableFinal]
        # OPTIMIZACIÓN: Eliminado coalesce(160) de densidad no controlada

        if not (variableFinal == ''):
            dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrogiro.select(
                                                    *(column_name for column_name in dfMatrizVarTransaccionPosMacrogiro.columns if column_name not in columna_cantidadExpand),
                                                    (F.when(F.col(variableFinal) < cVariableSufijoInt,0).otherwise(F.col(cVariableFinal)).alias(cVariableFinal))
                                                )
        
    #Lee las columnas generadas en la 2da transpuesta tipo FLG (SUM)
    for nColumns in nameColumn:
      if 'POS' in nColumns:      
        if  'FLG' in nColumns:
          if  'MAX' not in nColumns:
            if  'X' in nColumns:
              columFlg.append(nColumns)
     
    #Se asigna el valor de la lista columFlg a la lista colsToExpandFlag 	
    colsToExpandFlag = columFlg
    
    #Se actualizan los sum_uXm para las variables tipo FLG calculas en la 2da transpuesta
    columna_flagExpand = []
    
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
        columna_flagExpand = [variableFinal]
        
        if not (variableFinal == ''): 
            dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrogiro.select(
                                                    *(column_name for column_name in dfMatrizVarTransaccionPosMacrogiro.columns if column_name not in columna_flagExpand),
                                                    (F.when(F.col(variableFinal) == cVariableSufijoInt,1).otherwise(0).alias(variableFinal))
                                                )

    #Se crea la columna FECACTUALIZACIONREGISTRO al dataframe dfMatrizVarTransaccionPosMacrogiro
    
    fechaActualizacionRegistro = funciones.obtenerFechaActual()
    
    columnas = dfMatrizVarTransaccionPosMacrogiro.columns
    dfMatrizVarTransaccionPosMacrogiro = dfMatrizVarTransaccionPosMacrogiro.select(*columnas + 
                                                                       [lit(PRM_FECHA_RUTINA).alias('FECRUTINA').cast('DATE')] + 
                                                                       [lit(fechaActualizacionRegistro).alias('FECACTUALIZACIONREGISTRO')]
                                                                    )
    
    #se almacena el dataframe en la tabla delta
    input_df = dfMatrizVarTransaccionPosMacrogiro.select(col('codunicocli').cast('string').alias('codunicocli'),
												col('codunicocli').cast('varchar(128)').alias('codclaveunicocli'),
												sha2(concat(lit('0001'), trim(col("codinternocomputacional"))),512).cast('varchar(128)').alias('codclavepartycli'),
												trim(col('codinternocomputacional')).cast('string').alias('codinternocomputacional'),
												col('codmesanalisis').cast('int').alias('codmesanalisis'),
												col('pos_flg_trx_ig01_u1m').cast('int').alias('pos_flg_trx_ig01_u1m'),
												col('pos_flg_trx_ig02_u1m').cast('int').alias('pos_flg_trx_ig02_u1m'),
												col('pos_flg_trx_ig03_u1m').cast('int').alias('pos_flg_trx_ig03_u1m'),
												col('pos_flg_trx_ig04_u1m').cast('int').alias('pos_flg_trx_ig04_u1m'),
												col('pos_flg_trx_ig05_u1m').cast('int').alias('pos_flg_trx_ig05_u1m'),
												col('pos_flg_trx_ig06_u1m').cast('int').alias('pos_flg_trx_ig06_u1m'),
												col('pos_flg_trx_ig07_u1m').cast('int').alias('pos_flg_trx_ig07_u1m'),
												col('pos_flg_trx_ig08_u1m').cast('int').alias('pos_flg_trx_ig08_u1m'),
												col('pos_flg_trx_g01_u1m').cast('int').alias('pos_flg_trx_g01_u1m'),
												col('pos_flg_trx_g04_u1m').cast('int').alias('pos_flg_trx_g04_u1m'),
												col('pos_flg_trx_g05_u1m').cast('int').alias('pos_flg_trx_g05_u1m'),
												col('pos_flg_trx_g06_u1m').cast('int').alias('pos_flg_trx_g06_u1m'),
												col('pos_flg_trx_g16_u1m').cast('int').alias('pos_flg_trx_g16_u1m'),
												col('pos_flg_trx_g17_u1m').cast('int').alias('pos_flg_trx_g17_u1m'),
												col('pos_flg_trx_g18_u1m').cast('int').alias('pos_flg_trx_g18_u1m'),
												col('pos_flg_trx_g23_u1m').cast('int').alias('pos_flg_trx_g23_u1m'),
												col('pos_flg_trx_g25_u1m').cast('int').alias('pos_flg_trx_g25_u1m'),
												col('pos_flg_trx_g28_u1m').cast('int').alias('pos_flg_trx_g28_u1m'),
												col('pos_flg_trx_g31_u1m').cast('int').alias('pos_flg_trx_g31_u1m'),
												col('pos_flg_trx_g34_u1m').cast('int').alias('pos_flg_trx_g34_u1m'),
												col('pos_flg_trx_g35_u1m').cast('int').alias('pos_flg_trx_g35_u1m'),
												col('pos_flg_trx_g39_u1m').cast('int').alias('pos_flg_trx_g39_u1m'),
												col('pos_flg_trx_g43_u1m').cast('int').alias('pos_flg_trx_g43_u1m'),
												col('pos_flg_trx_g45_u1m').cast('int').alias('pos_flg_trx_g45_u1m'),
												col('pos_flg_trx_g46_u1m').cast('int').alias('pos_flg_trx_g46_u1m'),
												col('pos_flg_trx_ig01_max_u3m').cast('int').alias('pos_flg_trx_ig01_max_u3m'),
												col('pos_flg_trx_ig01_max_u6m').cast('int').alias('pos_flg_trx_ig01_max_u6m'),
												col('pos_flg_trx_ig01_max_u9m').cast('int').alias('pos_flg_trx_ig01_max_u9m'),
												col('pos_flg_trx_ig01_max_u12').cast('int').alias('pos_flg_trx_ig01_max_u12'),
												col('pos_flg_trx_ig02_max_u3m').cast('int').alias('pos_flg_trx_ig02_max_u3m'),
												col('pos_flg_trx_ig02_max_u6m').cast('int').alias('pos_flg_trx_ig02_max_u6m'),
												col('pos_flg_trx_ig02_max_u9m').cast('int').alias('pos_flg_trx_ig02_max_u9m'),
												col('pos_flg_trx_ig02_max_u12').cast('int').alias('pos_flg_trx_ig02_max_u12'),
												col('pos_flg_trx_ig03_max_u3m').cast('int').alias('pos_flg_trx_ig03_max_u3m'),
												col('pos_flg_trx_ig03_max_u6m').cast('int').alias('pos_flg_trx_ig03_max_u6m'),
												col('pos_flg_trx_ig03_max_u9m').cast('int').alias('pos_flg_trx_ig03_max_u9m'),
												col('pos_flg_trx_ig03_max_u12').cast('int').alias('pos_flg_trx_ig03_max_u12'),
												col('pos_flg_trx_ig04_max_u3m').cast('int').alias('pos_flg_trx_ig04_max_u3m'),
												col('pos_flg_trx_ig04_max_u6m').cast('int').alias('pos_flg_trx_ig04_max_u6m'),
												col('pos_flg_trx_ig04_max_u9m').cast('int').alias('pos_flg_trx_ig04_max_u9m'),
												col('pos_flg_trx_ig04_max_u12').cast('int').alias('pos_flg_trx_ig04_max_u12'),
												col('pos_flg_trx_ig05_max_u3m').cast('int').alias('pos_flg_trx_ig05_max_u3m'),
												col('pos_flg_trx_ig05_max_u6m').cast('int').alias('pos_flg_trx_ig05_max_u6m'),
												col('pos_flg_trx_ig05_max_u9m').cast('int').alias('pos_flg_trx_ig05_max_u9m'),
												col('pos_flg_trx_ig05_max_u12').cast('int').alias('pos_flg_trx_ig05_max_u12'),
												col('pos_flg_trx_ig06_max_u3m').cast('int').alias('pos_flg_trx_ig06_max_u3m'),
												col('pos_flg_trx_ig06_max_u6m').cast('int').alias('pos_flg_trx_ig06_max_u6m'),
												col('pos_flg_trx_ig06_max_u9m').cast('int').alias('pos_flg_trx_ig06_max_u9m'),
												col('pos_flg_trx_ig06_max_u12').cast('int').alias('pos_flg_trx_ig06_max_u12'),
												col('pos_flg_trx_ig07_max_u3m').cast('int').alias('pos_flg_trx_ig07_max_u3m'),
												col('pos_flg_trx_ig07_max_u6m').cast('int').alias('pos_flg_trx_ig07_max_u6m'),
												col('pos_flg_trx_ig07_max_u9m').cast('int').alias('pos_flg_trx_ig07_max_u9m'),
												col('pos_flg_trx_ig07_max_u12').cast('int').alias('pos_flg_trx_ig07_max_u12'),
												col('pos_flg_trx_ig08_max_u3m').cast('int').alias('pos_flg_trx_ig08_max_u3m'),
												col('pos_flg_trx_ig08_max_u6m').cast('int').alias('pos_flg_trx_ig08_max_u6m'),
												col('pos_flg_trx_ig08_max_u9m').cast('int').alias('pos_flg_trx_ig08_max_u9m'),
												col('pos_flg_trx_ig08_max_u12').cast('int').alias('pos_flg_trx_ig08_max_u12'),
												col('pos_flg_trx_g01_max_u3m').cast('int').alias('pos_flg_trx_g01_max_u3m'),
												col('pos_flg_trx_g01_max_u6m').cast('int').alias('pos_flg_trx_g01_max_u6m'),
												col('pos_flg_trx_g01_max_u9m').cast('int').alias('pos_flg_trx_g01_max_u9m'),
												col('pos_flg_trx_g01_max_u12').cast('int').alias('pos_flg_trx_g01_max_u12'),
												col('pos_flg_trx_g04_max_u3m').cast('int').alias('pos_flg_trx_g04_max_u3m'),
												col('pos_flg_trx_g04_max_u6m').cast('int').alias('pos_flg_trx_g04_max_u6m'),
												col('pos_flg_trx_g04_max_u9m').cast('int').alias('pos_flg_trx_g04_max_u9m'),
												col('pos_flg_trx_g04_max_u12').cast('int').alias('pos_flg_trx_g04_max_u12'),
												col('pos_flg_trx_g05_max_u3m').cast('int').alias('pos_flg_trx_g05_max_u3m'),
												col('pos_flg_trx_g05_max_u6m').cast('int').alias('pos_flg_trx_g05_max_u6m'),
												col('pos_flg_trx_g05_max_u9m').cast('int').alias('pos_flg_trx_g05_max_u9m'),
												col('pos_flg_trx_g05_max_u12').cast('int').alias('pos_flg_trx_g05_max_u12'),
												col('pos_flg_trx_g06_max_u3m').cast('int').alias('pos_flg_trx_g06_max_u3m'),
												col('pos_flg_trx_g06_max_u6m').cast('int').alias('pos_flg_trx_g06_max_u6m'),
												col('pos_flg_trx_g06_max_u9m').cast('int').alias('pos_flg_trx_g06_max_u9m'),
												col('pos_flg_trx_g06_max_u12').cast('int').alias('pos_flg_trx_g06_max_u12'),
												col('pos_flg_trx_g16_max_u3m').cast('int').alias('pos_flg_trx_g16_max_u3m'),
												col('pos_flg_trx_g16_max_u6m').cast('int').alias('pos_flg_trx_g16_max_u6m'),
												col('pos_flg_trx_g16_max_u9m').cast('int').alias('pos_flg_trx_g16_max_u9m'),
												col('pos_flg_trx_g16_max_u12').cast('int').alias('pos_flg_trx_g16_max_u12'),
												col('pos_flg_trx_g17_max_u3m').cast('int').alias('pos_flg_trx_g17_max_u3m'),
												col('pos_flg_trx_g17_max_u6m').cast('int').alias('pos_flg_trx_g17_max_u6m'),
												col('pos_flg_trx_g17_max_u9m').cast('int').alias('pos_flg_trx_g17_max_u9m'),
												col('pos_flg_trx_g17_max_u12').cast('int').alias('pos_flg_trx_g17_max_u12'),
												col('pos_flg_trx_g18_max_u3m').cast('int').alias('pos_flg_trx_g18_max_u3m'),
												col('pos_flg_trx_g18_max_u6m').cast('int').alias('pos_flg_trx_g18_max_u6m'),
												col('pos_flg_trx_g18_max_u9m').cast('int').alias('pos_flg_trx_g18_max_u9m'),
												col('pos_flg_trx_g18_max_u12').cast('int').alias('pos_flg_trx_g18_max_u12'),
												col('pos_flg_trx_g23_max_u3m').cast('int').alias('pos_flg_trx_g23_max_u3m'),
												col('pos_flg_trx_g23_max_u6m').cast('int').alias('pos_flg_trx_g23_max_u6m'),
												col('pos_flg_trx_g23_max_u9m').cast('int').alias('pos_flg_trx_g23_max_u9m'),
												col('pos_flg_trx_g23_max_u12').cast('int').alias('pos_flg_trx_g23_max_u12'),
												col('pos_flg_trx_g25_max_u3m').cast('int').alias('pos_flg_trx_g25_max_u3m'),
												col('pos_flg_trx_g25_max_u6m').cast('int').alias('pos_flg_trx_g25_max_u6m'),
												col('pos_flg_trx_g25_max_u9m').cast('int').alias('pos_flg_trx_g25_max_u9m'),
												col('pos_flg_trx_g25_max_u12').cast('int').alias('pos_flg_trx_g25_max_u12'),
												col('pos_flg_trx_g28_max_u3m').cast('int').alias('pos_flg_trx_g28_max_u3m'),
												col('pos_flg_trx_g28_max_u6m').cast('int').alias('pos_flg_trx_g28_max_u6m'),
												col('pos_flg_trx_g28_max_u9m').cast('int').alias('pos_flg_trx_g28_max_u9m'),
												col('pos_flg_trx_g28_max_u12').cast('int').alias('pos_flg_trx_g28_max_u12'),
												col('pos_flg_trx_g31_max_u3m').cast('int').alias('pos_flg_trx_g31_max_u3m'),
												col('pos_flg_trx_g31_max_u6m').cast('int').alias('pos_flg_trx_g31_max_u6m'),
												col('pos_flg_trx_g31_max_u9m').cast('int').alias('pos_flg_trx_g31_max_u9m'),
												col('pos_flg_trx_g31_max_u12').cast('int').alias('pos_flg_trx_g31_max_u12'),
												col('pos_flg_trx_g34_max_u3m').cast('int').alias('pos_flg_trx_g34_max_u3m'),
												col('pos_flg_trx_g34_max_u6m').cast('int').alias('pos_flg_trx_g34_max_u6m'),
												col('pos_flg_trx_g34_max_u9m').cast('int').alias('pos_flg_trx_g34_max_u9m'),
												col('pos_flg_trx_g34_max_u12').cast('int').alias('pos_flg_trx_g34_max_u12'),
												col('pos_flg_trx_g35_max_u3m').cast('int').alias('pos_flg_trx_g35_max_u3m'),
												col('pos_flg_trx_g35_max_u6m').cast('int').alias('pos_flg_trx_g35_max_u6m'),
												col('pos_flg_trx_g35_max_u9m').cast('int').alias('pos_flg_trx_g35_max_u9m'),
												col('pos_flg_trx_g35_max_u12').cast('int').alias('pos_flg_trx_g35_max_u12'),
												col('pos_flg_trx_g39_max_u3m').cast('int').alias('pos_flg_trx_g39_max_u3m'),
												col('pos_flg_trx_g39_max_u6m').cast('int').alias('pos_flg_trx_g39_max_u6m'),
												col('pos_flg_trx_g39_max_u9m').cast('int').alias('pos_flg_trx_g39_max_u9m'),
												col('pos_flg_trx_g39_max_u12').cast('int').alias('pos_flg_trx_g39_max_u12'),
												col('pos_flg_trx_g43_max_u3m').cast('int').alias('pos_flg_trx_g43_max_u3m'),
												col('pos_flg_trx_g43_max_u6m').cast('int').alias('pos_flg_trx_g43_max_u6m'),
												col('pos_flg_trx_g43_max_u9m').cast('int').alias('pos_flg_trx_g43_max_u9m'),
												col('pos_flg_trx_g43_max_u12').cast('int').alias('pos_flg_trx_g43_max_u12'),
												col('pos_flg_trx_g45_max_u3m').cast('int').alias('pos_flg_trx_g45_max_u3m'),
												col('pos_flg_trx_g45_max_u6m').cast('int').alias('pos_flg_trx_g45_max_u6m'),
												col('pos_flg_trx_g45_max_u9m').cast('int').alias('pos_flg_trx_g45_max_u9m'),
												col('pos_flg_trx_g45_max_u12').cast('int').alias('pos_flg_trx_g45_max_u12'),
												col('pos_flg_trx_g46_max_u3m').cast('int').alias('pos_flg_trx_g46_max_u3m'),
												col('pos_flg_trx_g46_max_u6m').cast('int').alias('pos_flg_trx_g46_max_u6m'),
												col('pos_flg_trx_g46_max_u9m').cast('int').alias('pos_flg_trx_g46_max_u9m'),
												col('pos_flg_trx_g46_max_u12').cast('int').alias('pos_flg_trx_g46_max_u12'),
												col('pos_flg_trx_ig01_x3m_u3m').cast('int').alias('pos_flg_trx_ig01_x3m_u3m'),
												col('pos_flg_trx_ig01_x6m_u6m').cast('int').alias('pos_flg_trx_ig01_x6m_u6m'),
												col('pos_flg_trx_ig01_x9m_u9m').cast('int').alias('pos_flg_trx_ig01_x9m_u9m'),
												col('pos_flg_trx_ig01_x12_u12').cast('int').alias('pos_flg_trx_ig01_x12_u12'),
												col('pos_flg_trx_ig02_x3m_u3m').cast('int').alias('pos_flg_trx_ig02_x3m_u3m'),
												col('pos_flg_trx_ig02_x6m_u6m').cast('int').alias('pos_flg_trx_ig02_x6m_u6m'),
												col('pos_flg_trx_ig02_x9m_u9m').cast('int').alias('pos_flg_trx_ig02_x9m_u9m'),
												col('pos_flg_trx_ig02_x12_u12').cast('int').alias('pos_flg_trx_ig02_x12_u12'),
												col('pos_flg_trx_ig03_x3m_u3m').cast('int').alias('pos_flg_trx_ig03_x3m_u3m'),
												col('pos_flg_trx_ig03_x6m_u6m').cast('int').alias('pos_flg_trx_ig03_x6m_u6m'),
												col('pos_flg_trx_ig03_x9m_u9m').cast('int').alias('pos_flg_trx_ig03_x9m_u9m'),
												col('pos_flg_trx_ig03_x12_u12').cast('int').alias('pos_flg_trx_ig03_x12_u12'),
												col('pos_flg_trx_ig04_x3m_u3m').cast('int').alias('pos_flg_trx_ig04_x3m_u3m'),
												col('pos_flg_trx_ig04_x6m_u6m').cast('int').alias('pos_flg_trx_ig04_x6m_u6m'),
												col('pos_flg_trx_ig04_x9m_u9m').cast('int').alias('pos_flg_trx_ig04_x9m_u9m'),
												col('pos_flg_trx_ig04_x12_u12').cast('int').alias('pos_flg_trx_ig04_x12_u12'),
												col('pos_flg_trx_ig05_x3m_u3m').cast('int').alias('pos_flg_trx_ig05_x3m_u3m'),
												col('pos_flg_trx_ig05_x6m_u6m').cast('int').alias('pos_flg_trx_ig05_x6m_u6m'),
												col('pos_flg_trx_ig05_x9m_u9m').cast('int').alias('pos_flg_trx_ig05_x9m_u9m'),
												col('pos_flg_trx_ig05_x12_u12').cast('int').alias('pos_flg_trx_ig05_x12_u12'),
												col('pos_flg_trx_ig06_x3m_u3m').cast('int').alias('pos_flg_trx_ig06_x3m_u3m'),
												col('pos_flg_trx_ig06_x6m_u6m').cast('int').alias('pos_flg_trx_ig06_x6m_u6m'),
												col('pos_flg_trx_ig06_x9m_u9m').cast('int').alias('pos_flg_trx_ig06_x9m_u9m'),
												col('pos_flg_trx_ig06_x12_u12').cast('int').alias('pos_flg_trx_ig06_x12_u12'),
												col('pos_flg_trx_ig07_x3m_u3m').cast('int').alias('pos_flg_trx_ig07_x3m_u3m'),
												col('pos_flg_trx_ig07_x6m_u6m').cast('int').alias('pos_flg_trx_ig07_x6m_u6m'),
												col('pos_flg_trx_ig07_x9m_u9m').cast('int').alias('pos_flg_trx_ig07_x9m_u9m'),
												col('pos_flg_trx_ig07_x12_u12').cast('int').alias('pos_flg_trx_ig07_x12_u12'),
												col('pos_flg_trx_ig08_x3m_u3m').cast('int').alias('pos_flg_trx_ig08_x3m_u3m'),
												col('pos_flg_trx_ig08_x6m_u6m').cast('int').alias('pos_flg_trx_ig08_x6m_u6m'),
												col('pos_flg_trx_ig08_x9m_u9m').cast('int').alias('pos_flg_trx_ig08_x9m_u9m'),
												col('pos_flg_trx_ig08_x12_u12').cast('int').alias('pos_flg_trx_ig08_x12_u12'),
												col('pos_flg_trx_g01_x3m_u3m').cast('int').alias('pos_flg_trx_g01_x3m_u3m'),
												col('pos_flg_trx_g01_x6m_u6m').cast('int').alias('pos_flg_trx_g01_x6m_u6m'),
												col('pos_flg_trx_g01_x9m_u9m').cast('int').alias('pos_flg_trx_g01_x9m_u9m'),
												col('pos_flg_trx_g01_x12_u12').cast('int').alias('pos_flg_trx_g01_x12_u12'),
												col('pos_flg_trx_g04_x3m_u3m').cast('int').alias('pos_flg_trx_g04_x3m_u3m'),
												col('pos_flg_trx_g04_x6m_u6m').cast('int').alias('pos_flg_trx_g04_x6m_u6m'),
												col('pos_flg_trx_g04_x9m_u9m').cast('int').alias('pos_flg_trx_g04_x9m_u9m'),
												col('pos_flg_trx_g04_x12_u12').cast('int').alias('pos_flg_trx_g04_x12_u12'),
												col('pos_flg_trx_g05_x3m_u3m').cast('int').alias('pos_flg_trx_g05_x3m_u3m'),
												col('pos_flg_trx_g05_x6m_u6m').cast('int').alias('pos_flg_trx_g05_x6m_u6m'),
												col('pos_flg_trx_g05_x9m_u9m').cast('int').alias('pos_flg_trx_g05_x9m_u9m'),
												col('pos_flg_trx_g05_x12_u12').cast('int').alias('pos_flg_trx_g05_x12_u12'),
												col('pos_flg_trx_g06_x3m_u3m').cast('int').alias('pos_flg_trx_g06_x3m_u3m'),
												col('pos_flg_trx_g06_x6m_u6m').cast('int').alias('pos_flg_trx_g06_x6m_u6m'),
												col('pos_flg_trx_g06_x9m_u9m').cast('int').alias('pos_flg_trx_g06_x9m_u9m'),
												col('pos_flg_trx_g06_x12_u12').cast('int').alias('pos_flg_trx_g06_x12_u12'),
												col('pos_flg_trx_g16_x3m_u3m').cast('int').alias('pos_flg_trx_g16_x3m_u3m'),
												col('pos_flg_trx_g16_x6m_u6m').cast('int').alias('pos_flg_trx_g16_x6m_u6m'),
												col('pos_flg_trx_g16_x9m_u9m').cast('int').alias('pos_flg_trx_g16_x9m_u9m'),
												col('pos_flg_trx_g16_x12_u12').cast('int').alias('pos_flg_trx_g16_x12_u12'),
												col('pos_flg_trx_g17_x3m_u3m').cast('int').alias('pos_flg_trx_g17_x3m_u3m'),
												col('pos_flg_trx_g17_x6m_u6m').cast('int').alias('pos_flg_trx_g17_x6m_u6m'),
												col('pos_flg_trx_g17_x9m_u9m').cast('int').alias('pos_flg_trx_g17_x9m_u9m'),
												col('pos_flg_trx_g17_x12_u12').cast('int').alias('pos_flg_trx_g17_x12_u12'),
												col('pos_flg_trx_g18_x3m_u3m').cast('int').alias('pos_flg_trx_g18_x3m_u3m'),
												col('pos_flg_trx_g18_x6m_u6m').cast('int').alias('pos_flg_trx_g18_x6m_u6m'),
												col('pos_flg_trx_g18_x9m_u9m').cast('int').alias('pos_flg_trx_g18_x9m_u9m'),
												col('pos_flg_trx_g18_x12_u12').cast('int').alias('pos_flg_trx_g18_x12_u12'),
												col('pos_flg_trx_g23_x3m_u3m').cast('int').alias('pos_flg_trx_g23_x3m_u3m'),
												col('pos_flg_trx_g23_x6m_u6m').cast('int').alias('pos_flg_trx_g23_x6m_u6m'),
												col('pos_flg_trx_g23_x9m_u9m').cast('int').alias('pos_flg_trx_g23_x9m_u9m'),
												col('pos_flg_trx_g23_x12_u12').cast('int').alias('pos_flg_trx_g23_x12_u12'),
												col('pos_flg_trx_g25_x3m_u3m').cast('int').alias('pos_flg_trx_g25_x3m_u3m'),
												col('pos_flg_trx_g25_x6m_u6m').cast('int').alias('pos_flg_trx_g25_x6m_u6m'),
												col('pos_flg_trx_g25_x9m_u9m').cast('int').alias('pos_flg_trx_g25_x9m_u9m'),
												col('pos_flg_trx_g25_x12_u12').cast('int').alias('pos_flg_trx_g25_x12_u12'),
												col('pos_flg_trx_g28_x3m_u3m').cast('int').alias('pos_flg_trx_g28_x3m_u3m'),
												col('pos_flg_trx_g28_x6m_u6m').cast('int').alias('pos_flg_trx_g28_x6m_u6m'),
												col('pos_flg_trx_g28_x9m_u9m').cast('int').alias('pos_flg_trx_g28_x9m_u9m'),
												col('pos_flg_trx_g28_x12_u12').cast('int').alias('pos_flg_trx_g28_x12_u12'),
												col('pos_flg_trx_g31_x3m_u3m').cast('int').alias('pos_flg_trx_g31_x3m_u3m'),
												col('pos_flg_trx_g31_x6m_u6m').cast('int').alias('pos_flg_trx_g31_x6m_u6m'),
												col('pos_flg_trx_g31_x9m_u9m').cast('int').alias('pos_flg_trx_g31_x9m_u9m'),
												col('pos_flg_trx_g31_x12_u12').cast('int').alias('pos_flg_trx_g31_x12_u12'),
												col('pos_flg_trx_g34_x3m_u3m').cast('int').alias('pos_flg_trx_g34_x3m_u3m'),
												col('pos_flg_trx_g34_x6m_u6m').cast('int').alias('pos_flg_trx_g34_x6m_u6m'),
												col('pos_flg_trx_g34_x9m_u9m').cast('int').alias('pos_flg_trx_g34_x9m_u9m'),
												col('pos_flg_trx_g34_x12_u12').cast('int').alias('pos_flg_trx_g34_x12_u12'),
												col('pos_flg_trx_g35_x3m_u3m').cast('int').alias('pos_flg_trx_g35_x3m_u3m'),
												col('pos_flg_trx_g35_x6m_u6m').cast('int').alias('pos_flg_trx_g35_x6m_u6m'),
												col('pos_flg_trx_g35_x9m_u9m').cast('int').alias('pos_flg_trx_g35_x9m_u9m'),
												col('pos_flg_trx_g35_x12_u12').cast('int').alias('pos_flg_trx_g35_x12_u12'),
												col('pos_flg_trx_g39_x3m_u3m').cast('int').alias('pos_flg_trx_g39_x3m_u3m'),
												col('pos_flg_trx_g39_x6m_u6m').cast('int').alias('pos_flg_trx_g39_x6m_u6m'),
												col('pos_flg_trx_g39_x9m_u9m').cast('int').alias('pos_flg_trx_g39_x9m_u9m'),
												col('pos_flg_trx_g39_x12_u12').cast('int').alias('pos_flg_trx_g39_x12_u12'),
												col('pos_flg_trx_g43_x3m_u3m').cast('int').alias('pos_flg_trx_g43_x3m_u3m'),
												col('pos_flg_trx_g43_x6m_u6m').cast('int').alias('pos_flg_trx_g43_x6m_u6m'),
												col('pos_flg_trx_g43_x9m_u9m').cast('int').alias('pos_flg_trx_g43_x9m_u9m'),
												col('pos_flg_trx_g43_x12_u12').cast('int').alias('pos_flg_trx_g43_x12_u12'),
												col('pos_flg_trx_g45_x3m_u3m').cast('int').alias('pos_flg_trx_g45_x3m_u3m'),
												col('pos_flg_trx_g45_x6m_u6m').cast('int').alias('pos_flg_trx_g45_x6m_u6m'),
												col('pos_flg_trx_g45_x9m_u9m').cast('int').alias('pos_flg_trx_g45_x9m_u9m'),
												col('pos_flg_trx_g45_x12_u12').cast('int').alias('pos_flg_trx_g45_x12_u12'),
												col('pos_flg_trx_g46_x3m_u3m').cast('int').alias('pos_flg_trx_g46_x3m_u3m'),
												col('pos_flg_trx_g46_x6m_u6m').cast('int').alias('pos_flg_trx_g46_x6m_u6m'),
												col('pos_flg_trx_g46_x9m_u9m').cast('int').alias('pos_flg_trx_g46_x9m_u9m'),
												col('pos_flg_trx_g46_x12_u12').cast('int').alias('pos_flg_trx_g46_x12_u12'),
												col('pos_mto_trx_ig01_u1m').cast('double').alias('pos_mto_trx_ig01_u1m'),
												col('pos_mto_trx_ig01_prm_u3m').cast('double').alias('pos_mto_trx_ig01_prm_u3m'),
												col('pos_mto_trx_ig01_prm_u6m').cast('double').alias('pos_mto_trx_ig01_prm_u6m'),
												col('pos_mto_trx_ig01_prm_u9m').cast('double').alias('pos_mto_trx_ig01_prm_u9m'),
												col('pos_mto_trx_ig01_prm_u12').cast('double').alias('pos_mto_trx_ig01_prm_u12'),
												col('pos_mto_trx_ig02_u1m').cast('double').alias('pos_mto_trx_ig02_u1m'),
												col('pos_mto_trx_ig02_prm_u3m').cast('double').alias('pos_mto_trx_ig02_prm_u3m'),
												col('pos_mto_trx_ig02_prm_u6m').cast('double').alias('pos_mto_trx_ig02_prm_u6m'),
												col('pos_mto_trx_ig02_prm_u9m').cast('double').alias('pos_mto_trx_ig02_prm_u9m'),
												col('pos_mto_trx_ig02_prm_u12').cast('double').alias('pos_mto_trx_ig02_prm_u12'),
												col('pos_mto_trx_ig03_u1m').cast('double').alias('pos_mto_trx_ig03_u1m'),
												col('pos_mto_trx_ig03_prm_u3m').cast('double').alias('pos_mto_trx_ig03_prm_u3m'),
												col('pos_mto_trx_ig03_prm_u6m').cast('double').alias('pos_mto_trx_ig03_prm_u6m'),
												col('pos_mto_trx_ig03_prm_u9m').cast('double').alias('pos_mto_trx_ig03_prm_u9m'),
												col('pos_mto_trx_ig03_prm_u12').cast('double').alias('pos_mto_trx_ig03_prm_u12'),
												col('pos_mto_trx_ig04_u1m').cast('double').alias('pos_mto_trx_ig04_u1m'),
												col('pos_mto_trx_ig04_prm_u3m').cast('double').alias('pos_mto_trx_ig04_prm_u3m'),
												col('pos_mto_trx_ig04_prm_u6m').cast('double').alias('pos_mto_trx_ig04_prm_u6m'),
												col('pos_mto_trx_ig04_prm_u9m').cast('double').alias('pos_mto_trx_ig04_prm_u9m'),
												col('pos_mto_trx_ig04_prm_u12').cast('double').alias('pos_mto_trx_ig04_prm_u12'),
												col('pos_mto_trx_ig05_u1m').cast('double').alias('pos_mto_trx_ig05_u1m'),
												col('pos_mto_trx_ig05_prm_u3m').cast('double').alias('pos_mto_trx_ig05_prm_u3m'),
												col('pos_mto_trx_ig05_prm_u6m').cast('double').alias('pos_mto_trx_ig05_prm_u6m'),
												col('pos_mto_trx_ig05_prm_u9m').cast('double').alias('pos_mto_trx_ig05_prm_u9m'),
												col('pos_mto_trx_ig05_prm_u12').cast('double').alias('pos_mto_trx_ig05_prm_u12'),
												col('pos_mto_trx_ig06_u1m').cast('double').alias('pos_mto_trx_ig06_u1m'),
												col('pos_mto_trx_ig06_prm_u3m').cast('double').alias('pos_mto_trx_ig06_prm_u3m'),
												col('pos_mto_trx_ig06_prm_u6m').cast('double').alias('pos_mto_trx_ig06_prm_u6m'),
												col('pos_mto_trx_ig06_prm_u9m').cast('double').alias('pos_mto_trx_ig06_prm_u9m'),
												col('pos_mto_trx_ig06_prm_u12').cast('double').alias('pos_mto_trx_ig06_prm_u12'),
												col('pos_mto_trx_ig07_u1m').cast('double').alias('pos_mto_trx_ig07_u1m'),
												col('pos_mto_trx_ig07_prm_u3m').cast('double').alias('pos_mto_trx_ig07_prm_u3m'),
												col('pos_mto_trx_ig07_prm_u6m').cast('double').alias('pos_mto_trx_ig07_prm_u6m'),
												col('pos_mto_trx_ig07_prm_u9m').cast('double').alias('pos_mto_trx_ig07_prm_u9m'),
												col('pos_mto_trx_ig07_prm_u12').cast('double').alias('pos_mto_trx_ig07_prm_u12'),
												col('pos_mto_trx_ig08_u1m').cast('double').alias('pos_mto_trx_ig08_u1m'),
												col('pos_mto_trx_ig08_prm_u3m').cast('double').alias('pos_mto_trx_ig08_prm_u3m'),
												col('pos_mto_trx_ig08_prm_u6m').cast('double').alias('pos_mto_trx_ig08_prm_u6m'),
												col('pos_mto_trx_ig08_prm_u9m').cast('double').alias('pos_mto_trx_ig08_prm_u9m'),
												col('pos_mto_trx_ig08_prm_u12').cast('double').alias('pos_mto_trx_ig08_prm_u12'),
												col('pos_mto_trx_g01_u1m').cast('double').alias('pos_mto_trx_g01_u1m'),
												col('pos_mto_trx_g01_prm_u3m').cast('double').alias('pos_mto_trx_g01_prm_u3m'),
												col('pos_mto_trx_g01_prm_u6m').cast('double').alias('pos_mto_trx_g01_prm_u6m'),
												col('pos_mto_trx_g01_prm_u9m').cast('double').alias('pos_mto_trx_g01_prm_u9m'),
												col('pos_mto_trx_g01_prm_u12').cast('double').alias('pos_mto_trx_g01_prm_u12'),
												col('pos_mto_trx_g04_u1m').cast('double').alias('pos_mto_trx_g04_u1m'),
												col('pos_mto_trx_g04_prm_u3m').cast('double').alias('pos_mto_trx_g04_prm_u3m'),
												col('pos_mto_trx_g04_prm_u6m').cast('double').alias('pos_mto_trx_g04_prm_u6m'),
												col('pos_mto_trx_g04_prm_u9m').cast('double').alias('pos_mto_trx_g04_prm_u9m'),
												col('pos_mto_trx_g04_prm_u12').cast('double').alias('pos_mto_trx_g04_prm_u12'),
												col('pos_mto_trx_g05_u1m').cast('double').alias('pos_mto_trx_g05_u1m'),
												col('pos_mto_trx_g05_prm_u3m').cast('double').alias('pos_mto_trx_g05_prm_u3m'),
												col('pos_mto_trx_g05_prm_u6m').cast('double').alias('pos_mto_trx_g05_prm_u6m'),
												col('pos_mto_trx_g05_prm_u9m').cast('double').alias('pos_mto_trx_g05_prm_u9m'),
												col('pos_mto_trx_g05_prm_u12').cast('double').alias('pos_mto_trx_g05_prm_u12'),
												col('pos_mto_trx_g06_u1m').cast('double').alias('pos_mto_trx_g06_u1m'),
												col('pos_mto_trx_g06_prm_u3m').cast('double').alias('pos_mto_trx_g06_prm_u3m'),
												col('pos_mto_trx_g06_prm_u6m').cast('double').alias('pos_mto_trx_g06_prm_u6m'),
												col('pos_mto_trx_g06_prm_u9m').cast('double').alias('pos_mto_trx_g06_prm_u9m'),
												col('pos_mto_trx_g06_prm_u12').cast('double').alias('pos_mto_trx_g06_prm_u12'),
												col('pos_mto_trx_g16_u1m').cast('double').alias('pos_mto_trx_g16_u1m'),
												col('pos_mto_trx_g16_prm_u3m').cast('double').alias('pos_mto_trx_g16_prm_u3m'),
												col('pos_mto_trx_g16_prm_u6m').cast('double').alias('pos_mto_trx_g16_prm_u6m'),
												col('pos_mto_trx_g16_prm_u9m').cast('double').alias('pos_mto_trx_g16_prm_u9m'),
												col('pos_mto_trx_g16_prm_u12').cast('double').alias('pos_mto_trx_g16_prm_u12'),
												col('pos_mto_trx_g17_u1m').cast('double').alias('pos_mto_trx_g17_u1m'),
												col('pos_mto_trx_g17_prm_u3m').cast('double').alias('pos_mto_trx_g17_prm_u3m'),
												col('pos_mto_trx_g17_prm_u6m').cast('double').alias('pos_mto_trx_g17_prm_u6m'),
												col('pos_mto_trx_g17_prm_u9m').cast('double').alias('pos_mto_trx_g17_prm_u9m'),
												col('pos_mto_trx_g17_prm_u12').cast('double').alias('pos_mto_trx_g17_prm_u12'),
												col('pos_mto_trx_g18_u1m').cast('double').alias('pos_mto_trx_g18_u1m'),
												col('pos_mto_trx_g18_prm_u3m').cast('double').alias('pos_mto_trx_g18_prm_u3m'),
												col('pos_mto_trx_g18_prm_u6m').cast('double').alias('pos_mto_trx_g18_prm_u6m'),
												col('pos_mto_trx_g18_prm_u9m').cast('double').alias('pos_mto_trx_g18_prm_u9m'),
												col('pos_mto_trx_g18_prm_u12').cast('double').alias('pos_mto_trx_g18_prm_u12'),
												col('pos_mto_trx_g23_u1m').cast('double').alias('pos_mto_trx_g23_u1m'),
												col('pos_mto_trx_g23_prm_u3m').cast('double').alias('pos_mto_trx_g23_prm_u3m'),
												col('pos_mto_trx_g23_prm_u6m').cast('double').alias('pos_mto_trx_g23_prm_u6m'),
												col('pos_mto_trx_g23_prm_u9m').cast('double').alias('pos_mto_trx_g23_prm_u9m'),
												col('pos_mto_trx_g23_prm_u12').cast('double').alias('pos_mto_trx_g23_prm_u12'),
												col('pos_mto_trx_g25_u1m').cast('double').alias('pos_mto_trx_g25_u1m'),
												col('pos_mto_trx_g25_prm_u3m').cast('double').alias('pos_mto_trx_g25_prm_u3m'),
												col('pos_mto_trx_g25_prm_u6m').cast('double').alias('pos_mto_trx_g25_prm_u6m'),
												col('pos_mto_trx_g25_prm_u9m').cast('double').alias('pos_mto_trx_g25_prm_u9m'),
												col('pos_mto_trx_g25_prm_u12').cast('double').alias('pos_mto_trx_g25_prm_u12'),
												col('pos_mto_trx_g28_u1m').cast('double').alias('pos_mto_trx_g28_u1m'),
												col('pos_mto_trx_g28_prm_u3m').cast('double').alias('pos_mto_trx_g28_prm_u3m'),
												col('pos_mto_trx_g28_prm_u6m').cast('double').alias('pos_mto_trx_g28_prm_u6m'),
												col('pos_mto_trx_g28_prm_u9m').cast('double').alias('pos_mto_trx_g28_prm_u9m'),
												col('pos_mto_trx_g28_prm_u12').cast('double').alias('pos_mto_trx_g28_prm_u12'),
												col('pos_mto_trx_g31_u1m').cast('double').alias('pos_mto_trx_g31_u1m'),
												col('pos_mto_trx_g31_prm_u3m').cast('double').alias('pos_mto_trx_g31_prm_u3m'),
												col('pos_mto_trx_g31_prm_u6m').cast('double').alias('pos_mto_trx_g31_prm_u6m'),
												col('pos_mto_trx_g31_prm_u9m').cast('double').alias('pos_mto_trx_g31_prm_u9m'),
												col('pos_mto_trx_g31_prm_u12').cast('double').alias('pos_mto_trx_g31_prm_u12'),
												col('pos_mto_trx_g34_u1m').cast('double').alias('pos_mto_trx_g34_u1m'),
												col('pos_mto_trx_g34_prm_u3m').cast('double').alias('pos_mto_trx_g34_prm_u3m'),
												col('pos_mto_trx_g34_prm_u6m').cast('double').alias('pos_mto_trx_g34_prm_u6m'),
												col('pos_mto_trx_g34_prm_u9m').cast('double').alias('pos_mto_trx_g34_prm_u9m'),
												col('pos_mto_trx_g34_prm_u12').cast('double').alias('pos_mto_trx_g34_prm_u12'),
												col('pos_mto_trx_g35_u1m').cast('double').alias('pos_mto_trx_g35_u1m'),
												col('pos_mto_trx_g35_prm_u3m').cast('double').alias('pos_mto_trx_g35_prm_u3m'),
												col('pos_mto_trx_g35_prm_u6m').cast('double').alias('pos_mto_trx_g35_prm_u6m'),
												col('pos_mto_trx_g35_prm_u9m').cast('double').alias('pos_mto_trx_g35_prm_u9m'),
												col('pos_mto_trx_g35_prm_u12').cast('double').alias('pos_mto_trx_g35_prm_u12'),
												col('pos_mto_trx_g39_u1m').cast('double').alias('pos_mto_trx_g39_u1m'),
												col('pos_mto_trx_g39_prm_u3m').cast('double').alias('pos_mto_trx_g39_prm_u3m'),
												col('pos_mto_trx_g39_prm_u6m').cast('double').alias('pos_mto_trx_g39_prm_u6m'),
												col('pos_mto_trx_g39_prm_u9m').cast('double').alias('pos_mto_trx_g39_prm_u9m'),
												col('pos_mto_trx_g39_prm_u12').cast('double').alias('pos_mto_trx_g39_prm_u12'),
												col('pos_mto_trx_g43_u1m').cast('double').alias('pos_mto_trx_g43_u1m'),
												col('pos_mto_trx_g43_prm_u3m').cast('double').alias('pos_mto_trx_g43_prm_u3m'),
												col('pos_mto_trx_g43_prm_u6m').cast('double').alias('pos_mto_trx_g43_prm_u6m'),
												col('pos_mto_trx_g43_prm_u9m').cast('double').alias('pos_mto_trx_g43_prm_u9m'),
												col('pos_mto_trx_g43_prm_u12').cast('double').alias('pos_mto_trx_g43_prm_u12'),
												col('pos_mto_trx_g45_u1m').cast('double').alias('pos_mto_trx_g45_u1m'),
												col('pos_mto_trx_g45_prm_u3m').cast('double').alias('pos_mto_trx_g45_prm_u3m'),
												col('pos_mto_trx_g45_prm_u6m').cast('double').alias('pos_mto_trx_g45_prm_u6m'),
												col('pos_mto_trx_g45_prm_u9m').cast('double').alias('pos_mto_trx_g45_prm_u9m'),
												col('pos_mto_trx_g45_prm_u12').cast('double').alias('pos_mto_trx_g45_prm_u12'),
												col('pos_mto_trx_g46_u1m').cast('double').alias('pos_mto_trx_g46_u1m'),
												col('pos_mto_trx_g46_prm_u3m').cast('double').alias('pos_mto_trx_g46_prm_u3m'),
												col('pos_mto_trx_g46_prm_u6m').cast('double').alias('pos_mto_trx_g46_prm_u6m'),
												col('pos_mto_trx_g46_prm_u9m').cast('double').alias('pos_mto_trx_g46_prm_u9m'),
												col('pos_mto_trx_g46_prm_u12').cast('double').alias('pos_mto_trx_g46_prm_u12'),
												col('pos_mto_trx_ig01_prm_p6m').cast('double').alias('pos_mto_trx_ig01_prm_p6m'),
												col('pos_mto_trx_ig02_prm_p6m').cast('double').alias('pos_mto_trx_ig02_prm_p6m'),
												col('pos_mto_trx_ig03_prm_p6m').cast('double').alias('pos_mto_trx_ig03_prm_p6m'),
												col('pos_mto_trx_ig04_prm_p6m').cast('double').alias('pos_mto_trx_ig04_prm_p6m'),
												col('pos_mto_trx_ig05_prm_p6m').cast('double').alias('pos_mto_trx_ig05_prm_p6m'),
												col('pos_mto_trx_ig06_prm_p6m').cast('double').alias('pos_mto_trx_ig06_prm_p6m'),
												col('pos_mto_trx_ig07_prm_p6m').cast('double').alias('pos_mto_trx_ig07_prm_p6m'),
												col('pos_mto_trx_ig08_prm_p6m').cast('double').alias('pos_mto_trx_ig08_prm_p6m'),
												col('pos_mto_trx_g01_prm_p6m').cast('double').alias('pos_mto_trx_g01_prm_p6m'),
												col('pos_mto_trx_g04_prm_p6m').cast('double').alias('pos_mto_trx_g04_prm_p6m'),
												col('pos_mto_trx_g05_prm_p6m').cast('double').alias('pos_mto_trx_g05_prm_p6m'),
												col('pos_mto_trx_g06_prm_p6m').cast('double').alias('pos_mto_trx_g06_prm_p6m'),
												col('pos_mto_trx_g16_prm_p6m').cast('double').alias('pos_mto_trx_g16_prm_p6m'),
												col('pos_mto_trx_g17_prm_p6m').cast('double').alias('pos_mto_trx_g17_prm_p6m'),
												col('pos_mto_trx_g18_prm_p6m').cast('double').alias('pos_mto_trx_g18_prm_p6m'),
												col('pos_mto_trx_g23_prm_p6m').cast('double').alias('pos_mto_trx_g23_prm_p6m'),
												col('pos_mto_trx_g25_prm_p6m').cast('double').alias('pos_mto_trx_g25_prm_p6m'),
												col('pos_mto_trx_g28_prm_p6m').cast('double').alias('pos_mto_trx_g28_prm_p6m'),
												col('pos_mto_trx_g31_prm_p6m').cast('double').alias('pos_mto_trx_g31_prm_p6m'),
												col('pos_mto_trx_g34_prm_p6m').cast('double').alias('pos_mto_trx_g34_prm_p6m'),
												col('pos_mto_trx_g35_prm_p6m').cast('double').alias('pos_mto_trx_g35_prm_p6m'),
												col('pos_mto_trx_g39_prm_p6m').cast('double').alias('pos_mto_trx_g39_prm_p6m'),
												col('pos_mto_trx_g43_prm_p6m').cast('double').alias('pos_mto_trx_g43_prm_p6m'),
												col('pos_mto_trx_g45_prm_p6m').cast('double').alias('pos_mto_trx_g45_prm_p6m'),
												col('pos_mto_trx_g46_prm_p6m').cast('double').alias('pos_mto_trx_g46_prm_p6m'),
												col('pos_mto_trx_ig01_prm_p3m').cast('double').alias('pos_mto_trx_ig01_prm_p3m'),
												col('pos_mto_trx_ig02_prm_p3m').cast('double').alias('pos_mto_trx_ig02_prm_p3m'),
												col('pos_mto_trx_ig03_prm_p3m').cast('double').alias('pos_mto_trx_ig03_prm_p3m'),
												col('pos_mto_trx_ig04_prm_p3m').cast('double').alias('pos_mto_trx_ig04_prm_p3m'),
												col('pos_mto_trx_ig05_prm_p3m').cast('double').alias('pos_mto_trx_ig05_prm_p3m'),
												col('pos_mto_trx_ig06_prm_p3m').cast('double').alias('pos_mto_trx_ig06_prm_p3m'),
												col('pos_mto_trx_ig07_prm_p3m').cast('double').alias('pos_mto_trx_ig07_prm_p3m'),
												col('pos_mto_trx_ig08_prm_p3m').cast('double').alias('pos_mto_trx_ig08_prm_p3m'),
												col('pos_mto_trx_g01_prm_p3m').cast('double').alias('pos_mto_trx_g01_prm_p3m'),
												col('pos_mto_trx_g04_prm_p3m').cast('double').alias('pos_mto_trx_g04_prm_p3m'),
												col('pos_mto_trx_g05_prm_p3m').cast('double').alias('pos_mto_trx_g05_prm_p3m'),
												col('pos_mto_trx_g06_prm_p3m').cast('double').alias('pos_mto_trx_g06_prm_p3m'),
												col('pos_mto_trx_g16_prm_p3m').cast('double').alias('pos_mto_trx_g16_prm_p3m'),
												col('pos_mto_trx_g17_prm_p3m').cast('double').alias('pos_mto_trx_g17_prm_p3m'),
												col('pos_mto_trx_g18_prm_p3m').cast('double').alias('pos_mto_trx_g18_prm_p3m'),
												col('pos_mto_trx_g23_prm_p3m').cast('double').alias('pos_mto_trx_g23_prm_p3m'),
												col('pos_mto_trx_g25_prm_p3m').cast('double').alias('pos_mto_trx_g25_prm_p3m'),
												col('pos_mto_trx_g28_prm_p3m').cast('double').alias('pos_mto_trx_g28_prm_p3m'),
												col('pos_mto_trx_g31_prm_p3m').cast('double').alias('pos_mto_trx_g31_prm_p3m'),
												col('pos_mto_trx_g34_prm_p3m').cast('double').alias('pos_mto_trx_g34_prm_p3m'),
												col('pos_mto_trx_g35_prm_p3m').cast('double').alias('pos_mto_trx_g35_prm_p3m'),
												col('pos_mto_trx_g39_prm_p3m').cast('double').alias('pos_mto_trx_g39_prm_p3m'),
												col('pos_mto_trx_g43_prm_p3m').cast('double').alias('pos_mto_trx_g43_prm_p3m'),
												col('pos_mto_trx_g45_prm_p3m').cast('double').alias('pos_mto_trx_g45_prm_p3m'),
												col('pos_mto_trx_g46_prm_p3m').cast('double').alias('pos_mto_trx_g46_prm_p3m'),
												col('pos_mto_trx_ig01_p1m').cast('double').alias('pos_mto_trx_ig01_p1m'),
												col('pos_mto_trx_ig02_p1m').cast('double').alias('pos_mto_trx_ig02_p1m'),
												col('pos_mto_trx_ig03_p1m').cast('double').alias('pos_mto_trx_ig03_p1m'),
												col('pos_mto_trx_ig04_p1m').cast('double').alias('pos_mto_trx_ig04_p1m'),
												col('pos_mto_trx_ig05_p1m').cast('double').alias('pos_mto_trx_ig05_p1m'),
												col('pos_mto_trx_ig06_p1m').cast('double').alias('pos_mto_trx_ig06_p1m'),
												col('pos_mto_trx_ig07_p1m').cast('double').alias('pos_mto_trx_ig07_p1m'),
												col('pos_mto_trx_ig08_p1m').cast('double').alias('pos_mto_trx_ig08_p1m'),
												col('pos_mto_trx_g01_p1m').cast('double').alias('pos_mto_trx_g01_p1m'),
												col('pos_mto_trx_g04_p1m').cast('double').alias('pos_mto_trx_g04_p1m'),
												col('pos_mto_trx_g05_p1m').cast('double').alias('pos_mto_trx_g05_p1m'),
												col('pos_mto_trx_g06_p1m').cast('double').alias('pos_mto_trx_g06_p1m'),
												col('pos_mto_trx_g16_p1m').cast('double').alias('pos_mto_trx_g16_p1m'),
												col('pos_mto_trx_g17_p1m').cast('double').alias('pos_mto_trx_g17_p1m'),
												col('pos_mto_trx_g18_p1m').cast('double').alias('pos_mto_trx_g18_p1m'),
												col('pos_mto_trx_g23_p1m').cast('double').alias('pos_mto_trx_g23_p1m'),
												col('pos_mto_trx_g25_p1m').cast('double').alias('pos_mto_trx_g25_p1m'),
												col('pos_mto_trx_g28_p1m').cast('double').alias('pos_mto_trx_g28_p1m'),
												col('pos_mto_trx_g31_p1m').cast('double').alias('pos_mto_trx_g31_p1m'),
												col('pos_mto_trx_g34_p1m').cast('double').alias('pos_mto_trx_g34_p1m'),
												col('pos_mto_trx_g35_p1m').cast('double').alias('pos_mto_trx_g35_p1m'),
												col('pos_mto_trx_g39_p1m').cast('double').alias('pos_mto_trx_g39_p1m'),
												col('pos_mto_trx_g43_p1m').cast('double').alias('pos_mto_trx_g43_p1m'),
												col('pos_mto_trx_g45_p1m').cast('double').alias('pos_mto_trx_g45_p1m'),
												col('pos_mto_trx_g46_p1m').cast('double').alias('pos_mto_trx_g46_p1m'),
												col('pos_mto_trx_ig01_max_u3m').cast('double').alias('pos_mto_trx_ig01_max_u3m'),
												col('pos_mto_trx_ig01_max_u6m').cast('double').alias('pos_mto_trx_ig01_max_u6m'),
												col('pos_mto_trx_ig01_max_u9m').cast('double').alias('pos_mto_trx_ig01_max_u9m'),
												col('pos_mto_trx_ig01_max_u12').cast('double').alias('pos_mto_trx_ig01_max_u12'),
												col('pos_mto_trx_ig02_max_u3m').cast('double').alias('pos_mto_trx_ig02_max_u3m'),
												col('pos_mto_trx_ig02_max_u6m').cast('double').alias('pos_mto_trx_ig02_max_u6m'),
												col('pos_mto_trx_ig02_max_u9m').cast('double').alias('pos_mto_trx_ig02_max_u9m'),
												col('pos_mto_trx_ig02_max_u12').cast('double').alias('pos_mto_trx_ig02_max_u12'),
												col('pos_mto_trx_ig03_max_u3m').cast('double').alias('pos_mto_trx_ig03_max_u3m'),
												col('pos_mto_trx_ig03_max_u6m').cast('double').alias('pos_mto_trx_ig03_max_u6m'),
												col('pos_mto_trx_ig03_max_u9m').cast('double').alias('pos_mto_trx_ig03_max_u9m'),
												col('pos_mto_trx_ig03_max_u12').cast('double').alias('pos_mto_trx_ig03_max_u12'),
												col('pos_mto_trx_ig04_max_u3m').cast('double').alias('pos_mto_trx_ig04_max_u3m'),
												col('pos_mto_trx_ig04_max_u6m').cast('double').alias('pos_mto_trx_ig04_max_u6m'),
												col('pos_mto_trx_ig04_max_u9m').cast('double').alias('pos_mto_trx_ig04_max_u9m'),
												col('pos_mto_trx_ig04_max_u12').cast('double').alias('pos_mto_trx_ig04_max_u12'),
												col('pos_mto_trx_ig05_max_u3m').cast('double').alias('pos_mto_trx_ig05_max_u3m'),
												col('pos_mto_trx_ig05_max_u6m').cast('double').alias('pos_mto_trx_ig05_max_u6m'),
												col('pos_mto_trx_ig05_max_u9m').cast('double').alias('pos_mto_trx_ig05_max_u9m'),
												col('pos_mto_trx_ig05_max_u12').cast('double').alias('pos_mto_trx_ig05_max_u12'),
												col('pos_mto_trx_ig06_max_u3m').cast('double').alias('pos_mto_trx_ig06_max_u3m'),
												col('pos_mto_trx_ig06_max_u6m').cast('double').alias('pos_mto_trx_ig06_max_u6m'),
												col('pos_mto_trx_ig06_max_u9m').cast('double').alias('pos_mto_trx_ig06_max_u9m'),
												col('pos_mto_trx_ig06_max_u12').cast('double').alias('pos_mto_trx_ig06_max_u12'),
												col('pos_mto_trx_ig07_max_u3m').cast('double').alias('pos_mto_trx_ig07_max_u3m'),
												col('pos_mto_trx_ig07_max_u6m').cast('double').alias('pos_mto_trx_ig07_max_u6m'),
												col('pos_mto_trx_ig07_max_u9m').cast('double').alias('pos_mto_trx_ig07_max_u9m'),
												col('pos_mto_trx_ig07_max_u12').cast('double').alias('pos_mto_trx_ig07_max_u12'),
												col('pos_mto_trx_ig08_max_u3m').cast('double').alias('pos_mto_trx_ig08_max_u3m'),
												col('pos_mto_trx_ig08_max_u6m').cast('double').alias('pos_mto_trx_ig08_max_u6m'),
												col('pos_mto_trx_ig08_max_u9m').cast('double').alias('pos_mto_trx_ig08_max_u9m'),
												col('pos_mto_trx_ig08_max_u12').cast('double').alias('pos_mto_trx_ig08_max_u12'),
												col('pos_mto_trx_g01_max_u3m').cast('double').alias('pos_mto_trx_g01_max_u3m'),
												col('pos_mto_trx_g01_max_u6m').cast('double').alias('pos_mto_trx_g01_max_u6m'),
												col('pos_mto_trx_g01_max_u9m').cast('double').alias('pos_mto_trx_g01_max_u9m'),
												col('pos_mto_trx_g01_max_u12').cast('double').alias('pos_mto_trx_g01_max_u12'),
												col('pos_mto_trx_g04_max_u3m').cast('double').alias('pos_mto_trx_g04_max_u3m'),
												col('pos_mto_trx_g04_max_u6m').cast('double').alias('pos_mto_trx_g04_max_u6m'),
												col('pos_mto_trx_g04_max_u9m').cast('double').alias('pos_mto_trx_g04_max_u9m'),
												col('pos_mto_trx_g04_max_u12').cast('double').alias('pos_mto_trx_g04_max_u12'),
												col('pos_mto_trx_g05_max_u3m').cast('double').alias('pos_mto_trx_g05_max_u3m'),
												col('pos_mto_trx_g05_max_u6m').cast('double').alias('pos_mto_trx_g05_max_u6m'),
												col('pos_mto_trx_g05_max_u9m').cast('double').alias('pos_mto_trx_g05_max_u9m'),
												col('pos_mto_trx_g05_max_u12').cast('double').alias('pos_mto_trx_g05_max_u12'),
												col('pos_mto_trx_g06_max_u3m').cast('double').alias('pos_mto_trx_g06_max_u3m'),
												col('pos_mto_trx_g06_max_u6m').cast('double').alias('pos_mto_trx_g06_max_u6m'),
												col('pos_mto_trx_g06_max_u9m').cast('double').alias('pos_mto_trx_g06_max_u9m'),
												col('pos_mto_trx_g06_max_u12').cast('double').alias('pos_mto_trx_g06_max_u12'),
												col('pos_mto_trx_g16_max_u3m').cast('double').alias('pos_mto_trx_g16_max_u3m'),
												col('pos_mto_trx_g16_max_u6m').cast('double').alias('pos_mto_trx_g16_max_u6m'),
												col('pos_mto_trx_g16_max_u9m').cast('double').alias('pos_mto_trx_g16_max_u9m'),
												col('pos_mto_trx_g16_max_u12').cast('double').alias('pos_mto_trx_g16_max_u12'),
												col('pos_mto_trx_g17_max_u3m').cast('double').alias('pos_mto_trx_g17_max_u3m'),
												col('pos_mto_trx_g17_max_u6m').cast('double').alias('pos_mto_trx_g17_max_u6m'),
												col('pos_mto_trx_g17_max_u9m').cast('double').alias('pos_mto_trx_g17_max_u9m'),
												col('pos_mto_trx_g17_max_u12').cast('double').alias('pos_mto_trx_g17_max_u12'),
												col('pos_mto_trx_g18_max_u3m').cast('double').alias('pos_mto_trx_g18_max_u3m'),
												col('pos_mto_trx_g18_max_u6m').cast('double').alias('pos_mto_trx_g18_max_u6m'),
												col('pos_mto_trx_g18_max_u9m').cast('double').alias('pos_mto_trx_g18_max_u9m'),
												col('pos_mto_trx_g18_max_u12').cast('double').alias('pos_mto_trx_g18_max_u12'),
												col('pos_mto_trx_g23_max_u3m').cast('double').alias('pos_mto_trx_g23_max_u3m'),
												col('pos_mto_trx_g23_max_u6m').cast('double').alias('pos_mto_trx_g23_max_u6m'),
												col('pos_mto_trx_g23_max_u9m').cast('double').alias('pos_mto_trx_g23_max_u9m'),
												col('pos_mto_trx_g23_max_u12').cast('double').alias('pos_mto_trx_g23_max_u12'),
												col('pos_mto_trx_g25_max_u3m').cast('double').alias('pos_mto_trx_g25_max_u3m'),
												col('pos_mto_trx_g25_max_u6m').cast('double').alias('pos_mto_trx_g25_max_u6m'),
												col('pos_mto_trx_g25_max_u9m').cast('double').alias('pos_mto_trx_g25_max_u9m'),
												col('pos_mto_trx_g25_max_u12').cast('double').alias('pos_mto_trx_g25_max_u12'),
												col('pos_mto_trx_g28_max_u3m').cast('double').alias('pos_mto_trx_g28_max_u3m'),
												col('pos_mto_trx_g28_max_u6m').cast('double').alias('pos_mto_trx_g28_max_u6m'),
												col('pos_mto_trx_g28_max_u9m').cast('double').alias('pos_mto_trx_g28_max_u9m'),
												col('pos_mto_trx_g28_max_u12').cast('double').alias('pos_mto_trx_g28_max_u12'),
												col('pos_mto_trx_g31_max_u3m').cast('double').alias('pos_mto_trx_g31_max_u3m'),
												col('pos_mto_trx_g31_max_u6m').cast('double').alias('pos_mto_trx_g31_max_u6m'),
												col('pos_mto_trx_g31_max_u9m').cast('double').alias('pos_mto_trx_g31_max_u9m'),
												col('pos_mto_trx_g31_max_u12').cast('double').alias('pos_mto_trx_g31_max_u12'),
												col('pos_mto_trx_g34_max_u3m').cast('double').alias('pos_mto_trx_g34_max_u3m'),
												col('pos_mto_trx_g34_max_u6m').cast('double').alias('pos_mto_trx_g34_max_u6m'),
												col('pos_mto_trx_g34_max_u9m').cast('double').alias('pos_mto_trx_g34_max_u9m'),
												col('pos_mto_trx_g34_max_u12').cast('double').alias('pos_mto_trx_g34_max_u12'),
												col('pos_mto_trx_g35_max_u3m').cast('double').alias('pos_mto_trx_g35_max_u3m'),
												col('pos_mto_trx_g35_max_u6m').cast('double').alias('pos_mto_trx_g35_max_u6m'),
												col('pos_mto_trx_g35_max_u9m').cast('double').alias('pos_mto_trx_g35_max_u9m'),
												col('pos_mto_trx_g35_max_u12').cast('double').alias('pos_mto_trx_g35_max_u12'),
												col('pos_mto_trx_g39_max_u3m').cast('double').alias('pos_mto_trx_g39_max_u3m'),
												col('pos_mto_trx_g39_max_u6m').cast('double').alias('pos_mto_trx_g39_max_u6m'),
												col('pos_mto_trx_g39_max_u9m').cast('double').alias('pos_mto_trx_g39_max_u9m'),
												col('pos_mto_trx_g39_max_u12').cast('double').alias('pos_mto_trx_g39_max_u12'),
												col('pos_mto_trx_g43_max_u3m').cast('double').alias('pos_mto_trx_g43_max_u3m'),
												col('pos_mto_trx_g43_max_u6m').cast('double').alias('pos_mto_trx_g43_max_u6m'),
												col('pos_mto_trx_g43_max_u9m').cast('double').alias('pos_mto_trx_g43_max_u9m'),
												col('pos_mto_trx_g43_max_u12').cast('double').alias('pos_mto_trx_g43_max_u12'),
												col('pos_mto_trx_g45_max_u3m').cast('double').alias('pos_mto_trx_g45_max_u3m'),
												col('pos_mto_trx_g45_max_u6m').cast('double').alias('pos_mto_trx_g45_max_u6m'),
												col('pos_mto_trx_g45_max_u9m').cast('double').alias('pos_mto_trx_g45_max_u9m'),
												col('pos_mto_trx_g45_max_u12').cast('double').alias('pos_mto_trx_g45_max_u12'),
												col('pos_mto_trx_g46_max_u3m').cast('double').alias('pos_mto_trx_g46_max_u3m'),
												col('pos_mto_trx_g46_max_u6m').cast('double').alias('pos_mto_trx_g46_max_u6m'),
												col('pos_mto_trx_g46_max_u9m').cast('double').alias('pos_mto_trx_g46_max_u9m'),
												col('pos_mto_trx_g46_max_u12').cast('double').alias('pos_mto_trx_g46_max_u12'),
												col('pos_mto_trx_ig01_min_u3m').cast('double').alias('pos_mto_trx_ig01_min_u3m'),
												col('pos_mto_trx_ig01_min_u6m').cast('double').alias('pos_mto_trx_ig01_min_u6m'),
												col('pos_mto_trx_ig01_min_u9m').cast('double').alias('pos_mto_trx_ig01_min_u9m'),
												col('pos_mto_trx_ig01_min_u12').cast('double').alias('pos_mto_trx_ig01_min_u12'),
												col('pos_mto_trx_ig02_min_u3m').cast('double').alias('pos_mto_trx_ig02_min_u3m'),
												col('pos_mto_trx_ig02_min_u6m').cast('double').alias('pos_mto_trx_ig02_min_u6m'),
												col('pos_mto_trx_ig02_min_u9m').cast('double').alias('pos_mto_trx_ig02_min_u9m'),
												col('pos_mto_trx_ig02_min_u12').cast('double').alias('pos_mto_trx_ig02_min_u12'),
												col('pos_mto_trx_ig03_min_u3m').cast('double').alias('pos_mto_trx_ig03_min_u3m'),
												col('pos_mto_trx_ig03_min_u6m').cast('double').alias('pos_mto_trx_ig03_min_u6m'),
												col('pos_mto_trx_ig03_min_u9m').cast('double').alias('pos_mto_trx_ig03_min_u9m'),
												col('pos_mto_trx_ig03_min_u12').cast('double').alias('pos_mto_trx_ig03_min_u12'),
												col('pos_mto_trx_ig04_min_u3m').cast('double').alias('pos_mto_trx_ig04_min_u3m'),
												col('pos_mto_trx_ig04_min_u6m').cast('double').alias('pos_mto_trx_ig04_min_u6m'),
												col('pos_mto_trx_ig04_min_u9m').cast('double').alias('pos_mto_trx_ig04_min_u9m'),
												col('pos_mto_trx_ig04_min_u12').cast('double').alias('pos_mto_trx_ig04_min_u12'),
												col('pos_mto_trx_ig05_min_u3m').cast('double').alias('pos_mto_trx_ig05_min_u3m'),
												col('pos_mto_trx_ig05_min_u6m').cast('double').alias('pos_mto_trx_ig05_min_u6m'),
												col('pos_mto_trx_ig05_min_u9m').cast('double').alias('pos_mto_trx_ig05_min_u9m'),
												col('pos_mto_trx_ig05_min_u12').cast('double').alias('pos_mto_trx_ig05_min_u12'),
												col('pos_mto_trx_ig06_min_u3m').cast('double').alias('pos_mto_trx_ig06_min_u3m'),
												col('pos_mto_trx_ig06_min_u6m').cast('double').alias('pos_mto_trx_ig06_min_u6m'),
												col('pos_mto_trx_ig06_min_u9m').cast('double').alias('pos_mto_trx_ig06_min_u9m'),
												col('pos_mto_trx_ig06_min_u12').cast('double').alias('pos_mto_trx_ig06_min_u12'),
												col('pos_mto_trx_ig07_min_u3m').cast('double').alias('pos_mto_trx_ig07_min_u3m'),
												col('pos_mto_trx_ig07_min_u6m').cast('double').alias('pos_mto_trx_ig07_min_u6m'),
												col('pos_mto_trx_ig07_min_u9m').cast('double').alias('pos_mto_trx_ig07_min_u9m'),
												col('pos_mto_trx_ig07_min_u12').cast('double').alias('pos_mto_trx_ig07_min_u12'),
												col('pos_mto_trx_ig08_min_u3m').cast('double').alias('pos_mto_trx_ig08_min_u3m'),
												col('pos_mto_trx_ig08_min_u6m').cast('double').alias('pos_mto_trx_ig08_min_u6m'),
												col('pos_mto_trx_ig08_min_u9m').cast('double').alias('pos_mto_trx_ig08_min_u9m'),
												col('pos_mto_trx_ig08_min_u12').cast('double').alias('pos_mto_trx_ig08_min_u12'),
												col('pos_mto_trx_g01_min_u3m').cast('double').alias('pos_mto_trx_g01_min_u3m'),
												col('pos_mto_trx_g01_min_u6m').cast('double').alias('pos_mto_trx_g01_min_u6m'),
												col('pos_mto_trx_g01_min_u9m').cast('double').alias('pos_mto_trx_g01_min_u9m'),
												col('pos_mto_trx_g01_min_u12').cast('double').alias('pos_mto_trx_g01_min_u12'),
												col('pos_mto_trx_g04_min_u3m').cast('double').alias('pos_mto_trx_g04_min_u3m'),
												col('pos_mto_trx_g04_min_u6m').cast('double').alias('pos_mto_trx_g04_min_u6m'),
												col('pos_mto_trx_g04_min_u9m').cast('double').alias('pos_mto_trx_g04_min_u9m'),
												col('pos_mto_trx_g04_min_u12').cast('double').alias('pos_mto_trx_g04_min_u12'),
												col('pos_mto_trx_g05_min_u3m').cast('double').alias('pos_mto_trx_g05_min_u3m'),
												col('pos_mto_trx_g05_min_u6m').cast('double').alias('pos_mto_trx_g05_min_u6m'),
												col('pos_mto_trx_g05_min_u9m').cast('double').alias('pos_mto_trx_g05_min_u9m'),
												col('pos_mto_trx_g05_min_u12').cast('double').alias('pos_mto_trx_g05_min_u12'),
												col('pos_mto_trx_g06_min_u3m').cast('double').alias('pos_mto_trx_g06_min_u3m'),
												col('pos_mto_trx_g06_min_u6m').cast('double').alias('pos_mto_trx_g06_min_u6m'),
												col('pos_mto_trx_g06_min_u9m').cast('double').alias('pos_mto_trx_g06_min_u9m'),
												col('pos_mto_trx_g06_min_u12').cast('double').alias('pos_mto_trx_g06_min_u12'),
												col('pos_mto_trx_g16_min_u3m').cast('double').alias('pos_mto_trx_g16_min_u3m'),
												col('pos_mto_trx_g16_min_u6m').cast('double').alias('pos_mto_trx_g16_min_u6m'),
												col('pos_mto_trx_g16_min_u9m').cast('double').alias('pos_mto_trx_g16_min_u9m'),
												col('pos_mto_trx_g16_min_u12').cast('double').alias('pos_mto_trx_g16_min_u12'),
												col('pos_mto_trx_g17_min_u3m').cast('double').alias('pos_mto_trx_g17_min_u3m'),
												col('pos_mto_trx_g17_min_u6m').cast('double').alias('pos_mto_trx_g17_min_u6m'),
												col('pos_mto_trx_g17_min_u9m').cast('double').alias('pos_mto_trx_g17_min_u9m'),
												col('pos_mto_trx_g17_min_u12').cast('double').alias('pos_mto_trx_g17_min_u12'),
												col('pos_mto_trx_g18_min_u3m').cast('double').alias('pos_mto_trx_g18_min_u3m'),
												col('pos_mto_trx_g18_min_u6m').cast('double').alias('pos_mto_trx_g18_min_u6m'),
												col('pos_mto_trx_g18_min_u9m').cast('double').alias('pos_mto_trx_g18_min_u9m'),
												col('pos_mto_trx_g18_min_u12').cast('double').alias('pos_mto_trx_g18_min_u12'),
												col('pos_mto_trx_g23_min_u3m').cast('double').alias('pos_mto_trx_g23_min_u3m'),
												col('pos_mto_trx_g23_min_u6m').cast('double').alias('pos_mto_trx_g23_min_u6m'),
												col('pos_mto_trx_g23_min_u9m').cast('double').alias('pos_mto_trx_g23_min_u9m'),
												col('pos_mto_trx_g23_min_u12').cast('double').alias('pos_mto_trx_g23_min_u12'),
												col('pos_mto_trx_g25_min_u3m').cast('double').alias('pos_mto_trx_g25_min_u3m'),
												col('pos_mto_trx_g25_min_u6m').cast('double').alias('pos_mto_trx_g25_min_u6m'),
												col('pos_mto_trx_g25_min_u9m').cast('double').alias('pos_mto_trx_g25_min_u9m'),
												col('pos_mto_trx_g25_min_u12').cast('double').alias('pos_mto_trx_g25_min_u12'),
												col('pos_mto_trx_g28_min_u3m').cast('double').alias('pos_mto_trx_g28_min_u3m'),
												col('pos_mto_trx_g28_min_u6m').cast('double').alias('pos_mto_trx_g28_min_u6m'),
												col('pos_mto_trx_g28_min_u9m').cast('double').alias('pos_mto_trx_g28_min_u9m'),
												col('pos_mto_trx_g28_min_u12').cast('double').alias('pos_mto_trx_g28_min_u12'),
												col('pos_mto_trx_g31_min_u3m').cast('double').alias('pos_mto_trx_g31_min_u3m'),
												col('pos_mto_trx_g31_min_u6m').cast('double').alias('pos_mto_trx_g31_min_u6m'),
												col('pos_mto_trx_g31_min_u9m').cast('double').alias('pos_mto_trx_g31_min_u9m'),
												col('pos_mto_trx_g31_min_u12').cast('double').alias('pos_mto_trx_g31_min_u12'),
												col('pos_mto_trx_g34_min_u3m').cast('double').alias('pos_mto_trx_g34_min_u3m'),
												col('pos_mto_trx_g34_min_u6m').cast('double').alias('pos_mto_trx_g34_min_u6m'),
												col('pos_mto_trx_g34_min_u9m').cast('double').alias('pos_mto_trx_g34_min_u9m'),
												col('pos_mto_trx_g34_min_u12').cast('double').alias('pos_mto_trx_g34_min_u12'),
												col('pos_mto_trx_g35_min_u3m').cast('double').alias('pos_mto_trx_g35_min_u3m'),
												col('pos_mto_trx_g35_min_u6m').cast('double').alias('pos_mto_trx_g35_min_u6m'),
												col('pos_mto_trx_g35_min_u9m').cast('double').alias('pos_mto_trx_g35_min_u9m'),
												col('pos_mto_trx_g35_min_u12').cast('double').alias('pos_mto_trx_g35_min_u12'),
												col('pos_mto_trx_g39_min_u3m').cast('double').alias('pos_mto_trx_g39_min_u3m'),
												col('pos_mto_trx_g39_min_u6m').cast('double').alias('pos_mto_trx_g39_min_u6m'),
												col('pos_mto_trx_g39_min_u9m').cast('double').alias('pos_mto_trx_g39_min_u9m'),
												col('pos_mto_trx_g39_min_u12').cast('double').alias('pos_mto_trx_g39_min_u12'),
												col('pos_mto_trx_g43_min_u3m').cast('double').alias('pos_mto_trx_g43_min_u3m'),
												col('pos_mto_trx_g43_min_u6m').cast('double').alias('pos_mto_trx_g43_min_u6m'),
												col('pos_mto_trx_g43_min_u9m').cast('double').alias('pos_mto_trx_g43_min_u9m'),
												col('pos_mto_trx_g43_min_u12').cast('double').alias('pos_mto_trx_g43_min_u12'),
												col('pos_mto_trx_g45_min_u3m').cast('double').alias('pos_mto_trx_g45_min_u3m'),
												col('pos_mto_trx_g45_min_u6m').cast('double').alias('pos_mto_trx_g45_min_u6m'),
												col('pos_mto_trx_g45_min_u9m').cast('double').alias('pos_mto_trx_g45_min_u9m'),
												col('pos_mto_trx_g45_min_u12').cast('double').alias('pos_mto_trx_g45_min_u12'),
												col('pos_mto_trx_g46_min_u3m').cast('double').alias('pos_mto_trx_g46_min_u3m'),
												col('pos_mto_trx_g46_min_u6m').cast('double').alias('pos_mto_trx_g46_min_u6m'),
												col('pos_mto_trx_g46_min_u9m').cast('double').alias('pos_mto_trx_g46_min_u9m'),
												col('pos_mto_trx_g46_min_u12').cast('double').alias('pos_mto_trx_g46_min_u12'),
												col('pos_tkt_trx_ig01_u1m').cast('double').alias('pos_tkt_trx_ig01_u1m'),
												col('pos_tkt_trx_ig01_prm_u3m').cast('double').alias('pos_tkt_trx_ig01_prm_u3m'),
												col('pos_tkt_trx_ig01_prm_u6m').cast('double').alias('pos_tkt_trx_ig01_prm_u6m'),
												col('pos_tkt_trx_ig01_prm_u9m').cast('double').alias('pos_tkt_trx_ig01_prm_u9m'),
												col('pos_tkt_trx_ig01_prm_u12').cast('double').alias('pos_tkt_trx_ig01_prm_u12'),
												col('pos_tkt_trx_ig02_u1m').cast('double').alias('pos_tkt_trx_ig02_u1m'),
												col('pos_tkt_trx_ig02_prm_u3m').cast('double').alias('pos_tkt_trx_ig02_prm_u3m'),
												col('pos_tkt_trx_ig02_prm_u6m').cast('double').alias('pos_tkt_trx_ig02_prm_u6m'),
												col('pos_tkt_trx_ig02_prm_u9m').cast('double').alias('pos_tkt_trx_ig02_prm_u9m'),
												col('pos_tkt_trx_ig02_prm_u12').cast('double').alias('pos_tkt_trx_ig02_prm_u12'),
												col('pos_tkt_trx_ig03_u1m').cast('double').alias('pos_tkt_trx_ig03_u1m'),
												col('pos_tkt_trx_ig03_prm_u3m').cast('double').alias('pos_tkt_trx_ig03_prm_u3m'),
												col('pos_tkt_trx_ig03_prm_u6m').cast('double').alias('pos_tkt_trx_ig03_prm_u6m'),
												col('pos_tkt_trx_ig03_prm_u9m').cast('double').alias('pos_tkt_trx_ig03_prm_u9m'),
												col('pos_tkt_trx_ig03_prm_u12').cast('double').alias('pos_tkt_trx_ig03_prm_u12'),
												col('pos_tkt_trx_ig04_u1m').cast('double').alias('pos_tkt_trx_ig04_u1m'),
												col('pos_tkt_trx_ig04_prm_u3m').cast('double').alias('pos_tkt_trx_ig04_prm_u3m'),
												col('pos_tkt_trx_ig04_prm_u6m').cast('double').alias('pos_tkt_trx_ig04_prm_u6m'),
												col('pos_tkt_trx_ig04_prm_u9m').cast('double').alias('pos_tkt_trx_ig04_prm_u9m'),
												col('pos_tkt_trx_ig04_prm_u12').cast('double').alias('pos_tkt_trx_ig04_prm_u12'),
												col('pos_tkt_trx_ig05_u1m').cast('double').alias('pos_tkt_trx_ig05_u1m'),
												col('pos_tkt_trx_ig05_prm_u3m').cast('double').alias('pos_tkt_trx_ig05_prm_u3m'),
												col('pos_tkt_trx_ig05_prm_u6m').cast('double').alias('pos_tkt_trx_ig05_prm_u6m'),
												col('pos_tkt_trx_ig05_prm_u9m').cast('double').alias('pos_tkt_trx_ig05_prm_u9m'),
												col('pos_tkt_trx_ig05_prm_u12').cast('double').alias('pos_tkt_trx_ig05_prm_u12'),
												col('pos_tkt_trx_ig06_u1m').cast('double').alias('pos_tkt_trx_ig06_u1m'),
												col('pos_tkt_trx_ig06_prm_u3m').cast('double').alias('pos_tkt_trx_ig06_prm_u3m'),
												col('pos_tkt_trx_ig06_prm_u6m').cast('double').alias('pos_tkt_trx_ig06_prm_u6m'),
												col('pos_tkt_trx_ig06_prm_u9m').cast('double').alias('pos_tkt_trx_ig06_prm_u9m'),
												col('pos_tkt_trx_ig06_prm_u12').cast('double').alias('pos_tkt_trx_ig06_prm_u12'),
												col('pos_tkt_trx_ig07_u1m').cast('double').alias('pos_tkt_trx_ig07_u1m'),
												col('pos_tkt_trx_ig07_prm_u3m').cast('double').alias('pos_tkt_trx_ig07_prm_u3m'),
												col('pos_tkt_trx_ig07_prm_u6m').cast('double').alias('pos_tkt_trx_ig07_prm_u6m'),
												col('pos_tkt_trx_ig07_prm_u9m').cast('double').alias('pos_tkt_trx_ig07_prm_u9m'),
												col('pos_tkt_trx_ig07_prm_u12').cast('double').alias('pos_tkt_trx_ig07_prm_u12'),
												col('pos_tkt_trx_ig08_u1m').cast('double').alias('pos_tkt_trx_ig08_u1m'),
												col('pos_tkt_trx_ig08_prm_u3m').cast('double').alias('pos_tkt_trx_ig08_prm_u3m'),
												col('pos_tkt_trx_ig08_prm_u6m').cast('double').alias('pos_tkt_trx_ig08_prm_u6m'),
												col('pos_tkt_trx_ig08_prm_u9m').cast('double').alias('pos_tkt_trx_ig08_prm_u9m'),
												col('pos_tkt_trx_ig08_prm_u12').cast('double').alias('pos_tkt_trx_ig08_prm_u12'),
												col('pos_tkt_trx_g01_u1m').cast('double').alias('pos_tkt_trx_g01_u1m'),
												col('pos_tkt_trx_g01_prm_u3m').cast('double').alias('pos_tkt_trx_g01_prm_u3m'),
												col('pos_tkt_trx_g01_prm_u6m').cast('double').alias('pos_tkt_trx_g01_prm_u6m'),
												col('pos_tkt_trx_g01_prm_u9m').cast('double').alias('pos_tkt_trx_g01_prm_u9m'),
												col('pos_tkt_trx_g01_prm_u12').cast('double').alias('pos_tkt_trx_g01_prm_u12'),
												col('pos_tkt_trx_g04_u1m').cast('double').alias('pos_tkt_trx_g04_u1m'),
												col('pos_tkt_trx_g04_prm_u3m').cast('double').alias('pos_tkt_trx_g04_prm_u3m'),
												col('pos_tkt_trx_g04_prm_u6m').cast('double').alias('pos_tkt_trx_g04_prm_u6m'),
												col('pos_tkt_trx_g04_prm_u9m').cast('double').alias('pos_tkt_trx_g04_prm_u9m'),
												col('pos_tkt_trx_g04_prm_u12').cast('double').alias('pos_tkt_trx_g04_prm_u12'),
												col('pos_tkt_trx_g05_u1m').cast('double').alias('pos_tkt_trx_g05_u1m'),
												col('pos_tkt_trx_g05_prm_u3m').cast('double').alias('pos_tkt_trx_g05_prm_u3m'),
												col('pos_tkt_trx_g05_prm_u6m').cast('double').alias('pos_tkt_trx_g05_prm_u6m'),
												col('pos_tkt_trx_g05_prm_u9m').cast('double').alias('pos_tkt_trx_g05_prm_u9m'),
												col('pos_tkt_trx_g05_prm_u12').cast('double').alias('pos_tkt_trx_g05_prm_u12'),
												col('pos_tkt_trx_g06_u1m').cast('double').alias('pos_tkt_trx_g06_u1m'),
												col('pos_tkt_trx_g06_prm_u3m').cast('double').alias('pos_tkt_trx_g06_prm_u3m'),
												col('pos_tkt_trx_g06_prm_u6m').cast('double').alias('pos_tkt_trx_g06_prm_u6m'),
												col('pos_tkt_trx_g06_prm_u9m').cast('double').alias('pos_tkt_trx_g06_prm_u9m'),
												col('pos_tkt_trx_g06_prm_u12').cast('double').alias('pos_tkt_trx_g06_prm_u12'),
												col('pos_tkt_trx_g16_u1m').cast('double').alias('pos_tkt_trx_g16_u1m'),
												col('pos_tkt_trx_g16_prm_u3m').cast('double').alias('pos_tkt_trx_g16_prm_u3m'),
												col('pos_tkt_trx_g16_prm_u6m').cast('double').alias('pos_tkt_trx_g16_prm_u6m'),
												col('pos_tkt_trx_g16_prm_u9m').cast('double').alias('pos_tkt_trx_g16_prm_u9m'),
												col('pos_tkt_trx_g16_prm_u12').cast('double').alias('pos_tkt_trx_g16_prm_u12'),
												col('pos_tkt_trx_g17_u1m').cast('double').alias('pos_tkt_trx_g17_u1m'),
												col('pos_tkt_trx_g17_prm_u3m').cast('double').alias('pos_tkt_trx_g17_prm_u3m'),
												col('pos_tkt_trx_g17_prm_u6m').cast('double').alias('pos_tkt_trx_g17_prm_u6m'),
												col('pos_tkt_trx_g17_prm_u9m').cast('double').alias('pos_tkt_trx_g17_prm_u9m'),
												col('pos_tkt_trx_g17_prm_u12').cast('double').alias('pos_tkt_trx_g17_prm_u12'),
												col('pos_tkt_trx_g18_u1m').cast('double').alias('pos_tkt_trx_g18_u1m'),
												col('pos_tkt_trx_g18_prm_u3m').cast('double').alias('pos_tkt_trx_g18_prm_u3m'),
												col('pos_tkt_trx_g18_prm_u6m').cast('double').alias('pos_tkt_trx_g18_prm_u6m'),
												col('pos_tkt_trx_g18_prm_u9m').cast('double').alias('pos_tkt_trx_g18_prm_u9m'),
												col('pos_tkt_trx_g18_prm_u12').cast('double').alias('pos_tkt_trx_g18_prm_u12'),
												col('pos_tkt_trx_g23_u1m').cast('double').alias('pos_tkt_trx_g23_u1m'),
												col('pos_tkt_trx_g23_prm_u3m').cast('double').alias('pos_tkt_trx_g23_prm_u3m'),
												col('pos_tkt_trx_g23_prm_u6m').cast('double').alias('pos_tkt_trx_g23_prm_u6m'),
												col('pos_tkt_trx_g23_prm_u9m').cast('double').alias('pos_tkt_trx_g23_prm_u9m'),
												col('pos_tkt_trx_g23_prm_u12').cast('double').alias('pos_tkt_trx_g23_prm_u12'),
												col('pos_tkt_trx_g25_u1m').cast('double').alias('pos_tkt_trx_g25_u1m'),
												col('pos_tkt_trx_g25_prm_u3m').cast('double').alias('pos_tkt_trx_g25_prm_u3m'),
												col('pos_tkt_trx_g25_prm_u6m').cast('double').alias('pos_tkt_trx_g25_prm_u6m'),
												col('pos_tkt_trx_g25_prm_u9m').cast('double').alias('pos_tkt_trx_g25_prm_u9m'),
												col('pos_tkt_trx_g25_prm_u12').cast('double').alias('pos_tkt_trx_g25_prm_u12'),
												col('pos_tkt_trx_g28_u1m').cast('double').alias('pos_tkt_trx_g28_u1m'),
												col('pos_tkt_trx_g28_prm_u3m').cast('double').alias('pos_tkt_trx_g28_prm_u3m'),
												col('pos_tkt_trx_g28_prm_u6m').cast('double').alias('pos_tkt_trx_g28_prm_u6m'),
												col('pos_tkt_trx_g28_prm_u9m').cast('double').alias('pos_tkt_trx_g28_prm_u9m'),
												col('pos_tkt_trx_g28_prm_u12').cast('double').alias('pos_tkt_trx_g28_prm_u12'),
												col('pos_tkt_trx_g31_u1m').cast('double').alias('pos_tkt_trx_g31_u1m'),
												col('pos_tkt_trx_g31_prm_u3m').cast('double').alias('pos_tkt_trx_g31_prm_u3m'),
												col('pos_tkt_trx_g31_prm_u6m').cast('double').alias('pos_tkt_trx_g31_prm_u6m'),
												col('pos_tkt_trx_g31_prm_u9m').cast('double').alias('pos_tkt_trx_g31_prm_u9m'),
												col('pos_tkt_trx_g31_prm_u12').cast('double').alias('pos_tkt_trx_g31_prm_u12'),
												col('pos_tkt_trx_g34_u1m').cast('double').alias('pos_tkt_trx_g34_u1m'),
												col('pos_tkt_trx_g34_prm_u3m').cast('double').alias('pos_tkt_trx_g34_prm_u3m'),
												col('pos_tkt_trx_g34_prm_u6m').cast('double').alias('pos_tkt_trx_g34_prm_u6m'),
												col('pos_tkt_trx_g34_prm_u9m').cast('double').alias('pos_tkt_trx_g34_prm_u9m'),
												col('pos_tkt_trx_g34_prm_u12').cast('double').alias('pos_tkt_trx_g34_prm_u12'),
												col('pos_tkt_trx_g35_u1m').cast('double').alias('pos_tkt_trx_g35_u1m'),
												col('pos_tkt_trx_g35_prm_u3m').cast('double').alias('pos_tkt_trx_g35_prm_u3m'),
												col('pos_tkt_trx_g35_prm_u6m').cast('double').alias('pos_tkt_trx_g35_prm_u6m'),
												col('pos_tkt_trx_g35_prm_u9m').cast('double').alias('pos_tkt_trx_g35_prm_u9m'),
												col('pos_tkt_trx_g35_prm_u12').cast('double').alias('pos_tkt_trx_g35_prm_u12'),
												col('pos_tkt_trx_g39_u1m').cast('double').alias('pos_tkt_trx_g39_u1m'),
												col('pos_tkt_trx_g39_prm_u3m').cast('double').alias('pos_tkt_trx_g39_prm_u3m'),
												col('pos_tkt_trx_g39_prm_u6m').cast('double').alias('pos_tkt_trx_g39_prm_u6m'),
												col('pos_tkt_trx_g39_prm_u9m').cast('double').alias('pos_tkt_trx_g39_prm_u9m'),
												col('pos_tkt_trx_g39_prm_u12').cast('double').alias('pos_tkt_trx_g39_prm_u12'),
												col('pos_tkt_trx_g43_u1m').cast('double').alias('pos_tkt_trx_g43_u1m'),
												col('pos_tkt_trx_g43_prm_u3m').cast('double').alias('pos_tkt_trx_g43_prm_u3m'),
												col('pos_tkt_trx_g43_prm_u6m').cast('double').alias('pos_tkt_trx_g43_prm_u6m'),
												col('pos_tkt_trx_g43_prm_u9m').cast('double').alias('pos_tkt_trx_g43_prm_u9m'),
												col('pos_tkt_trx_g43_prm_u12').cast('double').alias('pos_tkt_trx_g43_prm_u12'),
												col('pos_tkt_trx_g45_u1m').cast('double').alias('pos_tkt_trx_g45_u1m'),
												col('pos_tkt_trx_g45_prm_u3m').cast('double').alias('pos_tkt_trx_g45_prm_u3m'),
												col('pos_tkt_trx_g45_prm_u6m').cast('double').alias('pos_tkt_trx_g45_prm_u6m'),
												col('pos_tkt_trx_g45_prm_u9m').cast('double').alias('pos_tkt_trx_g45_prm_u9m'),
												col('pos_tkt_trx_g45_prm_u12').cast('double').alias('pos_tkt_trx_g45_prm_u12'),
												col('pos_tkt_trx_g46_u1m').cast('double').alias('pos_tkt_trx_g46_u1m'),
												col('pos_tkt_trx_g46_prm_u3m').cast('double').alias('pos_tkt_trx_g46_prm_u3m'),
												col('pos_tkt_trx_g46_prm_u6m').cast('double').alias('pos_tkt_trx_g46_prm_u6m'),
												col('pos_tkt_trx_g46_prm_u9m').cast('double').alias('pos_tkt_trx_g46_prm_u9m'),
												col('pos_tkt_trx_g46_prm_u12').cast('double').alias('pos_tkt_trx_g46_prm_u12'),
												col('pos_tkt_trx_ig01_prm_p6m').cast('double').alias('pos_tkt_trx_ig01_prm_p6m'),
												col('pos_tkt_trx_ig02_prm_p6m').cast('double').alias('pos_tkt_trx_ig02_prm_p6m'),
												col('pos_tkt_trx_ig03_prm_p6m').cast('double').alias('pos_tkt_trx_ig03_prm_p6m'),
												col('pos_tkt_trx_ig04_prm_p6m').cast('double').alias('pos_tkt_trx_ig04_prm_p6m'),
												col('pos_tkt_trx_ig05_prm_p6m').cast('double').alias('pos_tkt_trx_ig05_prm_p6m'),
												col('pos_tkt_trx_ig06_prm_p6m').cast('double').alias('pos_tkt_trx_ig06_prm_p6m'),
												col('pos_tkt_trx_ig07_prm_p6m').cast('double').alias('pos_tkt_trx_ig07_prm_p6m'),
												col('pos_tkt_trx_ig08_prm_p6m').cast('double').alias('pos_tkt_trx_ig08_prm_p6m'),
												col('pos_tkt_trx_g01_prm_p6m').cast('double').alias('pos_tkt_trx_g01_prm_p6m'),
												col('pos_tkt_trx_g04_prm_p6m').cast('double').alias('pos_tkt_trx_g04_prm_p6m'),
												col('pos_tkt_trx_g05_prm_p6m').cast('double').alias('pos_tkt_trx_g05_prm_p6m'),
												col('pos_tkt_trx_g06_prm_p6m').cast('double').alias('pos_tkt_trx_g06_prm_p6m'),
												col('pos_tkt_trx_g16_prm_p6m').cast('double').alias('pos_tkt_trx_g16_prm_p6m'),
												col('pos_tkt_trx_g17_prm_p6m').cast('double').alias('pos_tkt_trx_g17_prm_p6m'),
												col('pos_tkt_trx_g18_prm_p6m').cast('double').alias('pos_tkt_trx_g18_prm_p6m'),
												col('pos_tkt_trx_g23_prm_p6m').cast('double').alias('pos_tkt_trx_g23_prm_p6m'),
												col('pos_tkt_trx_g25_prm_p6m').cast('double').alias('pos_tkt_trx_g25_prm_p6m'),
												col('pos_tkt_trx_g28_prm_p6m').cast('double').alias('pos_tkt_trx_g28_prm_p6m'),
												col('pos_tkt_trx_g31_prm_p6m').cast('double').alias('pos_tkt_trx_g31_prm_p6m'),
												col('pos_tkt_trx_g34_prm_p6m').cast('double').alias('pos_tkt_trx_g34_prm_p6m'),
												col('pos_tkt_trx_g35_prm_p6m').cast('double').alias('pos_tkt_trx_g35_prm_p6m'),
												col('pos_tkt_trx_g39_prm_p6m').cast('double').alias('pos_tkt_trx_g39_prm_p6m'),
												col('pos_tkt_trx_g43_prm_p6m').cast('double').alias('pos_tkt_trx_g43_prm_p6m'),
												col('pos_tkt_trx_g45_prm_p6m').cast('double').alias('pos_tkt_trx_g45_prm_p6m'),
												col('pos_tkt_trx_g46_prm_p6m').cast('double').alias('pos_tkt_trx_g46_prm_p6m'),
												col('pos_tkt_trx_ig01_prm_p3m').cast('double').alias('pos_tkt_trx_ig01_prm_p3m'),
												col('pos_tkt_trx_ig02_prm_p3m').cast('double').alias('pos_tkt_trx_ig02_prm_p3m'),
												col('pos_tkt_trx_ig03_prm_p3m').cast('double').alias('pos_tkt_trx_ig03_prm_p3m'),
												col('pos_tkt_trx_ig04_prm_p3m').cast('double').alias('pos_tkt_trx_ig04_prm_p3m'),
												col('pos_tkt_trx_ig05_prm_p3m').cast('double').alias('pos_tkt_trx_ig05_prm_p3m'),
												col('pos_tkt_trx_ig06_prm_p3m').cast('double').alias('pos_tkt_trx_ig06_prm_p3m'),
												col('pos_tkt_trx_ig07_prm_p3m').cast('double').alias('pos_tkt_trx_ig07_prm_p3m'),
												col('pos_tkt_trx_ig08_prm_p3m').cast('double').alias('pos_tkt_trx_ig08_prm_p3m'),
												col('pos_tkt_trx_g01_prm_p3m').cast('double').alias('pos_tkt_trx_g01_prm_p3m'),
												col('pos_tkt_trx_g04_prm_p3m').cast('double').alias('pos_tkt_trx_g04_prm_p3m'),
												col('pos_tkt_trx_g05_prm_p3m').cast('double').alias('pos_tkt_trx_g05_prm_p3m'),
												col('pos_tkt_trx_g06_prm_p3m').cast('double').alias('pos_tkt_trx_g06_prm_p3m'),
												col('pos_tkt_trx_g16_prm_p3m').cast('double').alias('pos_tkt_trx_g16_prm_p3m'),
												col('pos_tkt_trx_g17_prm_p3m').cast('double').alias('pos_tkt_trx_g17_prm_p3m'),
												col('pos_tkt_trx_g18_prm_p3m').cast('double').alias('pos_tkt_trx_g18_prm_p3m'),
												col('pos_tkt_trx_g23_prm_p3m').cast('double').alias('pos_tkt_trx_g23_prm_p3m'),
												col('pos_tkt_trx_g25_prm_p3m').cast('double').alias('pos_tkt_trx_g25_prm_p3m'),
												col('pos_tkt_trx_g28_prm_p3m').cast('double').alias('pos_tkt_trx_g28_prm_p3m'),
												col('pos_tkt_trx_g31_prm_p3m').cast('double').alias('pos_tkt_trx_g31_prm_p3m'),
												col('pos_tkt_trx_g34_prm_p3m').cast('double').alias('pos_tkt_trx_g34_prm_p3m'),
												col('pos_tkt_trx_g35_prm_p3m').cast('double').alias('pos_tkt_trx_g35_prm_p3m'),
												col('pos_tkt_trx_g39_prm_p3m').cast('double').alias('pos_tkt_trx_g39_prm_p3m'),
												col('pos_tkt_trx_g43_prm_p3m').cast('double').alias('pos_tkt_trx_g43_prm_p3m'),
												col('pos_tkt_trx_g45_prm_p3m').cast('double').alias('pos_tkt_trx_g45_prm_p3m'),
												col('pos_tkt_trx_g46_prm_p3m').cast('double').alias('pos_tkt_trx_g46_prm_p3m'),
												col('pos_tkt_trx_ig01_p1m').cast('double').alias('pos_tkt_trx_ig01_p1m'),
												col('pos_tkt_trx_ig02_p1m').cast('double').alias('pos_tkt_trx_ig02_p1m'),
												col('pos_tkt_trx_ig03_p1m').cast('double').alias('pos_tkt_trx_ig03_p1m'),
												col('pos_tkt_trx_ig04_p1m').cast('double').alias('pos_tkt_trx_ig04_p1m'),
												col('pos_tkt_trx_ig05_p1m').cast('double').alias('pos_tkt_trx_ig05_p1m'),
												col('pos_tkt_trx_ig06_p1m').cast('double').alias('pos_tkt_trx_ig06_p1m'),
												col('pos_tkt_trx_ig07_p1m').cast('double').alias('pos_tkt_trx_ig07_p1m'),
												col('pos_tkt_trx_ig08_p1m').cast('double').alias('pos_tkt_trx_ig08_p1m'),
												col('pos_tkt_trx_g01_p1m').cast('double').alias('pos_tkt_trx_g01_p1m'),
												col('pos_tkt_trx_g04_p1m').cast('double').alias('pos_tkt_trx_g04_p1m'),
												col('pos_tkt_trx_g05_p1m').cast('double').alias('pos_tkt_trx_g05_p1m'),
												col('pos_tkt_trx_g06_p1m').cast('double').alias('pos_tkt_trx_g06_p1m'),
												col('pos_tkt_trx_g16_p1m').cast('double').alias('pos_tkt_trx_g16_p1m'),
												col('pos_tkt_trx_g17_p1m').cast('double').alias('pos_tkt_trx_g17_p1m'),
												col('pos_tkt_trx_g18_p1m').cast('double').alias('pos_tkt_trx_g18_p1m'),
												col('pos_tkt_trx_g23_p1m').cast('double').alias('pos_tkt_trx_g23_p1m'),
												col('pos_tkt_trx_g25_p1m').cast('double').alias('pos_tkt_trx_g25_p1m'),
												col('pos_tkt_trx_g28_p1m').cast('double').alias('pos_tkt_trx_g28_p1m'),
												col('pos_tkt_trx_g31_p1m').cast('double').alias('pos_tkt_trx_g31_p1m'),
												col('pos_tkt_trx_g34_p1m').cast('double').alias('pos_tkt_trx_g34_p1m'),
												col('pos_tkt_trx_g35_p1m').cast('double').alias('pos_tkt_trx_g35_p1m'),
												col('pos_tkt_trx_g39_p1m').cast('double').alias('pos_tkt_trx_g39_p1m'),
												col('pos_tkt_trx_g43_p1m').cast('double').alias('pos_tkt_trx_g43_p1m'),
												col('pos_tkt_trx_g45_p1m').cast('double').alias('pos_tkt_trx_g45_p1m'),
												col('pos_tkt_trx_g46_p1m').cast('double').alias('pos_tkt_trx_g46_p1m'),
												col('pos_tkt_trx_ig01_max_u3m').cast('double').alias('pos_tkt_trx_ig01_max_u3m'),
												col('pos_tkt_trx_ig01_max_u6m').cast('double').alias('pos_tkt_trx_ig01_max_u6m'),
												col('pos_tkt_trx_ig01_max_u9m').cast('double').alias('pos_tkt_trx_ig01_max_u9m'),
												col('pos_tkt_trx_ig01_max_u12').cast('double').alias('pos_tkt_trx_ig01_max_u12'),
												col('pos_tkt_trx_ig02_max_u3m').cast('double').alias('pos_tkt_trx_ig02_max_u3m'),
												col('pos_tkt_trx_ig02_max_u6m').cast('double').alias('pos_tkt_trx_ig02_max_u6m'),
												col('pos_tkt_trx_ig02_max_u9m').cast('double').alias('pos_tkt_trx_ig02_max_u9m'),
												col('pos_tkt_trx_ig02_max_u12').cast('double').alias('pos_tkt_trx_ig02_max_u12'),
												col('pos_tkt_trx_ig03_max_u3m').cast('double').alias('pos_tkt_trx_ig03_max_u3m'),
												col('pos_tkt_trx_ig03_max_u6m').cast('double').alias('pos_tkt_trx_ig03_max_u6m'),
												col('pos_tkt_trx_ig03_max_u9m').cast('double').alias('pos_tkt_trx_ig03_max_u9m'),
												col('pos_tkt_trx_ig03_max_u12').cast('double').alias('pos_tkt_trx_ig03_max_u12'),
												col('pos_tkt_trx_ig04_max_u3m').cast('double').alias('pos_tkt_trx_ig04_max_u3m'),
												col('pos_tkt_trx_ig04_max_u6m').cast('double').alias('pos_tkt_trx_ig04_max_u6m'),
												col('pos_tkt_trx_ig04_max_u9m').cast('double').alias('pos_tkt_trx_ig04_max_u9m'),
												col('pos_tkt_trx_ig04_max_u12').cast('double').alias('pos_tkt_trx_ig04_max_u12'),
												col('pos_tkt_trx_ig05_max_u3m').cast('double').alias('pos_tkt_trx_ig05_max_u3m'),
												col('pos_tkt_trx_ig05_max_u6m').cast('double').alias('pos_tkt_trx_ig05_max_u6m'),
												col('pos_tkt_trx_ig05_max_u9m').cast('double').alias('pos_tkt_trx_ig05_max_u9m'),
												col('pos_tkt_trx_ig05_max_u12').cast('double').alias('pos_tkt_trx_ig05_max_u12'),
												col('pos_tkt_trx_ig06_max_u3m').cast('double').alias('pos_tkt_trx_ig06_max_u3m'),
												col('pos_tkt_trx_ig06_max_u6m').cast('double').alias('pos_tkt_trx_ig06_max_u6m'),
												col('pos_tkt_trx_ig06_max_u9m').cast('double').alias('pos_tkt_trx_ig06_max_u9m'),
												col('pos_tkt_trx_ig06_max_u12').cast('double').alias('pos_tkt_trx_ig06_max_u12'),
												col('pos_tkt_trx_ig07_max_u3m').cast('double').alias('pos_tkt_trx_ig07_max_u3m'),
												col('pos_tkt_trx_ig07_max_u6m').cast('double').alias('pos_tkt_trx_ig07_max_u6m'),
												col('pos_tkt_trx_ig07_max_u9m').cast('double').alias('pos_tkt_trx_ig07_max_u9m'),
												col('pos_tkt_trx_ig07_max_u12').cast('double').alias('pos_tkt_trx_ig07_max_u12'),
												col('pos_tkt_trx_ig08_max_u3m').cast('double').alias('pos_tkt_trx_ig08_max_u3m'),
												col('pos_tkt_trx_ig08_max_u6m').cast('double').alias('pos_tkt_trx_ig08_max_u6m'),
												col('pos_tkt_trx_ig08_max_u9m').cast('double').alias('pos_tkt_trx_ig08_max_u9m'),
												col('pos_tkt_trx_ig08_max_u12').cast('double').alias('pos_tkt_trx_ig08_max_u12'),
												col('pos_tkt_trx_g01_max_u3m').cast('double').alias('pos_tkt_trx_g01_max_u3m'),
												col('pos_tkt_trx_g01_max_u6m').cast('double').alias('pos_tkt_trx_g01_max_u6m'),
												col('pos_tkt_trx_g01_max_u9m').cast('double').alias('pos_tkt_trx_g01_max_u9m'),
												col('pos_tkt_trx_g01_max_u12').cast('double').alias('pos_tkt_trx_g01_max_u12'),
												col('pos_tkt_trx_g04_max_u3m').cast('double').alias('pos_tkt_trx_g04_max_u3m'),
												col('pos_tkt_trx_g04_max_u6m').cast('double').alias('pos_tkt_trx_g04_max_u6m'),
												col('pos_tkt_trx_g04_max_u9m').cast('double').alias('pos_tkt_trx_g04_max_u9m'),
												col('pos_tkt_trx_g04_max_u12').cast('double').alias('pos_tkt_trx_g04_max_u12'),
												col('pos_tkt_trx_g05_max_u3m').cast('double').alias('pos_tkt_trx_g05_max_u3m'),
												col('pos_tkt_trx_g05_max_u6m').cast('double').alias('pos_tkt_trx_g05_max_u6m'),
												col('pos_tkt_trx_g05_max_u9m').cast('double').alias('pos_tkt_trx_g05_max_u9m'),
												col('pos_tkt_trx_g05_max_u12').cast('double').alias('pos_tkt_trx_g05_max_u12'),
												col('pos_tkt_trx_g06_max_u3m').cast('double').alias('pos_tkt_trx_g06_max_u3m'),
												col('pos_tkt_trx_g06_max_u6m').cast('double').alias('pos_tkt_trx_g06_max_u6m'),
												col('pos_tkt_trx_g06_max_u9m').cast('double').alias('pos_tkt_trx_g06_max_u9m'),
												col('pos_tkt_trx_g06_max_u12').cast('double').alias('pos_tkt_trx_g06_max_u12'),
												col('pos_tkt_trx_g16_max_u3m').cast('double').alias('pos_tkt_trx_g16_max_u3m'),
												col('pos_tkt_trx_g16_max_u6m').cast('double').alias('pos_tkt_trx_g16_max_u6m'),
												col('pos_tkt_trx_g16_max_u9m').cast('double').alias('pos_tkt_trx_g16_max_u9m'),
												col('pos_tkt_trx_g16_max_u12').cast('double').alias('pos_tkt_trx_g16_max_u12'),
												col('pos_tkt_trx_g17_max_u3m').cast('double').alias('pos_tkt_trx_g17_max_u3m'),
												col('pos_tkt_trx_g17_max_u6m').cast('double').alias('pos_tkt_trx_g17_max_u6m'),
												col('pos_tkt_trx_g17_max_u9m').cast('double').alias('pos_tkt_trx_g17_max_u9m'),
												col('pos_tkt_trx_g17_max_u12').cast('double').alias('pos_tkt_trx_g17_max_u12'),
												col('pos_tkt_trx_g18_max_u3m').cast('double').alias('pos_tkt_trx_g18_max_u3m'),
												col('pos_tkt_trx_g18_max_u6m').cast('double').alias('pos_tkt_trx_g18_max_u6m'),
												col('pos_tkt_trx_g18_max_u9m').cast('double').alias('pos_tkt_trx_g18_max_u9m'),
												col('pos_tkt_trx_g18_max_u12').cast('double').alias('pos_tkt_trx_g18_max_u12'),
												col('pos_tkt_trx_g23_max_u3m').cast('double').alias('pos_tkt_trx_g23_max_u3m'),
												col('pos_tkt_trx_g23_max_u6m').cast('double').alias('pos_tkt_trx_g23_max_u6m'),
												col('pos_tkt_trx_g23_max_u9m').cast('double').alias('pos_tkt_trx_g23_max_u9m'),
												col('pos_tkt_trx_g23_max_u12').cast('double').alias('pos_tkt_trx_g23_max_u12'),
												col('pos_tkt_trx_g25_max_u3m').cast('double').alias('pos_tkt_trx_g25_max_u3m'),
												col('pos_tkt_trx_g25_max_u6m').cast('double').alias('pos_tkt_trx_g25_max_u6m'),
												col('pos_tkt_trx_g25_max_u9m').cast('double').alias('pos_tkt_trx_g25_max_u9m'),
												col('pos_tkt_trx_g25_max_u12').cast('double').alias('pos_tkt_trx_g25_max_u12'),
												col('pos_tkt_trx_g28_max_u3m').cast('double').alias('pos_tkt_trx_g28_max_u3m'),
												col('pos_tkt_trx_g28_max_u6m').cast('double').alias('pos_tkt_trx_g28_max_u6m'),
												col('pos_tkt_trx_g28_max_u9m').cast('double').alias('pos_tkt_trx_g28_max_u9m'),
												col('pos_tkt_trx_g28_max_u12').cast('double').alias('pos_tkt_trx_g28_max_u12'),
												col('pos_tkt_trx_g31_max_u3m').cast('double').alias('pos_tkt_trx_g31_max_u3m'),
												col('pos_tkt_trx_g31_max_u6m').cast('double').alias('pos_tkt_trx_g31_max_u6m'),
												col('pos_tkt_trx_g31_max_u9m').cast('double').alias('pos_tkt_trx_g31_max_u9m'),
												col('pos_tkt_trx_g31_max_u12').cast('double').alias('pos_tkt_trx_g31_max_u12'),
												col('pos_tkt_trx_g34_max_u3m').cast('double').alias('pos_tkt_trx_g34_max_u3m'),
												col('pos_tkt_trx_g34_max_u6m').cast('double').alias('pos_tkt_trx_g34_max_u6m'),
												col('pos_tkt_trx_g34_max_u9m').cast('double').alias('pos_tkt_trx_g34_max_u9m'),
												col('pos_tkt_trx_g34_max_u12').cast('double').alias('pos_tkt_trx_g34_max_u12'),
												col('pos_tkt_trx_g35_max_u3m').cast('double').alias('pos_tkt_trx_g35_max_u3m'),
												col('pos_tkt_trx_g35_max_u6m').cast('double').alias('pos_tkt_trx_g35_max_u6m'),
												col('pos_tkt_trx_g35_max_u9m').cast('double').alias('pos_tkt_trx_g35_max_u9m'),
												col('pos_tkt_trx_g35_max_u12').cast('double').alias('pos_tkt_trx_g35_max_u12'),
												col('pos_tkt_trx_g39_max_u3m').cast('double').alias('pos_tkt_trx_g39_max_u3m'),
												col('pos_tkt_trx_g39_max_u6m').cast('double').alias('pos_tkt_trx_g39_max_u6m'),
												col('pos_tkt_trx_g39_max_u9m').cast('double').alias('pos_tkt_trx_g39_max_u9m'),
												col('pos_tkt_trx_g39_max_u12').cast('double').alias('pos_tkt_trx_g39_max_u12'),
												col('pos_tkt_trx_g43_max_u3m').cast('double').alias('pos_tkt_trx_g43_max_u3m'),
												col('pos_tkt_trx_g43_max_u6m').cast('double').alias('pos_tkt_trx_g43_max_u6m'),
												col('pos_tkt_trx_g43_max_u9m').cast('double').alias('pos_tkt_trx_g43_max_u9m'),
												col('pos_tkt_trx_g43_max_u12').cast('double').alias('pos_tkt_trx_g43_max_u12'),
												col('pos_tkt_trx_g45_max_u3m').cast('double').alias('pos_tkt_trx_g45_max_u3m'),
												col('pos_tkt_trx_g45_max_u6m').cast('double').alias('pos_tkt_trx_g45_max_u6m'),
												col('pos_tkt_trx_g45_max_u9m').cast('double').alias('pos_tkt_trx_g45_max_u9m'),
												col('pos_tkt_trx_g45_max_u12').cast('double').alias('pos_tkt_trx_g45_max_u12'),
												col('pos_tkt_trx_g46_max_u3m').cast('double').alias('pos_tkt_trx_g46_max_u3m'),
												col('pos_tkt_trx_g46_max_u6m').cast('double').alias('pos_tkt_trx_g46_max_u6m'),
												col('pos_tkt_trx_g46_max_u9m').cast('double').alias('pos_tkt_trx_g46_max_u9m'),
												col('pos_tkt_trx_g46_max_u12').cast('double').alias('pos_tkt_trx_g46_max_u12'),
												col('pos_tkt_trx_ig01_min_u3m').cast('double').alias('pos_tkt_trx_ig01_min_u3m'),
												col('pos_tkt_trx_ig01_min_u6m').cast('double').alias('pos_tkt_trx_ig01_min_u6m'),
												col('pos_tkt_trx_ig01_min_u9m').cast('double').alias('pos_tkt_trx_ig01_min_u9m'),
												col('pos_tkt_trx_ig01_min_u12').cast('double').alias('pos_tkt_trx_ig01_min_u12'),
												col('pos_tkt_trx_ig02_min_u3m').cast('double').alias('pos_tkt_trx_ig02_min_u3m'),
												col('pos_tkt_trx_ig02_min_u6m').cast('double').alias('pos_tkt_trx_ig02_min_u6m'),
												col('pos_tkt_trx_ig02_min_u9m').cast('double').alias('pos_tkt_trx_ig02_min_u9m'),
												col('pos_tkt_trx_ig02_min_u12').cast('double').alias('pos_tkt_trx_ig02_min_u12'),
												col('pos_tkt_trx_ig03_min_u3m').cast('double').alias('pos_tkt_trx_ig03_min_u3m'),
												col('pos_tkt_trx_ig03_min_u6m').cast('double').alias('pos_tkt_trx_ig03_min_u6m'),
												col('pos_tkt_trx_ig03_min_u9m').cast('double').alias('pos_tkt_trx_ig03_min_u9m'),
												col('pos_tkt_trx_ig03_min_u12').cast('double').alias('pos_tkt_trx_ig03_min_u12'),
												col('pos_tkt_trx_ig04_min_u3m').cast('double').alias('pos_tkt_trx_ig04_min_u3m'),
												col('pos_tkt_trx_ig04_min_u6m').cast('double').alias('pos_tkt_trx_ig04_min_u6m'),
												col('pos_tkt_trx_ig04_min_u9m').cast('double').alias('pos_tkt_trx_ig04_min_u9m'),
												col('pos_tkt_trx_ig04_min_u12').cast('double').alias('pos_tkt_trx_ig04_min_u12'),
												col('pos_tkt_trx_ig05_min_u3m').cast('double').alias('pos_tkt_trx_ig05_min_u3m'),
												col('pos_tkt_trx_ig05_min_u6m').cast('double').alias('pos_tkt_trx_ig05_min_u6m'),
												col('pos_tkt_trx_ig05_min_u9m').cast('double').alias('pos_tkt_trx_ig05_min_u9m'),
												col('pos_tkt_trx_ig05_min_u12').cast('double').alias('pos_tkt_trx_ig05_min_u12'),
												col('pos_tkt_trx_ig06_min_u3m').cast('double').alias('pos_tkt_trx_ig06_min_u3m'),
												col('pos_tkt_trx_ig06_min_u6m').cast('double').alias('pos_tkt_trx_ig06_min_u6m'),
												col('pos_tkt_trx_ig06_min_u9m').cast('double').alias('pos_tkt_trx_ig06_min_u9m'),
												col('pos_tkt_trx_ig06_min_u12').cast('double').alias('pos_tkt_trx_ig06_min_u12'),
												col('pos_tkt_trx_ig07_min_u3m').cast('double').alias('pos_tkt_trx_ig07_min_u3m'),
												col('pos_tkt_trx_ig07_min_u6m').cast('double').alias('pos_tkt_trx_ig07_min_u6m'),
												col('pos_tkt_trx_ig07_min_u9m').cast('double').alias('pos_tkt_trx_ig07_min_u9m'),
												col('pos_tkt_trx_ig07_min_u12').cast('double').alias('pos_tkt_trx_ig07_min_u12'),
												col('pos_tkt_trx_ig08_min_u3m').cast('double').alias('pos_tkt_trx_ig08_min_u3m'),
												col('pos_tkt_trx_ig08_min_u6m').cast('double').alias('pos_tkt_trx_ig08_min_u6m'),
												col('pos_tkt_trx_ig08_min_u9m').cast('double').alias('pos_tkt_trx_ig08_min_u9m'),
												col('pos_tkt_trx_ig08_min_u12').cast('double').alias('pos_tkt_trx_ig08_min_u12'),
												col('pos_tkt_trx_g01_min_u3m').cast('double').alias('pos_tkt_trx_g01_min_u3m'),
												col('pos_tkt_trx_g01_min_u6m').cast('double').alias('pos_tkt_trx_g01_min_u6m'),
												col('pos_tkt_trx_g01_min_u9m').cast('double').alias('pos_tkt_trx_g01_min_u9m'),
												col('pos_tkt_trx_g01_min_u12').cast('double').alias('pos_tkt_trx_g01_min_u12'),
												col('pos_tkt_trx_g04_min_u3m').cast('double').alias('pos_tkt_trx_g04_min_u3m'),
												col('pos_tkt_trx_g04_min_u6m').cast('double').alias('pos_tkt_trx_g04_min_u6m'),
												col('pos_tkt_trx_g04_min_u9m').cast('double').alias('pos_tkt_trx_g04_min_u9m'),
												col('pos_tkt_trx_g04_min_u12').cast('double').alias('pos_tkt_trx_g04_min_u12'),
												col('pos_tkt_trx_g05_min_u3m').cast('double').alias('pos_tkt_trx_g05_min_u3m'),
												col('pos_tkt_trx_g05_min_u6m').cast('double').alias('pos_tkt_trx_g05_min_u6m'),
												col('pos_tkt_trx_g05_min_u9m').cast('double').alias('pos_tkt_trx_g05_min_u9m'),
												col('pos_tkt_trx_g05_min_u12').cast('double').alias('pos_tkt_trx_g05_min_u12'),
												col('pos_tkt_trx_g06_min_u3m').cast('double').alias('pos_tkt_trx_g06_min_u3m'),
												col('pos_tkt_trx_g06_min_u6m').cast('double').alias('pos_tkt_trx_g06_min_u6m'),
												col('pos_tkt_trx_g06_min_u9m').cast('double').alias('pos_tkt_trx_g06_min_u9m'),
												col('pos_tkt_trx_g06_min_u12').cast('double').alias('pos_tkt_trx_g06_min_u12'),
												col('pos_tkt_trx_g16_min_u3m').cast('double').alias('pos_tkt_trx_g16_min_u3m'),
												col('pos_tkt_trx_g16_min_u6m').cast('double').alias('pos_tkt_trx_g16_min_u6m'),
												col('pos_tkt_trx_g16_min_u9m').cast('double').alias('pos_tkt_trx_g16_min_u9m'),
												col('pos_tkt_trx_g16_min_u12').cast('double').alias('pos_tkt_trx_g16_min_u12'),
												col('pos_tkt_trx_g17_min_u3m').cast('double').alias('pos_tkt_trx_g17_min_u3m'),
												col('pos_tkt_trx_g17_min_u6m').cast('double').alias('pos_tkt_trx_g17_min_u6m'),
												col('pos_tkt_trx_g17_min_u9m').cast('double').alias('pos_tkt_trx_g17_min_u9m'),
												col('pos_tkt_trx_g17_min_u12').cast('double').alias('pos_tkt_trx_g17_min_u12'),
												col('pos_tkt_trx_g18_min_u3m').cast('double').alias('pos_tkt_trx_g18_min_u3m'),
												col('pos_tkt_trx_g18_min_u6m').cast('double').alias('pos_tkt_trx_g18_min_u6m'),
												col('pos_tkt_trx_g18_min_u9m').cast('double').alias('pos_tkt_trx_g18_min_u9m'),
												col('pos_tkt_trx_g18_min_u12').cast('double').alias('pos_tkt_trx_g18_min_u12'),
												col('pos_tkt_trx_g23_min_u3m').cast('double').alias('pos_tkt_trx_g23_min_u3m'),
												col('pos_tkt_trx_g23_min_u6m').cast('double').alias('pos_tkt_trx_g23_min_u6m'),
												col('pos_tkt_trx_g23_min_u9m').cast('double').alias('pos_tkt_trx_g23_min_u9m'),
												col('pos_tkt_trx_g23_min_u12').cast('double').alias('pos_tkt_trx_g23_min_u12'),
												col('pos_tkt_trx_g25_min_u3m').cast('double').alias('pos_tkt_trx_g25_min_u3m'),
												col('pos_tkt_trx_g25_min_u6m').cast('double').alias('pos_tkt_trx_g25_min_u6m'),
												col('pos_tkt_trx_g25_min_u9m').cast('double').alias('pos_tkt_trx_g25_min_u9m'),
												col('pos_tkt_trx_g25_min_u12').cast('double').alias('pos_tkt_trx_g25_min_u12'),
												col('pos_tkt_trx_g28_min_u3m').cast('double').alias('pos_tkt_trx_g28_min_u3m'),
												col('pos_tkt_trx_g28_min_u6m').cast('double').alias('pos_tkt_trx_g28_min_u6m'),
												col('pos_tkt_trx_g28_min_u9m').cast('double').alias('pos_tkt_trx_g28_min_u9m'),
												col('pos_tkt_trx_g28_min_u12').cast('double').alias('pos_tkt_trx_g28_min_u12'),
												col('pos_tkt_trx_g31_min_u3m').cast('double').alias('pos_tkt_trx_g31_min_u3m'),
												col('pos_tkt_trx_g31_min_u6m').cast('double').alias('pos_tkt_trx_g31_min_u6m'),
												col('pos_tkt_trx_g31_min_u9m').cast('double').alias('pos_tkt_trx_g31_min_u9m'),
												col('pos_tkt_trx_g31_min_u12').cast('double').alias('pos_tkt_trx_g31_min_u12'),
												col('pos_tkt_trx_g34_min_u3m').cast('double').alias('pos_tkt_trx_g34_min_u3m'),
												col('pos_tkt_trx_g34_min_u6m').cast('double').alias('pos_tkt_trx_g34_min_u6m'),
												col('pos_tkt_trx_g34_min_u9m').cast('double').alias('pos_tkt_trx_g34_min_u9m'),
												col('pos_tkt_trx_g34_min_u12').cast('double').alias('pos_tkt_trx_g34_min_u12'),
												col('pos_tkt_trx_g35_min_u3m').cast('double').alias('pos_tkt_trx_g35_min_u3m'),
												col('pos_tkt_trx_g35_min_u6m').cast('double').alias('pos_tkt_trx_g35_min_u6m'),
												col('pos_tkt_trx_g35_min_u9m').cast('double').alias('pos_tkt_trx_g35_min_u9m'),
												col('pos_tkt_trx_g35_min_u12').cast('double').alias('pos_tkt_trx_g35_min_u12'),
												col('pos_tkt_trx_g39_min_u3m').cast('double').alias('pos_tkt_trx_g39_min_u3m'),
												col('pos_tkt_trx_g39_min_u6m').cast('double').alias('pos_tkt_trx_g39_min_u6m'),
												col('pos_tkt_trx_g39_min_u9m').cast('double').alias('pos_tkt_trx_g39_min_u9m'),
												col('pos_tkt_trx_g39_min_u12').cast('double').alias('pos_tkt_trx_g39_min_u12'),
												col('pos_tkt_trx_g43_min_u3m').cast('double').alias('pos_tkt_trx_g43_min_u3m'),
												col('pos_tkt_trx_g43_min_u6m').cast('double').alias('pos_tkt_trx_g43_min_u6m'),
												col('pos_tkt_trx_g43_min_u9m').cast('double').alias('pos_tkt_trx_g43_min_u9m'),
												col('pos_tkt_trx_g43_min_u12').cast('double').alias('pos_tkt_trx_g43_min_u12'),
												col('pos_tkt_trx_g45_min_u3m').cast('double').alias('pos_tkt_trx_g45_min_u3m'),
												col('pos_tkt_trx_g45_min_u6m').cast('double').alias('pos_tkt_trx_g45_min_u6m'),
												col('pos_tkt_trx_g45_min_u9m').cast('double').alias('pos_tkt_trx_g45_min_u9m'),
												col('pos_tkt_trx_g45_min_u12').cast('double').alias('pos_tkt_trx_g45_min_u12'),
												col('pos_tkt_trx_g46_min_u3m').cast('double').alias('pos_tkt_trx_g46_min_u3m'),
												col('pos_tkt_trx_g46_min_u6m').cast('double').alias('pos_tkt_trx_g46_min_u6m'),
												col('pos_tkt_trx_g46_min_u9m').cast('double').alias('pos_tkt_trx_g46_min_u9m'),
												col('pos_tkt_trx_g46_min_u12').cast('double').alias('pos_tkt_trx_g46_min_u12'),
												col('pos_ctd_trx_ig01_u1m').cast('double').alias('pos_ctd_trx_ig01_u1m'),
												col('pos_ctd_trx_ig01_prm_u3m').cast('double').alias('pos_ctd_trx_ig01_prm_u3m'),
												col('pos_ctd_trx_ig01_prm_u6m').cast('double').alias('pos_ctd_trx_ig01_prm_u6m'),
												col('pos_ctd_trx_ig01_prm_u9m').cast('double').alias('pos_ctd_trx_ig01_prm_u9m'),
												col('pos_ctd_trx_ig01_prm_u12').cast('double').alias('pos_ctd_trx_ig01_prm_u12'),
												col('pos_ctd_trx_ig02_u1m').cast('double').alias('pos_ctd_trx_ig02_u1m'),
												col('pos_ctd_trx_ig02_prm_u3m').cast('double').alias('pos_ctd_trx_ig02_prm_u3m'),
												col('pos_ctd_trx_ig02_prm_u6m').cast('double').alias('pos_ctd_trx_ig02_prm_u6m'),
												col('pos_ctd_trx_ig02_prm_u9m').cast('double').alias('pos_ctd_trx_ig02_prm_u9m'),
												col('pos_ctd_trx_ig02_prm_u12').cast('double').alias('pos_ctd_trx_ig02_prm_u12'),
												col('pos_ctd_trx_ig03_u1m').cast('double').alias('pos_ctd_trx_ig03_u1m'),
												col('pos_ctd_trx_ig03_prm_u3m').cast('double').alias('pos_ctd_trx_ig03_prm_u3m'),
												col('pos_ctd_trx_ig03_prm_u6m').cast('double').alias('pos_ctd_trx_ig03_prm_u6m'),
												col('pos_ctd_trx_ig03_prm_u9m').cast('double').alias('pos_ctd_trx_ig03_prm_u9m'),
												col('pos_ctd_trx_ig03_prm_u12').cast('double').alias('pos_ctd_trx_ig03_prm_u12'),
												col('pos_ctd_trx_ig04_u1m').cast('double').alias('pos_ctd_trx_ig04_u1m'),
												col('pos_ctd_trx_ig04_prm_u3m').cast('double').alias('pos_ctd_trx_ig04_prm_u3m'),
												col('pos_ctd_trx_ig04_prm_u6m').cast('double').alias('pos_ctd_trx_ig04_prm_u6m'),
												col('pos_ctd_trx_ig04_prm_u9m').cast('double').alias('pos_ctd_trx_ig04_prm_u9m'),
												col('pos_ctd_trx_ig04_prm_u12').cast('double').alias('pos_ctd_trx_ig04_prm_u12'),
												col('pos_ctd_trx_ig05_u1m').cast('double').alias('pos_ctd_trx_ig05_u1m'),
												col('pos_ctd_trx_ig05_prm_u3m').cast('double').alias('pos_ctd_trx_ig05_prm_u3m'),
												col('pos_ctd_trx_ig05_prm_u6m').cast('double').alias('pos_ctd_trx_ig05_prm_u6m'),
												col('pos_ctd_trx_ig05_prm_u9m').cast('double').alias('pos_ctd_trx_ig05_prm_u9m'),
												col('pos_ctd_trx_ig05_prm_u12').cast('double').alias('pos_ctd_trx_ig05_prm_u12'),
												col('pos_ctd_trx_ig06_u1m').cast('double').alias('pos_ctd_trx_ig06_u1m'),
												col('pos_ctd_trx_ig06_prm_u3m').cast('double').alias('pos_ctd_trx_ig06_prm_u3m'),
												col('pos_ctd_trx_ig06_prm_u6m').cast('double').alias('pos_ctd_trx_ig06_prm_u6m'),
												col('pos_ctd_trx_ig06_prm_u9m').cast('double').alias('pos_ctd_trx_ig06_prm_u9m'),
												col('pos_ctd_trx_ig06_prm_u12').cast('double').alias('pos_ctd_trx_ig06_prm_u12'),
												col('pos_ctd_trx_ig07_u1m').cast('double').alias('pos_ctd_trx_ig07_u1m'),
												col('pos_ctd_trx_ig07_prm_u3m').cast('double').alias('pos_ctd_trx_ig07_prm_u3m'),
												col('pos_ctd_trx_ig07_prm_u6m').cast('double').alias('pos_ctd_trx_ig07_prm_u6m'),
												col('pos_ctd_trx_ig07_prm_u9m').cast('double').alias('pos_ctd_trx_ig07_prm_u9m'),
												col('pos_ctd_trx_ig07_prm_u12').cast('double').alias('pos_ctd_trx_ig07_prm_u12'),
												col('pos_ctd_trx_ig08_u1m').cast('double').alias('pos_ctd_trx_ig08_u1m'),
												col('pos_ctd_trx_ig08_prm_u3m').cast('double').alias('pos_ctd_trx_ig08_prm_u3m'),
												col('pos_ctd_trx_ig08_prm_u6m').cast('double').alias('pos_ctd_trx_ig08_prm_u6m'),
												col('pos_ctd_trx_ig08_prm_u9m').cast('double').alias('pos_ctd_trx_ig08_prm_u9m'),
												col('pos_ctd_trx_ig08_prm_u12').cast('double').alias('pos_ctd_trx_ig08_prm_u12'),
												col('pos_ctd_trx_g01_u1m').cast('double').alias('pos_ctd_trx_g01_u1m'),
												col('pos_ctd_trx_g01_prm_u3m').cast('double').alias('pos_ctd_trx_g01_prm_u3m'),
												col('pos_ctd_trx_g01_prm_u6m').cast('double').alias('pos_ctd_trx_g01_prm_u6m'),
												col('pos_ctd_trx_g01_prm_u9m').cast('double').alias('pos_ctd_trx_g01_prm_u9m'),
												col('pos_ctd_trx_g01_prm_u12').cast('double').alias('pos_ctd_trx_g01_prm_u12'),
												col('pos_ctd_trx_g04_u1m').cast('double').alias('pos_ctd_trx_g04_u1m'),
												col('pos_ctd_trx_g04_prm_u3m').cast('double').alias('pos_ctd_trx_g04_prm_u3m'),
												col('pos_ctd_trx_g04_prm_u6m').cast('double').alias('pos_ctd_trx_g04_prm_u6m'),
												col('pos_ctd_trx_g04_prm_u9m').cast('double').alias('pos_ctd_trx_g04_prm_u9m'),
												col('pos_ctd_trx_g04_prm_u12').cast('double').alias('pos_ctd_trx_g04_prm_u12'),
												col('pos_ctd_trx_g05_u1m').cast('double').alias('pos_ctd_trx_g05_u1m'),
												col('pos_ctd_trx_g05_prm_u3m').cast('double').alias('pos_ctd_trx_g05_prm_u3m'),
												col('pos_ctd_trx_g05_prm_u6m').cast('double').alias('pos_ctd_trx_g05_prm_u6m'),
												col('pos_ctd_trx_g05_prm_u9m').cast('double').alias('pos_ctd_trx_g05_prm_u9m'),
												col('pos_ctd_trx_g05_prm_u12').cast('double').alias('pos_ctd_trx_g05_prm_u12'),
												col('pos_ctd_trx_g06_u1m').cast('double').alias('pos_ctd_trx_g06_u1m'),
												col('pos_ctd_trx_g06_prm_u3m').cast('double').alias('pos_ctd_trx_g06_prm_u3m'),
												col('pos_ctd_trx_g06_prm_u6m').cast('double').alias('pos_ctd_trx_g06_prm_u6m'),
												col('pos_ctd_trx_g06_prm_u9m').cast('double').alias('pos_ctd_trx_g06_prm_u9m'),
												col('pos_ctd_trx_g06_prm_u12').cast('double').alias('pos_ctd_trx_g06_prm_u12'),
												col('pos_ctd_trx_g16_u1m').cast('double').alias('pos_ctd_trx_g16_u1m'),
												col('pos_ctd_trx_g16_prm_u3m').cast('double').alias('pos_ctd_trx_g16_prm_u3m'),
												col('pos_ctd_trx_g16_prm_u6m').cast('double').alias('pos_ctd_trx_g16_prm_u6m'),
												col('pos_ctd_trx_g16_prm_u9m').cast('double').alias('pos_ctd_trx_g16_prm_u9m'),
												col('pos_ctd_trx_g16_prm_u12').cast('double').alias('pos_ctd_trx_g16_prm_u12'),
												col('pos_ctd_trx_g17_u1m').cast('double').alias('pos_ctd_trx_g17_u1m'),
												col('pos_ctd_trx_g17_prm_u3m').cast('double').alias('pos_ctd_trx_g17_prm_u3m'),
												col('pos_ctd_trx_g17_prm_u6m').cast('double').alias('pos_ctd_trx_g17_prm_u6m'),
												col('pos_ctd_trx_g17_prm_u9m').cast('double').alias('pos_ctd_trx_g17_prm_u9m'),
												col('pos_ctd_trx_g17_prm_u12').cast('double').alias('pos_ctd_trx_g17_prm_u12'),
												col('pos_ctd_trx_g18_u1m').cast('double').alias('pos_ctd_trx_g18_u1m'),
												col('pos_ctd_trx_g18_prm_u3m').cast('double').alias('pos_ctd_trx_g18_prm_u3m'),
												col('pos_ctd_trx_g18_prm_u6m').cast('double').alias('pos_ctd_trx_g18_prm_u6m'),
												col('pos_ctd_trx_g18_prm_u9m').cast('double').alias('pos_ctd_trx_g18_prm_u9m'),
												col('pos_ctd_trx_g18_prm_u12').cast('double').alias('pos_ctd_trx_g18_prm_u12'),
												col('pos_ctd_trx_g23_u1m').cast('double').alias('pos_ctd_trx_g23_u1m'),
												col('pos_ctd_trx_g23_prm_u3m').cast('double').alias('pos_ctd_trx_g23_prm_u3m'),
												col('pos_ctd_trx_g23_prm_u6m').cast('double').alias('pos_ctd_trx_g23_prm_u6m'),
												col('pos_ctd_trx_g23_prm_u9m').cast('double').alias('pos_ctd_trx_g23_prm_u9m'),
												col('pos_ctd_trx_g23_prm_u12').cast('double').alias('pos_ctd_trx_g23_prm_u12'),
												col('pos_ctd_trx_g25_u1m').cast('double').alias('pos_ctd_trx_g25_u1m'),
												col('pos_ctd_trx_g25_prm_u3m').cast('double').alias('pos_ctd_trx_g25_prm_u3m'),
												col('pos_ctd_trx_g25_prm_u6m').cast('double').alias('pos_ctd_trx_g25_prm_u6m'),
												col('pos_ctd_trx_g25_prm_u9m').cast('double').alias('pos_ctd_trx_g25_prm_u9m'),
												col('pos_ctd_trx_g25_prm_u12').cast('double').alias('pos_ctd_trx_g25_prm_u12'),
												col('pos_ctd_trx_g28_u1m').cast('double').alias('pos_ctd_trx_g28_u1m'),
												col('pos_ctd_trx_g28_prm_u3m').cast('double').alias('pos_ctd_trx_g28_prm_u3m'),
												col('pos_ctd_trx_g28_prm_u6m').cast('double').alias('pos_ctd_trx_g28_prm_u6m'),
												col('pos_ctd_trx_g28_prm_u9m').cast('double').alias('pos_ctd_trx_g28_prm_u9m'),
												col('pos_ctd_trx_g28_prm_u12').cast('double').alias('pos_ctd_trx_g28_prm_u12'),
												col('pos_ctd_trx_g31_u1m').cast('double').alias('pos_ctd_trx_g31_u1m'),
												col('pos_ctd_trx_g31_prm_u3m').cast('double').alias('pos_ctd_trx_g31_prm_u3m'),
												col('pos_ctd_trx_g31_prm_u6m').cast('double').alias('pos_ctd_trx_g31_prm_u6m'),
												col('pos_ctd_trx_g31_prm_u9m').cast('double').alias('pos_ctd_trx_g31_prm_u9m'),
												col('pos_ctd_trx_g31_prm_u12').cast('double').alias('pos_ctd_trx_g31_prm_u12'),
												col('pos_ctd_trx_g34_u1m').cast('double').alias('pos_ctd_trx_g34_u1m'),
												col('pos_ctd_trx_g34_prm_u3m').cast('double').alias('pos_ctd_trx_g34_prm_u3m'),
												col('pos_ctd_trx_g34_prm_u6m').cast('double').alias('pos_ctd_trx_g34_prm_u6m'),
												col('pos_ctd_trx_g34_prm_u9m').cast('double').alias('pos_ctd_trx_g34_prm_u9m'),
												col('pos_ctd_trx_g34_prm_u12').cast('double').alias('pos_ctd_trx_g34_prm_u12'),
												col('pos_ctd_trx_g35_u1m').cast('double').alias('pos_ctd_trx_g35_u1m'),
												col('pos_ctd_trx_g35_prm_u3m').cast('double').alias('pos_ctd_trx_g35_prm_u3m'),
												col('pos_ctd_trx_g35_prm_u6m').cast('double').alias('pos_ctd_trx_g35_prm_u6m'),
												col('pos_ctd_trx_g35_prm_u9m').cast('double').alias('pos_ctd_trx_g35_prm_u9m'),
												col('pos_ctd_trx_g35_prm_u12').cast('double').alias('pos_ctd_trx_g35_prm_u12'),
												col('pos_ctd_trx_g39_u1m').cast('double').alias('pos_ctd_trx_g39_u1m'),
												col('pos_ctd_trx_g39_prm_u3m').cast('double').alias('pos_ctd_trx_g39_prm_u3m'),
												col('pos_ctd_trx_g39_prm_u6m').cast('double').alias('pos_ctd_trx_g39_prm_u6m'),
												col('pos_ctd_trx_g39_prm_u9m').cast('double').alias('pos_ctd_trx_g39_prm_u9m'),
												col('pos_ctd_trx_g39_prm_u12').cast('double').alias('pos_ctd_trx_g39_prm_u12'),
												col('pos_ctd_trx_g43_u1m').cast('double').alias('pos_ctd_trx_g43_u1m'),
												col('pos_ctd_trx_g43_prm_u3m').cast('double').alias('pos_ctd_trx_g43_prm_u3m'),
												col('pos_ctd_trx_g43_prm_u6m').cast('double').alias('pos_ctd_trx_g43_prm_u6m'),
												col('pos_ctd_trx_g43_prm_u9m').cast('double').alias('pos_ctd_trx_g43_prm_u9m'),
												col('pos_ctd_trx_g43_prm_u12').cast('double').alias('pos_ctd_trx_g43_prm_u12'),
												col('pos_ctd_trx_g45_u1m').cast('double').alias('pos_ctd_trx_g45_u1m'),
												col('pos_ctd_trx_g45_prm_u3m').cast('double').alias('pos_ctd_trx_g45_prm_u3m'),
												col('pos_ctd_trx_g45_prm_u6m').cast('double').alias('pos_ctd_trx_g45_prm_u6m'),
												col('pos_ctd_trx_g45_prm_u9m').cast('double').alias('pos_ctd_trx_g45_prm_u9m'),
												col('pos_ctd_trx_g45_prm_u12').cast('double').alias('pos_ctd_trx_g45_prm_u12'),
												col('pos_ctd_trx_g46_u1m').cast('double').alias('pos_ctd_trx_g46_u1m'),
												col('pos_ctd_trx_g46_prm_u3m').cast('double').alias('pos_ctd_trx_g46_prm_u3m'),
												col('pos_ctd_trx_g46_prm_u6m').cast('double').alias('pos_ctd_trx_g46_prm_u6m'),
												col('pos_ctd_trx_g46_prm_u9m').cast('double').alias('pos_ctd_trx_g46_prm_u9m'),
												col('pos_ctd_trx_g46_prm_u12').cast('double').alias('pos_ctd_trx_g46_prm_u12'),
												col('pos_ctd_trx_ig01_prm_p6m').cast('double').alias('pos_ctd_trx_ig01_prm_p6m'),
												col('pos_ctd_trx_ig02_prm_p6m').cast('double').alias('pos_ctd_trx_ig02_prm_p6m'),
												col('pos_ctd_trx_ig03_prm_p6m').cast('double').alias('pos_ctd_trx_ig03_prm_p6m'),
												col('pos_ctd_trx_ig04_prm_p6m').cast('double').alias('pos_ctd_trx_ig04_prm_p6m'),
												col('pos_ctd_trx_ig05_prm_p6m').cast('double').alias('pos_ctd_trx_ig05_prm_p6m'),
												col('pos_ctd_trx_ig06_prm_p6m').cast('double').alias('pos_ctd_trx_ig06_prm_p6m'),
												col('pos_ctd_trx_ig07_prm_p6m').cast('double').alias('pos_ctd_trx_ig07_prm_p6m'),
												col('pos_ctd_trx_ig08_prm_p6m').cast('double').alias('pos_ctd_trx_ig08_prm_p6m'),
												col('pos_ctd_trx_g01_prm_p6m').cast('double').alias('pos_ctd_trx_g01_prm_p6m'),
												col('pos_ctd_trx_g04_prm_p6m').cast('double').alias('pos_ctd_trx_g04_prm_p6m'),
												col('pos_ctd_trx_g05_prm_p6m').cast('double').alias('pos_ctd_trx_g05_prm_p6m'),
												col('pos_ctd_trx_g06_prm_p6m').cast('double').alias('pos_ctd_trx_g06_prm_p6m'),
												col('pos_ctd_trx_g16_prm_p6m').cast('double').alias('pos_ctd_trx_g16_prm_p6m'),
												col('pos_ctd_trx_g17_prm_p6m').cast('double').alias('pos_ctd_trx_g17_prm_p6m'),
												col('pos_ctd_trx_g18_prm_p6m').cast('double').alias('pos_ctd_trx_g18_prm_p6m'),
												col('pos_ctd_trx_g23_prm_p6m').cast('double').alias('pos_ctd_trx_g23_prm_p6m'),
												col('pos_ctd_trx_g25_prm_p6m').cast('double').alias('pos_ctd_trx_g25_prm_p6m'),
												col('pos_ctd_trx_g28_prm_p6m').cast('double').alias('pos_ctd_trx_g28_prm_p6m'),
												col('pos_ctd_trx_g31_prm_p6m').cast('double').alias('pos_ctd_trx_g31_prm_p6m'),
												col('pos_ctd_trx_g34_prm_p6m').cast('double').alias('pos_ctd_trx_g34_prm_p6m'),
												col('pos_ctd_trx_g35_prm_p6m').cast('double').alias('pos_ctd_trx_g35_prm_p6m'),
												col('pos_ctd_trx_g39_prm_p6m').cast('double').alias('pos_ctd_trx_g39_prm_p6m'),
												col('pos_ctd_trx_g43_prm_p6m').cast('double').alias('pos_ctd_trx_g43_prm_p6m'),
												col('pos_ctd_trx_g45_prm_p6m').cast('double').alias('pos_ctd_trx_g45_prm_p6m'),
												col('pos_ctd_trx_g46_prm_p6m').cast('double').alias('pos_ctd_trx_g46_prm_p6m'),
												col('pos_ctd_trx_ig01_prm_p3m').cast('double').alias('pos_ctd_trx_ig01_prm_p3m'),
												col('pos_ctd_trx_ig02_prm_p3m').cast('double').alias('pos_ctd_trx_ig02_prm_p3m'),
												col('pos_ctd_trx_ig03_prm_p3m').cast('double').alias('pos_ctd_trx_ig03_prm_p3m'),
												col('pos_ctd_trx_ig04_prm_p3m').cast('double').alias('pos_ctd_trx_ig04_prm_p3m'),
												col('pos_ctd_trx_ig05_prm_p3m').cast('double').alias('pos_ctd_trx_ig05_prm_p3m'),
												col('pos_ctd_trx_ig06_prm_p3m').cast('double').alias('pos_ctd_trx_ig06_prm_p3m'),
												col('pos_ctd_trx_ig07_prm_p3m').cast('double').alias('pos_ctd_trx_ig07_prm_p3m'),
												col('pos_ctd_trx_ig08_prm_p3m').cast('double').alias('pos_ctd_trx_ig08_prm_p3m'),
												col('pos_ctd_trx_g01_prm_p3m').cast('double').alias('pos_ctd_trx_g01_prm_p3m'),
												col('pos_ctd_trx_g04_prm_p3m').cast('double').alias('pos_ctd_trx_g04_prm_p3m'),
												col('pos_ctd_trx_g05_prm_p3m').cast('double').alias('pos_ctd_trx_g05_prm_p3m'),
												col('pos_ctd_trx_g06_prm_p3m').cast('double').alias('pos_ctd_trx_g06_prm_p3m'),
												col('pos_ctd_trx_g16_prm_p3m').cast('double').alias('pos_ctd_trx_g16_prm_p3m'),
												col('pos_ctd_trx_g17_prm_p3m').cast('double').alias('pos_ctd_trx_g17_prm_p3m'),
												col('pos_ctd_trx_g18_prm_p3m').cast('double').alias('pos_ctd_trx_g18_prm_p3m'),
												col('pos_ctd_trx_g23_prm_p3m').cast('double').alias('pos_ctd_trx_g23_prm_p3m'),
												col('pos_ctd_trx_g25_prm_p3m').cast('double').alias('pos_ctd_trx_g25_prm_p3m'),
												col('pos_ctd_trx_g28_prm_p3m').cast('double').alias('pos_ctd_trx_g28_prm_p3m'),
												col('pos_ctd_trx_g31_prm_p3m').cast('double').alias('pos_ctd_trx_g31_prm_p3m'),
												col('pos_ctd_trx_g34_prm_p3m').cast('double').alias('pos_ctd_trx_g34_prm_p3m'),
												col('pos_ctd_trx_g35_prm_p3m').cast('double').alias('pos_ctd_trx_g35_prm_p3m'),
												col('pos_ctd_trx_g39_prm_p3m').cast('double').alias('pos_ctd_trx_g39_prm_p3m'),
												col('pos_ctd_trx_g43_prm_p3m').cast('double').alias('pos_ctd_trx_g43_prm_p3m'),
												col('pos_ctd_trx_g45_prm_p3m').cast('double').alias('pos_ctd_trx_g45_prm_p3m'),
												col('pos_ctd_trx_g46_prm_p3m').cast('double').alias('pos_ctd_trx_g46_prm_p3m'),
												col('pos_ctd_trx_ig01_p1m').cast('double').alias('pos_ctd_trx_ig01_p1m'),
												col('pos_ctd_trx_ig02_p1m').cast('double').alias('pos_ctd_trx_ig02_p1m'),
												col('pos_ctd_trx_ig03_p1m').cast('double').alias('pos_ctd_trx_ig03_p1m'),
												col('pos_ctd_trx_ig04_p1m').cast('double').alias('pos_ctd_trx_ig04_p1m'),
												col('pos_ctd_trx_ig05_p1m').cast('double').alias('pos_ctd_trx_ig05_p1m'),
												col('pos_ctd_trx_ig06_p1m').cast('double').alias('pos_ctd_trx_ig06_p1m'),
												col('pos_ctd_trx_ig07_p1m').cast('double').alias('pos_ctd_trx_ig07_p1m'),
												col('pos_ctd_trx_ig08_p1m').cast('double').alias('pos_ctd_trx_ig08_p1m'),
												col('pos_ctd_trx_g01_p1m').cast('double').alias('pos_ctd_trx_g01_p1m'),
												col('pos_ctd_trx_g04_p1m').cast('double').alias('pos_ctd_trx_g04_p1m'),
												col('pos_ctd_trx_g05_p1m').cast('double').alias('pos_ctd_trx_g05_p1m'),
												col('pos_ctd_trx_g06_p1m').cast('double').alias('pos_ctd_trx_g06_p1m'),
												col('pos_ctd_trx_g16_p1m').cast('double').alias('pos_ctd_trx_g16_p1m'),
												col('pos_ctd_trx_g17_p1m').cast('double').alias('pos_ctd_trx_g17_p1m'),
												col('pos_ctd_trx_g18_p1m').cast('double').alias('pos_ctd_trx_g18_p1m'),
												col('pos_ctd_trx_g23_p1m').cast('double').alias('pos_ctd_trx_g23_p1m'),
												col('pos_ctd_trx_g25_p1m').cast('double').alias('pos_ctd_trx_g25_p1m'),
												col('pos_ctd_trx_g28_p1m').cast('double').alias('pos_ctd_trx_g28_p1m'),
												col('pos_ctd_trx_g31_p1m').cast('double').alias('pos_ctd_trx_g31_p1m'),
												col('pos_ctd_trx_g34_p1m').cast('double').alias('pos_ctd_trx_g34_p1m'),
												col('pos_ctd_trx_g35_p1m').cast('double').alias('pos_ctd_trx_g35_p1m'),
												col('pos_ctd_trx_g39_p1m').cast('double').alias('pos_ctd_trx_g39_p1m'),
												col('pos_ctd_trx_g43_p1m').cast('double').alias('pos_ctd_trx_g43_p1m'),
												col('pos_ctd_trx_g45_p1m').cast('double').alias('pos_ctd_trx_g45_p1m'),
												col('pos_ctd_trx_g46_p1m').cast('double').alias('pos_ctd_trx_g46_p1m'),
												col('pos_ctd_trx_ig01_max_u3m').cast('double').alias('pos_ctd_trx_ig01_max_u3m'),
												col('pos_ctd_trx_ig01_max_u6m').cast('double').alias('pos_ctd_trx_ig01_max_u6m'),
												col('pos_ctd_trx_ig01_max_u9m').cast('double').alias('pos_ctd_trx_ig01_max_u9m'),
												col('pos_ctd_trx_ig01_max_u12').cast('double').alias('pos_ctd_trx_ig01_max_u12'),
												col('pos_ctd_trx_ig02_max_u3m').cast('double').alias('pos_ctd_trx_ig02_max_u3m'),
												col('pos_ctd_trx_ig02_max_u6m').cast('double').alias('pos_ctd_trx_ig02_max_u6m'),
												col('pos_ctd_trx_ig02_max_u9m').cast('double').alias('pos_ctd_trx_ig02_max_u9m'),
												col('pos_ctd_trx_ig02_max_u12').cast('double').alias('pos_ctd_trx_ig02_max_u12'),
												col('pos_ctd_trx_ig03_max_u3m').cast('double').alias('pos_ctd_trx_ig03_max_u3m'),
												col('pos_ctd_trx_ig03_max_u6m').cast('double').alias('pos_ctd_trx_ig03_max_u6m'),
												col('pos_ctd_trx_ig03_max_u9m').cast('double').alias('pos_ctd_trx_ig03_max_u9m'),
												col('pos_ctd_trx_ig03_max_u12').cast('double').alias('pos_ctd_trx_ig03_max_u12'),
												col('pos_ctd_trx_ig04_max_u3m').cast('double').alias('pos_ctd_trx_ig04_max_u3m'),
												col('pos_ctd_trx_ig04_max_u6m').cast('double').alias('pos_ctd_trx_ig04_max_u6m'),
												col('pos_ctd_trx_ig04_max_u9m').cast('double').alias('pos_ctd_trx_ig04_max_u9m'),
												col('pos_ctd_trx_ig04_max_u12').cast('double').alias('pos_ctd_trx_ig04_max_u12'),
												col('pos_ctd_trx_ig05_max_u3m').cast('double').alias('pos_ctd_trx_ig05_max_u3m'),
												col('pos_ctd_trx_ig05_max_u6m').cast('double').alias('pos_ctd_trx_ig05_max_u6m'),
												col('pos_ctd_trx_ig05_max_u9m').cast('double').alias('pos_ctd_trx_ig05_max_u9m'),
												col('pos_ctd_trx_ig05_max_u12').cast('double').alias('pos_ctd_trx_ig05_max_u12'),
												col('pos_ctd_trx_ig06_max_u3m').cast('double').alias('pos_ctd_trx_ig06_max_u3m'),
												col('pos_ctd_trx_ig06_max_u6m').cast('double').alias('pos_ctd_trx_ig06_max_u6m'),
												col('pos_ctd_trx_ig06_max_u9m').cast('double').alias('pos_ctd_trx_ig06_max_u9m'),
												col('pos_ctd_trx_ig06_max_u12').cast('double').alias('pos_ctd_trx_ig06_max_u12'),
												col('pos_ctd_trx_ig07_max_u3m').cast('double').alias('pos_ctd_trx_ig07_max_u3m'),
												col('pos_ctd_trx_ig07_max_u6m').cast('double').alias('pos_ctd_trx_ig07_max_u6m'),
												col('pos_ctd_trx_ig07_max_u9m').cast('double').alias('pos_ctd_trx_ig07_max_u9m'),
												col('pos_ctd_trx_ig07_max_u12').cast('double').alias('pos_ctd_trx_ig07_max_u12'),
												col('pos_ctd_trx_ig08_max_u3m').cast('double').alias('pos_ctd_trx_ig08_max_u3m'),
												col('pos_ctd_trx_ig08_max_u6m').cast('double').alias('pos_ctd_trx_ig08_max_u6m'),
												col('pos_ctd_trx_ig08_max_u9m').cast('double').alias('pos_ctd_trx_ig08_max_u9m'),
												col('pos_ctd_trx_ig08_max_u12').cast('double').alias('pos_ctd_trx_ig08_max_u12'),
												col('pos_ctd_trx_g01_max_u3m').cast('double').alias('pos_ctd_trx_g01_max_u3m'),
												col('pos_ctd_trx_g01_max_u6m').cast('double').alias('pos_ctd_trx_g01_max_u6m'),
												col('pos_ctd_trx_g01_max_u9m').cast('double').alias('pos_ctd_trx_g01_max_u9m'),
												col('pos_ctd_trx_g01_max_u12').cast('double').alias('pos_ctd_trx_g01_max_u12'),
												col('pos_ctd_trx_g04_max_u3m').cast('double').alias('pos_ctd_trx_g04_max_u3m'),
												col('pos_ctd_trx_g04_max_u6m').cast('double').alias('pos_ctd_trx_g04_max_u6m'),
												col('pos_ctd_trx_g04_max_u9m').cast('double').alias('pos_ctd_trx_g04_max_u9m'),
												col('pos_ctd_trx_g04_max_u12').cast('double').alias('pos_ctd_trx_g04_max_u12'),
												col('pos_ctd_trx_g05_max_u3m').cast('double').alias('pos_ctd_trx_g05_max_u3m'),
												col('pos_ctd_trx_g05_max_u6m').cast('double').alias('pos_ctd_trx_g05_max_u6m'),
												col('pos_ctd_trx_g05_max_u9m').cast('double').alias('pos_ctd_trx_g05_max_u9m'),
												col('pos_ctd_trx_g05_max_u12').cast('double').alias('pos_ctd_trx_g05_max_u12'),
												col('pos_ctd_trx_g06_max_u3m').cast('double').alias('pos_ctd_trx_g06_max_u3m'),
												col('pos_ctd_trx_g06_max_u6m').cast('double').alias('pos_ctd_trx_g06_max_u6m'),
												col('pos_ctd_trx_g06_max_u9m').cast('double').alias('pos_ctd_trx_g06_max_u9m'),
												col('pos_ctd_trx_g06_max_u12').cast('double').alias('pos_ctd_trx_g06_max_u12'),
												col('pos_ctd_trx_g16_max_u3m').cast('double').alias('pos_ctd_trx_g16_max_u3m'),
												col('pos_ctd_trx_g16_max_u6m').cast('double').alias('pos_ctd_trx_g16_max_u6m'),
												col('pos_ctd_trx_g16_max_u9m').cast('double').alias('pos_ctd_trx_g16_max_u9m'),
												col('pos_ctd_trx_g16_max_u12').cast('double').alias('pos_ctd_trx_g16_max_u12'),
												col('pos_ctd_trx_g17_max_u3m').cast('double').alias('pos_ctd_trx_g17_max_u3m'),
												col('pos_ctd_trx_g17_max_u6m').cast('double').alias('pos_ctd_trx_g17_max_u6m'),
												col('pos_ctd_trx_g17_max_u9m').cast('double').alias('pos_ctd_trx_g17_max_u9m'),
												col('pos_ctd_trx_g17_max_u12').cast('double').alias('pos_ctd_trx_g17_max_u12'),
												col('pos_ctd_trx_g18_max_u3m').cast('double').alias('pos_ctd_trx_g18_max_u3m'),
												col('pos_ctd_trx_g18_max_u6m').cast('double').alias('pos_ctd_trx_g18_max_u6m'),
												col('pos_ctd_trx_g18_max_u9m').cast('double').alias('pos_ctd_trx_g18_max_u9m'),
												col('pos_ctd_trx_g18_max_u12').cast('double').alias('pos_ctd_trx_g18_max_u12'),
												col('pos_ctd_trx_g23_max_u3m').cast('double').alias('pos_ctd_trx_g23_max_u3m'),
												col('pos_ctd_trx_g23_max_u6m').cast('double').alias('pos_ctd_trx_g23_max_u6m'),
												col('pos_ctd_trx_g23_max_u9m').cast('double').alias('pos_ctd_trx_g23_max_u9m'),
												col('pos_ctd_trx_g23_max_u12').cast('double').alias('pos_ctd_trx_g23_max_u12'),
												col('pos_ctd_trx_g25_max_u3m').cast('double').alias('pos_ctd_trx_g25_max_u3m'),
												col('pos_ctd_trx_g25_max_u6m').cast('double').alias('pos_ctd_trx_g25_max_u6m'),
												col('pos_ctd_trx_g25_max_u9m').cast('double').alias('pos_ctd_trx_g25_max_u9m'),
												col('pos_ctd_trx_g25_max_u12').cast('double').alias('pos_ctd_trx_g25_max_u12'),
												col('pos_ctd_trx_g28_max_u3m').cast('double').alias('pos_ctd_trx_g28_max_u3m'),
												col('pos_ctd_trx_g28_max_u6m').cast('double').alias('pos_ctd_trx_g28_max_u6m'),
												col('pos_ctd_trx_g28_max_u9m').cast('double').alias('pos_ctd_trx_g28_max_u9m'),
												col('pos_ctd_trx_g28_max_u12').cast('double').alias('pos_ctd_trx_g28_max_u12'),
												col('pos_ctd_trx_g31_max_u3m').cast('double').alias('pos_ctd_trx_g31_max_u3m'),
												col('pos_ctd_trx_g31_max_u6m').cast('double').alias('pos_ctd_trx_g31_max_u6m'),
												col('pos_ctd_trx_g31_max_u9m').cast('double').alias('pos_ctd_trx_g31_max_u9m'),
												col('pos_ctd_trx_g31_max_u12').cast('double').alias('pos_ctd_trx_g31_max_u12'),
												col('pos_ctd_trx_g34_max_u3m').cast('double').alias('pos_ctd_trx_g34_max_u3m'),
												col('pos_ctd_trx_g34_max_u6m').cast('double').alias('pos_ctd_trx_g34_max_u6m'),
												col('pos_ctd_trx_g34_max_u9m').cast('double').alias('pos_ctd_trx_g34_max_u9m'),
												col('pos_ctd_trx_g34_max_u12').cast('double').alias('pos_ctd_trx_g34_max_u12'),
												col('pos_ctd_trx_g35_max_u3m').cast('double').alias('pos_ctd_trx_g35_max_u3m'),
												col('pos_ctd_trx_g35_max_u6m').cast('double').alias('pos_ctd_trx_g35_max_u6m'),
												col('pos_ctd_trx_g35_max_u9m').cast('double').alias('pos_ctd_trx_g35_max_u9m'),
												col('pos_ctd_trx_g35_max_u12').cast('double').alias('pos_ctd_trx_g35_max_u12'),
												col('pos_ctd_trx_g39_max_u3m').cast('double').alias('pos_ctd_trx_g39_max_u3m'),
												col('pos_ctd_trx_g39_max_u6m').cast('double').alias('pos_ctd_trx_g39_max_u6m'),
												col('pos_ctd_trx_g39_max_u9m').cast('double').alias('pos_ctd_trx_g39_max_u9m'),
												col('pos_ctd_trx_g39_max_u12').cast('double').alias('pos_ctd_trx_g39_max_u12'),
												col('pos_ctd_trx_g43_max_u3m').cast('double').alias('pos_ctd_trx_g43_max_u3m'),
												col('pos_ctd_trx_g43_max_u6m').cast('double').alias('pos_ctd_trx_g43_max_u6m'),
												col('pos_ctd_trx_g43_max_u9m').cast('double').alias('pos_ctd_trx_g43_max_u9m'),
												col('pos_ctd_trx_g43_max_u12').cast('double').alias('pos_ctd_trx_g43_max_u12'),
												col('pos_ctd_trx_g45_max_u3m').cast('double').alias('pos_ctd_trx_g45_max_u3m'),
												col('pos_ctd_trx_g45_max_u6m').cast('double').alias('pos_ctd_trx_g45_max_u6m'),
												col('pos_ctd_trx_g45_max_u9m').cast('double').alias('pos_ctd_trx_g45_max_u9m'),
												col('pos_ctd_trx_g45_max_u12').cast('double').alias('pos_ctd_trx_g45_max_u12'),
												col('pos_ctd_trx_g46_max_u3m').cast('double').alias('pos_ctd_trx_g46_max_u3m'),
												col('pos_ctd_trx_g46_max_u6m').cast('double').alias('pos_ctd_trx_g46_max_u6m'),
												col('pos_ctd_trx_g46_max_u9m').cast('double').alias('pos_ctd_trx_g46_max_u9m'),
												col('pos_ctd_trx_g46_max_u12').cast('double').alias('pos_ctd_trx_g46_max_u12'),
												col('pos_ctd_trx_ig01_min_u3m').cast('double').alias('pos_ctd_trx_ig01_min_u3m'),
												col('pos_ctd_trx_ig01_min_u6m').cast('double').alias('pos_ctd_trx_ig01_min_u6m'),
												col('pos_ctd_trx_ig01_min_u9m').cast('double').alias('pos_ctd_trx_ig01_min_u9m'),
												col('pos_ctd_trx_ig01_min_u12').cast('double').alias('pos_ctd_trx_ig01_min_u12'),
												col('pos_ctd_trx_ig02_min_u3m').cast('double').alias('pos_ctd_trx_ig02_min_u3m'),
												col('pos_ctd_trx_ig02_min_u6m').cast('double').alias('pos_ctd_trx_ig02_min_u6m'),
												col('pos_ctd_trx_ig02_min_u9m').cast('double').alias('pos_ctd_trx_ig02_min_u9m'),
												col('pos_ctd_trx_ig02_min_u12').cast('double').alias('pos_ctd_trx_ig02_min_u12'),
												col('pos_ctd_trx_ig03_min_u3m').cast('double').alias('pos_ctd_trx_ig03_min_u3m'),
												col('pos_ctd_trx_ig03_min_u6m').cast('double').alias('pos_ctd_trx_ig03_min_u6m'),
												col('pos_ctd_trx_ig03_min_u9m').cast('double').alias('pos_ctd_trx_ig03_min_u9m'),
												col('pos_ctd_trx_ig03_min_u12').cast('double').alias('pos_ctd_trx_ig03_min_u12'),
												col('pos_ctd_trx_ig04_min_u3m').cast('double').alias('pos_ctd_trx_ig04_min_u3m'),
												col('pos_ctd_trx_ig04_min_u6m').cast('double').alias('pos_ctd_trx_ig04_min_u6m'),
												col('pos_ctd_trx_ig04_min_u9m').cast('double').alias('pos_ctd_trx_ig04_min_u9m'),
												col('pos_ctd_trx_ig04_min_u12').cast('double').alias('pos_ctd_trx_ig04_min_u12'),
												col('pos_ctd_trx_ig05_min_u3m').cast('double').alias('pos_ctd_trx_ig05_min_u3m'),
												col('pos_ctd_trx_ig05_min_u6m').cast('double').alias('pos_ctd_trx_ig05_min_u6m'),
												col('pos_ctd_trx_ig05_min_u9m').cast('double').alias('pos_ctd_trx_ig05_min_u9m'),
												col('pos_ctd_trx_ig05_min_u12').cast('double').alias('pos_ctd_trx_ig05_min_u12'),
												col('pos_ctd_trx_ig06_min_u3m').cast('double').alias('pos_ctd_trx_ig06_min_u3m'),
												col('pos_ctd_trx_ig06_min_u6m').cast('double').alias('pos_ctd_trx_ig06_min_u6m'),
												col('pos_ctd_trx_ig06_min_u9m').cast('double').alias('pos_ctd_trx_ig06_min_u9m'),
												col('pos_ctd_trx_ig06_min_u12').cast('double').alias('pos_ctd_trx_ig06_min_u12'),
												col('pos_ctd_trx_ig07_min_u3m').cast('double').alias('pos_ctd_trx_ig07_min_u3m'),
												col('pos_ctd_trx_ig07_min_u6m').cast('double').alias('pos_ctd_trx_ig07_min_u6m'),
												col('pos_ctd_trx_ig07_min_u9m').cast('double').alias('pos_ctd_trx_ig07_min_u9m'),
												col('pos_ctd_trx_ig07_min_u12').cast('double').alias('pos_ctd_trx_ig07_min_u12'),
												col('pos_ctd_trx_ig08_min_u3m').cast('double').alias('pos_ctd_trx_ig08_min_u3m'),
												col('pos_ctd_trx_ig08_min_u6m').cast('double').alias('pos_ctd_trx_ig08_min_u6m'),
												col('pos_ctd_trx_ig08_min_u9m').cast('double').alias('pos_ctd_trx_ig08_min_u9m'),
												col('pos_ctd_trx_ig08_min_u12').cast('double').alias('pos_ctd_trx_ig08_min_u12'),
												col('pos_ctd_trx_g01_min_u3m').cast('double').alias('pos_ctd_trx_g01_min_u3m'),
												col('pos_ctd_trx_g01_min_u6m').cast('double').alias('pos_ctd_trx_g01_min_u6m'),
												col('pos_ctd_trx_g01_min_u9m').cast('double').alias('pos_ctd_trx_g01_min_u9m'),
												col('pos_ctd_trx_g01_min_u12').cast('double').alias('pos_ctd_trx_g01_min_u12'),
												col('pos_ctd_trx_g04_min_u3m').cast('double').alias('pos_ctd_trx_g04_min_u3m'),
												col('pos_ctd_trx_g04_min_u6m').cast('double').alias('pos_ctd_trx_g04_min_u6m'),
												col('pos_ctd_trx_g04_min_u9m').cast('double').alias('pos_ctd_trx_g04_min_u9m'),
												col('pos_ctd_trx_g04_min_u12').cast('double').alias('pos_ctd_trx_g04_min_u12'),
												col('pos_ctd_trx_g05_min_u3m').cast('double').alias('pos_ctd_trx_g05_min_u3m'),
												col('pos_ctd_trx_g05_min_u6m').cast('double').alias('pos_ctd_trx_g05_min_u6m'),
												col('pos_ctd_trx_g05_min_u9m').cast('double').alias('pos_ctd_trx_g05_min_u9m'),
												col('pos_ctd_trx_g05_min_u12').cast('double').alias('pos_ctd_trx_g05_min_u12'),
												col('pos_ctd_trx_g06_min_u3m').cast('double').alias('pos_ctd_trx_g06_min_u3m'),
												col('pos_ctd_trx_g06_min_u6m').cast('double').alias('pos_ctd_trx_g06_min_u6m'),
												col('pos_ctd_trx_g06_min_u9m').cast('double').alias('pos_ctd_trx_g06_min_u9m'),
												col('pos_ctd_trx_g06_min_u12').cast('double').alias('pos_ctd_trx_g06_min_u12'),
												col('pos_ctd_trx_g16_min_u3m').cast('double').alias('pos_ctd_trx_g16_min_u3m'),
												col('pos_ctd_trx_g16_min_u6m').cast('double').alias('pos_ctd_trx_g16_min_u6m'),
												col('pos_ctd_trx_g16_min_u9m').cast('double').alias('pos_ctd_trx_g16_min_u9m'),
												col('pos_ctd_trx_g16_min_u12').cast('double').alias('pos_ctd_trx_g16_min_u12'),
												col('pos_ctd_trx_g17_min_u3m').cast('double').alias('pos_ctd_trx_g17_min_u3m'),
												col('pos_ctd_trx_g17_min_u6m').cast('double').alias('pos_ctd_trx_g17_min_u6m'),
												col('pos_ctd_trx_g17_min_u9m').cast('double').alias('pos_ctd_trx_g17_min_u9m'),
												col('pos_ctd_trx_g17_min_u12').cast('double').alias('pos_ctd_trx_g17_min_u12'),
												col('pos_ctd_trx_g18_min_u3m').cast('double').alias('pos_ctd_trx_g18_min_u3m'),
												col('pos_ctd_trx_g18_min_u6m').cast('double').alias('pos_ctd_trx_g18_min_u6m'),
												col('pos_ctd_trx_g18_min_u9m').cast('double').alias('pos_ctd_trx_g18_min_u9m'),
												col('pos_ctd_trx_g18_min_u12').cast('double').alias('pos_ctd_trx_g18_min_u12'),
												col('pos_ctd_trx_g23_min_u3m').cast('double').alias('pos_ctd_trx_g23_min_u3m'),
												col('pos_ctd_trx_g23_min_u6m').cast('double').alias('pos_ctd_trx_g23_min_u6m'),
												col('pos_ctd_trx_g23_min_u9m').cast('double').alias('pos_ctd_trx_g23_min_u9m'),
												col('pos_ctd_trx_g23_min_u12').cast('double').alias('pos_ctd_trx_g23_min_u12'),
												col('pos_ctd_trx_g25_min_u3m').cast('double').alias('pos_ctd_trx_g25_min_u3m'),
												col('pos_ctd_trx_g25_min_u6m').cast('double').alias('pos_ctd_trx_g25_min_u6m'),
												col('pos_ctd_trx_g25_min_u9m').cast('double').alias('pos_ctd_trx_g25_min_u9m'),
												col('pos_ctd_trx_g25_min_u12').cast('double').alias('pos_ctd_trx_g25_min_u12'),
												col('pos_ctd_trx_g28_min_u3m').cast('double').alias('pos_ctd_trx_g28_min_u3m'),
												col('pos_ctd_trx_g28_min_u6m').cast('double').alias('pos_ctd_trx_g28_min_u6m'),
												col('pos_ctd_trx_g28_min_u9m').cast('double').alias('pos_ctd_trx_g28_min_u9m'),
												col('pos_ctd_trx_g28_min_u12').cast('double').alias('pos_ctd_trx_g28_min_u12'),
												col('pos_ctd_trx_g31_min_u3m').cast('double').alias('pos_ctd_trx_g31_min_u3m'),
												col('pos_ctd_trx_g31_min_u6m').cast('double').alias('pos_ctd_trx_g31_min_u6m'),
												col('pos_ctd_trx_g31_min_u9m').cast('double').alias('pos_ctd_trx_g31_min_u9m'),
												col('pos_ctd_trx_g31_min_u12').cast('double').alias('pos_ctd_trx_g31_min_u12'),
												col('pos_ctd_trx_g34_min_u3m').cast('double').alias('pos_ctd_trx_g34_min_u3m'),
												col('pos_ctd_trx_g34_min_u6m').cast('double').alias('pos_ctd_trx_g34_min_u6m'),
												col('pos_ctd_trx_g34_min_u9m').cast('double').alias('pos_ctd_trx_g34_min_u9m'),
												col('pos_ctd_trx_g34_min_u12').cast('double').alias('pos_ctd_trx_g34_min_u12'),
												col('pos_ctd_trx_g35_min_u3m').cast('double').alias('pos_ctd_trx_g35_min_u3m'),
												col('pos_ctd_trx_g35_min_u6m').cast('double').alias('pos_ctd_trx_g35_min_u6m'),
												col('pos_ctd_trx_g35_min_u9m').cast('double').alias('pos_ctd_trx_g35_min_u9m'),
												col('pos_ctd_trx_g35_min_u12').cast('double').alias('pos_ctd_trx_g35_min_u12'),
												col('pos_ctd_trx_g39_min_u3m').cast('double').alias('pos_ctd_trx_g39_min_u3m'),
												col('pos_ctd_trx_g39_min_u6m').cast('double').alias('pos_ctd_trx_g39_min_u6m'),
												col('pos_ctd_trx_g39_min_u9m').cast('double').alias('pos_ctd_trx_g39_min_u9m'),
												col('pos_ctd_trx_g39_min_u12').cast('double').alias('pos_ctd_trx_g39_min_u12'),
												col('pos_ctd_trx_g43_min_u3m').cast('double').alias('pos_ctd_trx_g43_min_u3m'),
												col('pos_ctd_trx_g43_min_u6m').cast('double').alias('pos_ctd_trx_g43_min_u6m'),
												col('pos_ctd_trx_g43_min_u9m').cast('double').alias('pos_ctd_trx_g43_min_u9m'),
												col('pos_ctd_trx_g43_min_u12').cast('double').alias('pos_ctd_trx_g43_min_u12'),
												col('pos_ctd_trx_g45_min_u3m').cast('double').alias('pos_ctd_trx_g45_min_u3m'),
												col('pos_ctd_trx_g45_min_u6m').cast('double').alias('pos_ctd_trx_g45_min_u6m'),
												col('pos_ctd_trx_g45_min_u9m').cast('double').alias('pos_ctd_trx_g45_min_u9m'),
												col('pos_ctd_trx_g45_min_u12').cast('double').alias('pos_ctd_trx_g45_min_u12'),
												col('pos_ctd_trx_g46_min_u3m').cast('double').alias('pos_ctd_trx_g46_min_u3m'),
												col('pos_ctd_trx_g46_min_u6m').cast('double').alias('pos_ctd_trx_g46_min_u6m'),
												col('pos_ctd_trx_g46_min_u9m').cast('double').alias('pos_ctd_trx_g46_min_u9m'),
												col('pos_ctd_trx_g46_min_u12').cast('double').alias('pos_ctd_trx_g46_min_u12'),
												col('pos_ctd_trx_ig01_rec').cast('double').alias('pos_ctd_trx_ig01_rec'),
												col('pos_ctd_trx_ig02_rec').cast('double').alias('pos_ctd_trx_ig02_rec'),
												col('pos_ctd_trx_ig03_rec').cast('double').alias('pos_ctd_trx_ig03_rec'),
												col('pos_ctd_trx_ig04_rec').cast('double').alias('pos_ctd_trx_ig04_rec'),
												col('pos_ctd_trx_ig05_rec').cast('double').alias('pos_ctd_trx_ig05_rec'),
												col('pos_ctd_trx_ig06_rec').cast('double').alias('pos_ctd_trx_ig06_rec'),
												col('pos_ctd_trx_ig07_rec').cast('double').alias('pos_ctd_trx_ig07_rec'),
												col('pos_ctd_trx_ig08_rec').cast('double').alias('pos_ctd_trx_ig08_rec'),
												col('pos_ctd_trx_g01_rec').cast('double').alias('pos_ctd_trx_g01_rec'),
												col('pos_ctd_trx_g04_rec').cast('double').alias('pos_ctd_trx_g04_rec'),
												col('pos_ctd_trx_g05_rec').cast('double').alias('pos_ctd_trx_g05_rec'),
												col('pos_ctd_trx_g06_rec').cast('double').alias('pos_ctd_trx_g06_rec'),
												col('pos_ctd_trx_g16_rec').cast('double').alias('pos_ctd_trx_g16_rec'),
												col('pos_ctd_trx_g17_rec').cast('double').alias('pos_ctd_trx_g17_rec'),
												col('pos_ctd_trx_g18_rec').cast('double').alias('pos_ctd_trx_g18_rec'),
												col('pos_ctd_trx_g23_rec').cast('double').alias('pos_ctd_trx_g23_rec'),
												col('pos_ctd_trx_g25_rec').cast('double').alias('pos_ctd_trx_g25_rec'),
												col('pos_ctd_trx_g28_rec').cast('double').alias('pos_ctd_trx_g28_rec'),
												col('pos_ctd_trx_g31_rec').cast('double').alias('pos_ctd_trx_g31_rec'),
												col('pos_ctd_trx_g34_rec').cast('double').alias('pos_ctd_trx_g34_rec'),
												col('pos_ctd_trx_g35_rec').cast('double').alias('pos_ctd_trx_g35_rec'),
												col('pos_ctd_trx_g39_rec').cast('double').alias('pos_ctd_trx_g39_rec'),
												col('pos_ctd_trx_g43_rec').cast('double').alias('pos_ctd_trx_g43_rec'),
												col('pos_ctd_trx_g45_rec').cast('double').alias('pos_ctd_trx_g45_rec'),
												col('pos_ctd_trx_g46_rec').cast('double').alias('pos_ctd_trx_g46_rec'),
												col('pos_ctd_trx_ig01_frq_u3m').cast('double').alias('pos_ctd_trx_ig01_frq_u3m'),
												col('pos_ctd_trx_ig02_frq_u3m').cast('double').alias('pos_ctd_trx_ig02_frq_u3m'),
												col('pos_ctd_trx_ig03_frq_u3m').cast('double').alias('pos_ctd_trx_ig03_frq_u3m'),
												col('pos_ctd_trx_ig04_frq_u3m').cast('double').alias('pos_ctd_trx_ig04_frq_u3m'),
												col('pos_ctd_trx_ig05_frq_u3m').cast('double').alias('pos_ctd_trx_ig05_frq_u3m'),
												col('pos_ctd_trx_ig06_frq_u3m').cast('double').alias('pos_ctd_trx_ig06_frq_u3m'),
												col('pos_ctd_trx_ig07_frq_u3m').cast('double').alias('pos_ctd_trx_ig07_frq_u3m'),
												col('pos_ctd_trx_ig08_frq_u3m').cast('double').alias('pos_ctd_trx_ig08_frq_u3m'),
												col('pos_ctd_trx_g01_frq_u3m').cast('double').alias('pos_ctd_trx_g01_frq_u3m'),
												col('pos_ctd_trx_g04_frq_u3m').cast('double').alias('pos_ctd_trx_g04_frq_u3m'),
												col('pos_ctd_trx_g05_frq_u3m').cast('double').alias('pos_ctd_trx_g05_frq_u3m'),
												col('pos_ctd_trx_g06_frq_u3m').cast('double').alias('pos_ctd_trx_g06_frq_u3m'),
												col('pos_ctd_trx_g16_frq_u3m').cast('double').alias('pos_ctd_trx_g16_frq_u3m'),
												col('pos_ctd_trx_g17_frq_u3m').cast('double').alias('pos_ctd_trx_g17_frq_u3m'),
												col('pos_ctd_trx_g18_frq_u3m').cast('double').alias('pos_ctd_trx_g18_frq_u3m'),
												col('pos_ctd_trx_g23_frq_u3m').cast('double').alias('pos_ctd_trx_g23_frq_u3m'),
												col('pos_ctd_trx_g25_frq_u3m').cast('double').alias('pos_ctd_trx_g25_frq_u3m'),
												col('pos_ctd_trx_g28_frq_u3m').cast('double').alias('pos_ctd_trx_g28_frq_u3m'),
												col('pos_ctd_trx_g31_frq_u3m').cast('double').alias('pos_ctd_trx_g31_frq_u3m'),
												col('pos_ctd_trx_g34_frq_u3m').cast('double').alias('pos_ctd_trx_g34_frq_u3m'),
												col('pos_ctd_trx_g35_frq_u3m').cast('double').alias('pos_ctd_trx_g35_frq_u3m'),
												col('pos_ctd_trx_g39_frq_u3m').cast('double').alias('pos_ctd_trx_g39_frq_u3m'),
												col('pos_ctd_trx_g43_frq_u3m').cast('double').alias('pos_ctd_trx_g43_frq_u3m'),
												col('pos_ctd_trx_g45_frq_u3m').cast('double').alias('pos_ctd_trx_g45_frq_u3m'),
												col('pos_ctd_trx_g46_frq_u3m').cast('double').alias('pos_ctd_trx_g46_frq_u3m'),
												col('pos_ctd_trx_ig01_frq_u6m').cast('double').alias('pos_ctd_trx_ig01_frq_u6m'),
												col('pos_ctd_trx_ig02_frq_u6m').cast('double').alias('pos_ctd_trx_ig02_frq_u6m'),
												col('pos_ctd_trx_ig03_frq_u6m').cast('double').alias('pos_ctd_trx_ig03_frq_u6m'),
												col('pos_ctd_trx_ig04_frq_u6m').cast('double').alias('pos_ctd_trx_ig04_frq_u6m'),
												col('pos_ctd_trx_ig05_frq_u6m').cast('double').alias('pos_ctd_trx_ig05_frq_u6m'),
												col('pos_ctd_trx_ig06_frq_u6m').cast('double').alias('pos_ctd_trx_ig06_frq_u6m'),
												col('pos_ctd_trx_ig07_frq_u6m').cast('double').alias('pos_ctd_trx_ig07_frq_u6m'),
												col('pos_ctd_trx_ig08_frq_u6m').cast('double').alias('pos_ctd_trx_ig08_frq_u6m'),
												col('pos_ctd_trx_g01_frq_u6m').cast('double').alias('pos_ctd_trx_g01_frq_u6m'),
												col('pos_ctd_trx_g04_frq_u6m').cast('double').alias('pos_ctd_trx_g04_frq_u6m'),
												col('pos_ctd_trx_g05_frq_u6m').cast('double').alias('pos_ctd_trx_g05_frq_u6m'),
												col('pos_ctd_trx_g06_frq_u6m').cast('double').alias('pos_ctd_trx_g06_frq_u6m'),
												col('pos_ctd_trx_g16_frq_u6m').cast('double').alias('pos_ctd_trx_g16_frq_u6m'),
												col('pos_ctd_trx_g17_frq_u6m').cast('double').alias('pos_ctd_trx_g17_frq_u6m'),
												col('pos_ctd_trx_g18_frq_u6m').cast('double').alias('pos_ctd_trx_g18_frq_u6m'),
												col('pos_ctd_trx_g23_frq_u6m').cast('double').alias('pos_ctd_trx_g23_frq_u6m'),
												col('pos_ctd_trx_g25_frq_u6m').cast('double').alias('pos_ctd_trx_g25_frq_u6m'),
												col('pos_ctd_trx_g28_frq_u6m').cast('double').alias('pos_ctd_trx_g28_frq_u6m'),
												col('pos_ctd_trx_g31_frq_u6m').cast('double').alias('pos_ctd_trx_g31_frq_u6m'),
												col('pos_ctd_trx_g34_frq_u6m').cast('double').alias('pos_ctd_trx_g34_frq_u6m'),
												col('pos_ctd_trx_g35_frq_u6m').cast('double').alias('pos_ctd_trx_g35_frq_u6m'),
												col('pos_ctd_trx_g39_frq_u6m').cast('double').alias('pos_ctd_trx_g39_frq_u6m'),
												col('pos_ctd_trx_g43_frq_u6m').cast('double').alias('pos_ctd_trx_g43_frq_u6m'),
												col('pos_ctd_trx_g45_frq_u6m').cast('double').alias('pos_ctd_trx_g45_frq_u6m'),
												col('pos_ctd_trx_g46_frq_u6m').cast('double').alias('pos_ctd_trx_g46_frq_u6m'),
												col('pos_ctd_trx_ig01_frq_u9m').cast('double').alias('pos_ctd_trx_ig01_frq_u9m'),
												col('pos_ctd_trx_ig02_frq_u9m').cast('double').alias('pos_ctd_trx_ig02_frq_u9m'),
												col('pos_ctd_trx_ig03_frq_u9m').cast('double').alias('pos_ctd_trx_ig03_frq_u9m'),
												col('pos_ctd_trx_ig04_frq_u9m').cast('double').alias('pos_ctd_trx_ig04_frq_u9m'),
												col('pos_ctd_trx_ig05_frq_u9m').cast('double').alias('pos_ctd_trx_ig05_frq_u9m'),
												col('pos_ctd_trx_ig06_frq_u9m').cast('double').alias('pos_ctd_trx_ig06_frq_u9m'),
												col('pos_ctd_trx_ig07_frq_u9m').cast('double').alias('pos_ctd_trx_ig07_frq_u9m'),
												col('pos_ctd_trx_ig08_frq_u9m').cast('double').alias('pos_ctd_trx_ig08_frq_u9m'),
												col('pos_ctd_trx_g01_frq_u9m').cast('double').alias('pos_ctd_trx_g01_frq_u9m'),
												col('pos_ctd_trx_g04_frq_u9m').cast('double').alias('pos_ctd_trx_g04_frq_u9m'),
												col('pos_ctd_trx_g05_frq_u9m').cast('double').alias('pos_ctd_trx_g05_frq_u9m'),
												col('pos_ctd_trx_g06_frq_u9m').cast('double').alias('pos_ctd_trx_g06_frq_u9m'),
												col('pos_ctd_trx_g16_frq_u9m').cast('double').alias('pos_ctd_trx_g16_frq_u9m'),
												col('pos_ctd_trx_g17_frq_u9m').cast('double').alias('pos_ctd_trx_g17_frq_u9m'),
												col('pos_ctd_trx_g18_frq_u9m').cast('double').alias('pos_ctd_trx_g18_frq_u9m'),
												col('pos_ctd_trx_g23_frq_u9m').cast('double').alias('pos_ctd_trx_g23_frq_u9m'),
												col('pos_ctd_trx_g25_frq_u9m').cast('double').alias('pos_ctd_trx_g25_frq_u9m'),
												col('pos_ctd_trx_g28_frq_u9m').cast('double').alias('pos_ctd_trx_g28_frq_u9m'),
												col('pos_ctd_trx_g31_frq_u9m').cast('double').alias('pos_ctd_trx_g31_frq_u9m'),
												col('pos_ctd_trx_g34_frq_u9m').cast('double').alias('pos_ctd_trx_g34_frq_u9m'),
												col('pos_ctd_trx_g35_frq_u9m').cast('double').alias('pos_ctd_trx_g35_frq_u9m'),
												col('pos_ctd_trx_g39_frq_u9m').cast('double').alias('pos_ctd_trx_g39_frq_u9m'),
												col('pos_ctd_trx_g43_frq_u9m').cast('double').alias('pos_ctd_trx_g43_frq_u9m'),
												col('pos_ctd_trx_g45_frq_u9m').cast('double').alias('pos_ctd_trx_g45_frq_u9m'),
												col('pos_ctd_trx_g46_frq_u9m').cast('double').alias('pos_ctd_trx_g46_frq_u9m'),
												col('pos_ctd_trx_ig01_frq_u12').cast('double').alias('pos_ctd_trx_ig01_frq_u12'),
												col('pos_ctd_trx_ig02_frq_u12').cast('double').alias('pos_ctd_trx_ig02_frq_u12'),
												col('pos_ctd_trx_ig03_frq_u12').cast('double').alias('pos_ctd_trx_ig03_frq_u12'),
												col('pos_ctd_trx_ig04_frq_u12').cast('double').alias('pos_ctd_trx_ig04_frq_u12'),
												col('pos_ctd_trx_ig05_frq_u12').cast('double').alias('pos_ctd_trx_ig05_frq_u12'),
												col('pos_ctd_trx_ig06_frq_u12').cast('double').alias('pos_ctd_trx_ig06_frq_u12'),
												col('pos_ctd_trx_ig07_frq_u12').cast('double').alias('pos_ctd_trx_ig07_frq_u12'),
												col('pos_ctd_trx_ig08_frq_u12').cast('double').alias('pos_ctd_trx_ig08_frq_u12'),
												col('pos_ctd_trx_g01_frq_u12').cast('double').alias('pos_ctd_trx_g01_frq_u12'),
												col('pos_ctd_trx_g04_frq_u12').cast('double').alias('pos_ctd_trx_g04_frq_u12'),
												col('pos_ctd_trx_g05_frq_u12').cast('double').alias('pos_ctd_trx_g05_frq_u12'),
												col('pos_ctd_trx_g06_frq_u12').cast('double').alias('pos_ctd_trx_g06_frq_u12'),
												col('pos_ctd_trx_g16_frq_u12').cast('double').alias('pos_ctd_trx_g16_frq_u12'),
												col('pos_ctd_trx_g17_frq_u12').cast('double').alias('pos_ctd_trx_g17_frq_u12'),
												col('pos_ctd_trx_g18_frq_u12').cast('double').alias('pos_ctd_trx_g18_frq_u12'),
												col('pos_ctd_trx_g23_frq_u12').cast('double').alias('pos_ctd_trx_g23_frq_u12'),
												col('pos_ctd_trx_g25_frq_u12').cast('double').alias('pos_ctd_trx_g25_frq_u12'),
												col('pos_ctd_trx_g28_frq_u12').cast('double').alias('pos_ctd_trx_g28_frq_u12'),
												col('pos_ctd_trx_g31_frq_u12').cast('double').alias('pos_ctd_trx_g31_frq_u12'),
												col('pos_ctd_trx_g34_frq_u12').cast('double').alias('pos_ctd_trx_g34_frq_u12'),
												col('pos_ctd_trx_g35_frq_u12').cast('double').alias('pos_ctd_trx_g35_frq_u12'),
												col('pos_ctd_trx_g39_frq_u12').cast('double').alias('pos_ctd_trx_g39_frq_u12'),
												col('pos_ctd_trx_g43_frq_u12').cast('double').alias('pos_ctd_trx_g43_frq_u12'),
												col('pos_ctd_trx_g45_frq_u12').cast('double').alias('pos_ctd_trx_g45_frq_u12'),
												col('pos_ctd_trx_g46_frq_u12').cast('double').alias('pos_ctd_trx_g46_frq_u12'),
												col('pos_mto_trx_ig01_g6m').cast('double').alias('pos_mto_trx_ig01_g6m'),
												col('pos_mto_trx_ig02_g6m').cast('double').alias('pos_mto_trx_ig02_g6m'),
												col('pos_mto_trx_ig03_g6m').cast('double').alias('pos_mto_trx_ig03_g6m'),
												col('pos_mto_trx_ig04_g6m').cast('double').alias('pos_mto_trx_ig04_g6m'),
												col('pos_mto_trx_ig05_g6m').cast('double').alias('pos_mto_trx_ig05_g6m'),
												col('pos_mto_trx_ig06_g6m').cast('double').alias('pos_mto_trx_ig06_g6m'),
												col('pos_mto_trx_ig07_g6m').cast('double').alias('pos_mto_trx_ig07_g6m'),
												col('pos_mto_trx_ig08_g6m').cast('double').alias('pos_mto_trx_ig08_g6m'),
												col('pos_mto_trx_g01_g6m').cast('double').alias('pos_mto_trx_g01_g6m'),
												col('pos_mto_trx_g04_g6m').cast('double').alias('pos_mto_trx_g04_g6m'),
												col('pos_mto_trx_g05_g6m').cast('double').alias('pos_mto_trx_g05_g6m'),
												col('pos_mto_trx_g06_g6m').cast('double').alias('pos_mto_trx_g06_g6m'),
												col('pos_mto_trx_g16_g6m').cast('double').alias('pos_mto_trx_g16_g6m'),
												col('pos_mto_trx_g17_g6m').cast('double').alias('pos_mto_trx_g17_g6m'),
												col('pos_mto_trx_g18_g6m').cast('double').alias('pos_mto_trx_g18_g6m'),
												col('pos_mto_trx_g23_g6m').cast('double').alias('pos_mto_trx_g23_g6m'),
												col('pos_mto_trx_g25_g6m').cast('double').alias('pos_mto_trx_g25_g6m'),
												col('pos_mto_trx_g28_g6m').cast('double').alias('pos_mto_trx_g28_g6m'),
												col('pos_mto_trx_g31_g6m').cast('double').alias('pos_mto_trx_g31_g6m'),
												col('pos_mto_trx_g34_g6m').cast('double').alias('pos_mto_trx_g34_g6m'),
												col('pos_mto_trx_g35_g6m').cast('double').alias('pos_mto_trx_g35_g6m'),
												col('pos_mto_trx_g39_g6m').cast('double').alias('pos_mto_trx_g39_g6m'),
												col('pos_mto_trx_g43_g6m').cast('double').alias('pos_mto_trx_g43_g6m'),
												col('pos_mto_trx_g45_g6m').cast('double').alias('pos_mto_trx_g45_g6m'),
												col('pos_mto_trx_g46_g6m').cast('double').alias('pos_mto_trx_g46_g6m'),
												col('pos_tkt_trx_ig01_g6m').cast('double').alias('pos_tkt_trx_ig01_g6m'),
												col('pos_tkt_trx_ig02_g6m').cast('double').alias('pos_tkt_trx_ig02_g6m'),
												col('pos_tkt_trx_ig03_g6m').cast('double').alias('pos_tkt_trx_ig03_g6m'),
												col('pos_tkt_trx_ig04_g6m').cast('double').alias('pos_tkt_trx_ig04_g6m'),
												col('pos_tkt_trx_ig05_g6m').cast('double').alias('pos_tkt_trx_ig05_g6m'),
												col('pos_tkt_trx_ig06_g6m').cast('double').alias('pos_tkt_trx_ig06_g6m'),
												col('pos_tkt_trx_ig07_g6m').cast('double').alias('pos_tkt_trx_ig07_g6m'),
												col('pos_tkt_trx_ig08_g6m').cast('double').alias('pos_tkt_trx_ig08_g6m'),
												col('pos_tkt_trx_g01_g6m').cast('double').alias('pos_tkt_trx_g01_g6m'),
												col('pos_tkt_trx_g04_g6m').cast('double').alias('pos_tkt_trx_g04_g6m'),
												col('pos_tkt_trx_g05_g6m').cast('double').alias('pos_tkt_trx_g05_g6m'),
												col('pos_tkt_trx_g06_g6m').cast('double').alias('pos_tkt_trx_g06_g6m'),
												col('pos_tkt_trx_g16_g6m').cast('double').alias('pos_tkt_trx_g16_g6m'),
												col('pos_tkt_trx_g17_g6m').cast('double').alias('pos_tkt_trx_g17_g6m'),
												col('pos_tkt_trx_g18_g6m').cast('double').alias('pos_tkt_trx_g18_g6m'),
												col('pos_tkt_trx_g23_g6m').cast('double').alias('pos_tkt_trx_g23_g6m'),
												col('pos_tkt_trx_g25_g6m').cast('double').alias('pos_tkt_trx_g25_g6m'),
												col('pos_tkt_trx_g28_g6m').cast('double').alias('pos_tkt_trx_g28_g6m'),
												col('pos_tkt_trx_g31_g6m').cast('double').alias('pos_tkt_trx_g31_g6m'),
												col('pos_tkt_trx_g34_g6m').cast('double').alias('pos_tkt_trx_g34_g6m'),
												col('pos_tkt_trx_g35_g6m').cast('double').alias('pos_tkt_trx_g35_g6m'),
												col('pos_tkt_trx_g39_g6m').cast('double').alias('pos_tkt_trx_g39_g6m'),
												col('pos_tkt_trx_g43_g6m').cast('double').alias('pos_tkt_trx_g43_g6m'),
												col('pos_tkt_trx_g45_g6m').cast('double').alias('pos_tkt_trx_g45_g6m'),
												col('pos_tkt_trx_g46_g6m').cast('double').alias('pos_tkt_trx_g46_g6m'),
												col('pos_ctd_trx_ig01_g6m').cast('double').alias('pos_ctd_trx_ig01_g6m'),
												col('pos_ctd_trx_ig02_g6m').cast('double').alias('pos_ctd_trx_ig02_g6m'),
												col('pos_ctd_trx_ig03_g6m').cast('double').alias('pos_ctd_trx_ig03_g6m'),
												col('pos_ctd_trx_ig04_g6m').cast('double').alias('pos_ctd_trx_ig04_g6m'),
												col('pos_ctd_trx_ig05_g6m').cast('double').alias('pos_ctd_trx_ig05_g6m'),
												col('pos_ctd_trx_ig06_g6m').cast('double').alias('pos_ctd_trx_ig06_g6m'),
												col('pos_ctd_trx_ig07_g6m').cast('double').alias('pos_ctd_trx_ig07_g6m'),
												col('pos_ctd_trx_ig08_g6m').cast('double').alias('pos_ctd_trx_ig08_g6m'),
												col('pos_ctd_trx_g01_g6m').cast('double').alias('pos_ctd_trx_g01_g6m'),
												col('pos_ctd_trx_g04_g6m').cast('double').alias('pos_ctd_trx_g04_g6m'),
												col('pos_ctd_trx_g05_g6m').cast('double').alias('pos_ctd_trx_g05_g6m'),
												col('pos_ctd_trx_g06_g6m').cast('double').alias('pos_ctd_trx_g06_g6m'),
												col('pos_ctd_trx_g16_g6m').cast('double').alias('pos_ctd_trx_g16_g6m'),
												col('pos_ctd_trx_g17_g6m').cast('double').alias('pos_ctd_trx_g17_g6m'),
												col('pos_ctd_trx_g18_g6m').cast('double').alias('pos_ctd_trx_g18_g6m'),
												col('pos_ctd_trx_g23_g6m').cast('double').alias('pos_ctd_trx_g23_g6m'),
												col('pos_ctd_trx_g25_g6m').cast('double').alias('pos_ctd_trx_g25_g6m'),
												col('pos_ctd_trx_g28_g6m').cast('double').alias('pos_ctd_trx_g28_g6m'),
												col('pos_ctd_trx_g31_g6m').cast('double').alias('pos_ctd_trx_g31_g6m'),
												col('pos_ctd_trx_g34_g6m').cast('double').alias('pos_ctd_trx_g34_g6m'),
												col('pos_ctd_trx_g35_g6m').cast('double').alias('pos_ctd_trx_g35_g6m'),
												col('pos_ctd_trx_g39_g6m').cast('double').alias('pos_ctd_trx_g39_g6m'),
												col('pos_ctd_trx_g43_g6m').cast('double').alias('pos_ctd_trx_g43_g6m'),
												col('pos_ctd_trx_g45_g6m').cast('double').alias('pos_ctd_trx_g45_g6m'),
												col('pos_ctd_trx_g46_g6m').cast('double').alias('pos_ctd_trx_g46_g6m'),
												col('pos_mto_trx_ig01_g3m').cast('double').alias('pos_mto_trx_ig01_g3m'),
												col('pos_mto_trx_ig02_g3m').cast('double').alias('pos_mto_trx_ig02_g3m'),
												col('pos_mto_trx_ig03_g3m').cast('double').alias('pos_mto_trx_ig03_g3m'),
												col('pos_mto_trx_ig04_g3m').cast('double').alias('pos_mto_trx_ig04_g3m'),
												col('pos_mto_trx_ig05_g3m').cast('double').alias('pos_mto_trx_ig05_g3m'),
												col('pos_mto_trx_ig06_g3m').cast('double').alias('pos_mto_trx_ig06_g3m'),
												col('pos_mto_trx_ig07_g3m').cast('double').alias('pos_mto_trx_ig07_g3m'),
												col('pos_mto_trx_ig08_g3m').cast('double').alias('pos_mto_trx_ig08_g3m'),
												col('pos_mto_trx_g01_g3m').cast('double').alias('pos_mto_trx_g01_g3m'),
												col('pos_mto_trx_g04_g3m').cast('double').alias('pos_mto_trx_g04_g3m'),
												col('pos_mto_trx_g05_g3m').cast('double').alias('pos_mto_trx_g05_g3m'),
												col('pos_mto_trx_g06_g3m').cast('double').alias('pos_mto_trx_g06_g3m'),
												col('pos_mto_trx_g16_g3m').cast('double').alias('pos_mto_trx_g16_g3m'),
												col('pos_mto_trx_g17_g3m').cast('double').alias('pos_mto_trx_g17_g3m'),
												col('pos_mto_trx_g18_g3m').cast('double').alias('pos_mto_trx_g18_g3m'),
												col('pos_mto_trx_g23_g3m').cast('double').alias('pos_mto_trx_g23_g3m'),
												col('pos_mto_trx_g25_g3m').cast('double').alias('pos_mto_trx_g25_g3m'),
												col('pos_mto_trx_g28_g3m').cast('double').alias('pos_mto_trx_g28_g3m'),
												col('pos_mto_trx_g31_g3m').cast('double').alias('pos_mto_trx_g31_g3m'),
												col('pos_mto_trx_g34_g3m').cast('double').alias('pos_mto_trx_g34_g3m'),
												col('pos_mto_trx_g35_g3m').cast('double').alias('pos_mto_trx_g35_g3m'),
												col('pos_mto_trx_g39_g3m').cast('double').alias('pos_mto_trx_g39_g3m'),
												col('pos_mto_trx_g43_g3m').cast('double').alias('pos_mto_trx_g43_g3m'),
												col('pos_mto_trx_g45_g3m').cast('double').alias('pos_mto_trx_g45_g3m'),
												col('pos_mto_trx_g46_g3m').cast('double').alias('pos_mto_trx_g46_g3m'),
												col('pos_tkt_trx_ig01_g3m').cast('double').alias('pos_tkt_trx_ig01_g3m'),
												col('pos_tkt_trx_ig02_g3m').cast('double').alias('pos_tkt_trx_ig02_g3m'),
												col('pos_tkt_trx_ig03_g3m').cast('double').alias('pos_tkt_trx_ig03_g3m'),
												col('pos_tkt_trx_ig04_g3m').cast('double').alias('pos_tkt_trx_ig04_g3m'),
												col('pos_tkt_trx_ig05_g3m').cast('double').alias('pos_tkt_trx_ig05_g3m'),
												col('pos_tkt_trx_ig06_g3m').cast('double').alias('pos_tkt_trx_ig06_g3m'),
												col('pos_tkt_trx_ig07_g3m').cast('double').alias('pos_tkt_trx_ig07_g3m'),
												col('pos_tkt_trx_ig08_g3m').cast('double').alias('pos_tkt_trx_ig08_g3m'),
												col('pos_tkt_trx_g01_g3m').cast('double').alias('pos_tkt_trx_g01_g3m'),
												col('pos_tkt_trx_g04_g3m').cast('double').alias('pos_tkt_trx_g04_g3m'),
												col('pos_tkt_trx_g05_g3m').cast('double').alias('pos_tkt_trx_g05_g3m'),
												col('pos_tkt_trx_g06_g3m').cast('double').alias('pos_tkt_trx_g06_g3m'),
												col('pos_tkt_trx_g16_g3m').cast('double').alias('pos_tkt_trx_g16_g3m'),
												col('pos_tkt_trx_g17_g3m').cast('double').alias('pos_tkt_trx_g17_g3m'),
												col('pos_tkt_trx_g18_g3m').cast('double').alias('pos_tkt_trx_g18_g3m'),
												col('pos_tkt_trx_g23_g3m').cast('double').alias('pos_tkt_trx_g23_g3m'),
												col('pos_tkt_trx_g25_g3m').cast('double').alias('pos_tkt_trx_g25_g3m'),
												col('pos_tkt_trx_g28_g3m').cast('double').alias('pos_tkt_trx_g28_g3m'),
												col('pos_tkt_trx_g31_g3m').cast('double').alias('pos_tkt_trx_g31_g3m'),
												col('pos_tkt_trx_g34_g3m').cast('double').alias('pos_tkt_trx_g34_g3m'),
												col('pos_tkt_trx_g35_g3m').cast('double').alias('pos_tkt_trx_g35_g3m'),
												col('pos_tkt_trx_g39_g3m').cast('double').alias('pos_tkt_trx_g39_g3m'),
												col('pos_tkt_trx_g43_g3m').cast('double').alias('pos_tkt_trx_g43_g3m'),
												col('pos_tkt_trx_g45_g3m').cast('double').alias('pos_tkt_trx_g45_g3m'),
												col('pos_tkt_trx_g46_g3m').cast('double').alias('pos_tkt_trx_g46_g3m'),
												col('pos_ctd_trx_ig01_g3m').cast('double').alias('pos_ctd_trx_ig01_g3m'),
												col('pos_ctd_trx_ig02_g3m').cast('double').alias('pos_ctd_trx_ig02_g3m'),
												col('pos_ctd_trx_ig03_g3m').cast('double').alias('pos_ctd_trx_ig03_g3m'),
												col('pos_ctd_trx_ig04_g3m').cast('double').alias('pos_ctd_trx_ig04_g3m'),
												col('pos_ctd_trx_ig05_g3m').cast('double').alias('pos_ctd_trx_ig05_g3m'),
												col('pos_ctd_trx_ig06_g3m').cast('double').alias('pos_ctd_trx_ig06_g3m'),
												col('pos_ctd_trx_ig07_g3m').cast('double').alias('pos_ctd_trx_ig07_g3m'),
												col('pos_ctd_trx_ig08_g3m').cast('double').alias('pos_ctd_trx_ig08_g3m'),
												col('pos_ctd_trx_g01_g3m').cast('double').alias('pos_ctd_trx_g01_g3m'),
												col('pos_ctd_trx_g04_g3m').cast('double').alias('pos_ctd_trx_g04_g3m'),
												col('pos_ctd_trx_g05_g3m').cast('double').alias('pos_ctd_trx_g05_g3m'),
												col('pos_ctd_trx_g06_g3m').cast('double').alias('pos_ctd_trx_g06_g3m'),
												col('pos_ctd_trx_g16_g3m').cast('double').alias('pos_ctd_trx_g16_g3m'),
												col('pos_ctd_trx_g17_g3m').cast('double').alias('pos_ctd_trx_g17_g3m'),
												col('pos_ctd_trx_g18_g3m').cast('double').alias('pos_ctd_trx_g18_g3m'),
												col('pos_ctd_trx_g23_g3m').cast('double').alias('pos_ctd_trx_g23_g3m'),
												col('pos_ctd_trx_g25_g3m').cast('double').alias('pos_ctd_trx_g25_g3m'),
												col('pos_ctd_trx_g28_g3m').cast('double').alias('pos_ctd_trx_g28_g3m'),
												col('pos_ctd_trx_g31_g3m').cast('double').alias('pos_ctd_trx_g31_g3m'),
												col('pos_ctd_trx_g34_g3m').cast('double').alias('pos_ctd_trx_g34_g3m'),
												col('pos_ctd_trx_g35_g3m').cast('double').alias('pos_ctd_trx_g35_g3m'),
												col('pos_ctd_trx_g39_g3m').cast('double').alias('pos_ctd_trx_g39_g3m'),
												col('pos_ctd_trx_g43_g3m').cast('double').alias('pos_ctd_trx_g43_g3m'),
												col('pos_ctd_trx_g45_g3m').cast('double').alias('pos_ctd_trx_g45_g3m'),
												col('pos_ctd_trx_g46_g3m').cast('double').alias('pos_ctd_trx_g46_g3m'),
												col('pos_mto_trx_ig01_g1m').cast('double').alias('pos_mto_trx_ig01_g1m'),
												col('pos_mto_trx_ig02_g1m').cast('double').alias('pos_mto_trx_ig02_g1m'),
												col('pos_mto_trx_ig03_g1m').cast('double').alias('pos_mto_trx_ig03_g1m'),
												col('pos_mto_trx_ig04_g1m').cast('double').alias('pos_mto_trx_ig04_g1m'),
												col('pos_mto_trx_ig05_g1m').cast('double').alias('pos_mto_trx_ig05_g1m'),
												col('pos_mto_trx_ig06_g1m').cast('double').alias('pos_mto_trx_ig06_g1m'),
												col('pos_mto_trx_ig07_g1m').cast('double').alias('pos_mto_trx_ig07_g1m'),
												col('pos_mto_trx_ig08_g1m').cast('double').alias('pos_mto_trx_ig08_g1m'),
												col('pos_mto_trx_g01_g1m').cast('double').alias('pos_mto_trx_g01_g1m'),
												col('pos_mto_trx_g04_g1m').cast('double').alias('pos_mto_trx_g04_g1m'),
												col('pos_mto_trx_g05_g1m').cast('double').alias('pos_mto_trx_g05_g1m'),
												col('pos_mto_trx_g06_g1m').cast('double').alias('pos_mto_trx_g06_g1m'),
												col('pos_mto_trx_g16_g1m').cast('double').alias('pos_mto_trx_g16_g1m'),
												col('pos_mto_trx_g17_g1m').cast('double').alias('pos_mto_trx_g17_g1m'),
												col('pos_mto_trx_g18_g1m').cast('double').alias('pos_mto_trx_g18_g1m'),
												col('pos_mto_trx_g23_g1m').cast('double').alias('pos_mto_trx_g23_g1m'),
												col('pos_mto_trx_g25_g1m').cast('double').alias('pos_mto_trx_g25_g1m'),
												col('pos_mto_trx_g28_g1m').cast('double').alias('pos_mto_trx_g28_g1m'),
												col('pos_mto_trx_g31_g1m').cast('double').alias('pos_mto_trx_g31_g1m'),
												col('pos_mto_trx_g34_g1m').cast('double').alias('pos_mto_trx_g34_g1m'),
												col('pos_mto_trx_g35_g1m').cast('double').alias('pos_mto_trx_g35_g1m'),
												col('pos_mto_trx_g39_g1m').cast('double').alias('pos_mto_trx_g39_g1m'),
												col('pos_mto_trx_g43_g1m').cast('double').alias('pos_mto_trx_g43_g1m'),
												col('pos_mto_trx_g45_g1m').cast('double').alias('pos_mto_trx_g45_g1m'),
												col('pos_mto_trx_g46_g1m').cast('double').alias('pos_mto_trx_g46_g1m'),
												col('pos_tkt_trx_ig01_g1m').cast('double').alias('pos_tkt_trx_ig01_g1m'),
												col('pos_tkt_trx_ig02_g1m').cast('double').alias('pos_tkt_trx_ig02_g1m'),
												col('pos_tkt_trx_ig03_g1m').cast('double').alias('pos_tkt_trx_ig03_g1m'),
												col('pos_tkt_trx_ig04_g1m').cast('double').alias('pos_tkt_trx_ig04_g1m'),
												col('pos_tkt_trx_ig05_g1m').cast('double').alias('pos_tkt_trx_ig05_g1m'),
												col('pos_tkt_trx_ig06_g1m').cast('double').alias('pos_tkt_trx_ig06_g1m'),
												col('pos_tkt_trx_ig07_g1m').cast('double').alias('pos_tkt_trx_ig07_g1m'),
												col('pos_tkt_trx_ig08_g1m').cast('double').alias('pos_tkt_trx_ig08_g1m'),
												col('pos_tkt_trx_g01_g1m').cast('double').alias('pos_tkt_trx_g01_g1m'),
												col('pos_tkt_trx_g04_g1m').cast('double').alias('pos_tkt_trx_g04_g1m'),
												col('pos_tkt_trx_g05_g1m').cast('double').alias('pos_tkt_trx_g05_g1m'),
												col('pos_tkt_trx_g06_g1m').cast('double').alias('pos_tkt_trx_g06_g1m'),
												col('pos_tkt_trx_g16_g1m').cast('double').alias('pos_tkt_trx_g16_g1m'),
												col('pos_tkt_trx_g17_g1m').cast('double').alias('pos_tkt_trx_g17_g1m'),
												col('pos_tkt_trx_g18_g1m').cast('double').alias('pos_tkt_trx_g18_g1m'),
												col('pos_tkt_trx_g23_g1m').cast('double').alias('pos_tkt_trx_g23_g1m'),
												col('pos_tkt_trx_g25_g1m').cast('double').alias('pos_tkt_trx_g25_g1m'),
												col('pos_tkt_trx_g28_g1m').cast('double').alias('pos_tkt_trx_g28_g1m'),
												col('pos_tkt_trx_g31_g1m').cast('double').alias('pos_tkt_trx_g31_g1m'),
												col('pos_tkt_trx_g34_g1m').cast('double').alias('pos_tkt_trx_g34_g1m'),
												col('pos_tkt_trx_g35_g1m').cast('double').alias('pos_tkt_trx_g35_g1m'),
												col('pos_tkt_trx_g39_g1m').cast('double').alias('pos_tkt_trx_g39_g1m'),
												col('pos_tkt_trx_g43_g1m').cast('double').alias('pos_tkt_trx_g43_g1m'),
												col('pos_tkt_trx_g45_g1m').cast('double').alias('pos_tkt_trx_g45_g1m'),
												col('pos_tkt_trx_g46_g1m').cast('double').alias('pos_tkt_trx_g46_g1m'),
												col('pos_ctd_trx_ig01_g1m').cast('double').alias('pos_ctd_trx_ig01_g1m'),
												col('pos_ctd_trx_ig02_g1m').cast('double').alias('pos_ctd_trx_ig02_g1m'),
												col('pos_ctd_trx_ig03_g1m').cast('double').alias('pos_ctd_trx_ig03_g1m'),
												col('pos_ctd_trx_ig04_g1m').cast('double').alias('pos_ctd_trx_ig04_g1m'),
												col('pos_ctd_trx_ig05_g1m').cast('double').alias('pos_ctd_trx_ig05_g1m'),
												col('pos_ctd_trx_ig06_g1m').cast('double').alias('pos_ctd_trx_ig06_g1m'),
												col('pos_ctd_trx_ig07_g1m').cast('double').alias('pos_ctd_trx_ig07_g1m'),
												col('pos_ctd_trx_ig08_g1m').cast('double').alias('pos_ctd_trx_ig08_g1m'),
												col('pos_ctd_trx_g01_g1m').cast('double').alias('pos_ctd_trx_g01_g1m'),
												col('pos_ctd_trx_g04_g1m').cast('double').alias('pos_ctd_trx_g04_g1m'),
												col('pos_ctd_trx_g05_g1m').cast('double').alias('pos_ctd_trx_g05_g1m'),
												col('pos_ctd_trx_g06_g1m').cast('double').alias('pos_ctd_trx_g06_g1m'),
												col('pos_ctd_trx_g16_g1m').cast('double').alias('pos_ctd_trx_g16_g1m'),
												col('pos_ctd_trx_g17_g1m').cast('double').alias('pos_ctd_trx_g17_g1m'),
												col('pos_ctd_trx_g18_g1m').cast('double').alias('pos_ctd_trx_g18_g1m'),
												col('pos_ctd_trx_g23_g1m').cast('double').alias('pos_ctd_trx_g23_g1m'),
												col('pos_ctd_trx_g25_g1m').cast('double').alias('pos_ctd_trx_g25_g1m'),
												col('pos_ctd_trx_g28_g1m').cast('double').alias('pos_ctd_trx_g28_g1m'),
												col('pos_ctd_trx_g31_g1m').cast('double').alias('pos_ctd_trx_g31_g1m'),
												col('pos_ctd_trx_g34_g1m').cast('double').alias('pos_ctd_trx_g34_g1m'),
												col('pos_ctd_trx_g35_g1m').cast('double').alias('pos_ctd_trx_g35_g1m'),
												col('pos_ctd_trx_g39_g1m').cast('double').alias('pos_ctd_trx_g39_g1m'),
												col('pos_ctd_trx_g43_g1m').cast('double').alias('pos_ctd_trx_g43_g1m'),
												col('pos_ctd_trx_g45_g1m').cast('double').alias('pos_ctd_trx_g45_g1m'),
												col('pos_ctd_trx_g46_g1m').cast('double').alias('pos_ctd_trx_g46_g1m'),
												col('fecrutina').cast('date').alias('fecrutina'),
												col('fecactualizacionregistro').cast('timestamp').alias('fecactualizacionregistro'),
												col('codmes').cast('int').alias('codmes'))

    dfMatrizVarTransaccionPosMacrogiro.unpersist()
    del dfMatrizVarTransaccionPosMacrogiro

    # OPTIMIZACIÓN: Repartition antes de escribir para balancear particiones
    # Esto balancea las particiones según la columna de partición Delta
    input_df = input_df.repartition(CONS_PARTITION_DELTA_NAME)

    write_delta(input_df,VAL_DESTINO_NAME, CONS_PARTITION_DELTA_NAME)

# COMMAND ----------

###
 # Método principal de ejecución
 #
 # @return {void}
 ##
def main():

    #Leemos la tabla de parametros de fecha inicio y fecha fin para el proceso
    mesInicio, mesFin = spark.sql("""
                                  SELECT CAST(CAST(CODMESINICIO AS INT) AS STRING), CAST(CAST(CODMESFIN AS INT) AS STRING) FROM {var:esquemaTabla}.{var:parametroVariables}
                                  """.\
                                  replace("{var:esquemaTabla}", PRM_ESQUEMA_TABLA).\
                                  replace("{var:parametroVariables}", PRM_TABLA_PARAMETROMATRIZVARIABLES)
                                 ).take(1)[0][0:2]
                                 
    #Retorna un Dataframe Pandas iterable con una distancia entre fecha inicio y fecha fin de 12 meses para cada registro del dataframe 
    totalMeses = funciones.calculoDfAnios(mesInicio,mesFin)

    if mesInicio == mesFin:
        codMesProceso = funciones.calcularCodmes(PRM_FECHA_RUTINA)
        totalMeses = funciones.calculoDfAnios(codMesProceso, codMesProceso)
    
    carpetaDetConceptoClienteSegundaTranspuesta = "detconceptocliente_"+PRM_TABLE_NAME.lower()
    carpetaClientePrimeraTranspuesta = "cli_"+PRM_TABLA_PRIMERATRANSPUESTA.lower()+"_"+CONS_GRUPO.lower()
    
    #Recorremos el dataframe pandas totalMeses según sea la cantidad de registros 
    for index,row in  totalMeses.iterrows():
    
        #Leemos el valor de la fila codMes
        codMes = str(row['codMes'])
        
        #Leemos el valor de la fila mes11atras
        codMes12Atras = str(row['mes11atras'])
        
        funciones.extraccionInfoBaseCliente(codMes, PRM_ESQUEMA_TABLA, PRM_CARPETA_RAIZ_DE_PROYECTO, carpetaDetConceptoClienteSegundaTranspuesta, spark)
         
        #Se extrae información de doce meses de la primera transpuesta
        dfInfo12Meses = extraccionInformacion12Meses(codMes, codMes12Atras, carpetaDetConceptoClienteSegundaTranspuesta, carpetaClientePrimeraTranspuesta)
        
        #Se procesa la logica de la segunda transpuesta
        dfMatrizVarTransaccionPosMacrogiro = agruparInformacionMesAnalisis(dfInfo12Meses)
        
        #Se borra la carpeta temporal en hdfs 
        rutas_a_eliminar = [PRM_CARPETA_RAIZ_DE_PROYECTO+"/temp/"+carpetaClientePrimeraTranspuesta,
                            PRM_CARPETA_RAIZ_DE_PROYECTO+"/temp/"+carpetaDetConceptoClienteSegundaTranspuesta]

        if len(rutas_a_eliminar) > 0:
            objeto.cleanPaths(rutas_a_eliminar, flag_ejecucion_cuarentena=FLAG_EJECUCION_CUARENTENA)
        else:
            print("No tiene rutas a eliminar")
        

# COMMAND ----------

###
 # @section Ejecución
 ##

# Inicio Proceso main()
if __name__ == "__main__":
    main()
 
#Retorna el valor en 1 si el proceso termina de manera satisfactoria.
# dbutils.notebook.exit(1)