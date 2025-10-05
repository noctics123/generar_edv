# coding: utf-8

# ||********************************************************************************************************
# || PROYECTO           : MATRIZ DE VARIABLES - CONCEPTOS
# || NOMBRE             : funciones.py
# || TABLA DESTINO      : NA
# || TABLAS FUENTES     : NA
# || OBJETIVO           : Funciones que permitirán ser reautilizadas en los demás script
# || TIPO               : PY
# || REPROCESABLE       : SI
# || SCHEDULER          : NA
# || JOB                : NA
# ||
# || VERSION     DESARROLLADOR                    PROVEEDOR      AT RESPONSABLE       FECHA          DESCRIPCION
# || 1.1         DIEGO GUERRA CRUZADO             Everis         ARTURO ROJAS         06/02/19       Desarrollo de las funciones que serán utilizadas en los demás script
# ||             GIULIANA PABLO ALEJANDRO
# || 2.1         ANTHONY CAPCHA MONTESINOS        Everis         ARTURO ROJAS         02/03/20       Se agregan nuevas funciones
# || 2.2         MARCOS IRVING MERA SANCHEZ       Everis         ARTURO ROJAS         29/04/20       Se agregan nuevas funciones
# || 3.0         HECTOR HUARANCCA NUÑEZ            BCP           RODRIGO ROJAS        20/11/23       Migracion  a Databricks
# || 3.1         LUIS RODRIGUEZ GARCES            BLUETAB        RODRIGO ROJAS        21/05/24       Agregación función windowAggregate_opt(retorna un df) y función calcularCodmesProceso(retorna codmes)
# || 3.2         LUIS RODRIGUEZ GARCES            BLUETAB        RODRIGO ROJAS        05/06/24       Paralelización de función windowAggregateOpt(retorna un df) y adición de parámetro a función calcularCodmesProceso(retorna codmes)
# *************************************************************************************************************

###
 # @section Import
 ##

import pyspark
from pyspark.sql import SparkSession
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import current_date
from pyspark.sql.functions import col, concat, rpad, from_unixtime, when, months_between,last_day,to_date,format_number,format_string,add_months,unix_timestamp,substring,concat_ws,lit
from pyspark.sql import functions as F
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType,DoubleType
import itertools
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import subprocess
import json
import pandas as pd
import codecs
import pytz

###
 # @section Constantes
 ##
CONS_FORMATO_FECHA_MES_ANIO = "%Y%m"
CONS_FORMATO_DE_ESCRITURA_EN_DISCO = "delta"
CONS_MODO_DE_ESCRITURA = "overwrite"
CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN = ["CODMES","CODUNICOCLI","CODINTERNOCOMPUTACIONAL"]
CONS_CAMPOS_IDENTIFICADOR_PARA_JOIN_SEGUNDA_TRANSP = ["CODMESANALISIS","CODUNICOCLI","CODINTERNOCOMPUTACIONAL"]
CONS_VALOR_HASH_PUNTO = "0b61241d7c17bcbb1baee7094d14b7c451efecc7ffcbd92598a0f13d313cc9ebc2a07e61f007baf58fbf94ff9a8695bdd5cae7ce03bbf1e94e93613a00f25f21"

CONS_FORMATO_FECHA_DIA_HORA = '%a %b %d %H:%M:%S %Y'

###
 # @section Funciones
 ##

###
 # Resta cierta cantidad de meses a una fecha.
 #
 # @param cantMeses {int} Cantidad de meses a restar.
 # @param mesInicio {string} Fecha a restar.
 # @return {string} Retorna una fecha en formato YYYYMM.
 ##
def restarMeses(cantMeses, mesInicio):
  fechaFin = ''

  #Aplicamos el formato a la fecha de inicio y lo convertimos a un tipo date
  fechaInicial = datetime.strptime(mesInicio, CONS_FORMATO_FECHA_MES_ANIO)

  #Restamos los meses
  resultadoFecha = fechaInicial - relativedelta(months = cantMeses)

  #Convertimos el date a string y lo formateamos YYYYMM
  fechaFin = str(resultadoFecha).replace("-","")[:6]

  return fechaFin

###
 # Obtiene un Dataframe Pandas de meses entre un intervalo de fechas a iterar.
 #
 # @param mesInicio {string} Fecha inicial de analisis.
 # @param mesFin {string} Fecha final de analisis.
 # @return {Dataframe Pandas} Dataframe de fechas a iterar
 ##
def calculoDfAnios(mesInicio, mesFin):
   arrFechas = []
   arrFechas12Atras = []
   cantidadDeMesesQueSeRestan = 1
   arrFechas.append(mesFin)
   mesDeAnalisis = mesFin

   while mesInicio != mesDeAnalisis:
       mesDeAnalisis = restarMeses(cantidadDeMesesQueSeRestan, mesDeAnalisis)
       arrFechas.append(mesDeAnalisis)
   
   for mesInicio in arrFechas:
       cantMeses = 11
       fechaAtras = restarMeses(cantMeses, mesInicio)
       arrFechas12Atras.append(fechaAtras)
   
   totalMeses = pd.DataFrame({'codMes': arrFechas, 'mes11atras': arrFechas12Atras})

   return totalMeses
###
 # Obtiene una lista de meses entre un intervalo de fechas a iterar.
 #
 # @param mesInicio {string} Fecha inicial de analisis.
 # @param mesFin {string} Fecha final de analisis.
 # @return {array[string]} Listado de fechas a calcular en formato YYYYMM.
 ##

def calculoDfMeses(mesInicio, mesFin):
  arrFechas = []
  cantidadDeMesesQueSeRestan = 1
  arrFechas.append(mesFin)
  mesDeAnalisis = mesFin

  while(mesInicio != mesDeAnalisis):
    mesDeAnalisis = restarMeses(cantidadDeMesesQueSeRestan, mesDeAnalisis)
    arrFechas.append(mesDeAnalisis)

  return arrFechas

###
 # Renombra columnas de un dataframe.
 #
 # @param df {string} nombre del dataframe.
 # @param columns {string} lista de columnas a renombrar
 ##

def rename_columns(df, columns):
    if isinstance(columns, dict):
        return df.select(*[F.col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
    else:
        raise ValueError("Ejemplo de datos a ingresar {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}...")

###
 # Crea el formato necesario para extraer la información de un determinado grupo mediante una consulta sql
 #
 # @param dfGrupo {dataframe} Dataframe con los nombres de las columnas que corresponden a un determinado grupo
 # @return {String}
 ##
def crearCadenaGrupo(dfGrupo):
    #Calcula la longitud de filas de un dataframe
    longDataFrame = len(dfGrupo.index)
    cadena = ''
    resul = ''
    iterador = 1

    for indiceFila, nombColumna in dfGrupo.iterrows():
        if(iterador < longDataFrame):
            cadena = resul + nombColumna['nomconcepto'].upper() + ','
        else:
            cadena = resul + nombColumna['nomconcepto'].upper()
        resul = cadena
        iterador += 1

    return cadena

###
 # Devuelve un Dataframe Spark con las columnas del subgrupo pasado por parámetro para el concepto RCC
 #
 # @param grupo {String} Nombre del subgrupo del concepto RCC
 # @param spark {Spark Object}
 # @return {Dataframe Spark}
 ##
def crearDfGrupo(grupo, spark, PRM_ESQUEMA_TABLA, PRM_SPARK_TABLA_PARAM_GRUPO,PRM_TABLA_PRIMERATRANSPUESTA):
    # EXCLUDE_ANALYZER: DataFrame.toPandas
    dfGrupo = spark.sql("select nomconcepto from " + PRM_ESQUEMA_TABLA+"."+PRM_SPARK_TABLA_PARAM_GRUPO+ " where nomgrupo='"+ grupo +"' and nomtablaconcepto = '"+PRM_TABLA_PRIMERATRANSPUESTA+"'").toPandas()

    cadenaGrupo = crearCadenaGrupo(dfGrupo)

    return cadenaGrupo
###
 # Función que procesa las columnas generadas dinamicamente
 #
 # @param df {Dataframe Spark} Datos de los últimos 12 meses
 # @param partitionCols {array[string]} Columnas por las que se realizá las agrupaciones
 # @param orderCol {string} Columna por la que se ordena
 # @param aggregations {array[Row(,,,)]} Funciones que se aplican a las columnas
 # @param codMes {string} Mes de analisis
 # @param tablaTmp {string} Nombre de la carpeta temporal
 # @param flag {int}
 # @param PRM_CARPETA_RAIZ_DE_PROYECTO {string} Ruta principal del proyecto
 # @return {void}
 ##
def windowAggregate(df, partitionCols, orderCol, aggregations, codMes, tablaTmp, flag, PRM_CARPETA_RAIZ_DE_PROYECTO, flg_null=1):
   from pyspark.sql import functions as F
   from pyspark.sql.functions import col
   from pyspark.sql.window import Window
   
   # Castear columnas espec铆ficas a string
   columnas_a_castear = ['CODMESANALISIS', 'CODMES']  
   for colName in columnas_a_castear:
       df = df.select(*[col(column_name).cast("string").alias(column_name) if column_name == colName else col(column_name) for column_name in df.columns]) 

   # Si no es un arreglo de "columnas", se convierte en uno
   if isinstance(partitionCols, str):
       partitionCols = [partitionCols]

   # Producto cartesiano entre las particiones de agrupaci贸n y los per铆odos (meses) existentes
   dfPartitions = df.select(partitionCols).dropDuplicates()
   dfPeriods = df.select(orderCol).dropDuplicates()
   dfCJ = dfPartitions.crossJoin(F.broadcast(dfPeriods))
   
   # Castear columnas en el DataFrame cruzado
   for colName in columnas_a_castear:
       dfCJ = dfCJ.select(*[col(column_name).cast("string").alias(column_name) if column_name == colName else col(column_name) for column_name in dfCJ.columns])

   # Datos enriquecidos con las particiones y los per铆odos (meses)
   dfFin = df.join(dfCJ, on=partitionCols + [orderCol], how="right_outer")
   dfFin.createOrReplaceTempView("dfFin")

   # Definici贸n de la forma en c贸mo se particiona y ordenan los resultados
   windowSpecBase = Window.partitionBy(partitionCols).orderBy(orderCol)

   # Iteramos todas las agregaciones que se realizan en los datos
   templateOperation = "F.{funcName}(col('{baseCol}')).over(windowSpecBase.rowsBetween({startRow},{endRow}))"
   templateSelect = "col('OPERACIONES').getItem({index}).alias('{newColname}')"
   templateWithColumn = ".alias('dfgen{numberWithColumn}').withColumn('OPERACIONES', F.split(F.concat({operationBody}), ',')).select('dfgen{numberWithColumn}.*', {selectBody})"
   templateQuery = "dfFin{withColumnBody}"

   maxNumberOperations = 100
   numberWithColumn = 0
   numberColumnsAdded = 0
   numberIteration = 0
   index = 0
   aggregations = list(aggregations)
   sizeAggregation = len(aggregations)
   operationBodyArray = []
   selectBodyArray = []
   withColumnBodyArray = []

   for aggregation in aggregations:
       # Solo se calcula los montos para la condici贸n donde el CODMESANALISIS es igual al CODMES
       if dfFin.filter(col('CODMESANALISIS') == col('CODMES')).count() > 0:
           # Nombre de la columna con la que se trabaja
           baseCol = aggregation[0]

           # Tipo de agregaci贸n que se realiza
           aggFun = aggregation[1]
           funcName = aggFun.__name__

           # Fila inicial
           startRow = aggregation[2]

           # Fila final
           endRow = aggregation[3]

           # Obtenemos el nombre que recibe la nueva columna
           if flag == 1:
               newColname = getNewColName2(baseCol, funcName, startRow, endRow)
           else:
               newColname = getNewColName(baseCol, funcName, startRow, endRow)

           # Aumentamos el n煤mero de columnas agregadas
           numberColumnsAdded += 1

           # Agregamos la operaci贸n al array
           operationBodyArray.append(
               templateOperation
               .replace("{funcName}", str(funcName))
               .replace("{baseCol}", str(baseCol))
               .replace("{startRow}", str(startRow))
               .replace("{endRow}", str(endRow))
           )

           # Agregamos el select al array
           selectBodyArray.append(
               templateSelect
               .replace("{index}", str(index))
               .replace("{newColname}", str(newColname))
           )

           # Aumentamos el 铆ndice del "select"
           index += 1

       # Aumentamos el n煤mero de iteraciones
       numberIteration += 1

       # Verificamos si no hay m谩s operaciones que agregar O si hemos llegado a las "maxNumberOperations"
       if (numberIteration == sizeAggregation) or (index == maxNumberOperations):
           numberWithColumn += 1

           # Generamos el "withColumnBody"
           withColumnBodyArray.append(
               templateWithColumn
               .replace("{operationBody}", str(getOperationBody(operationBodyArray)))
               .replace("{selectBody}", str(getSelectBody(selectBodyArray)))
               .replace("{numberWithColumn}", str(numberWithColumn))
           )

           # Reiniciamos los arrays
           operationBodyArray = []
           selectBodyArray = []

           # Reiniciamos el 铆ndice de "select"
           index = 0
   
   if flg_null == 1:
       dfFin = dfFin.na.fill(0)

   queryG1 = templateQuery.replace("{withColumnBody}", str(getWithColumnBody(withColumnBodyArray[0:6])))
   dfAgrupadoG1 = eval(queryG1)
   dfAgrupadoG1Filter = dfAgrupadoG1.filter((col('CODMESANALISIS') == col('CODMES')))
   
   queryG2 = templateQuery.replace("{withColumnBody}", str(getWithColumnBody(withColumnBodyArray[6:])))
   dfAgrupadoG2 = eval(queryG2)
   dfAgrupadoG2Filter = dfAgrupadoG2.filter((col('CODMESANALISIS') == col('CODMES')))
   
   columnsDfCoreG1 = getColumnListDfCore(dfAgrupadoG1Filter.columns, df.columns)
   dfAgrupadoG1FilterCore = dfAgrupadoG1Filter.select(columnsDfCoreG1)
   
   columnsDfCoreG2 = getColumnListDfCore(dfAgrupadoG2Filter.columns, df.columns)
   dfAgrupadoG2FilterCore = dfAgrupadoG2Filter.select(columnsDfCoreG2)
   
   dfCoreFinal = dfAgrupadoG1FilterCore.join(dfAgrupadoG2FilterCore, ['CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL'])
   dfCoreFinal.write.format(CONS_FORMATO_DE_ESCRITURA_EN_DISCO).mode(CONS_MODO_DE_ESCRITURA).save(f"{PRM_CARPETA_RAIZ_DE_PROYECTO}/temp/{tablaTmp}/CODMES={codMes}")
###
 # Obtiene el listado de columnas generados dinámicamente em el proceso windowAggregate()
 #
 # @param columnsDfAgrupado {List} Listado de columnas de la primera transpuesta y segunda transpuesta
 # @param columnsDfOriginal {List} Listado de columnas de la primera transpuesta
 # @return {List} Listado de columnas generados dinamicamente en el proceso windowAggregate().
 ##
def getColumnListDfCore(columnsDfAgrupado, columnsDfOriginal):

  columnsDfAgrupado = pd.DataFrame(columnsDfAgrupado, columns=['column'])

  columnsDfAgrupado['column'] = columnsDfAgrupado['column'].str.upper()

  columnsDfcore = columnsDfAgrupado[~ columnsDfAgrupado['column'].isin(columnsDfOriginal)]

  columnsDfcoreList = ["CODMESANALISIS", "CODMES", "CODINTERNOCOMPUTACIONAL", "CODUNICOCLI"]

  columnsDfcoreList.extend(columnsDfcore['column'].tolist())

  return columnsDfcoreList

###
 # Obtiene el nombre de la nueva columna que agregamos
 #
 # @param baseCol {string} Nombre de la columna con la que hará los cálculos
 # @param funcName {string} Nombre de la función que aplicará a la columna
 # @param startRow {int} Columna inicio
 # @param endRow {int} Columna fin
 ##
def getNewColName(baseCol, funcName, startRow, endRow):
    newColname = ''

    if ('avg' in funcName):
        if (abs(startRow) == 0):
            newColname = baseCol + '_U1M'
        elif (abs(endRow) == 6):
            newColname=baseCol + '_PRM_P' + str(abs(endRow)) + 'M'
        elif (abs(endRow) == 3):
            newColname = baseCol + '_PRM_P' + str(abs(endRow)) + 'M'
        elif (abs(endRow) == 1):
            newColname = baseCol + '_P' + str(abs(endRow)) + 'M'
        elif (abs(startRow) == 11):
            newColname=baseCol + '_PRM_U' + str(abs(startRow) + 1)
        else:
            newColname = baseCol + '_PRM_U' + str(abs(startRow) + 1) + 'M'
    elif ('min' in funcName):
        if (str(abs(startRow) + 1) == '12'):
            newColname = baseCol + '_MIN_U' + str(abs(startRow) + 1)
        else:
            newColname=baseCol + '_MIN_U' + str(abs(startRow) + 1) + 'M'
    elif ('max' in funcName):
        if (str(abs(startRow) + 1) == '12'):
            newColname=baseCol + '_MAX_U' + str(abs(startRow) + 1)
        else:
            newColname = baseCol + '_MAX_U' + str(abs(startRow) + 1) + 'M'
    elif ('sum' in funcName):
        if (abs(endRow) == 6):
            newColname=baseCol + '_SUM_P' + str(abs(endRow)) + 'M'
        elif (abs(endRow) == 3):
            newColname=baseCol + '_SUM_P' + str(abs(endRow)) + 'M'
        elif (abs(endRow) == 1):
            newColname = baseCol + '_P' + str(abs(endRow)) + 'M'
        elif (str(abs(startRow) + 1) == '12'):
            newColname = baseCol + '_SUM_U' + str(abs(startRow) + 1)
            #newColname = baseCol + '_X' + str(abs(startRow) + 1)+'_U' + str(abs(startRow) + 1)
        else:
            newColname = baseCol + '_SUM_U' + str(abs(startRow) + 1) + 'M'
    return newColname

###
 # Obtiene el nombre de la nueva columna que agregamos
 #
 # @param baseCol {string} Nombre de la columna con la que hará los cálculos
 # @param funcName {string} Nombre de la función que aplicará a la columna
 # @param startRow {int} Columna inicio
 # @param endRow {int} Columna fin
 ##
def getNewColName2(baseCol, funcName, startRow, endRow):
    newColname = ''

    if ('avg' in funcName):
        if (abs(startRow) == 0):
            newColname = baseCol + '_U1M'
        elif (abs(endRow) == 6):
            newColname = baseCol + '_PRM_P' + str(abs(endRow)) + 'M'
        elif (abs(endRow) == 3):
            newColname = baseCol + '_PRM_P' + str(abs(endRow)) + 'M'
        elif (abs(endRow) == 1):
            newColname = baseCol + '_P' + str(abs(endRow)) + 'M'
        elif (abs(startRow) == 11):
            newColname = baseCol + '_PRM_U' + str(abs(startRow) + 1)
        else:
            newColname = baseCol + '_PRM_U' + str(abs(startRow) + 1)+'M'
    elif ('min' in funcName):
        if (str(abs(startRow) + 1) == '12'):
            newColname = baseCol + '_MIN_U' + str(abs(startRow) + 1)
        else:
            newColname = baseCol + '_MIN_U' + str(abs(startRow) + 1) + 'M'
    elif ('max' in funcName):
        if (str(abs(startRow) + 1) == '12'):
            newColname = baseCol + '_MAX_U' + str(abs(startRow) + 1)
        else:
            newColname = baseCol + '_MAX_U' + str(abs(startRow) + 1) + 'M'
    elif ('sum' in funcName):
        if (str(abs(startRow) + 1) == '12'):
            newColname = baseCol + '_X' + str(abs(startRow) + 1) + '_U' + str(abs(startRow) + 1)
        else:
            newColname = baseCol + '_X' + str(abs(startRow) + 1) + 'M_U' + str(abs(startRow) + 1) + 'M'

    return newColname

###
 # Crea el formato para el cálculo de las frecuencias de un determinado mes de las columnas del dataframe enviadas como parámetro
 #
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta
 # @param valorMes {int} Mes a considerar para el cálculo de la frecuencia
 # @return {String} Retorna el formato final para el cálculo de las frecuencia de un determinado mes para una lista de columnas de un dataframe
 ##
def formatoFrecuencia(listaColumnDataFrame, valorMes):
    cadena = ""

    for columna in listaColumnDataFrame:
        mes = str(abs(valorMes) + 1)
        if (mes == '12'):
          sufijoColumn = columna + '_FRQ_U' + mes
        else:
          sufijoColumn = columna + '_FRQ_U' + mes + 'M'
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " COUNT( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<=" + mes + " AND NVL(V0."+ columna +",0)>0 THEN V0."+ columna +" ELSE NULL END ) "+ sufijoColumn

    return cadena

###
 # Calcula las frecuencias de las columnas del dataframe enviadas como parámetro según un mes determinado.
 #
 # @param listaFrecuencias {List} Lista los meses en los que se realizará el cálculo de la frecuencia
 # @param columnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de la frecuencia
 # @param spark {Spark Object}
 # @return {Dataframe Spark} Devuelve un dataframe con el cálculo de la frecuencia para los meses y columnas enviadas como parámetro
 ##
def calcularFrecuencias(listaFrecuencias, columnDataFrame, spark):
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(v1.CODMESANALISIS, 0, 4),'-',SUBSTRING(v1.CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(v1.CODMES, 0, 4),'-',SUBSTRING(v1.CODMES, 5, 2))))+1 MES_ORDEN, v1.CODMESANALISIS, v1.CODMES, v1.CODUNICOCLI, v1.CODINTERNOCOMPUTACIONAL, "+ ','.join(columnDataFrame) +" FROM dfViewInfoMesesSql v1")

    dfMesOrden.createOrReplaceTempView("dfViewMesOrdenFreq")

    resultFormatoFreq = ""

    for frec in listaFrecuencias:
      resultFormatoFreq = resultFormatoFreq + formatoFrecuencia(columnDataFrame, frec)

    dfFreqMesOrden = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+resultFormatoFreq[:-1]+" FROM  dfViewMesOrdenFreq V0  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")

    #spark.sql("DROP VIEW IF EXISTS dfViewMesOrdenFreq")

    return dfFreqMesOrden

###
 # Obtendrá el nombre de las columnas que contenga el elemento de búsqueda enviado como parámetro
 #
 # @param columnDataFrame {List} Lista las columnas de un dataframe en donde se realizará la búsqueda de la palabra enviada como parametro
 # @param búsqueda {string} Palabra a ser buscada entre las columnas de un dataframe
 # @return {String}  Retornará en una cadena los nombres de las columnas de un dataframe que contengan el elemento de búsqueda enviado como parametro
 ##
def extraccionColumnasFlg(columnDataFrame, busqueda):
  dfFinal = pd.DataFrame(columnDataFrame, columns = ["col_names"])
  columnas = dfFinal[dfFinal["col_names"].str.contains(busqueda)]["col_names"].tolist()
  resultFinal = "','".join(columnas)

  return resultFinal

###
 # Extrae en una lista los nombres de las columnas de un dataframe que coincidan en su parte final con la palabra enviada como parámetro
 #
 # @param columnDataFrame {List} Lista las columnas de un data frame en donde se realizará la búsqueda de la palabra enviada como parámetro
 # @param busqueda {string} Palabra a ser buscada entre las columnas de un dataframe
 # @return {String} Retornará en una cadena los nombres de las columnas de un dataframe que coincidan en su parte final con la palabra enviada como parámetro
 ##
def extraccionColumnas(columnDataFrame, busqueda):
    dfFinal = pd.DataFrame(columnDataFrame, columns = ["col_names"])
    columnas = dfFinal[dfFinal["col_names"].str.endswith(busqueda)]["col_names"].tolist()
    resultFinal = ",".join(columnas)

    return resultFinal

###
 #  Crea el formato requerido para realizar el casteo de las columnas del dataframe según el tipo de dato solicitado
 #
 # @param listaColumnDataFrame {List} Lista de columnas a ser casteadas
 # @param tipoDato {List} Tipo de dato al cual serán casteadas las columnas solicitadas
 # @return {String} Formato requerido para el casteo de las columnas del dataframe según el tipo de dato solicitado
 ##
def casteoColumns(listaColumnDataFrame, tipoDato):
  iterador = 1
  resultadoCasteo = ''

  for columna in listaColumnDataFrame[4:]:
      columnCasteada = "col('"+ columna +"').cast('"+ tipoDato +"')"
      if iterador < len(listaColumnDataFrame[4:]):
          columnCasteada = columnCasteada + ","
      resultadoCasteo = resultadoCasteo + columnCasteada
      iterador += 1

  resultadoCasteo = "col('CODMESANALISIS').cast('string'),col('CODMES').cast('string'),col('CODUNICOCLI').cast('string'),col('CODINTERNOCOMPUTACIONAL').cast('string'),"+resultadoCasteo

  return resultadoCasteo

 # Crea el formato para el cálculo de Recencia de las columnas de un dataframe enviadas como parámetro
 #
 # @param listaColumnDataFrame {List} Listado de las columnas en las que se realizará el cálculo de la Recencia
 # @return {String} Formato requerido para el cálculo de Recencia de columnas del dataframe
 ##
def formatoRecencia(listaColumnDataFrame):
    iterador = 1
    resultRec = ''

    for columna in listaColumnDataFrame:
        columnRec = columna + '_REC'
        if iterador < len(listaColumnDataFrame):
            columnRec = columnRec + ","
        cadenaRec = "NVL(MIN(CASE WHEN  NVL("+columna+",0) > 0  THEN  MES_ORDEN ELSE NULL END),0) "+ columnRec
        resultRec = resultRec + cadenaRec
        iterador += 1

    return resultRec

###
 # Cálculo de Recencia de columnas de dataframe
 #
 # @param listaColumnDataFrame {List} Listado de las columnas en las que se realizará el cálculo de la Recencia
 # @param spark {Spark Object}
 # @return {Dataframe} Retorna un dataframe con el que contendrá el cálculo de Recencia
 ##
def calculoRecencia(listaColumnDataFrame, spark):
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(v1.CODMESANALISIS, 0, 4),'-',SUBSTRING(v1.CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(v1.CODMES, 0, 4),'-',SUBSTRING(v1.CODMES, 5, 2))))+1 MES_ORDEN ,v1.* FROM dfViewInfoMesesSql v1")

    dfMesOrden.createOrReplaceTempView("dfViewMesOrden")

    resFormatoRec = formatoRecencia(listaColumnDataFrame)

    dfRecMesOrden = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ resFormatoRec +" FROM  dfViewMesOrden  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")

    #spark.sql("DROP VIEW IF EXISTS dfViewMesOrden")

    return dfRecMesOrden

###
 # Crea el formato requerido para realizar el cálculo de los promedios para las variables de tipo antigüedad de las columnas del dataframe enviadas como parámetro
 #
 # @param listaColumnDataFrame {List} Listado de las columnas en las que se realizará el cálculo del promedio de las variables de tipo antigüedad
 # @param indice {int} Mes en que se calculará el promedio de las variables de tipo antigüedad
 # @return {String} Formato para el cálculo de las variables de tipo antigüedad
 ##
def formatoPromAnt(listaColumnDataFrame, indice):
    cadena = ""

    for columna in listaColumnDataFrame:
      if(indice == 0):
        sufijoColumn = columna +'_U1M'
      elif(indice == 11):
        sufijoColumn = columna + '_PRM_U' + str(indice + 1)
      else:
        sufijoColumn = columna + '_PRM_U' + str(indice + 1) + "M"
      sufijoColumn = sufijoColumn + ","
      nMes = str(abs(indice)+ 1)
      cadena = cadena + " AVG( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ nMes +" THEN V0."+ columna +" ELSE NULL END) "+ sufijoColumn

    return cadena

###
 # Cálculo las variables de tipo Antigüedad (ANT)
 #
 # @param mesTip {List} Contiene las lista de los meses en que se calularán las variables de tipo antigüedad
 # @param listaColumnDataFrame {List} Contiene la lista de las columnas en las que se calcularán las variables de tipo antigüedad
 # @param spark {Spark Object}
 # @return {Dataframe Spark} Retorna un nuevo dataframe con el cálculo de las variables de tipo antigüedad
 ##
def calcularAnt(listaMeses, listaColumnDataFrame, spark):
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(v1.CODMESANALISIS, 0, 4),'-',SUBSTRING(v1.CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(v1.CODMES, 0, 4),'-',SUBSTRING(v1.CODMES, 5, 2))))+1 MES_ORDEN, v1.CODMESANALISIS, v1.CODMES, v1.CODUNICOCLI, v1.CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql v1")

    dfMesOrden.createOrReplaceTempView("dfViewMesOrdenFreq")

    resultFormatoAnt = ""

    for mes in listaMeses:
      resultFormatoAnt = resultFormatoAnt + formatoPromAnt(listaColumnDataFrame, mes)

    dfAntMesOrden = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL," +resultFormatoAnt[:-1]+ " FROM  dfViewMesOrdenFreq V0  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")

    #spark.catalog.dropTempView('dfViewMesOrdenFreq')
    #spark.sql("DROP VIEW IF EXISTS dfViewMesOrdenFreq")

    return dfAntMesOrden

###
 # Crea el formato para el calculo de los mínimos de los últimos meses de las columnas de tipo clasificación
 #
 # @param listaColumnDataFrame {List} Lista las columnas de tipo clasificación que serán consideradas
 # @param indice {int} Indica el mes en que se realizará el calculo de los mínimos de las columnas de tipo clasificación
 # @return {String} Retorna el formato a considerar para el caálculo de los mínimos las columnas de tipo clasificación
 ##
def formatoMinTip(listaColumnDataFrame, indice):
    cadena = ""

    if(indice <= 11 and indice > 0):
       for columna in listaColumnDataFrame:
        if(indice == 11):
           sufijoColumn = columna + '_MIN_U' + str(indice + 1)
        else:
           sufijoColumn = columna + '_MIN_U' + str(indice + 1) + "M"
        sufijoColumn = sufijoColumn + ","
        nMes = str(abs(indice) + 1)
        cadena = cadena + " MIN( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ nMes +" THEN V0."+ columna +" ELSE NULL END ) "+ sufijoColumn

    return cadena

###
 # Crea el formato para el cálculo de los máximos de los últimos meses de las columnas de tipo clasificación
 #
 # @param listaColumnDataFrame {List} Lista las columnas de tipo clasificación que serán consideradas
 # @param indice {int} Indica el mes en que se realizará el cálculo de los máximos de las columnas de tipo clasificación
 # @return {String} Retorna el formato a considerar para el cálculo de los máximos de las columnas de tipo clasificación
 ##
def formatoMaxUltTip(listaColumnDataFrame, indice):
    cadena = ""

    for columna in listaColumnDataFrame:
      if(indice == 0):
        sufijoColumn = columna + '_U1M'
      elif(indice == 11):
        sufijoColumn = columna + '_MAX_U' + str(indice + 1)
      else:
        sufijoColumn = columna + '_MAX_U' + str(indice + 1) + "M"
      sufijoColumn = sufijoColumn + ","
      valorMes = str(abs(indice) + 1)
      cadena = cadena + " MAX( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ valorMes +" THEN V0."+ columna +" ELSE NULL END ) "+ sufijoColumn

    return cadena

###
 # Calcula los máximos y mínimos de los últimos meses de las columnas de tipo clasificación
 #
 # @param listaMeses {List} Lista los meses en que se calcularán los máximos y mínimos de las columnas de tipo clasificación
 # @param listaColumnDataFrame {List} Lista las columnas en las que se calcularán los máximos y miínimos de las columnas de tipo clasificación
 # @param spark {Spark Object}
 # @return {Dataframe Spark} Retorna un dataframe con los cálculos de los mínimos y máximos de los últimos meses enviados como parámetro
 ##
def calcularTipMinMaxUlt(listaMeses, listaColumnDataFrame, spark):
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(v1.CODMESANALISIS, 0, 4),'-',SUBSTRING(v1.CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(v1.CODMES, 0, 4),'-',SUBSTRING(v1.CODMES, 5, 2))))+1 MES_ORDEN, v1.CODMESANALISIS, v1.CODMES, v1.CODUNICOCLI, v1.CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql v1")

    dfMesOrden.createOrReplaceTempView("dfViewMesOrdenTip")

    respFormatoTip = ""

    for mes in listaMeses:
      respFormatoTip = respFormatoTip + formatoMaxUltTip(listaColumnDataFrame, mes) + formatoMinTip(listaColumnDataFrame, mes)

    dfFreqMesOrden = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ respFormatoTip[:-1] +" FROM  dfViewMesOrdenTip V0  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")

    #spark.catalog.dropTempView('dfViewMesOrdenTip')
    #spark.sql("DROP VIEW IF EXISTS dfViewMesOrdenTip")

    return dfFreqMesOrden

###
 # Crea el cuerpo de la sentencia DATAFRAME que se ejecuta
 # La sentencia creada tiene la estructura:
 #
 # F.{funcName}(col('{baseCol}')).over(windowSpecBase.rowsBetween({startRow},{endRow})),
 # F.{funcName}(col('{baseCol}')).over(windowSpecBase.rowsBetween({startRow},{endRow})),
 # F.{funcName}(col('{baseCol}')).over(windowSpecBase.rowsBetween({startRow},{endRow})),
 #
 # @param operationBodyArray {List} Elementos que conforman la sentencia.
 # @return {String}
 ##
def getOperationBody(operationBodyArray):
  operationBody = ""

  templateBase = "{operationBodyElement}, lit(','),"
  templateLast = "{operationBodyElement}"

  i = 0
  sizeArray = len(operationBodyArray)
  for operationBodyElement in operationBodyArray:
    i = i + 1

    #Seleccionamos la plantilla
    template = ""
    if(i == sizeArray):
      template = templateLast
    else:
      template = templateBase

    #Agregamos el elemento
    operationBody = operationBody + template.replace("{operationBodyElement}", str(operationBodyElement))

  return operationBody

###
 # Crea el select de la sentencia DATAFRAME que se ejecuta
 # La sentencia creada tiene la estructura:
 #
 # col('OPERACIONES').getItem({index}).alias('{newColname}'),
 # col('OPERACIONES').getItem({index}).alias('{newColname}'),
 # col('OPERACIONES').getItem({index}).alias('{newColname}'),
 #
 # @param selectBodyArray {List} Elementos que conforman la sentencia.
 # @return {String}
 ##
def getSelectBody(selectBodyArray):
  selectBody = ""

  templateBase = "{selectBodyElement},"
  templateLast = "{selectBodyElement}"

  i = 0
  sizeArray = len(selectBodyArray)
  for selectBodyElement in selectBodyArray:
    i = i + 1

    #Seleccionamos la plantilla
    template = ""
    if(i == sizeArray):
      template = templateLast
    else:
      template = templateBase

    #Agregamos el elemento
    selectBody = selectBody + template.replace("{selectBodyElement}", str(selectBodyElement))

  return selectBody

###
 # Crea la sentencia WITHCOLUMN para partir el array de resultados del campo "OPERACIONES" en múltiples columnas del DATAFRAME que se ejecuta
 # La sentencia creada tiene la estructura:
 #
 # .alias('dfgen{numberWithColumn}').withColumn('OPERACIONES', F.split(F.concat({operationBody}), ',')).select('dfgen{numberWithColumn}.*', {selectBody}),
 #
 # @param withColumnBodyArray {List} Elementos que conforman la sentencia.
 # @return {String}
 ##
def getWithColumnBody(withColumnBodyArray):
  withColumnBody = ""

  templateBase = "{withColumnBodyElement}"
  templateLast = "{withColumnBodyElement}"

  i = 0
  sizeArray = len(withColumnBodyArray)
  for withColumnBodyElement in withColumnBodyArray:
    i = i + 1

    #Seleccionamos la plantilla
    template = ""
    if(i == sizeArray):
      template = templateLast
    else:
      template = templateBase

    #Agregamos el elemento
    withColumnBody = withColumnBody + template.replace("{withColumnBodyElement}", str(withColumnBodyElement))

  return withColumnBody

###
 # Ejecuta el comando de borrado de carpetas temporales de HDFS
 #
 # @param args_list {string}  Comandos hdfs
 # @return {string} El resultado de la ejecución de sentencias hdfs
 ##
#def run_cmd(args_list):
#  print('Running system command: {0}'.format(' '.join(args_list)))
#  proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#  s_output, s_err = proc.communicate()
#  s_return =  proc.returncode

#  return s_return, s_output, s_err

###
 # Lista los parámetros de configuración de spark
 #
 # @param rutaArchivo {string} Nombre de la ruta donde de encuentra el archivo de configuración de recursos Spark en un .JSON
 # @return {String} Parámetros de configuración de recursos Spark definidas en un .JSON
 ##
#def cargaConfiguracionDeRecursosSpark(rutaArchivo):
 #   diccionario = ''
 #   data = cargaConfiguracionDeParametrosProceso(rutaArchivo)
 #   spark = "SparkSession.builder"
 #   dataIterar = json.dumps(data).replace("{","").replace("}","").replace("'","").split(",")
 #   iterador = 1
 #   for diccionario in dataIterar:
 #       spark = spark + ".config("+ diccionario.split(":")[0] + "," + diccionario.split(":")[1]+")"
 #       if(iterador == len(dataIterar)):
 #           spark = spark +".enableHiveSupport().getOrCreate()"
 #       iterador += 1
 #   return spark

###
 # Lee los parametros de configuracion del proceso
 #
 # @param rutaArchivo {string} Nombre de la ruta donde de encuentra el archivo de configuración de parametros del proceso en un .JSON
 # @return {String} Contenido del archivo .JSON
 ##
#def cargaConfiguracionDeParametrosProceso(rutaArchivo):
#    data = json.load(codecs.open(rutaArchivo, 'r', 'utf-8-sig'))
#    return data

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
def extraccionInfoBaseCliente(nCodMes, esquemaTabla, rutaCarpetaProyecto, carpetaDetConceptoClienteSegundaTranspuesta, spark):
   query = f"""
       SELECT
           CODMES,
           CODUNICOCLI CODUNICOCLI_LAST,
           MAX(CODINTERNOCOMPUTACIONALCONCEPTO) CODINTERNOCOMPUTACIONAL_LAST
       FROM
           {esquemaTabla}.HM_DETCONCEPTOCLIENTE
       WHERE
           CODMES = {nCodMes}
       GROUP BY CODMES, CODUNICOCLI
   """
   
   df_cliente_cuc = spark.sql(query)
   
   output_path = f"{rutaCarpetaProyecto}/temp/{carpetaDetConceptoClienteSegundaTranspuesta}/CODMES={nCodMes}"
   df_cliente_cuc.write.format(CONS_FORMATO_DE_ESCRITURA_EN_DISCO).mode(CONS_MODO_DE_ESCRITURA).save(output_path)


###
 # Crea para cada una de las columnas del dataframe con las operaciones de agregación (SUM,MAX,MIN)
 #
 # @param dfColumnList {Lista} Columnas del dataframe sobre las cuales se concatenarán las operaciones de agregación
 # @return {Lista} Lista en la cual se asociará para cada una de las columnas del dataframe una respectiva operación de agregación (SUM,MIN,MAX)

def calcularOperadoresColumnasDf(dfColumnList):
   dfColumnListOperations = []
   
   for dfColumn in dfColumnList:
       dfColumn_upper = dfColumn.upper()
       operation = 'sum'
       
       if any(pattern in dfColumn_upper for pattern in ['_FLG_', '_ANTMAX_', '_TIP_', '_CTDDIA_']):
           operation = 'max'
       elif '_ANTMIN_' in dfColumn_upper:
           operation = 'min'
       
       dfColumnListOperations.append(f'{operation}({dfColumn}) as {dfColumn}')
   
   return dfColumnListOperations

###
 # Obtiene un Dataframe Pandas de meses entre un intervalo de fechas a iterar.
 #
 # @param mesInicio {string} Fecha inicial de analisis.
 # @param mesFin {string} Fecha final de analisis.
 # @return {Dataframe Pandas} Dataframe de fechas a iterar 
 ##
def calculoDfOtrosAnios(mesInicio, mesFin, cantMeses):
    arrFechas = []
    arrFechas24Atras = []
    cantidadDeMesesQueSeRestan = 1
    arrFechas.append(mesFin)
    mesDeAnalisis = mesFin
  
    while(mesInicio != mesDeAnalisis):
        mesDeAnalisis = restarMeses(cantidadDeMesesQueSeRestan, mesDeAnalisis)
        arrFechas.append(mesDeAnalisis)
    for mesInicio in arrFechas:
        fechaAtras = restarMeses(cantMeses,mesInicio)
        arrFechas24Atras.append(fechaAtras)
    totalMeses = pd.DataFrame({'codMes':arrFechas,'mes24atras':arrFechas24Atras})
   
    return totalMeses

###
# Crea el formato para el cálculo de las medias de un determinado mes de las columnas del dataframe enviadas como parámetro 
 # 
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta
# @param mes {int} Mes a considerar para el cálculo de las medias
# @return {String} Retorna el formato final para el cálculo de las medias de un determinado mes para una lista de columnas de un dataframe
## 
def formatoMedias(listaColumnDataFrame, mes, prefijoFinal, pre, pre12m):
    cadena = ""

    listprevmes = {'3': ('4','6'), '6': ('7','12'),'1':('2','2')}
    pmes,fmes = listprevmes.get(mes,("",""))
    
        
    for columna in listaColumnDataFrame:
        if ((pre in 'U') and (mes== '12') and pre12m == 0):
            sufijoColumn = columna + prefijoFinal + '_' + pre + str(mes)
        else:
            sufijoColumn = columna + prefijoFinal + '_' + pre + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        
        
        if pre in 'U':
          cadena = cadena + " AVG( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +" THEN "+ columna +" ELSE NULL END) "+ sufijoColumn
        else:
          cadena = cadena + " AVG( CASE WHEN MES_ORDEN>="+ str(pmes) +" AND MES_ORDEN<="+ str(fmes) +" THEN "+ columna +" ELSE NULL END) "+ sufijoColumn
    
    return cadena

###
# Calcula las medias de las columnas del dataframe enviadas como parámetro según un mes determinado. 
 # 
 # @param listaMeses {List} Lista los meses en los que se realizará el cálculo de las medias
# @param listaColumnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de las medias
# @param spark {Spark Object} 
 # @return {Dataframe Spark} Devuelve un dataframe con el cálculo de los máximos para los meses y columnas enviadas como parámetro
##           
def calcularMedias(listaMeses, listaColumnDataFrame, Prefijo, spark, ult = 0, pre12m=0):
    spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesOrdenMed")
    
    resultFormato = ""
    
    prefijoFinal = {0:'U'}
    pre = prefijoFinal.get (ult, 'P')
     
    for mes in listaMeses:
      prefijoFinal = ''
      
      if mes != '1':
        prefijoFinal = '_'+Prefijo
      resultFormato = resultFormato + formatoMedias(listaColumnDataFrame, mes, prefijoFinal, pre, pre12m)
      dfPromMesOrden = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL," +resultFormato[:-1]+ " FROM  dfViewMesOrdenMed GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.sql("DROP VIEW IF EXISTS dfViewMesOrdenMed")
        
    return dfPromMesOrden


###
 # Crea el formato para el cálculo de los mínimos de un determinado mes de las columnas del dataframe enviadas como parámetro 
 # 
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta
 # @param mes {int} Mes a considerar para el cálculo de los mínimos
 # @return {String} Retorna el formato final para el cálculo de los mínimos de un determinado mes para una lista de columnas de un dataframe
 ##  
def formatoMinimos(listaColumnDataFrame, mes):
    cadena = ""
    for columna in listaColumnDataFrame:
        sufijoColumn = columna + '_MIN_U' + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " MIN( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +" THEN "+ columna +" ELSE NULL END ) "+ sufijoColumn    
        
    return cadena


###
 # Calcula las medias de las columnas del dataframe enviadas como parámetro según un mes determinado. 
 # 
 # @param listaMeses {List} Lista los meses en los que se realizará el cálculo de los mínimos
 # @param listaColumnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de los mínimos
 # @param spark {Spark Object} 
 # @return {Dataframe Spark} Devuelve un dataframe con el cálculo de los mínimos para los meses y columnas enviadas como parámetro
 ##   
def calcularMinimos(listaMeses, listaColumnDataFrame, spark):
   spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, " + ','.join(listaColumnDataFrame) + " FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesMin") 
   
   respFormatoMin = ""
   for mes in listaMeses:
       respFormatoMin += formatoMinimos(listaColumnDataFrame, mes)
   
   dfMesOrdenMin = spark.sql(f"SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,{respFormatoMin[:-1]} FROM dfViewMesMin GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
   #spark.sql("DROP VIEW IF EXISTS dfViewMesMin")
   return dfMesOrdenMin


###
 # Obtiene una lista con las columnas de un dataframe que coincidan con el sufio a buscar 
 # 
 # @param columnas {List} Lista de las columnas de un datframe
 # @param sufijo {string} Sufijo a buscar en las columnas de una datframe
 # @return {List} Devuelve una lista con las columnas de un dataframe que coincidan con el sufio a buscar
 ## 
def buscarColumnas(columnas, sufijo):
   return [columna for columna in columnas if sufijo in columna]

###
 # Renombra las columnas de un datframe enviado como parametro 
 # 
 # @param df {Dataframe} Dataframe donde se realizara el renombre de las columnas
 # @param columnas {List} Lista de las columnas de un datframe
 # @param prefijoAnt {string} Sufijo a ser modificado 
 # @param prefijoPost {string} Sufijo nuevo
 # @return {Dataframe} Devuelve un datframe con las columnas renombradas
 ## 
def cambiarNombreColumna(df, columnas , prefijoAnt, prefijoPost):
  for columna in columnas:
    df = df.withColumnRenamed(columna,columna.replace(prefijoAnt,prefijoPost))
  return df

###
 # Crea el formato para el cálculo de las sumas de un determinado mes de las columnas del dataframe enviadas como parámetro 
 #
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta
 # @param mes {int} Mes a considerar para el cálculo de las sumas
 # @return {String} Retorna el formato final para el cálculo de las sumas de un determinado mes para una lista de columnas de un dataframe
## 
def formatoSuma(listaColumnDataFrame, mes, prefijoFinal, pre, pre12m):
    cadena = ""

    listprevmes = {'3': ('4','6'), '6': ('7','12'),'1':('2','2')}
    pmes,fmes = listprevmes.get(mes,("",""))
    
        
    for columna in listaColumnDataFrame:
        if ((pre in 'U') and (mes== '12') and pre12m == 0):
            sufijoColumn = columna + prefijoFinal + '_' + pre + str(mes)
        else:
            sufijoColumn = columna + prefijoFinal + '_' + pre + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        
        
        if pre in 'U':
          cadena = cadena + " SUM( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +" THEN "+ columna +" ELSE NULL END) "+ sufijoColumn
        else:
          cadena = cadena + " SUM( CASE WHEN MES_ORDEN>="+ str(pmes) +" AND MES_ORDEN<="+ str(fmes) +" THEN "+ columna +" ELSE NULL END) "+ sufijoColumn
    
    return cadena

###
 # Calcula las sumas de las columnas del dataframe enviadas como parámetro según un mes determinado. 
 #
 # @param listaMeses {List} Lista los meses en los que se realizará el cálculo de las sumas
 # @param listaColumnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de las sumas
 # @param spark {Spark Object}
 # @return {Dataframe Spark} Devuelve un dataframe con el cálculo de los máximos para los meses y columnas enviadas como parámetro
##           
def calcularSuma(listaMeses, listaColumnDataFrame, Prefijo, spark, ult=0, pre12m=0):
    spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, " + ','.join(listaColumnDataFrame) + " FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesOrdenSum")
    
    resultFormato = ""
    prefijoFinal = {0: 'U'}
    pre = prefijoFinal.get(ult, 'P')
    
    try:
        for mes in listaMeses:
            prefijoFinal = ''
            if mes != '1':
                prefijoFinal = '_' + Prefijo
            resultFormato += formatoSuma(listaColumnDataFrame, mes, prefijoFinal, pre, pre12m)
        # Ejecutar la consulta DENTRO del try y DESPUÉS de construir resultFormato
        dfSumMesOrden = spark.sql(
            f"SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,{resultFormato[:-1]} "
            "FROM dfViewMesOrdenSum "
            "GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL"
        )
    except Exception:
        raise
    return dfSumMesOrden



###
# Crea el formato para el cálculo de los maximos de un determinado mes de las columnas del dataframe enviadas como parámetro
#
# @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta
# @param mes {int} Mes a considerar para el cálculo de los máximos
# @return {String} Retorna el formato final para el cálculo de los máximos de un determinado mes para una lista de columnas de un dataframe
##
def formatoMaximos(listaColumnDataFrame, mes, prefijoFinal, pre, pre12m):
    cadena = ""
    
    listprevmes = {'3': ('4','6'), '6': ('7','12'),'1':('2','2')}
    pmes,fmes = listprevmes.get(mes,("",""))
    
    for columna in listaColumnDataFrame:
        if ((pre in 'U') and (mes== '12') and pre12m == 0):
            sufijoColumn = columna + prefijoFinal + '_' + pre + str(mes)
        else:
            sufijoColumn = columna + prefijoFinal + '_' + pre + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        
        if pre in 'U':
          cadena = cadena + " MAX( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +" THEN "+ columna +" ELSE NULL END) "+ sufijoColumn
        else:
          cadena = cadena + " MAX( CASE WHEN MES_ORDEN>="+ str(pmes) +" AND MES_ORDEN<="+ str(fmes) +" THEN "+ columna +" ELSE NULL END) "+ sufijoColumn

    return cadena

###
# Calcula los máximos de las columnas del dataframe enviadas como parámetro según un mes determinado.
#
# @param listaMeses {List} Lista los meses en los que se realizará el cálculo de los máximos
# @param listaColumnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de los máximos
# @param spark {Spark Object}
# @return {Dataframe Spark} Devuelve un dataframe con el cálculo de los máximos para los meses y columnas enviadas como parámetro
##
def calcularMaximos(listaMeses, listaColumnDataFrame, Prefijo, spark, ult = 0, pre12m=0):
    spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, " + ','.join(listaColumnDataFrame) + " FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesMaxTotal")
    respFormatoMax = ""
    
    prefijoFinal = {0:'U'}
    pre = prefijoFinal.get (ult, 'P')
    
    for mes in listaMeses:
      prefijoFinal = ''
      
      if mes != '1':
        prefijoFinal = '_'+Prefijo
      
      respFormatoMax = respFormatoMax + formatoMaximos(listaColumnDataFrame, mes, prefijoFinal, pre, pre12m)
      dfMesOrdenMaxTotal = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL," + respFormatoMax[:-1] + " FROM  dfViewMesMaxTotal  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.sql("DROP VIEW IF EXISTS dfViewMesMaxTotal")
    
    return dfMesOrdenMaxTotal

###
# Crea el formato para el cálculo de los maximos o minimos de un determinado mes o orden del mes de las columnas del dataframe enviadas como parámetro que cumplan una condicion
#
# @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta
# @param mes {int} Mes a considerar para el cálculo de los máximos
# @param L1 {int} Limite inferior de la columna del dataframe enviada
# @param flg_codmes {int} Indica si se considerará codmes o mes_orden para el cálculo de los máximos
# @param flgminomax {int} Indica si se considerará el minimo o el maximo
# @return {String} Retorna el formato final para el cálculo de los máximos o minimos de un determinado mes para una lista de columnas de un dataframe
##
def formatoMinoMax(listaColumnDataFrame, mes, L1=0, flg_codmes=0, flgminomax=0):
    cadena = ""
    var_ret = ""
    estad = ""
    if flg_codmes==0: var_ret = "CODMES"
    else: var_ret = "MES_ORDEN"
    if flgminomax==0:
      estad = "MIN"
      if flg_codmes==0:
        ppre = "_PRI_"
      else:
        ppre = "_DS_U_"
    else:
      estad = "MAX"
      if flg_codmes==0:
        ppre = "_U_"
      else:
        ppre = "_DS_PRI_"

    for columna in listaColumnDataFrame:
        sufijoColumn = columna + ppre +str(L1)+'_U' + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " " +estad+"( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +" AND "+ columna +" > "+ str(L1) +" THEN "+ var_ret +" ELSE NULL END ) "+ sufijoColumn

    return cadena

###
# cálculo de los maximos o minimos de un determinado mes o orden del mes de las columnas del dataframe enviadas como parámetro que cumplan una condicion
#
# @param listaMeses {List} Lista los meses en los que se realizará el cálculo de los máximos
# @param listaColumnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de los máximos
# @param spark {Spark Object}
# @param L1 {int} Limite inferior de la columna del dataframe enviada
# @param flg_codmes {int} Indica si se considerará codmes o mes_orden para el cálculo de los máximos
# @param flgminomax {int} Indica si se considerará el minimo o el maximo
# @return {Dataframe Spark} Devuelve un dataframe con el cálculo de los máximos o minimos para los meses y columnas enviadas como parámetro
##
def calcularMinoMax(listaMeses, listaColumnDataFrame, spark, L1, flg_codmes=0,flgminomax=0):
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesMinoMax")
    respFormatoMinoMax = ""
    for mes in listaMeses:
        respFormatoMinoMax = respFormatoMinoMax + formatoMinoMax(listaColumnDataFrame, mes, L1, flg_codmes, flgminomax)
    dfMesOrdenMinoMax = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ respFormatoMinoMax[:-1] +" FROM  dfViewMesMinoMax  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.sql("DROP VIEW IF EXISTS dfViewMesMinoMax")
    return dfMesOrdenMinoMax

###
# Crea el formato para el cálculo de los maximos o minimos de un determinado mes o orden del mes de las columnas del dataframe enviadas como parámetro que cumplan una condicion
#
# @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta
# @param mes {int} Mes a considerar para el cálculo de los máximos
# @param L1 {int} Limite inferior (variable) de la columna del dataframe enviada
# @param flg_codmes {int} Indica si se considerará codmes o mes_orden para el cálculo de los máximos
# @param flgminomax {int} Indica si se considerará el minimo o el maximo
# @return {String} Retorna el formato final para el cálculo de los máximos o minimos de un determinado mes para una lista de columnas de un dataframe
##
def formatoMinoMaxv2(listaColumnDataFrame, mes, L1, flg_codmes=0, flgminomax=0):
    cadena = ""
    var_ret = ""
    estad = ""
    if flg_codmes==0: var_ret = "CODMES"
    else: var_ret = "MES_ORDEN"
    if flgminomax==0:
      estad = "MIN"
      if flg_codmes==0: ppre = "_PRI3_"
      else: ppre = "_DS_U_"
    else:
      estad = "MAX"
      if flg_codmes==0: ppre = "_U3_"
      else: ppre = "_DS_PRI_"

    for columna in listaColumnDataFrame:
        sufijoColumn = columna + ppre + 'U' + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " " +estad+"( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +" AND "+ columna +" > "+ str(L1) +" THEN "+ var_ret +" ELSE NULL END ) "+ sufijoColumn

    return cadena

###
# cálculo de los maximos o minimos de un determinado mes o orden del mes de las columnas del dataframe enviadas como parámetro que cumplan una condicion
#
# @param listaMeses {List} Lista los meses en los que se realizará el cálculo de los máximos
# @param listaColumnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de los máximos
# @param spark {Spark Object}
# @param L1 {int} Limite inferior (variable) de la columna del dataframe enviada
# @param flg_codmes {int} Indica si se considerará codmes o mes_orden para el cálculo de los máximos
# @param flgminomax {int} Indica si se considerará el minimo o el maximo
# @return {Dataframe Spark} Devuelve un dataframe con el cálculo de los máximos o minimos para los meses y columnas enviadas como parámetro
##
def calcularMinoMaxv2(listaMeses, listaColumnDataFrame, spark, L1, flg_codmes=0,flgminomax=0):
    listaColumnDataFrame.append(L1)
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesMinoMax")
    listaColumnDataFrame.pop()
    respFormatoMinoMax = ""
    for mes in listaMeses:
        respFormatoMinoMax = respFormatoMinoMax + formatoMinoMaxv2(listaColumnDataFrame, mes, L1, flg_codmes, flgminomax)
    dfMesOrdenMinoMax = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ respFormatoMinoMax[:-1] +" FROM  dfViewMesMinoMax  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.sql("DROP VIEW IF EXISTS dfViewMesMinoMax")
    return dfMesOrdenMinoMax

###
# Crea el formato para el cálculo de los maximos o minimos de un determinado mes o orden del mes de las columnas del dataframe enviadas como parámetro que cumplan una condicion
#
# @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta
# @param mes {int} Mes a considerar para el cálculo de los máximos
# @param c0 {int} Columna auxiliar enviada para utilizar en la condicion
# @param L1 {int} Limite inferior de la columna del dataframe enviada
# @param L2 {int} Limite inferior de la columna auxiliar
# @param flg_codmes {int} Indica si se considerará codmes o mes_orden para el cálculo de los máximos
# @param flgminomax {int} Indica si se considerará el minimo o el maximo
# @return {String} Retorna el formato final para el cálculo de los máximos o minimos de un determinado mes para una lista de columnas de un dataframe
##
def formatoMinoMaxv3(listaColumnDataFrame, mes, c0, L1=0, L2=0, flg_codmes=0, flgminomax=0):
    cadena = ""
    var_ret = ""
    estad = ""
    if flg_codmes==0: var_ret = "CODMES"
    else: var_ret = "MES_ORDEN"
    if flgminomax==0:
      estad = "MIN"
      if flg_codmes==0: ppre = "_PRI1_"
      else: ppre = "_DS_U1_"
    else:
      estad = "MAX"
      if flg_codmes==0: ppre = "_U1_"
      else: ppre = "_DS_PRI1_"

    for columna in listaColumnDataFrame:
        sufijoColumn = columna + ppre +str(L1)+'_'+str(L2)+"_U"+str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " " +estad+"( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +" AND NVL("+ columna +",0) > "+ str(L1) +" AND NVL("+str(c0)+",0) > "+ str(L2) +" THEN "+ var_ret +" ELSE NULL END ) "+ sufijoColumn

    return cadena

###
# cálculo de los maximos o minimos de un determinado mes o orden del mes de las columnas del dataframe enviadas como parámetro que cumplan una condicion
#
# @param listaMeses {List} Lista los meses en los que se realizará el cálculo de los máximos
# @param listaColumnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de los máximos
# @param spark {Spark Object}
# @param c0 {int} Columna auxiliar enviada para utilizar en la condicion
# @param L1 {int} Limite inferior de la columna del dataframe enviada
# @param L2 {int} Limite inferior de la columna auxiliar
# @param flg_codmes {int} Indica si se considerará codmes o mes_orden para el cálculo de los máximos
# @param flgminomax {int} Indica si se considerará el minimo o el maximo
# @return {Dataframe Spark} Devuelve un dataframe con el cálculo de los máximos o minimos para los meses y columnas enviadas como parámetro
##
def calcularMinoMaxv3(listaMeses, listaColumnDataFrame, spark, c0, L1=0, L2=0, flg_codmes=0,flgminomax=0):
    listaColumnDataFrame.append(c0)
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesMinoMax")
    listaColumnDataFrame.pop()
    respFormatoMinoMax = ""
    for mes in listaMeses:
        respFormatoMinoMax = respFormatoMinoMax + formatoMinoMaxv3(listaColumnDataFrame, mes, c0, L1, L2, flg_codmes, flgminomax)
    dfMesOrdenMinoMax = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ respFormatoMinoMax[:-1] +" FROM  dfViewMesMinoMax  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.sql("DROP VIEW IF EXISTS dfViewMesMinoMax")
    return dfMesOrdenMinoMax

###
 # Crea el formato para el cálculo de las frecuencias de un determinado mes de las columnas del dataframe enviadas como parámetro que cumplan con algunas condiciones
 #
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta que se imputaran en cero, en caso sea missing
 # @param mes {int} Mes a considerar para el cálculo de la frecuencia
 # @param estad {string} Estadistico a considerar para el agrupamiento de la frecuencia
 # @param ppre {string} Prefijo del operador
 # @param L1 {int} Limite inferior de la columna enviada
 # @return {String} Retorna el formato final para el cálculo de las frecuencia de un determinado mes para una lista de columnas de un dataframe
 ##
def formatoFrq(listaColumnDataFrame, mes, estad, ppre, L1=0):
    cadena = ""
    for columna in listaColumnDataFrame:
        sufijoColumn = columna + ppre +str(L1)+'_U' + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " " +estad+"(CASE WHEN (MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +") THEN (CASE WHEN NVL("+columna+",0)>"+str(L1)+" THEN 1 WHEN "+columna+" IS NOT NULL THEN 0 ELSE NULL END) ELSE NULL END)"+ sufijoColumn
    return cadena

###
 # Calcula las frecuencias de las columnas del dataframe enviadas como parámetro según un mes determinado.
 #
 # @param listaFrecuencias {List} Lista los meses en los que se realizará el cálculo de la frecuencia
 # @param columnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de la frecuencia
 # @param spark {Spark Object}
 # @param estad {string} Estadistico a considerar para el agrupamiento de la frecuencia
 # @param ppre {string} Prefijo del operador
 # @param L1 {int} Limite inferior de la columna enviada luego de imputarla en cero, en caso sea missing
 # @return {Dataframe Spark} Devuelve un dataframe con el cálculo de la frecuencia para los meses y columnas enviadas como parámetro
 ##
def calcularFrq(listaMeses, listaColumnDataFrame, spark, estad, ppre, L1=0):
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesFrq")
    respFormatoFrq = ""
    for mes in listaMeses:
        respFormatoFrq = respFormatoFrq + formatoFrq(listaColumnDataFrame, mes, estad, ppre, L1)
    dfMesOrdenFrq = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ respFormatoFrq[:-1] +" FROM  dfViewMesFrq  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.catalog.dropTempView('dfViewMesFrq')
    #spark.sql("DROP VIEW IF EXISTS dfViewMesFrq")
    return dfMesOrdenFrq

###
 # Crea el formato para el cálculo de las frecuencias de un determinado mes de las columnas del dataframe enviadas como parámetro que cumplan con algunas condiciones
 #
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta, que se imputaran en cero, en caso sea missing
 # @param mes {int} Mes a considerar para el cálculo de la frecuencia
 # @param estad {string} Estadistico a considerar para el agrupamiento de la frecuencia
 # @param ppre {string} Prefijo del operador
 # @param c0 {int} Limite inferior (variable) de la columna enviada, que se imputaran en cero, en caso sea missing
 # @return {String} Retorna el formato final para el cálculo de las frecuencia de un determinado mes para una lista de columnas de un dataframe
 ##
def formatoFrqv2(listaColumnDataFrame, mes, estad, ppre, c0):
    cadena = ""
    for columna in listaColumnDataFrame:
        sufijoColumn = columna + ppre +'U' + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " " +estad+"(CASE WHEN (MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +") THEN (CASE WHEN NVL("+columna+",0)>NVL("+str(c0)+",0) THEN 1 WHEN "+columna+" IS NOT NULL AND "+c0+" IS NOT NULL THEN 0 ELSE NULL END) ELSE NULL END)"+ sufijoColumn
    return cadena

###
 # Calcula las frecuencias de las columnas del dataframe enviadas como parámetro según un mes determinado.
 #
 # @param listaFrecuencias {List} Lista los meses en los que se realizará el cálculo de la frecuencia
 # @param columnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de la frecuencia
 # @param spark {Spark Object}
 # @param estad {string} Estadistico a considerar para el agrupamiento de la frecuencia
 # @param ppre {string} Prefijo del operador
 # @param c0 {int} Limite inferior (variable) de la columna enviada luego de imputarla en cero, en caso sea missing
 # @return {Dataframe Spark} Devuelve un dataframe con el cálculo de la frecuencia para los meses y columnas enviadas como parámetro
 ##
def calcularFrqv2(listaMeses, listaColumnDataFrame, spark, estad, ppre, c0):
    listaColumnDataFrame.append(c0)
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesFrq")
    listaColumnDataFrame.pop()
    respFormatoFrq = ""
    for mes in listaMeses:
        respFormatoFrq = respFormatoFrq + formatoFrqv2(listaColumnDataFrame, mes, estad, ppre, c0)
    dfMesOrdenFrq = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ respFormatoFrq[:-1] +" FROM  dfViewMesFrq  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.sql("DROP VIEW IF EXISTS dfViewMesFrq")
    return dfMesOrdenFrq

###
 # Crea el formato para el cálculo de las frecuencias de un determinado mes de las columnas del dataframe enviadas como parámetro que cumplan con algunas condiciones
 #
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta, que se imputaran en cero, en caso sea missing
 # @param mes {int} Mes a considerar para el cálculo de la frecuencia
 # @param estad {string} Estadistico a considerar para el agrupamiento de la frecuencia
 # @param ppre {string} Prefijo del operador
 # @param c0 {int} Limite inferior (variable) de la columna enviada, que se imputaran en cero, en caso sea missing
 # @return {String} Retorna el formato final para el cálculo de las frecuencia de un determinado mes para una lista de columnas de un dataframe
 ##
def formatoFrqv3(listaColumnDataFrame, mes, estad, ppre, c0, L1=0, L2=0):
    cadena = ""
    for columna in listaColumnDataFrame:
        sufijoColumn = columna + ppre +str(L1)+"_"+str(L2)+ "_U" + str(mes) + "M"
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " " +estad+"(CASE WHEN (MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +") THEN (CASE WHEN NVL("+columna+",0)>"+str(L1)+" AND NVL("+str(c0)+",0)>"+str(L2)+" THEN 1 WHEN "+columna+" IS NOT NULL AND "+c0+" IS NOT NULL THEN 0 ELSE NULL END) ELSE NULL END)"+ sufijoColumn
    return cadena

###
 # Calcula las frecuencias de las columnas del dataframe enviadas como parámetro según un mes determinado.
 #
 # @param listaFrecuencias {List} Lista los meses en los que se realizará el cálculo de la frecuencia
 # @param columnDataFrame {List} Lista los nombres de las columnas del dataframe sobre las cuales se realizará el cálculo de la frecuencia
 # @param spark {Spark Object}
 # @param estad {string} Estadistico a considerar para el agrupamiento de la frecuencia
 # @param ppre {string} Prefijo del operador
 # @param c0 {int} Columna auxiliar
 # @param L1 {int} Limite inferior de la columna enviada
 # @param L2 {int} Limite inferior de la columna auxiliar
 # @return {Dataframe Spark} Devuelve un dataframe con el cálculo de la frecuencia para los meses y columnas enviadas como parámetro
 ##
def calcularFrqv3(listaMeses, listaColumnDataFrame, spark, estad, ppre, c0, L1=0, L2=0):
    listaColumnDataFrame.append(c0)
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesFrq")
    listaColumnDataFrame.pop()
    respFormatoFrq = ""
    for mes in listaMeses:
        respFormatoFrq = respFormatoFrq + formatoFrqv3(listaColumnDataFrame, mes, estad, ppre, c0, L1, L2)
    dfMesOrdenFrq = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ respFormatoFrq[:-1] +" FROM  dfViewMesFrq  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.sql("DROP VIEW IF EXISTS dfViewMesFrq")
    return dfMesOrdenFrq

  ############# new
  ###
 # Crea el formato para el cálculo del minimo mes de apertura
 #
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta, que se imputaran en cero, en caso sea missing
 # @param mes {int} Mes a considerar para el cálculo de la frecuencia
 # @param L1 {int} Limite inferior de la columna del dataframe enviada
 # @return {String} Retorna el formato final para el cálculo del minimo mes de apertura
 ##
def formatoMinApertura(listaColumnDataFrame, mes, L1=0):
    cadena = ""
    for columna in listaColumnDataFrame:
        sufijoColumn = "FATC_FEC_CODMES_AP_TC"
        sufijoColumn = sufijoColumn + ","
        cadena = cadena + " " +"MIN( CASE WHEN MES_ORDEN>=1 AND MES_ORDEN<="+ str(mes) +" AND "+ columna +" > "+ str(L1) +" THEN "+ columna +" ELSE 300001 END ) "+ sufijoColumn

    return cadena

  ###
 # cálculo del minimo mes de apertura
 #
 # @param listaColumnDataFrame {List} Listado de columnas de la primera transpuesta y segunda transpuesta, que se imputaran en cero, en caso sea missing
 # @param mes {int} Mes a considerar para el cálculo de la frecuencia
 # @param L1 {int} Limite inferior de la columna del dataframe enviada
 # @return {String} Retorna el cálculo del minimo mes de apertura
 ##
def calcularMinApertura(listaMeses, listaColumnDataFrame, spark, L1):
    dfMesOrden = spark.sql("SELECT (MONTHS_BETWEEN(CONCAT(SUBSTRING(CODMESANALISIS, 0, 4),'-',SUBSTRING(CODMESANALISIS, 5, 2)), CONCAT(SUBSTRING(CODMES, 0, 4),'-',SUBSTRING(CODMES, 5, 2))))+1 MES_ORDEN, CODMESANALISIS, CODMES, CODUNICOCLI, CODINTERNOCOMPUTACIONAL, "+ ','.join(listaColumnDataFrame) +" FROM dfViewInfoMesesSql").createOrReplaceTempView("dfViewMesMinoMax")
    respFormatoMinoMax = ""
    for mes in listaMeses:
        respFormatoMinoMax = respFormatoMinoMax + formatoMinApertura(listaColumnDataFrame, mes, L1)
    dfMesOrdenMinoMax = spark.sql("SELECT CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL,"+ respFormatoMinoMax[:-1] +" FROM  dfViewMesMinoMax  GROUP BY CODMESANALISIS,CODUNICOCLI,CODINTERNOCOMPUTACIONAL")
    #spark.sql("DROP VIEW IF EXISTS dfViewMesMinoMax")
    return dfMesOrdenMinoMax
 ###
 # Obtención de la fecha actual en dia, hora, minuto y segundos
 #
 # @return {String} Retorna la fecha actual en formato de dia, hora, minuto y segundos 
 ##
def obtenerFechaActual():
    horaActual = datetime.now(pytz.timezone('America/Lima'))
    cadenaFechaActual = horaActual.strftime(CONS_FORMATO_FECHA_DIA_HORA)
    fechaRegistro = datetime.strptime(cadenaFechaActual, CONS_FORMATO_FECHA_DIA_HORA)
    
    return fechaRegistro
    

###
 # Función que procesa las columnas generadas dinamicamente
 #
 # @param df {Dataframe Spark} Datos de los últimos 12 meses
 # @param partitionCols {array[string]} Columnas por las que se realizá las agrupaciones
 # @param orderCol {string} Columna por la que se ordena
 # @param aggregations {array[Row(,,,)]} Funciones que se aplican a las columnas
 # @param codMes {string} Mes de analisis
 # @param tablaTmp {string} Nombre de la carpeta temporal
 # @param flag {int}
 # @param PRM_CARPETA_RAIZ_DE_PROYECTO {string} Ruta principal del proyecto
 # @return {void}
def windowAggregate_opt(df, partitionCols, orderCol, aggregations, codMes, tablaTmp, flag, PRM_CARPETA_RAIZ_DE_PROYECTO, flg_null = 1):

  columnas_a_castear = ['CODMESANALISIS', 'CODMES']  
  for colName in columnas_a_castear:
    df = df.select(*[col(column_name).cast("string").alias(column_name) if column_name == colName else col(column_name) for column_name in df.columns]) 

  #Si no es un arreglo de "columnas", se convierte en uno
  if(type(partitionCols) == str):
      partitionCols = [partitionCols]

  #Producto cartesiano entre las particiones de agrupación y los períodos (meses) existentes
  dfPartitions = df.select(partitionCols).dropDuplicates()
  dfPeriods = df.select(orderCol).dropDuplicates()
  dfCJ =  dfPartitions.crossJoin(F.broadcast(dfPeriods))
  columnas_a_castear = ['CODMESANALISIS', 'CODMES']  
  for colName in columnas_a_castear:
    dfCJ = dfCJ.select(*[col(column_name).cast("string").alias(column_name) if column_name == colName else col(column_name) for column_name in dfCJ.columns])

  #Datos enriquecidos con los particiones y los períodos (meses)
  dfFin = df.join(dfCJ, on = partitionCols + [orderCol], how = "right_outer")
  dfFin.createOrReplaceTempView("dfFin")

  #Definición de la forma en cómo se particiona y ordenan los resultados
  windowSpecBase = Window.partitionBy(partitionCols).orderBy(orderCol)

  #Iteramos todas las agregaciones que se realizan en los datos
  templateOperation = "F.{funcName}(col('{baseCol}')).over(windowSpecBase.rowsBetween({startRow},{endRow}))"
  templateSelect = "col('OPERACIONES').getItem({index}).alias('{newColname}')"
  templateWithColumn = ".alias('dfgen{numberWithColumn}').withColumn('OPERACIONES', F.split(F.concat({operationBody}), ',')).select('dfgen{numberWithColumn}.*', {selectBody})"
  templateQuery = "dfFin{withColumnBody}"

  maxNumberOperations = 100
  numberWithColumn = 0
  numberColumnsAdded = 0
  numberIteration = 0
  index = 0
  aggregations = list(aggregations)
  sizeAggregation = len(aggregations)
  operationBodyArray = []
  selectBodyArray = []
  withColumnBodyArray = []

  for aggregation in aggregations:

    #Solo se calcula los montos para la condicion donde el CODMESANALISIS es igual al CODMES, evitamos iteraciones innecesarias
    if dfFin[dfFin.CODMESANALISIS == dfFin.CODMES]:
      #Nombre de la columna con la que se trabaja
      baseCol = aggregation[0]

      #Tipo de agregación que se realiza
      aggFun = aggregation[1]
      funcName=aggFun.__name__

      #Fila inicial
      startRow = aggregation[2]

      #Fila final
      endRow = aggregation[3]

      #Obtenemos el nombre que recibe la nueva columna
      if flag == 1:
        newColname = getNewColName2(baseCol, funcName, startRow, endRow)
      else:
        newColname = getNewColName(baseCol, funcName, startRow, endRow)

      #Aumentamos el número de columnas agregadas
      numberColumnsAdded = numberColumnsAdded + 1

      #Agregamos la operación al array
      operationBodyArray.append(templateOperation.\
      replace("{funcName}", str(funcName)).\
      replace("{baseCol}", str(baseCol)).\
      replace("{startRow}", str(startRow)).\
      replace("{endRow}", str(endRow))
      )

      #Agregamos el select al array
      selectBodyArray.append(templateSelect.\
      replace("{index}", str(index)).\
      replace("{newColname}", str(newColname))
      )

      #Aumentamos el índice del "select"
      index = index + 1

    #Aumentamos el número de iteraciones
    numberIteration = numberIteration + 1

    #Verificamos si no hay más operaciones que agregar O si hemos llegado a las "maxNumberOperations" operaciones
    if(numberIteration == sizeAggregation) or (index == maxNumberOperations):
      numberWithColumn = numberWithColumn + 1

      #Generamos el "withColumnBody"
      withColumnBodyArray.append(\
      templateWithColumn.\
      replace("{operationBody}", str(getOperationBody(operationBodyArray))).\
      replace("{selectBody}", str(getSelectBody(selectBodyArray))).\
      replace("{numberWithColumn}", str(numberWithColumn))\
      )

      #Reiniciamos los arrays
      operationBodyArray = []
      selectBodyArray = []

      #Reiniciamos el índice de "select"
      index = 0
  
  if flg_null == 1:
    dfFin = dfFin.na.fill(0)

  queryG1 = templateQuery.replace("{withColumnBody}", str(getWithColumnBody(withColumnBodyArray[0:6])))
  dfAgrupadoG1 = eval(queryG1)
  dfAgrupadoG1Filter = dfAgrupadoG1.filter( (col('CODMESANALISIS') == col('CODMES')))
  queryG2 = templateQuery.replace("{withColumnBody}", str(getWithColumnBody(withColumnBodyArray[6:])))
  dfAgrupadoG2 = eval(queryG2)
  dfAgrupadoG2Filter = dfAgrupadoG2.filter( (col('CODMESANALISIS') == col('CODMES')))
  columnsDfCoreG1 = getColumnListDfCore(dfAgrupadoG1Filter.columns, df.columns)
  dfAgrupadoG1FilterCore = dfAgrupadoG1Filter.select(columnsDfCoreG1)
  columnsDfCoreG2 = getColumnListDfCore(dfAgrupadoG2Filter.columns, df.columns)
  dfAgrupadoG2FilterCore = dfAgrupadoG2Filter.select(columnsDfCoreG2)
  dfCoreFinal = dfAgrupadoG1FilterCore.join(dfAgrupadoG2FilterCore, ['CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL'])  

  return dfCoreFinal

# Cálculo del codmes del proceso mediante la fecrutina.
#
# @param fecRutina {date} Fecha rutina en la que se ejecuta el proceso.
# @param intervalo {int} Numero condicional para el calculo del codmes del proceso.
# @return {int} Retorna el codmes del proceso.
##
def calcularCodmesProceso(fecRutina, intervalo):
  #formato fecharutina
  cadena_fecharutina = fecRutina
  formato = "%Y-%m-%d"
  fec_rutina_date = datetime.strptime(cadena_fecharutina, formato)
  lima_timezone = pytz.timezone('America/Lima')
  fec_rutina = lima_timezone.localize(fec_rutina_date)
 
  #obtiene solo el dia de la fecha
  NDIA = fec_rutina.day
 
  # Formatea la fecha como "YYYYMM"
  CODMES_str = fec_rutina.strftime("%Y%m")
       
  #vonvertir en numerico
  CODMES = int(CODMES_str)
       
  #logica obtener codmes
  if NDIA >= int(intervalo):
    CODMES_IN = CODMES
  else:
    if CODMES_str[4:6]=='01':
      CODMES_IN = CODMES-89
    else:
      CODMES_IN = CODMES-1
 
  return CODMES_IN
  
  
def windowAggregateOpt(df, partitionCols, orderCol, aggregations, flag, flg_null = 1):

  # Convertir iterador en lista
  aggs_list = list(aggregations)
  
  # Definir cuántos subconjuntos quieres crear, por ejemplo, dividir en 4 partes
  subsets = [aggs_list[i::4] for i in range(4)]
  
  
  def execute_aggregation(df, partitionCols, orderCol, aggregations, flag, flg_null = 1):
    #Catear columnas CODMESANALISIS y CODMES
    columnas_a_castear = ['CODMESANALISIS', 'CODMES']  
    for colName in columnas_a_castear:
      df = df.select(*[col(column_name).cast("string").alias(column_name) if column_name == colName else col(column_name) for column_name in df.columns]) 

    #Si no es un arreglo de "columnas", se convierte en uno
    if(type(partitionCols) == str):
        partitionCols = [partitionCols]

    #Producto cartesiano entre las particiones de agrupación y los períodos (meses) existentes
    dfPartitions = df.select(partitionCols).dropDuplicates()
    dfPeriods = df.select(orderCol).dropDuplicates()
    dfCJ =  dfPartitions.crossJoin(F.broadcast(dfPeriods))
    columnas_a_castear = ['CODMESANALISIS', 'CODMES']  
    for colName in columnas_a_castear:
      dfCJ = dfCJ.select(*[col(column_name).cast("string").alias(column_name) if column_name == colName else col(column_name) for column_name in dfCJ.columns])    

    #Datos enriquecidos con los particiones y los períodos (meses)
    dfFin = df.join(dfCJ, on = partitionCols + [orderCol], how = "right_outer")
    dfFin.createOrReplaceTempView("dfFin")

    #Definición de la forma en cómo se particiona y ordenan los resultados
    windowSpecBase = Window.partitionBy(partitionCols).orderBy(orderCol)

    #Iteramos todas las agregaciones que se realizan en los datos
    templateOperation = "F.{funcName}(col('{baseCol}')).over(windowSpecBase.rowsBetween({startRow},{endRow}))"
    templateSelect = "col('OPERACIONES').getItem({index}).alias('{newColname}')"
    #templateWithColumn = (".alias('dfgen{numberWithColumn}')"".selectExpr('dfgen{numberWithColumn}.*', 'split(concat({operationBody}), \",\") as OPERACIONES')"".selectExpr('dfgen{numberWithColumn}.*', {selectBody})")
    templateWithColumn = ".alias('dfgen{numberWithColumn}').withColumn('OPERACIONES', F.split(F.concat({operationBody}), ',')).select('dfgen{numberWithColumn}.*', {selectBody})"
    templateQuery = "dfFin{withColumnBody}"

    maxNumberOperations = 100
    numberWithColumn = 0
    numberColumnsAdded = 0
    numberIteration = 0
    index = 0
    aggregations = list(aggregations)
    sizeAggregation = len(aggregations)
    operationBodyArray = []
    selectBodyArray = []
    withColumnBodyArray = []

    for aggregation in aggregations:

      #Solo se calcula los montos para la condicion donde el CODMESANALISIS es igual al CODMES, evitamos iteraciones innecesarias
      if dfFin[dfFin.CODMESANALISIS == dfFin.CODMES]:
        #Nombre de la columna con la que se trabaja
        baseCol = aggregation[0]

        #Tipo de agregación que se realiza
        aggFun = aggregation[1]
        funcName=aggFun.__name__

        #Fila inicial
        startRow = aggregation[2]

        #Fila final
        endRow = aggregation[3]

        #Obtenemos el nombre que recibe la nueva columna
        if flag == 1:
          newColname = getNewColName2(baseCol, funcName, startRow, endRow)
        else:
          newColname = getNewColName(baseCol, funcName, startRow, endRow)

        #Aumentamos el número de columnas agregadas
        numberColumnsAdded = numberColumnsAdded + 1

        #Agregamos la operación al array
        operationBodyArray.append(templateOperation.\
        replace("{funcName}", str(funcName)).\
        replace("{baseCol}", str(baseCol)).\
        replace("{startRow}", str(startRow)).\
        replace("{endRow}", str(endRow))
        )

        #Agregamos el select al array
        selectBodyArray.append(templateSelect.\
        replace("{index}", str(index)).\
        replace("{newColname}", str(newColname))
        )

        #Aumentamos el índice del "select"
        index = index + 1

      #Aumentamos el número de iteraciones
      numberIteration = numberIteration + 1

      #Verificamos si no hay más operaciones que agregar O si hemos llegado a las "maxNumberOperations" operaciones
      if(numberIteration == sizeAggregation) or (index == maxNumberOperations):
        numberWithColumn = numberWithColumn + 1

        #Generamos el "withColumnBody"
        withColumnBodyArray.append(\
        templateWithColumn.\
        replace("{operationBody}", str(getOperationBody(operationBodyArray))).\
        replace("{selectBody}", str(getSelectBody(selectBodyArray))).\
        replace("{numberWithColumn}", str(numberWithColumn))\
        )

        #Reiniciamos los arrays
        operationBodyArray = []
        selectBodyArray = []

        #Reiniciamos el índice de "select"
        index = 0
  
    if flg_null == 1:
      dfFin = dfFin.na.fill(0)

    queryG1 = templateQuery.replace("{withColumnBody}", str(getWithColumnBody(withColumnBodyArray[0:6])))
    dfAgrupadoG1 = eval(queryG1)
    dfAgrupadoG1Filter = dfAgrupadoG1.filter( (col('CODMESANALISIS') == col('CODMES')))
    queryG2 = templateQuery.replace("{withColumnBody}", str(getWithColumnBody(withColumnBodyArray[6:])))
    dfAgrupadoG2 = eval(queryG2)
    dfAgrupadoG2Filter = dfAgrupadoG2.filter( (col('CODMESANALISIS') == col('CODMES')))
    columnsDfCoreG1 = getColumnListDfCore(dfAgrupadoG1Filter.columns, df.columns)
    dfAgrupadoG1FilterCore = dfAgrupadoG1Filter.select(columnsDfCoreG1)
    columnsDfCoreG2 = getColumnListDfCore(dfAgrupadoG2Filter.columns, df.columns)
    dfAgrupadoG2FilterCore = dfAgrupadoG2Filter.select(columnsDfCoreG2)
    dfCoreFinal = dfAgrupadoG1FilterCore.join(dfAgrupadoG2FilterCore, ['CODMESANALISIS','CODMES','CODUNICOCLI','CODINTERNOCOMPUTACIONAL'])

    return dfCoreFinal


  df_views = {}    
  with concurrent.futures.ThreadPoolExecutor(4) as executor:
      future_to_df = {executor.submit(execute_aggregation,df, partitionCols, orderCol, subset, flag): i for i, subset in enumerate(subsets, 1)}
      for future in concurrent.futures.as_completed(future_to_df):
          index = future_to_df[future]
          df = future.result()
          view_name = f"dfCoreFinal{index}"
          df.createOrReplaceTempView(view_name)
          df_views[view_name] = df            
  
  # asignación de vistas
  dfCoreFinal1 = df_views['dfCoreFinal1']
  dfCoreFinal2 = df_views['dfCoreFinal2']
  dfCoreFinal3 = df_views['dfCoreFinal3']
  dfCoreFinal4 = df_views['dfCoreFinal4']
  
  # Unión de DataFrames
  dfCoreFinal = dfCoreFinal1.join(dfCoreFinal2, ["CODMESANALISIS","CODMES","CODUNICOCLI","CODINTERNOCOMPUTACIONAL"], "outer")\
      .join(dfCoreFinal3, ["CODMESANALISIS","CODMES","CODUNICOCLI","CODINTERNOCOMPUTACIONAL"], "outer")\
      .join(dfCoreFinal4, ["CODMESANALISIS","CODMES","CODUNICOCLI","CODINTERNOCOMPUTACIONAL"], "outer") 
  return dfCoreFinal

# Cálculo del codmes del proceso mediante la fecRutina.
#
# @param fecRutina {date} Fecha rutina en la que se ejecuta el proceso.
# @return {string} Retorna el codmes del proceso.
##
def calcularCodmes(fecRutina):
   # Formato fecharutina
   fec_rutina_date = datetime.strptime(fecRutina, '%Y-%m-%d')
   lima_timezone = pytz.timezone('America/Lima')
   fec_rutina = lima_timezone.localize(fec_rutina_date)
   
   # Formatea la fecha como "YYYYMM"
   CODMES_str = fec_rutina.strftime('%Y%m')
   CODMES_int = int(CODMES_str)

   # Calcular 煤ltimo d铆a del mes
   fechaProceso = f"{fecRutina[:8]}01"
   fechaProcesoDT = datetime.strptime(fechaProceso, '%Y-%m-%d') 
   fechaProcesoDT = fechaProcesoDT + relativedelta(months=1) - timedelta(days=1) 
   ult_day = fechaProcesoDT.strftime('%Y-%m-%d')
           
   # Logica obtener codmes
   if fecRutina == ult_day:
       CODMES = CODMES_int
   else:
       if CODMES_str[4:6] == '01':
           CODMES = CODMES_int - 89
       else:
           CODMES = CODMES_int - 1

   return str(CODMES)