from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col
import time

#Procesa los ficheros "estáticos" mediante Spark

c=SparkConf()
spark = SparkSession.builder.config(conf=c).appName("ProcessData").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

start_time=time.time()
onlyfiles = [f for f in listdir("./StaticFiles/SegundoProcesado") if isfile(join("./StaticFiles/SegundoProcesado", f))]

for i in onlyfiles:

    df = spark.read.option("header", "true").csv( f"./StaticFiles/SegundoProcesado/{i}")
    df = df.select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)
    df=df.withColumn('Ultimo', col('Ultimo').cast('float'))\
    .withColumn('Apertura', col('Apertura').cast('float'))\
    .withColumn('Maximo', col('Maximo').cast('float'))\
    .withColumn('Minimo', col('Minimo').cast('float'))\
    .withColumn('Index', col('Index').cast('int'))\
    .withColumn('Year', col('Year').cast('int'))\
    .withColumn('Month', col('Month').cast('int'))\
    .withColumn('Day', col('Day').cast('int'))\

    #Máximo valor en el cierre
    df_max_ultimo=df.agg({"Ultimo": "max"})
    df_max_ultimo=df_max_ultimo.join(df, df.Ultimo==df_max_ultimo["max(Ultimo)"]).select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)

    #Mínimo valor histórico alcanzado
    df_min_ultimo=df.agg({"Ultimo": "min"})
    df_min_ultimo=df_min_ultimo.join(df, df.Ultimo==df_min_ultimo["min(Ultimo)"]).select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)

    #Máximo pico de valor
    df_max_maximo=df.agg({"Maximo": "max"})
    df_max_maximo=df_max_maximo.join(df, df.Maximo==df_max_maximo["max(Maximo)"]).select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)

    #Mínimo histórico de valor mínimo
    df_min_minimo=df.agg({"Minimo": "min"})
    df_min_minimo=df_min_minimo.join(df, df.Minimo==df_min_minimo["min(Minimo)"]).select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)

    #Histórico desde el comienzo hasta alcanzar el valor máximo
    df.createOrReplaceTempView("DF1")
    sqlDF_max = spark.sql("SELECT * FROM DF1 e WHERE e.Index>=(SELECT Index FROM DF1 e2 WHERE e2.Maximo=(SELECT MAX(Maximo) FROM DF1))")

    #Histórico desde el comienzo hasta alcanzar el valor mínimo
    df.createOrReplaceTempView("DF1")
    sqlDF_min = spark.sql("SELECT * FROM DF1 e WHERE e.Index>=(SELECT Index FROM DF1 e2 WHERE e2.Minimo=(SELECT MIN(Minimo) FROM DF1))")

    #Valor medio al cierre
    df_avg_ultimo=df.agg({"Ultimo": "avg"})

    #Valores medios de cierre por mes
    df_avg_month=df.groupBy('Year', 'Month').agg({"Ultimo": "avg"})

    #Extrae los valores que superan a la media y los mostrará ordenados por los mas reciente
    df.createOrReplaceTempView("DF1")
    df_high_values=spark.sql("SELECT e.Index, e.Year, e.Month, e.Day, e.Ultimo FROM DF1 e WHERE e.Ultimo>=(SELECT AVG(e2.Ultimo) FROM DF1 e2)")

    df_max_ultimo.show()
    df_min_ultimo.show()
    df_max_maximo.show()
    df_min_minimo.show()
    sqlDF_max.sort(col('Index').asc()).show()
    sqlDF_min.sort(col('Index').asc()).show()
    df_avg_ultimo.show()
    df_avg_month.sort(col('Year').desc(), col('Month').desc()).show()
    df_high_values.sort(col('Index').asc()).show()

print ("It took ", str(time.time()-start_time), " s")
