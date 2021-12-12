from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import lit, col, monotonically_increasing_id
import re

c=SparkConf()
spark = SparkSession.builder.config(conf=c).appName("ProcessData").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

onlyfiles = [f for f in listdir("./data") if isfile(join("./data", f))]

df_rec_max=None
df_rec_min=None
df_rec_avg=None
df_rec_maxavg=None
df_rec_minavg=None
df_rec_minclose=None
df_rec_avg_min_close=None

for i in onlyfiles:

    df = spark.read.option("header", "true").csv( f"data/{i}")
    df = df.select(df.price_high, df.price_low, df.price_close, df.time_period_start)
    df = df.withColumn("Nombre", lit(re.sub(r'BINANCE_SPOT_', '', re.sub(r'.csv', '', str(i)))))
    df=df.withColumn("Index",monotonically_increasing_id())\
    .withColumn('price_high', col('price_high').cast('float'))\
    .withColumn('price_low', col('price_low').cast('float'))\
    .withColumn('price_close', col('price_close').cast('float'))\
    .withColumn('Year', df.time_period_start[1:4])\
    .withColumn('Month', df.time_period_start[6:2])\
    .withColumn('Day', df.time_period_start[9:2])

    df=df.withColumn('Day', col('Day').cast('int'))\
    .withColumn('Month', col('Month').cast('int'))\
    .withColumn('Year', col('Year').cast('int'))\
    .withColumn('Index', col('Index').cast('int'))

    #Maximo pico
    df_max_maximo=df.agg({"price_high": "max"})
    df_max_maximo=df_max_maximo.join(df, df.price_high==df_max_maximo["max(price_high)"]).select(df_max_maximo["max(price_high)"], df.Nombre)

    #Pico mas bajo
    df_min_minimo=df.agg({"price_low": "min"})
    df_min_minimo=df_min_minimo.join(df, df.price_low==df_min_minimo["min(price_low)"]).select(df_min_minimo["min(price_low)"], df.Nombre)

     #Medias de cierre
    df_avg_close=df.groupBy('Nombre').agg({"price_close": "avg"})
    df_avg_close=df_avg_close.withColumn('Media', col('avg(price_close)').cast('float'))
    df_avg_close=df_avg_close.select(df_avg_close.Nombre, df_avg_close.Media)

    #Media de máximos
    df_avg_maximo=df.groupBy('Nombre').agg({"price_high": "avg"})
    
    #Media de mínimos
    df_avg_minimo=df.groupBy('Nombre').agg({"price_low": "avg"})

    #Cierres mas bajos
    df_min_close=df.agg({"price_close": "min"})
    df_min_close=df_min_close.join(df, df.price_close==df_min_close["min(price_close)"]).select(df_min_close["min(price_close)"], df.Nombre, df.Day)
    df_min_close=df_min_close.withColumn('MinimosCierre', col('min(price_close)').cast('float'))


    if df_rec_max is None:
        df_rec_max=df_max_maximo
        df_rec_min=df_min_minimo
        df_rec_avg=df_avg_close
        df_rec_maxavg=df_avg_maximo
        df_rec_minavg=df_avg_minimo
        df_rec_minclose=df_min_close
    else:
        df_rec_max=df_rec_max.union(df_max_maximo)
        df_rec_min=df_rec_min.union(df_min_minimo)
        df_rec_avg=df_rec_avg.union(df_avg_close)
        df_rec_maxavg=df_rec_maxavg.union(df_avg_maximo)
        df_rec_minavg=df_rec_minavg.union(df_avg_minimo)
        df_rec_minclose=df_rec_minclose.union(df_min_close)

#Media de cierres mas bajos por día
df_rec_avg_min_close=df_rec_minclose.groupBy('Day').agg({"MinimosCierre": "avg"})


df_rec_max.sort(col("max(price_high)").desc()).show()
df_rec_min.sort(col("min(price_low)").asc()).show()
df_rec_avg.show()
df_rec_maxavg.sort(col("avg(price_high)").asc()).show()
df_rec_minavg.sort(col("avg(price_low)").desc()).show()
df_rec_minclose.sort(col("MinimosCierre").asc()).show()
df_rec_avg_min_close.sort(col("avg(MinimosCierre)").asc()).show()