from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import lit, when, col
import re

#time_period_start,time_period_end,time_open,time_close,price_open,price_high,price_low,price_close,volume_traded,trades_count
#almacenar medias de los cierres por mes
#almacenar registro desde el primer dato hasta el max de maximos, minimo de minimos etc...
#count de cuantos meses superan la media de cierre
#count de cuantos han superado su propia media el último mes

c=SparkConf()
spark = SparkSession.builder.config(conf=c).appName("ProcessData").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

onlyfiles = [f for f in listdir("./data-csv") if isfile(join("./data-csv", f))]

df_rec_max=None
df_rec_min=None
df_rec_avg=None

for i in onlyfiles:

    df = spark.read.option("header", "true").csv( f"data-csv/{i}")
    df = df.select(df.price_high, df.price_low, df.price_close, df.time_period_start)
    df = df.withColumn("Nombre", lit(re.sub(r'BINANCE_SPOT_', '', re.sub(r'.csv', '', str(i)))))
    df=df.withColumn('price_high', col('price_high').cast('float'))\
    .withColumn('price_low', col('price_low').cast('float'))\
    .withColumn('price_close', col('price_close').cast('float'))\
    .withColumn('Month', col('time_period_start')[6:2])

    df=df.withColumn('Month', col('Month').cast('int'))

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


    if df_rec_max is None:
        df_rec_max=df_max_maximo
        df_rec_min=df_min_minimo
        df_rec_avg=df_avg_close
    else:
        df_rec_max=df_rec_max.union(df_max_maximo)
        df_rec_min=df_rec_min.union(df_min_minimo)
        df_rec_avg=df_rec_avg.union(df_avg_close)


df_rec_max.sort(col("max(price_high)").desc()).show()
df_rec_min.sort(col("min(price_low)").asc()).show()
df_rec_avg.show()

#df_rec.repartition(1).write.option("header","true").csv("Save1")
