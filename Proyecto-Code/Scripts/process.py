from collections import UserList
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import lit, when, col

c=SparkConf()
spark = SparkSession.builder.config(conf=c).appName("ProcessData").getOrCreate()

df = spark.read.option("header", "true").csv("Archivos filtrados Pandas/Ethereum.csv")
df = df.select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)
df=df.withColumn('Ultimo', col('Ultimo').cast('float'))\
.withColumn('Apertura', col('Apertura').cast('float'))\
.withColumn('Maximo', col('Maximo').cast('float'))\
.withColumn('Minimo', col('Minimo').cast('float'))
df_max_ultimo=df.agg({"Ultimo": "max"})
df_max_maximo=df.agg({"Maximo": "max"})
df_min_ultimo=df.agg({"Ultimo": "min"})
#df_max_maximo=df.orderBy(col("Maximo").desc()).head().select(df.Index, df.Maximo)
#df_max_minimo=df.orderBy(col("Minimo").desc()).head().select(df.Index, df.Minimo)
df_max_ultimo=df_max_ultimo.join(df, df.Ultimo==df_max_ultimo["max(Ultimo)"]).select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)
df_max_maximo=df_max_maximo.join(df, df.Maximo==df_max_maximo["max(Maximo)"]).select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)
df_min_ultimo=df_min_ultimo.join(df, df.Ultimo==df_min_ultimo["min(Ultimo)"]).select(df.Index, df.Fecha, df.Ultimo, df.Apertura, df.Maximo, df.Minimo, df.Var, df.Year, df.Month, df.Day)

df_max_ultimo.repartition(1).write.option("header","true").csv("Max-Cierre-Ethereum")
df_min_ultimo.repartition(1).write.option("header","true").csv("Min-Cierre-Ethereum")
df_max_maximo.repartition(1).write.option("header","true").csv("Max-Maximos-Ethereum")
