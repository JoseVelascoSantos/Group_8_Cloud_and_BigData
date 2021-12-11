from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, lit, when, monotonically_increasing_id
import re


c=SparkConf()
spark = SparkSession.builder.config(conf=c).appName("filter").getOrCreate()

df = spark.read.option("header", "true").csv("Archivos sin filtrar/Histórico-Ethereum(ETH).csv")
df=df.withColumn("Index",monotonically_increasing_id())\
.withColumnRenamed("Último", "Ultimo")\
.withColumnRenamed("Máximo", "Maximo")\
.withColumnRenamed("Mínimo", "Minimo")\
.withColumnRenamed("% var.", "Var")\
.select(col("Index"), col("Fecha"), col("Ultimo"), col("Apertura"), col("Maximo"), col("Minimo"), col("Var"), df.Fecha[7:4].alias("Year"), df.Fecha[4:2].alias("Month"), df.Fecha[1:2].alias("Day"))

df.repartition(1).write.option("header","true").csv("Ethereum-SparkFilter")