from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, monotonically_increasing_id

#Añade la columna Index y almacena el resultado de seleccionar las columnas con las que se va a operar.

c=SparkConf()
spark = SparkSession.builder.config(conf=c).appName("filter").getOrCreate()

onlyfiles = [f for f in listdir("./StaticFiles/SinProcesar") if isfile(join("./StaticFiles/SinProcesar", f))]

for i in onlyfiles:
    df = spark.read.option("header", "true").csv( f"./StaticFiles/SinProcesar/{i}")
    df=df.withColumn("Index",monotonically_increasing_id())\
    .withColumnRenamed("Último", "Ultimo")\
    .withColumnRenamed("Máximo", "Maximo")\
    .withColumnRenamed("Mínimo", "Minimo")\
    .withColumnRenamed("% var.", "Var")\
    .select(col("Index"), col("Fecha"), col("Ultimo"), col("Apertura"), col("Maximo"), col("Minimo"), col("Var"), df.Fecha[7:4].alias("Year"), df.Fecha[4:2].alias("Month"), df.Fecha[1:2].alias("Day"))

    #Una vez almacenado el fichero, es necesario renombrarlo y extraerlo a la carpeta anterior
    df.repartition(1).write.option("header","true").csv(f"./StaticFiles/PrimerProcesado/{i}")