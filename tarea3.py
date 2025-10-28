#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()
# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)
#imprimimos el esquema
df.printSchema()
# Muestra las primeras filas del DataFrame
df.show()
# Estadisticas básicas
df.summary().show()
# Consulta: Filtrar por valor y seleccionar columnas
print("Dias con valor mayor a 5000\n")
dias = df.filter(F.col('VALOR') > 5000).select('VALOR','VIGENCIADESDE','VIGENCIAHASTA')
dias.show()
# Ordenar filas por los valores en la columna "VALOR" en orden descendente
print("Valores ordenados de mayor a menor\n")
sorted_df = df.sort(F.col("VALOR").desc())
sorted_df.show()
