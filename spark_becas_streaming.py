from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# 1. Crear la sesión de Spark para Streaming
spark = SparkSession.builder \
    .appName("StreamingBecasExterior") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema del JSON que llega desde Kafka
schema = StructType([
    StructField("pais_de_destino", StringType(), True),
    StructField("nivel_academico", StringType(), True),
    StructField("rango_de_edad", StringType(), True),
    StructField("genero", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Leer el flujo de datos (stream) desde el topic de Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "becas-stream") \
    .load()

# 4. Transformar los datos binarios de Kafka a un DataFrame estructurado
df_json = df_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. Agrupar en tiempo real por todas las variables simuladas y ordenar de forma descendente
from pyspark.sql.functions import col

conteo_streaming = df_json.groupBy(
    "pais_de_destino", 
    "nivel_academico", 
    "rango_de_edad", 
    "genero"
).count().sort(col("count").desc())

# 6. Mostrar los resultados en la consola de manera continua (modo update o complete)
query = conteo_streaming.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
