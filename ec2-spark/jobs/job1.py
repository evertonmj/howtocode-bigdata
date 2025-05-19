from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Inicializa a SparkSession com suporte ao S3
spark = SparkSession.builder \
    .appName("StreamingS3") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Caminho do bucket S3 (modifique com o nome real do seu bucket e pasta)
s3_path = "s3a://ucsal-bigdata-ever-01/dados/"

# Lê arquivos CSV no S3 como streaming
df = spark.readStream \
    .option("header", "true") \
    .schema("id INT, nome STRING, timestamp STRING") \
    .csv(s3_path)

# Transformações
resultado = df.select("id", "nome", "timestamp")

# Saída no console
query = resultado.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
