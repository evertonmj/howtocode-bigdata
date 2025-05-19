from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("ProcessarArquivoGrandeS3") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Caminho do seu arquivo grande no S3
caminho_s3 = "s3a://evert-bg-01/data/dataset.csv"

df = spark.read \
    .option("header", "true") \
    .csv(caminho_s3)

# Processamento simples
df_filtro = df.filter(col("app_id").cast("int") > 50000)

# Mostrar resultado
df_filtro.show(20)

df_count = df_filtro.groupBy(col("app_name")).count()

df_count.show(20)

# (Opcional) salvar resultado no S3
# df_filtro.write.mode("overwrite").csv("s3a://ucsal-bigdata-ever-01/saida/filtrado/")