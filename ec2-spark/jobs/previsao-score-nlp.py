from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col

# Criar SparkSession
spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .master("local[*]") \
    .getOrCreate()

caminho_s3 = "s3a://evert-bg-01/data/dataset.csv"

df = spark.read \
    .option("header", "true") \
    .csv(caminho_s3)

# Remover linhas nulas e converter label para double
df = df.filter(col("review_text").isNotNull())
df = df.withColumn("review_score", col("review_score").cast("double"))
df = df.filter(col("review_score").isNotNull())

# Pré-processamento: NLP pipeline
tokenizer = Tokenizer(inputCol="review_text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000) 
idf = IDF(inputCol="rawFeatures", outputCol="features")

# Classificador alterado para Decision Tree
dt = DecisionTreeClassifier(featuresCol="features", labelCol="review_score")

# Pipeline completo
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, dt])

# Treinamento/teste
train, test = df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)

# Avaliação
predictions = model.transform(test)
evaluator = BinaryClassificationEvaluator(labelCol="review_score", metricName="areaUnderROC")
roc = evaluator.evaluate(predictions)

print(f"Área sob a curva ROC: {roc:.4f}")

# Exibir previsões exemplo
predictions.select("review_text", "review_score", "prediction", "probability").show(10, truncate=False)

spark.stop()
