from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Create SparkSession
spark = SparkSession.builder \
    .appName("SentimentAnalysisDecisionTree") \
    .master("local[*]") \
    .getOrCreate()

# Load data from S3
caminho_s3 = "s3a://evert-bg-01/data/dataset.csv"
df = spark.read.option("header", "true").csv(caminho_s3)

# Filter out null reviews
df = df.filter(col("review_text").isNotNull())

# Cast review_score to double
df = df.withColumn("review_score", col("review_score").cast("double"))

# Filter out rows with null or invalid scores
df = df.filter(col("review_score").isNotNull())

# Convert review_score to binary label: 1 if score >= 3, else 0
df = df.withColumn("label", when(col("review_score") >= 3.0, 1.0).otherwise(0.0))

# Ensure labels are only 0.0 or 1.0
df = df.filter((col("label") == 0.0) | (col("label") == 1.0))

# NLP preprocessing pipeline
tokenizer = Tokenizer(inputCol="review_text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")

# Decision Tree classifier
dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")

# Full ML pipeline
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, dt])

# Split data into train and test sets
train, test = df.randomSplit([0.8, 0.2], seed=42)

# Train the model
model = pipeline.fit(train)

# Make predictions
predictions = model.transform(test)

# Evaluate model performance
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
roc = evaluator.evaluate(predictions)
print(f"Área sob a curva ROC: {roc:.4f}")

# Show some predictions
predictions.select("review_text", "review_score", "label", "prediction", "probability").show(10, truncate=False)

# Stop Spark session
spark.stop()
