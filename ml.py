from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("RiskScoring_JSON").config("spark.executor.memory", "4g") .config("spark.driver.memory", "4g").config("spark.executor.memoryOverhead", "1g").getOrCreate()
df = spark.read.json("objets-trouves-restitution.json")
df = df.select("date", "gc_obo_date_heure_restitution_c","gc_obo_gare_origine_r_name","gc_obo_nature_c","gc_obo_type_c")
df = df.withColumn("restitue", when(df.gc_obo_date_heure_restitution_c.isNotNull(), 1).otherwise(0))
df = df.withColumn("restitution_delay", datediff(to_date("gc_obo_date_heure_restitution_c"), to_date("date")))
gare_indexer = StringIndexer(
    inputCol="gc_obo_gare_origine_r_name", 
    outputCol="gare_index", 
    handleInvalid="keep" 
)

objet_indexer = StringIndexer(
    inputCol="gc_obo_nature_c", 
    outputCol="objet_index", 
    handleInvalid="keep"
)
assembler = VectorAssembler(inputCols=["gare_index", "objet_index"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="restitue", probabilityCol="probability")
pipeline = Pipeline(stages=[gare_indexer, objet_indexer, assembler, lr])
model = pipeline.fit(df)
predictions = model.transform(df)
evaluator = BinaryClassificationEvaluator(labelCol="restitue", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
print(f"🎯 AUC (Area Under ROC) : {auc:.2f}")
model.write().overwrite().save("risk_scoring_model")