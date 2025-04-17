from pyspark.sql import SparkSession

INDEX_NAME = 'spark_index'

spark = SparkSession.Builder() \
    .appName("Adaptive") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.1") \
    .config("es.nodes", "localhost") \
    .config("es.port", "9200") \
    .config("es.nodes.wan.only", "true") \
    .master("local[4]").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load(
    'data/training_pool.csv')
    
df = df.select([col for col in df.columns if col != 'target'])

df.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", '%s' % (INDEX_NAME)) \
    .mode("overwrite") \
    .save()
    # .option("es.resource", '%s/%s' % (INDEX_NAME, "doc")) \

# print('done trước nhá')
# Explicitly stop the Spark session
spark.stop()

# print('done')