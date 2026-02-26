from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("TestSpark").getOrCreate()
print("SparkSession OK!")
spark.stop()
