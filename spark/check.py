from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("Spark version:", spark.version)
print("Hadoop version:", spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
