import os
from pyspark.sql import SparkSession

# Set correct Python path for executors (inside Docker)
os.environ["PYSPARK_PYTHON"] = "python3"

spark = SparkSession.builder \
    .appName("Local-to-Docker-Spark") \
    .master("spark://localhost:7077") \
    .config("spark.driver.host", "host.docker.internal") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.ui.port", "4041") \
    .config("spark.pyspark.python", "python3") \
    .config("spark.pyspark.driver.python", "python3") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print("=== Spark Configuration ===")
print("Spark master:", spark.sparkContext.master)
print("Default parallelism:", spark.sparkContext.defaultParallelism)

# Test that doesn't require data processing - just creation and schema
data = spark.createDataFrame([
    (1, "This framework generates embeddings for each input sentence"),
    (2, "Sentences are passed as a list of strings."),
    (3, "The quick brown fox jumps over the lazy dog")
], ["id", "text"])

print("=== Results ===")
print("Schema:")
data.printSchema()

# Try a collect instead of count to see if it works
try:
    rows = data.collect()
    print(f"Successfully collected {len(rows)} rows!")
    for row in rows:
        print(f"  {row.id}: {row.text[:50]}...")
except Exception as e:
    print(f"Collection failed: {e}")

print("The show")
data.show()

print("=== SUCCESS: Spark cluster connectivity is working! ===")

spark.stop()
