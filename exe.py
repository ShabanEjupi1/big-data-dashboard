# exe.py - example fix
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

# ========== CONFIG ==========
# Option A: local (test)
spark = SparkSession.builder \
    .appName("ReadParquetExample") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Option B (cluster) - uncomment and adjust if you run on cluster:
# spark = SparkSession.builder \
#     .appName("ReadParquetExample") \
#     .master("spark://10.0.0.8:7077") \
#     .config("spark.executor.instances", "9") \
#     .config("spark.executor.cores", "4") \
#     .config("spark.executor.memory", "8g") \
#     .getOrCreate()

# ========== READ PARQUET ==========
parquet_path = "/home/krenuser/big-data-dashboard/data/crypto/"

df = spark.read.option("mergeSchema", "true").parquet(parquet_path)

print("Schema e DataFrame:")
df.printSchema()

print("Shembull rreshtash:")
df.show(5, truncate=False)

try:
    cnt = df.count()
    print(f"Rows: {cnt}")
except Exception as e:
    print(f"Gabim gjatë count(): {e}")

# OPTIONAL: eksporte JSON i fundit për dashboard
# latest_json = "/home/krenuser/big-data-dashboard/dashboard_latest.json"
# df.limit(1000).toPandas().to_json(latest_json, orient="records", date_format="iso")

spark.stop()