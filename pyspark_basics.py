# Initialize Spark
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("PySpark Basics") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .config("spark.sql.repl.eagerEval.maxNumRows", 10) \
    .config("spark.sql.repl.eagerEval.truncate", 0) \
    .getOrCreate()

# Set Spark logging level
spark.sparkContext.setLogLevel("WARN")

# ... existing code ... 