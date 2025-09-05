from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, row_number, to_date, sum as ssum
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PortfolioBronzeSilverGold").getOrCreate()

raw_base = "/mnt/portfolio/raw"
bronze_base = "/mnt/portfolio/bronze"
silver_base = "/mnt/portfolio/silver"
gold_base = "/mnt/portfolio/gold"

orders_raw = spark.read.option("header", True).csv(f"{raw_base}/orders.csv")
orders_bronze = (orders_raw
                 .withColumn("ingestion_ts", current_timestamp())
                 .withColumn("source_file", input_file_name()))
orders_bronze.write.mode("overwrite").format("delta").save(f"{bronze_base}/orders")

w = Window.partitionBy("order_id").orderBy(col("ingestion_ts").desc())
orders_silver = (orders_bronze
                 .withColumn("rn", row_number().over(w))
                 .where(col("rn") == 1)
                 .drop("rn", "source_file"))
orders_silver.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"{silver_base}/orders")

orders_gold = (orders_silver
               .withColumn("order_day", to_date("order_date"))
               .groupBy("order_day")
               .agg(ssum(col("amount").cast("double")).alias("revenue")))
orders_gold.write.mode("overwrite").format("delta").save(f"{gold_base}/daily_revenue")

print("Bronze Silver Gold pipeline completed")
