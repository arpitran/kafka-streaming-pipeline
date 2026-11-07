from pyspark.sql import SparkSession, functions as F

URL = "raw/yellow_tripdata_2024-01.parquet"  # curl from https://d37ci6vzurychx.cloudfront.net/trip-data/ first; Spark cannot read https
spark = SparkSession.builder.appName("lakehouse").master("local[*]").getOrCreate()

bronze = spark.read.parquet(URL)
bronze.write.mode("overwrite").parquet("lake/bronze/trips")

silver = (
    spark.read.parquet("lake/bronze/trips")
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("fare_amount") > 0)
    .dropDuplicates(["tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount"])
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
)
assert silver.count() > 0, "silver is empty"
silver.write.mode("overwrite").partitionBy("pickup_date").parquet("lake/silver/trips")

gold = (
    spark.read.parquet("lake/silver/trips")
    .groupBy("pickup_date", "PULocationID")
    .agg(F.count("*").alias("trips"), F.avg("fare_amount").alias("avg_fare"))
)
gold.write.mode("overwrite").parquet("lake/gold/zone_daily")
