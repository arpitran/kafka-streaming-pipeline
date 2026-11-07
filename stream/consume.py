from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.appName("stream").master("local[*]")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
         .getOrCreate())
schema = T.StructType([T.StructField("id", T.LongType()), T.StructField("zone", T.IntegerType()),
                       T.StructField("ts", T.TimestampType()), T.StructField("fare", T.DoubleType())])
df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "trips").option("startingOffsets", "earliest").load()
      .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
      .withWatermark("ts", "1 hour").dropDuplicates(["id", "ts"]))
out = df.groupBy(F.window("ts", "1 hour"), "zone").agg(F.count("*").alias("trips"))
(out.writeStream.outputMode("append").format("parquet").option("path", "lake/gold/zone_stream")
    .option("checkpointLocation", "lake/_chk/zone_stream").start().awaitTermination())
