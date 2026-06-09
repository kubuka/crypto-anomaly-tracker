from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
)


def silver_transform():
    print("Starting Silver Transform...")

    spark = SparkSession.builder.appName("SilverTransform").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    if spark is None:
        print("Failed to create Spark session.")
        return
    else:
        print("Spark session created successfully.")

    bronze_path = "data/bronze/coins.json"
    output_path = "data/silver/silver_coins"

    json_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("name", StringType(), True),
            StructField("current_price", DoubleType(), True),
            StructField("market_cap", LongType(), True),
            StructField("total_volume", LongType(), True),
            StructField("last_updated", TimestampType(), True),
        ]
    )

    try:
        print(f"Reading JSON file from {bronze_path}...")

        df_silver = (
            spark.read.schema(json_schema)
            .option("multiline", "true")
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .json(bronze_path)
        )

        print("JSON file read successfully.")
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return

    try:
        print(f"Writing DataFrame to {output_path} in Parquet format...")
        df_silver.write.mode("overwrite").parquet(output_path)
        print("DataFrame written successfully.")
    except Exception as e:
        print(f"Error writing DataFrame: {e}")
        return

    spark.stop()


silver_transform()
