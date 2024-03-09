from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

def read_json_to_df(spark, file_path):
    return (
        spark.read.format("json")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("mergeSchema", "true")
        .option("readChangeFeed", "true")
        .option("enableChangeDataFeed", "true")
        .json(file_path)
    )

def add_metadata_columns(df, source_system):
    return (
        df.withColumn("ingestion_time", current_timestamp())
        .withColumn("source_system", lit(source_system))
        .withColumn("raw_file_name", input_file_name())
    )

def write_to_delta(df, path):
    df.write.mode("append").format("delta").option("mergeSchema", "true").save(path)

def process_raw_data_to_bronze(spark, date_path, datasets):
    for dataset, source_system in datasets:
        raw_path = f"s3a://apiroyale-raw/APIRoyale/players/sub_type={dataset}/extracted_at={date_path}"
        df = read_json_to_df(spark, raw_path)

        bronze_path = f"s3a://apiroyale-bronze/APIRoyale/players/sub_type={dataset}/ingested_at={date_path}/players.{dataset}"
        df = add_metadata_columns(df, source_system)
        write_to_delta(df, bronze_path)


# Create SparkSession
spark = (
    SparkSession.builder.appName("api-royale")
    .config(
        "spark.sql.extensions", 
        "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

# Get the current date
date_path = date.today().strftime("%Y-%m-%d")

#"Select the dataset and its schema (it will be useful to add the source_system column) that will be ingested into the Bronze layer."
datasets = [
    ("battlelog", "api.players.battlelog"),
    ("players", "api.players.players"),
    ("upcomingchests", "api.players.upcomingchests")
]

# Process data
process_raw_data_to_bronze(spark, date_path, datasets)