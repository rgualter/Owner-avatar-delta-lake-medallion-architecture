from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
from delta.tables import DeltaTable
from pyspark.sql.types import DateType, StringType, TimestampType, BooleanType, IntegerType


def read_silver_table(spark, path):
    return spark.read.format("delta").load(path)

def process_battle_stats(df):
    df_select = df.select("battle_id", to_date("battle_time").alias("battle_date"), "isvictory", "team_tag", "ingestion_time", "game_mode_name")
    return (
        df_select
        .groupBy('battle_date', 'game_mode_name', 'team_tag')
        .agg(
            sum(when(col('isvictory'), 1).otherwise(0)).alias('victory_count'),
            sum(when(~col('isvictory'), 1).otherwise(0)).alias('defeat_count')
        )
        .withColumn("victory_count", col("victory_count").cast(IntegerType()))
        .withColumn("defeat_count", col("defeat_count").cast(IntegerType()))
        .orderBy(desc("battle_date"))
    )

def create_battlelog_analytics_table(spark,battlelog_analytics_path):
    DeltaTable.createIfNotExists(spark) \
        .tableName("battlelog_analytics") \
        .addColumn("battle_date", DateType()) \
        .addColumn("team_tag", ArrayType(StringType(),True))\
        .addColumn("game_mode_name", StringType()) \
        .addColumn("victory_count", IntegerType()) \
        .addColumn("defeat_count", IntegerType()) \
        .partitionedBy("battle_date") \
        .location(battlelog_analytics_path) \
        .execute()

def write_battlelog_analytics(df):
    df.write.mode("overwrite").format("delta").save(battlelog_analytics_path)

spark = (
    SparkSession.builder.appName("api-royale")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)

## ====  START PROCESS BATTLELOG
battlelog_stage_path = "s3a://apiroyale-stage/APIRoyale/players/sub_type=battlelog/players.battlelog"
battlelog_analytics_path = "s3a://apiroyale-analytics/APIRoyale/players/sub_type=battlelog/players.battlelog.analytics"

battlelog_stage_df = read_silver_table(spark, battlelog_stage_path)
battlelog_analytics_process = process_battle_stats(battlelog_stage_df)

create_battlelog_analytics_table(spark,battlelog_analytics_path)
write_battlelog_analytics(battlelog_analytics_process)

#write_battlelog_analytics(processed_stats)
## ===== END PROCESS BATTLELOG

