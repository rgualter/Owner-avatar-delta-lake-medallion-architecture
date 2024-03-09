from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
from delta.tables import DeltaTable

def read_delta_df(spark, file_path):
    return spark.read.format("delta").load(file_path)

def transform_battlelog_summary(spark, df):
    df.createOrReplaceTempView("battlelog")
    query_battlelog_summary = """
        WITH query_battlelog_summary AS (
            SELECT
                ingestion_time,
                raw_file_name,
                to_timestamp(battleTime, "yyyyMMdd'T'HHmmss.SSS'Z'") AS battle_time,
                gameMode.id AS game_mode_id,
                gameMode.name AS game_mode_name,
                arena.id AS arena_id,
                arena.name AS arena_name,
                team.tag AS team_tag,
                team.name AS team_name,
                team.crowns AS team_crowns,
                team.kingTowerHitPoints AS team_king_tower_hit_points,
                team.princessTowersHitPoints AS team_princess_towers_hit_points,
                team.elixirLeaked AS team_elixir_leaked,
                team.cards AS team_cards,
                opponent.tag AS opponent_tag,
                opponent.crowns AS opponent_crowns,
                opponent.kingTowerHitPoints AS opponent_king_tower_hit_points,
                opponent.princessTowersHitPoints AS opponent_princess_towers_hit_points,
                opponent.elixirLeaked AS opponent_elixir_leaked,
                opponent.cards AS opponent_cards,
                CASE WHEN team.crowns > opponent.crowns THEN True ELSE False END AS isvictory,
                sha2(concat(battletime, CAST(team.tag AS STRING), CAST(opponent.tag AS STRING)), 256) AS battle_id,
                ROW_NUMBER() OVER (PARTITION BY sha2(concat(battletime, CAST(team.tag AS STRING), CAST(opponent.tag AS STRING)), 256) ORDER BY ingestion_time DESC) AS rn
            FROM
                battlelog
        )
        SELECT *
        FROM query_battlelog_summary
        WHERE rn = 1
        ORDER BY ingestion_time DESC
    """
    return spark.sql(query_battlelog_summary)

def merge_battlelog(spark, df_battlelog_summary, battlelog_stage_path):
    target_battlelog = DeltaTable.forPath(spark, battlelog_stage_path)
    target_battlelog.alias("target").merge(
        df_battlelog_summary.alias("source"), "target.battle_id = source.battle_id"
    ).whenNotMatchedInsertAll().execute()

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
    .config(
        "spark.databricks.delta.schema.autoMerge.enabled", 
        "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")


## ====  START PROCESS BATTLELOG
date_path = date.today().strftime("%Y-%m-%d")

battlelog_bronze_path = f"s3a://apiroyale-bronze/APIRoyale/players/sub_type=battlelog/ingested_at={date_path}/players.battlelog"
battlelog_stage_path = "s3a://apiroyale-stage/APIRoyale/players/sub_type=battlelog/players.battlelog"

df = read_delta_df(spark, battlelog_bronze_path)
df_battlelog_summary = transform_battlelog_summary(spark, df)
merge_battlelog(spark, df_battlelog_summary, battlelog_stage_path)

## ===== END PROCESS BATTLELOG