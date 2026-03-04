from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, ArrayType

twitch_stream_schema = StructType([
    StructField("ingestion_timestamp_utc", TimestampType(), True),
    StructField("twitch_data", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("game_id", StringType(), True),
            StructField("game_name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("viewer_count", LongType(), True),
            StructField("started_at", StringType(), True),
            StructField("language", StringType(), True),
            StructField("thumbnail_url", StringType(), True),
            StructField("tag_ids", ArrayType(StringType()), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("is_mature", StringType(), True)
        ])
    ), True)
])

schema_steam = StructType([
    StructField("ingestion_timestamp_utc", TimestampType(), True),
    StructField("id", StringType(), True),
    StructField("jogo", StringType(), True),
    StructField("qtd_jogadores", LongType(), True)
])

configs = {

    "twitch": {
        "raw": "/Volumes/workspace/default/my_volume/raw2/twitch/",
        "checkpoint": "/Volumes/workspace/default/my_volume/checkpoint/bronze/twitch",
        "schema_location": "/Volumes/workspace/default/my_volume/schema/twitch",
        "schema": twitch_stream_schema,
        "table": "bronze_twitch"

    },
    "steam": {

        "raw": "/Volumes/workspace/default/my_volume/raw2/steam/",
        "checkpoint": "/Volumes/workspace/default/my_volume/checkpoint/bronze/steam",
        "schema_location": "/Volumes/workspace/default/my_volume/schema/steam",
        "schema": schema_steam,
        "table": "bronze_steam"
    }

}

def ingest_bronze(name, cfg):
    df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", cfg["schema_location"])
        .option("cloudFiles.maxFilesPerTrigger", 1000) 
        .schema(cfg["schema"])
        .load(cfg['raw']))

    query = (df.writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", cfg["checkpoint"])
             # Ativa Optimize Write para esse stream (nativo do Databricks)
             .option("optimizeWrite", "true") 
             .trigger(availableNow=True)
             .toTable(cfg["table"]))

    query.awaitTermination()

for source_name, source_cfg in configs.items():
    ingest_bronze(source_name, source_cfg)