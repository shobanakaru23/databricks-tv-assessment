# Databricks notebook source
df_shows = spark.table("bronze_shows")
df_episodes = spark.table("bronze_episodes")
df_cast = spark.table("bronze_cast")

# COMMAND ----------

from pyspark.sql.functions import col

silver_shows = df_shows.select(
    col("id").alias("show_id"),
    col("name").alias("show_name"),
    col("language"),
    col("genres"),
    col("premiered")
)

# COMMAND ----------

from pyspark.sql.functions import explode

silver_shows = silver_shows.withColumn("genre", explode(col("genres"))) \
                           .drop("genres")

# COMMAND ----------

silver_episodes = df_episodes.select(
    col("id").alias("episode_id"),
    col("show_id"),
    col("name").alias("episode_name"),
    col("season"),
    col("number").alias("episode_number"),
    col("airdate"),
    col("runtime")
)

# COMMAND ----------

silver_cast = df_cast.select(
    col("show_id"),
    col("person.id").alias("person_id"),
    col("person.name").alias("cast_name"),
    col("character.name").alias("character_name")
)

# COMMAND ----------

from pyspark.sql.functions import when

silver_episodes = silver_episodes.withColumn(
    "runtime",
    when(col("runtime").isNull(), 0).otherwise(col("runtime"))
)

# COMMAND ----------

silver_episodes.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .saveAsTable("silver_episodes")

# COMMAND ----------

silver_episodes.filter("show_id IS NULL").show()

# COMMAND ----------

silver_shows.write.format("delta").mode("overwrite").saveAsTable("silver_shows")
silver_episodes.write.format("delta").mode("overwrite").saveAsTable("silver_episodes")
silver_cast.write.format("delta").mode("overwrite").saveAsTable("silver_cast")

# COMMAND ----------

from pyspark.sql.functions import rand, floor

salted_shows = silver_shows.withColumn("salt", floor(rand() * 5))
salted_episodes = silver_episodes.withColumn("salt", floor(rand() * 5))

# COMMAND ----------

fact_df = salted_shows.join(
    salted_episodes,
    ["show_id", "salt"]
)

# COMMAND ----------

fact_df = fact_df.drop("salt")

# COMMAND ----------

from pyspark.sql.functions import broadcast

fact_df = silver_shows.alias("s") \
    .join(broadcast(silver_episodes).alias("e"), "show_id") \
    .join(silver_cast.alias("c"), "show_id", "left") \
    .select(
        "show_id",
        "show_name",
        "language",
        "genre",
        "season",
        "episode_name",
        "airdate",
        "runtime",
        "cast_name",
        "character_name"
    )

# COMMAND ----------



# COMMAND ----------

fact_df_b.explain(True)

# COMMAND ----------

fact_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("show_id") \
    .saveAsTable("fact_show_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE fact_show_data
# MAGIC ZORDER BY (genre)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_show_data LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_episodes

# COMMAND ----------

def test_runtime_positive(spark):
    df = spark.table("silver_episodes")
    assert df.filter("runtime <= 0").count() == 0

# COMMAND ----------

pip install pytest pyspark

# COMMAND ----------

!pytest test_module.py

# COMMAND ----------

 df = spark.table("silver_shows")
    assert df.count() == df.select("id").distinct().count()
    

# COMMAND ----------

def test_runtime_positive(spark):
    df = spark.table("silver_episodes")
    assert df.filter("runtime <= 0").count() == 0

# COMMAND ----------

pytest test_data_quality.py