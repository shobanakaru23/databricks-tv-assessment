# Databricks notebook source
fact_df = spark.table("fact_show_data")

# COMMAND ----------

total_shows = fact_df.groupBy(
    "show_id", "show_name",
).count().withColumnRenamed("count", "total_shows_cnt")

# COMMAND ----------

episodes_per_season = fact_df.groupBy(
    "show_id", "show_name", "season"
).count().withColumnRenamed("count", "episode_count")

# COMMAND ----------

from pyspark.sql.functions import avg

avg_runtime = fact_df.groupBy(
    "show_id", "show_name"
).agg(avg("runtime").alias("avg_runtime"))

# COMMAND ----------

top_cast = fact_df.groupBy("cast_name") \
    .count() \
    .orderBy("count", ascending=False)

# COMMAND ----------

top_genres = fact_df.groupBy("genre") \
    .count() \
    .orderBy("count", ascending=False)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

window_spec = Window.partitionBy("show_id").orderBy(col("runtime").desc())

longest_episodes = fact_df.withColumn(
    "rank",
    rank().over(window_spec)
).filter("rank = 1")

# COMMAND ----------

gold_df = episodes_per_season \
    .join(avg_runtime, ["show_id", "show_name"])

# COMMAND ----------

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_layer")

# COMMAND ----------

from pyspark.sql.functions import broadcast

df = silver_shows.join(
    broadcast(silver_episodes),
    "show_id"
)

df.explain(True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     count(show_name) as total_shows,
# MAGIC     avg_runtime
# MAGIC FROM tvasses.default.gold_layer
# MAGIC ORDER BY total_shows DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_layer LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     show_id,
# MAGIC     show_name,
# MAGIC     episode_name,
# MAGIC     runtime,
# MAGIC     RANK() OVER (PARTITION BY show_id ORDER BY runtime DESC) AS rank
# MAGIC FROM fact_show_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     show_name,
# MAGIC     genre,
# MAGIC     COUNT(*) AS total_episodes,
# MAGIC     AVG(runtime) AS avg_runtime
# MAGIC FROM fact_show_data
# MAGIC GROUP BY show_name, genre;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fact_show_data
# MAGIC WHERE show_id = 1 AND genre = 'Drama';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     show_name,
# MAGIC     episode_count,
# MAGIC     avg_runtime
# MAGIC FROM tvasses.default.gold_layer
# MAGIC ORDER BY episode_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH filtered_data AS (
# MAGIC     SELECT * FROM fact_show_data WHERE runtime > 30
# MAGIC )
# MAGIC SELECT show_name, COUNT(*)
# MAGIC FROM filtered_data
# MAGIC GROUP BY show_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tvasses.default.gold_layer

# COMMAND ----------

df = spark.sql("""
SELECT 
    show_id,
    COUNT(*) as total
FROM tvasses.default.gold_layer
GROUP BY show_id
""")

# COMMAND ----------

df.explain(True)