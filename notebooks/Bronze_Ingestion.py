# Databricks notebook source
import requests
import json
from pyspark.sql import SparkSession

# COMMAND ----------

shows_url = "https://api.tvmaze.com/shows"
shows_response = requests.get(shows_url)
shows_data = shows_response.json()

# COMMAND ----------

print(shows_data)

# COMMAND ----------

episodes_data = []
cast_data = []

for show in shows_data[:50]: 
    show_id = show["id"]
    
    ep_url = f"https://api.tvmaze.com/shows/{show_id}/episodes"
    cast_url = f"https://api.tvmaze.com/shows/{show_id}/cast"
    
    try:
        # Episodes
        episodes = requests.get(ep_url).json()
        for ep in episodes:
            ep["show_id"] = show_id   
            episodes_data.append(ep)
        
        # Cast
        casts = requests.get(cast_url).json()
        for c in casts:
            c["show_id"] = show_id  
            cast_data.append(c)
            
    except Exception as e:
        print(f"Error for show_id {show_id}: {e}")

# COMMAND ----------

import json

df_shows = spark.read.json(
    spark.sparkContext.parallelize([json.dumps(shows_data)])
)

# COMMAND ----------

df_episodes = spark.read.json(
    spark.sparkContext.parallelize([json.dumps(episodes_data)])
)

# COMMAND ----------

df_cast = spark.read.json(
    spark.sparkContext.parallelize([json.dumps(cast_data)])
)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.tvassessmentnew.dfs.core.windows.net",
  "fkH8wiGcAJYHsvtd1g5ujKqq0eysCue+Bw45LBHxeX6VH0VVMchKXkM8+mURfWepfcLgOkhbm9Io+AStp0rD4A=="
)

# COMMAND ----------

df_shows.write.mode("overwrite").json(
    "abfss://raw@tvassessmentnew.dfs.core.windows.net/shows/"
)

df_episodes.write.mode("overwrite").json(
    "abfss://raw@tvassessmentnew.dfs.core.windows.net/episodes/"
)

df_cast.write.mode("overwrite").json(
    "abfss://raw@tvassessmentnew.dfs.core.windows.net/cast/"
)

# COMMAND ----------


bronze_shows_df = spark.read.json("abfss://raw@tvassessmentnew.dfs.core.windows.net/shows/")
bronze_episodes_df = spark.read.json("abfss://raw@tvassessmentnew.dfs.core.windows.net/episodes/")
bronze_cast_df = spark.read.json("abfss://raw@tvassessmentnew.dfs.core.windows.net/cast/")

# COMMAND ----------

#SCHEMA EVOLUTION

bronze_shows_df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("bronze_shows")

bronze_episodes_df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("bronze_episodes")

bronze_cast_df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("bronze_cast")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tvasses.default.tv_shows AS 
# MAGIC SELECT * FROM tvasses.default.bronze_shows;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tvasses.default.tv_episodes AS 
# MAGIC SELECT * FROM tvasses.default.bronze_episodes;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tvasses.default.tv_cast AS 
# MAGIC SELECT * FROM tvasses.default.bronze_cast;

# COMMAND ----------

spark.sql("SELECT * FROM tv_shows LIMIT 10").show()
spark.sql("SELECT * FROM tv_episodes LIMIT 10").show()
spark.sql("SELECT * FROM tv_cast LIMIT 10").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE tvasses.default.tv_shows 
# MAGIC TO `shobana@gmail.com`;