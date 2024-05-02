# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1: Process the data

# COMMAND ----------

# MAGIC %md
# MAGIC Fix quarantined records

# COMMAND ----------

df_movie_quar = spark.read.format("delta").option("header", "true").load("/mnt/storagededatabricks_bronze/movie_quarantined")

# COMMAND ----------

from pyspark.sql.functions import abs

df_movie_quar = df_movie_quar.withColumn('RunTime',abs(df_movie_quar["RunTime"]))

# COMMAND ----------

df_movie_cleaned = spark.read.format("delta").option("header", "true").load("/mnt/storagededatabricks_bronze/movie_cleaned")

# COMMAND ----------

df_movie = df_movie_quar.union(df_movie_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC Check genre id and name

# COMMAND ----------

df_genre = spark.read.format("delta").option("header", "true").load("/mnt/storagededatabricks_bronze/genres")

# COMMAND ----------

from pyspark.sql.functions import col

missing_genre_records = df_genre.filter(col("id").isNotNull() & col("name").isNull())

# COMMAND ----------

display(missing_genre_records)

# COMMAND ----------

# MAGIC %md
# MAGIC Correct budget

# COMMAND ----------

from pyspark.sql.functions import when

df_movie = df_movie.withColumn("budget", when(col("budget") < 1000000, 1000000).otherwise(col("budget")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Save the data

# COMMAND ----------

#save movie dataframe as delta files
df_movie.write.format("delta").mode("overwrite").save("/mnt/storagededatabricks_silver/movie")

# COMMAND ----------

#save genres dataframe as delta files
df_genre.write.format("delta").mode("overwrite").save("/mnt/storagededatabricks_bronze/genres")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
