# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Read anc combine the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

# MAGIC %md
# MAGIC Deal with movie_0.json

# COMMAND ----------

file_path1 = "dbfs:/mnt/storagededatabricks_landing/movie_0.json"
df1 = spark.read.option("multiline","true").json(file_path1)

# COMMAND ----------

from pyspark.sql.functions import explode, col

# Explode the 'movie' column into separate rows
df1 = df1.select(explode(col("movie")).alias("movie"))

# COMMAND ----------

# MAGIC %md
# MAGIC Deal with movie_1.json

# COMMAND ----------

file_path2 = "dbfs:/mnt/storagededatabricks_landing/movie_1.json"
df2 = spark.read.option("multiline","true").json(file_path2)

# COMMAND ----------

# Explode the 'movie' column into separate rows
df2 = df2.select(explode(col("movie")).alias("movie"))

# COMMAND ----------

# MAGIC %md
# MAGIC Deal with movie_2.json

# COMMAND ----------

file_path3 = "dbfs:/mnt/storagededatabricks_landing/movie_2.json"
df3 = spark.read.option("multiline","true").json(file_path3)

# COMMAND ----------

df3 = df3.select(explode(col("movie")).alias("movie"))

# COMMAND ----------

# MAGIC %md
# MAGIC Deal with movie_3.json

# COMMAND ----------

file_path4 = "dbfs:/mnt/storagededatabricks_landing/movie_3.json"
df4 = spark.read.option("multiline","true").json(file_path4)

# COMMAND ----------

df4 = df4.select(explode(col("movie")).alias("movie"))

# COMMAND ----------

# MAGIC %md
# MAGIC Deal with movie_4.json

# COMMAND ----------

file_path5 = "dbfs:/mnt/storagededatabricks_landing/movie_4.json"
df5 = spark.read.option("multiline","true").json(file_path5)

# COMMAND ----------

df5 = df5.select(explode(col("movie")).alias("movie"))

# COMMAND ----------

# MAGIC %md
# MAGIC Deal with movie_5.json

# COMMAND ----------

file_path6 = "dbfs:/mnt/storagededatabricks_landing/movie_5.json"
df6 = spark.read.option("multiline","true").json(file_path6)

# COMMAND ----------

df6 = df6.select(explode(col("movie")).alias("movie"))

# COMMAND ----------

# MAGIC %md
# MAGIC Deal with movie_6.json

# COMMAND ----------

file_path7 = "dbfs:/mnt/storagededatabricks_landing/movie_6.json"
df7 = spark.read.option("multiline","true").json(file_path7)

# COMMAND ----------

df7 = df7.select(explode(col("movie")).alias("movie"))

# COMMAND ----------

# MAGIC %md
# MAGIC Deal with movie_7.json

# COMMAND ----------

file_path8 = "dbfs:/mnt/storagededatabricks_landing/movie_7.json"
df8 = spark.read.option("multiline","true").json(file_path8)

# COMMAND ----------

df8 = df8.select(explode(col("movie")).alias("movie"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Combine the data
# MAGIC
# MAGIC Now that we have created our DataFrame, we can combine them into movie, genres and orginal_language

# COMMAND ----------

# MAGIC %md
# MAGIC create movie table

# COMMAND ----------

# combine 8 dataframe
combined_df = df1.union(df2).union(df3).union(df4).union(df5).union(df6).union(df7).union(df8)

# COMMAND ----------

# select necessary columns
combined_df_movie = combined_df.select(
  col("movie.Id").alias("Id"),
  col("movie.Title").alias("Title"),
  col("movie.Overview").alias("Overview"),
  col("movie.Tagline").alias("Tagline"),
  col("movie.Budget").alias("Budget"),
  col("movie.Revenue").alias("Revenue"),
  col("movie.ImdbUrl").alias("ImdbUrl"),
  col("movie.TmdbUrl").alias("TmdbUrl"),
  col("movie.PosterUrl").alias("PosterUrl"),
  col("movie.BackdropUrl").alias("BackdropUrl"),
  col("movie.ReleaseDate").alias("ReleaseDate"),
  col("movie.RunTime").alias("RunTime"),
  col("movie.Price").alias("Price"),
  col("movie.CreatedDate").alias("CreatedDate")
)

# COMMAND ----------

# MAGIC %md
# MAGIC create genres table

# COMMAND ----------

# Select the 'genres' column 
combined_df_genres = combined_df.select(col("movie.genres").alias("genres"))

# COMMAND ----------

# Explode the 'genres' array column into separate rows
combined_df_genres = combined_df_genres.select(explode(col("genres")).alias("genres"))

# COMMAND ----------

# Select the 'id' and 'name' columns from the exploded 'genres' DataFrame
combined_df_genres = combined_df_genres.select(
col("genres.id").alias("id"),
col("genres.name").alias("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC create original_language table

# COMMAND ----------

combined_df_language = combined_df.select(col("movie.OriginalLanguage").alias("OriginalLanguage"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Validate the data
# MAGIC Now that we have created our table, we need to validate the data

# COMMAND ----------

# MAGIC %md
# MAGIC Create column "status" to separate loaded and new data

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, when

# Create a window specification based on Id and CreatedDate in descending order
windowSpec = Window.partitionBy("Id").orderBy(col("CreatedDate").desc())

# COMMAND ----------

# Assign "new" to the movie with the latest CreatedDate and "loaded" to others
combined_df_movie = combined_df_movie.withColumn("status", when(rank().over(windowSpec) == 1, "new").otherwise("loaded"))

# COMMAND ----------

from pyspark.sql.functions import col, when

# mark "quarantined" in movie table
combined_df_movie = combined_df_movie.withColumn("quarantined", when(col("RunTime") < 0, 1).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC Create column "quarantined" to seperate cleaned and dirty data

# COMMAND ----------

# seperate quarantined records and clean records
combined_df_movie_quarantine = combined_df_movie.filter(col("quarantined") == 1)
combined_df_movie_cleaned = combined_df_movie.filter(col("quarantined") == 0)

# COMMAND ----------

combined_df_movie_quarantine.count(), combined_df_movie_cleaned.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Save the data
# MAGIC Now that we have created our table, we can save them as delta file

# COMMAND ----------

#save combined dataframe as delta files
combined_df.write.format("delta").mode("overwrite").save("/mnt/storagededatabricks_bronze/full_data")

# COMMAND ----------

#save movie dataframe as delta files
combined_df_movie_quarantine.write.format("delta").mode("overwrite").save("/mnt/storagededatabricks_bronze/movie_quarantined")
combined_df_movie_cleaned.write.format("delta").mode("overwrite").save("/mnt/storagededatabricks_bronze/movie_cleaned")

# COMMAND ----------

#save genres dataframe as delta files
combined_df_genres.write.format("delta").mode("overwrite").save("/mnt/storagededatabricks_bronze/genres")

# COMMAND ----------

#save language dataframe as delta files
combined_df_language.write.format("delta").mode("overwrite").save("/mnt/storagededatabricks_bronze/language")
