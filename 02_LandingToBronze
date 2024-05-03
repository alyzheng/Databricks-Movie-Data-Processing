# Transforming Raw Data in Bronze Tables

In this section, we will process raw data and convert into three tables: movie, genre and original language.


## Stream Data 

1. Create a [`02_LandingToBronze`] notebook.

2. First let's load the data files. Run the following code foe each data file in a python notebook. 
    ```python
    from pyspark.sql.functions import explode, col
    # Read a stream from delta table
    file_path1 = "dbfs:/mnt/storagededatabricks_landing/movie_0.json"
    df1 = spark.read.option("multiline","true").json(file_path1)
    df1 = df1.select(explode(col("movie")).alias("movie"))
    ```

3. Now we need to create movie dataframe by combining the raw data. Run the following code foe each data file in a python notebook. 
    ```python
    combined_df = df1.union(df2).union(df3).union(df4).union(df5).union(df6).union(df7).union(df8)
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
    ```

4. For the movie dataframe, we need to sepeate those negiative value in RunTime and mark these records as "quarantined"
    ```python
    combined_df_movie = combined_df_movie.withColumn("quarantined", when(col("RunTime") < 0, 1).otherwise(0))
    combined_df_movie_quarantine = combined_df_movie.filter(col("quarantined") == 1)
    combined_df_movie_cleaned = combined_df_movie.filter(col("quarantined") == 0)
    ```

5. For the movie dataframe, we need to create "status" column to make sure that only latest record for each movie will be displayed
    ```python
    windowSpec = Window.partitionBy("Id").orderBy(col("CreatedDate").desc())
    combined_df_movie = combined_df_movie.withColumn("status", when(rank().over(windowSpec) == 1, "new").otherwise("loaded"))
    ```

6. Now we need to create genre dataframe 
    ```python
    combined_df_genres = combined_df.select(col("movie.genres").alias("genres"))
    combined_df_genres = combined_df_genres.select(explode(col("genres")).alias("genres"))
    combined_df_genres = combined_df_genres.select(
    col("genres.id").alias("id"),
    col("genres.name").alias("name")
    )
    ```
7. Next we need to create language dataframe
    ```python
    combined_df_language = combined_df.select(col("movie.OriginalLanguage").alias("OriginalLanguage"))
    ```

    
