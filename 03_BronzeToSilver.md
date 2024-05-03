# Transforming Bronze Tables in Silver Tables

In this section, we will convert bronze tables to silver tables.


## Process movie dataframe

1. Create a [`03_BronzeToSilver`] notebook.

2. For RunTime, take the absolute value of the original negative value, and combine cleaned and quarantined records 
    ```python
    from pyspark.sql.functions import abs
    df_movie_quar = df_movie_quar.withColumn('RunTime',abs(df_movie_quar["RunTime"]))
    df_movie = df_movie_quar.union(df_movie_cleaned)
    ```
    
3. Correct budget less than 1 million, we should replace it with 1 million
   ```python
   df_movie = df_movie.withColumn("budget", when(col("budget") < 1000000, 1000000).otherwise(col("budget")))
   ```

## Process genre dataframe

1. Check whether there is missing values in genre name
    ```python
    missing_genre_records = df_genre.filter(col("id").isNotNull() & col("name").isNull())
    display(missing_genre_records)
    ```
