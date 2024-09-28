import pyspark
from pyspark.sql import SparkSession

# Create Spark session with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("Python Spark SQL basic example") \
    .config('spark.driver.extraClassPath', "/Users/shakthimaha/Desktop/flask/postgresql-42.2.29.jre7.jar") \
    .getOrCreate()

def extract_users_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/etl_pipeline") \
        .option("dbtable", "users") \
        .option("user", "postgres") \
        .option("password", "ROOT123") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return users_df

def extract_movies_to_df():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/etl_pipeline") \
        .option("dbtable", "movies") \
        .option("user", "postgres") \
        .option("password", "ROOT123") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return movies_df

def transform_avg_ratings(movies_df, users_df):
    # Calculate average rating for each movie
    avg_rating = users_df.groupBy("movie_id").mean("rating")
    
    # Rename the column for clarity
    avg_rating = avg_rating.withColumnRenamed("avg(rating)", "avg_rating")
    
    # Join the movies_df with avg_rating DataFrame on movie_id
    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)
    
    # Optionally drop the movie_id column from the joined DataFrame
    df = df.drop("movie_id")
    
    return df

def load_df_to_db(df):
    # Define writing mode
    mode = "overwrite"
    
    # PostgreSQL JDBC URL
    url = "jdbc:postgresql://localhost:5433/etl_pipeline"
    
    # Connection properties
    properties = {
        "user": "postgres",   # Corrected username
        "password": "ROOT123",
        "driver": "org.postgresql.Driver"
    }
    
    # Write DataFrame to PostgreSQL
    df.write.jdbc(
        url=url,
        table="average_ratings",  # Correct table name
        mode=mode,
        properties=properties
    )

# Main program
if __name__ == "__main__":
    # Extract movies and users data
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    
    # Transform data by calculating average ratings
    ratings_df = transform_avg_ratings(movies_df, users_df)
    
    # Load the transformed data into the database
    load_df_to_db(ratings_df)