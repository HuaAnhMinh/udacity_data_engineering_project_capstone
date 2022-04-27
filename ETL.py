import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col
from py4j.protocol import Py4JJavaError


def spark_init():
    """Initialize SparkSession

    Steps:
        1. Save aws credentials.
        2. Create SparkSession.

    Return:
        spark: Spark Session
    """
    os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAVM4CLAILYNCUGLC3'
    os.environ['AWS_SECRET_ACCESS_KEY'] = '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'

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def read_data_source(spark):
    """Read data source from S3.

    Args:
        spark: Spark Session

    Steps:
        1. Read imdb movies metadata.
        2. Read imdb ratings.
        3. Read movielens metadata for movies.
        4. Read movielens ratings.

    Return:
        imdb_movies_metadata: DataFrame
        imdb_ratings: DataFrame
        movielens_movies: DataFrame
        movielens_ratings: DataFrame

    Raises:
        Exception: If data source is not found.
    """
    imdb_movies_metadata = spark.read.csv("s3a://minhha4-capstone-data-source/imdb_metadata/title.basics.tsv",
                                          header=True, sep="\t", inferSchema=True)
    imdb_ratings = spark.read.json("s3a://minhha4-capstone-data-source/imdb_ratings/sample.json", multiLine=True)
    movielens_metadata = spark.read.csv("s3a://minhha4-capstone-data-source/movielens/movies_metadata.csv",
                                        header=True, inferSchema=True)
    movielens_ratings = spark.read.csv("s3a://minhha4-capstone-data-source/movielens/ratings.csv",
                                       header=True, inferSchema=True)

    return imdb_movies_metadata, imdb_ratings, movielens_metadata, movielens_ratings


def process_movies_data(imdb_movies_metadata):
    """Load data into movies_df table.

    Args:
        imdb_movies_metadata: DataFrame for imdb movies metadata.

    Steps:
        1. Select only required columns.
        2. Rename columns.
        3. Remove any row that has imdb_id or original_title or start_year is null.
        4. Remove any row that has duplicate imdb_id or duplicate combination of original_title and start_year.
        5. Cast data type (start_year -> int, end_year -> int, runtime_minutes -> int).
        6. Data quality checks for movies_df.
        7. Repartition data by start_year to improve performance.

    Returns:
        movies_df: DataFrame

    Raises:
        Exception: start_year column has wrong data type.
        Exception: end_year column has wrong data type.
        Exception: runtime_minutes column has wrong data type.
        Exception: There is imdb_id that is not in imdb_movies_metadata.
    """

    # Select necessary columns
    movies_df = imdb_movies_metadata \
        .select(["tconst", "primaryTitle", "originalTitle", "startYear", "endYear", "runtimeMinutes"])

    # Rename columns to match the data model
    movies_df = movies_df.withColumnRenamed("tconst", "imdb_id")
    movies_df = movies_df.withColumnRenamed("primaryTitle", "primary_title")
    movies_df = movies_df.withColumnRenamed("originalTitle", "original_title")
    movies_df = movies_df.withColumnRenamed("startYear", "start_year")
    movies_df = movies_df.withColumnRenamed("endYear", "end_year")
    movies_df = movies_df.withColumnRenamed("runtimeMinutes", "runtime_minutes")

    # Remove any row that has imdb_id or original_title or start_year is null
    movies_df = movies_df.filter(movies_df.imdb_id.isNotNull())
    movies_df = movies_df.filter(movies_df.original_title.isNotNull())
    movies_df = movies_df.filter(movies_df.start_year.isNotNull())

    # Remove any row that has duplicate imdb_id or duplicate combination of original_title and start_year
    movies_df = movies_df.dropDuplicates(["imdb_id"])
    movies_df = movies_df.dropDuplicates(["original_title", "start_year"])

    # Cast data type (start_year -> int, end_year -> int, runtime_minutes -> int)
    try:
        movies_df = movies_df.withColumn("start_year", movies_df["start_year"].cast("integer"))
    except Py4JJavaError:
        raise Exception("start_year column has wrong data type")

    try:
        movies_df = movies_df.withColumn("end_year", movies_df["end_year"].cast("integer"))
    except Py4JJavaError:
        raise Exception("end_year column has wrong data type")

    try:
        movies_df = movies_df.withColumn("runtime_minutes", movies_df["runtime_minutes"].cast("integer"))
    except Py4JJavaError:
        raise Exception("runtime_minutes column has wrong data type")

    movies_df = movies_df.withColumn("runtime_minutes",
                                     when(movies_df.runtime_minutes == 0, None).otherwise(movies_df.runtime_minutes))

    # Data quality checks for movies_df
    movies_df = movies_df.select("*").where("start_year <= end_year or end_year is null")
    movies_df = movies_df.filter(movies_df.runtime_minutes >= 0)
    if movies_df.join(imdb_movies_metadata, movies_df.imdb_id == imdb_movies_metadata.tconst, "leftanti").count() > 0:
        raise Exception("There is imdb_id that is not in imdb_movies_metadata")

    # Repartition data by start_year to improve performance
    movies_df = movies_df.repartition(movies_df.start_year)

    return movies_df


def process_users_data(spark, imdb_ratings, movielens_ratings):
    """Load data into users_df table.

    Args:
        spark: SparkSession
        imdb_ratings: DataFrame for ratings on IMDB
        movielens_ratings: DataFrame for ratings on MovieLens

    Steps:
        1. Create view for imdb_ratings, movielens_ratings.
        2. Load platform id and platform name to users_df.
        3. Add user id.
        4. Data quality checks for users_df.

    Returns:
        users_df: DataFrame for users

    Raises:
        Exception: There is platform_id that is null.
        Exception: There is platform that is not imdb or movielens
        Exception: There is id that is null.
        Exception: There is userId that is not in movielens_ratings.
        Exception: There is userId that is not in imdb_ratings.
    """

    # Create view for imdb_ratings, movielens_ratings
    imdb_ratings.createOrReplaceTempView("imdb_ratings")
    movielens_ratings.createOrReplaceTempView("movielens_ratings")

    # Load platform id and platform name to users_df
    users_df = spark.sql("""
        SELECT DISTINCT
            userId AS platform_id,
            'movielens' AS platform
        FROM movielens_ratings
        WHERE userId IS NOT NULL
        UNION
        SELECT DISTINCT
            review_id AS platform_id,
            'imdb' AS platform
        FROM imdb_ratings
        WHERE review_id IS NOT NULL
    """)

    # Add user id
    users_df = users_df.withColumn("id", row_number().over(Window.orderBy(col("platform_id"))))

    # Data quality checks for users_df
    if users_df.filter("platform_id is null").count() > 0:
        raise Exception("There is platform_id that is null")

    if users_df.filter((users_df.platform != "imdb") & (users_df.platform != "movielens")).count() > 0:
        raise Exception("There is platform that is not imdb or movielens")

    if users_df.filter("id is null").count() > 0:
        raise Exception("There is id that is null")

    valid_movielens = users_df.filter(users_df.platform == 'movielens')
    if valid_movielens.join(movielens_ratings, valid_movielens.platform_id == movielens_ratings.userId, "leftanti")\
            .count() > 0:
        raise Exception("There is userId that is not in movielens_ratings")

    valid_imdb = users_df.filter(users_df.platform == 'imdb')
    if valid_imdb.join(imdb_ratings, valid_imdb.platform_id == imdb_ratings.review_id, "leftanti").count() > 0:
        raise Exception("There is review_id that is not in imdb_ratings")

    return users_df


def process_timestamp_data(spark):
    """Load data into timestamp_df table.

    Args:
        spark: SparkSession

    Steps:
        1. Load timestamp data to timestamp_df.
        2. Repartition data by year and month to improve performance.
        3. Data quality checks for timestamp_df.

    Returns:
        timestamp_df: DataFrame for timestamp

    Raises:
        Exception: There is duplicate timestamp.
        Exception: There is a row that has wrong extraction.
    """

    # Load data into timestamp_df table.
    timestamp_df = spark.sql("""
        SELECT DISTINCT
            timestamp,
            DATE_FORMAT(timestamp, 'yyyy') AS year,
            DATE_FORMAT(timestamp, 'MM') AS month,
            DATE_FORMAT(timestamp, 'dd') AS day
        FROM (
            SELECT DISTINCT
                DATE_FORMAT(FROM_UNIXTIME(timestamp), 'yyyy-MM-dd') AS timestamp
            FROM movielens_ratings
            WHERE timestamp is not null
            UNION
            SELECT DISTINCT
                DATE_FORMAT(review_date, 'yyyy-MM-dd') AS timestamp
            FROM imdb_ratings
            WHERE review_date is not null
        )
    """)

    # Repartition data by year and month to improve performance
    timestamp_df = timestamp_df.repartition(timestamp_df.year, timestamp_df.month)

    # Data quality checks for timestamp_df
    if timestamp_df.filter("timestamp is null").count() > 0:
        timestamp_df = timestamp_df.filter("timestamp is not null")

    count_timestamp = timestamp_df.count()
    if timestamp_df.select("timestamp").distinct().count() != count_timestamp:
        raise Exception("There is duplicate timestamp")

    timestamp_df.createOrReplaceTempView("timestamp")
    extraction_df = spark.sql("""
        SELECT count(count_correct_year), count(count_correct_month), count(count_correct_day)
        FROM
            (SELECT
                DATE_FORMAT(timestamp.timestamp, 'yyyy') = year AS count_correct_year,
                DATE_FORMAT(timestamp.timestamp, 'MM') = month AS count_correct_month,
                DATE_FORMAT(timestamp.timestamp, 'dd') = day AS count_correct_day
            FROM
                timestamp)
        WHERE count_correct_year = true AND count_correct_month = true AND count_correct_day = true
    """)
    if extraction_df.select("count(count_correct_year)").first()[0] != count_timestamp or \
            extraction_df.select("count(count_correct_month)").first()[0] != count_timestamp or \
            extraction_df.select("count(count_correct_day)").first()[0] != count_timestamp:
        raise Exception("There is a row that has wrong extraction")

    return timestamp_df


def process_ratings_data(spark, movies_df, movielens_metadata, users_df, timestamp_df):
    """Load data into ratings_df table.

    Args:
        spark: SparkSession
        movies_df: DataFrame for movies
        movielens_metadata: DataFrame for movielens_metadata
        users_df: DataFrame for users
        timestamp_df: DataFrame for timestamp

    Steps:
        1. Create view for movies_df, movielens_metadata, users_df.
        2. Create staging table for original_title and start_year of a movie.
        3. Load data into ratings_df table.
        4. Data quality checks for ratings_df

    Returns:
        ratings_df: DataFrame for ratings

    Raises:
        Exception: There is a row that has wrong user_id.
        Exception: There is a row that has wrong movie_id.
        Exception: There is a row that has wrong rating_date.
        Exception: Score must be from 0 to 5.
        Exception: There is a row that has null score.
    """

    # Create view for movies_df, movielens_metadata, users_df
    movies_df.createOrReplaceTempView("movies")
    movielens_metadata.createOrReplaceTempView("movielens_metadata")
    users_df.createOrReplaceTempView("users")

    # Create staging table for original_title and start_year of a movie
    imdb_ratings_split_movies_and_years = spark.sql("""
        SELECT
            rating,
            review_date,
            review_id,
            SPLIT(movie, '[()]')[0] AS original_title,
            SUBSTR(SPLIT(movie, '[()]')[1], 1, 4) AS year
        FROM imdb_ratings
        WHERE movie IS NOT NULL
    """)
    imdb_ratings_split_movies_and_years.createOrReplaceTempView("imdb_ratings_split_movies_and_years")

    # Load data into ratings_df table.
    ratings_df = spark.sql("""
        SELECT
            m2.imdb_id AS movie_id,
            u.id AS user_id,
            DATE_FORMAT(FROM_UNIXTIME(r1.timestamp), 'yyyy-MM-dd') AS rating_date,
            r1.rating AS score
        FROM movielens_ratings r1
        INNER JOIN movielens_metadata m1
            ON r1.movieId = m1.id
        INNER JOIN movies m2
            ON m1.imdb_id = m2.imdb_id
        INNER JOIN users u
            ON r1.userId = u.platform_id
        WHERE u.platform = 'movielens' AND r1.rating IS NOT NULL
        UNION
        SELECT
            m1.imdb_id AS movie_id,
            u.id AS user_id,
            DATE_FORMAT(review_date, 'yyyy-MM-dd') AS rating_date,
            (m2.rating / 2) AS score
        FROM movies m1
        INNER JOIN imdb_ratings_split_movies_and_years m2
            ON m1.original_title = m2.original_title AND m1.start_year = m2.year
        INNER JOIN users u
            ON m2.review_id = u.platform_id
        WHERE u.platform = 'imdb' AND m2.rating IS NOT NULL
    """)

    # Data quality checks for ratings_df
    if ratings_df.join(users_df, ratings_df.user_id == users_df.id, 'leftanti').count() > 0:
        raise Exception("There is a row that has wrong user_id")

    if ratings_df.join(movies_df, ratings_df.movie_id == movies_df.imdb_id, 'leftanti').count() > 0:
        raise Exception("There is a row that has wrong movie_id")

    if ratings_df.join(timestamp_df, ratings_df.rating_date == timestamp_df.timestamp, 'leftanti').count() > 0:
        raise Exception("There is a row that has wrong rating_date")

    if ratings_df.filter((ratings_df.score < 0) | (ratings_df.score > 5)).count() > 0:
        raise Exception("Score must be from 0 to 5")

    if ratings_df.filter("score is null").count() > 0:
        raise Exception("There is a row that has null score")

    return ratings_df


def save_to_files(movies_df, users_df, timestamp_df, ratings_df):
    """Save data to files.

    Args:
        movies_df: Dataframe of movies.
        users_df: Dataframe of users.
        timestamp_df: Dataframe of timestamp.
        ratings_df: Dataframe of ratings.

    Steps:
        1. Save movies_df to movies.csv.
        2. Save users_df to users.csv.
        3. Save timestamp_df to timestamp.csv.
        4. Save ratings_df to ratings.csv.
    """

    movies_df.coalesce(1).write.csv("./destination/movies")
    users_df.coalesce(1).write.csv("./destination/users")
    timestamp_df.coalesce(1).write.csv("./destination/timestamp")
    ratings_df.coalesce(1).write.csv("./destination/ratings")


def main():
    spark = spark_init()
    imdb_movies_metadata, imdb_ratings, movielens_metadata, movielens_ratings = read_data_source(spark)
    movies_df = process_movies_data(imdb_movies_metadata)
    users_df = process_users_data(spark, imdb_ratings, movielens_ratings)
    timestamp_df = process_timestamp_data(spark)
    ratings_df = process_ratings_data(spark, movies_df, movielens_metadata, users_df, timestamp_df)
    save_to_files(movies_df, users_df, timestamp_df, ratings_df)


if __name__ == "__main__":
    main()
