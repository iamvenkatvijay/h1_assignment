from pyspark.sql import functions as f
from pyspark.sql.functions import col


def preprocess(spark, paths):
    print('Curation process started')
    df = extract(spark, paths['raw_output'])
    df = transform(df)
    df = load_curated_data(df, paths['pre_processed_data'])
    return df


def extract(spark, path):
    df = spark.read.parquet(path)
    return df


def transform(df):
    df = df.withColumn('year', f.year('Date')) \
        .withColumn('week_no', f.weekofyear('Date'))\
        .withColumn('quarter', f.quarter('Date'))\
        .withColumn('vaccinated_12to18', col('vaccinated_12+')-col('vaccinated_18+'))
    return df


def load_curated_data(df, path):
    # To avoid deleting all partitions, use below conf in code or spark-submit
    # spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    df.write.mode("overwrite").partitionBy("year", "week_no").parquet(path)
    return df
