
def process(spark, paths):
    df = extract(spark, paths['pre_processed_data'])
    df_qtr = transform(df)
    load(df_qtr, paths['processed_data'])
    return df_qtr


def extract(spark, path):
    df = spark.read.parquet(path)
    return df


def transform(df):
    df = df.groupby('year', 'quarter') \
        .sum('vaccinated_total', 'vaccinated_12+', 'vaccinated_18+', 'vaccinated_65+', 'vaccinated_12to18') \
        .withColumnRenamed("SUM(vaccinated_total)", "vaccinated_total") \
        .withColumnRenamed("SUM(vaccinated_12+)", "vaccinated_12+") \
        .withColumnRenamed("SUM(vaccinated_18+)", "vaccinated_18+") \
        .withColumnRenamed("SUM(vaccinated_65+)", "vaccinated_65+") \
        .withColumnRenamed("SUM(vaccinated_12to18)", "vaccinated_12to18")
    return df


def load(df, path):
    df.repartition(1).write.mode("OverWrite").option("header", True).csv(path)

