import pandas
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as f
import datetime


def ingest(spark, path_dict):
    df = extract(spark, path_dict['raw_input'])
    df = validate(df)
    df = load_data(df, path_dict['raw_output'])
    return df


def extract(spark, input_path):
    pdf = pandas.read_excel(input_path)
    df_schema = StructType([StructField("Date", DateType(), True)
                               , StructField("State", StringType(), True)
                               , StructField("vaccinated_total", IntegerType(), True)
                               , StructField("vaccinated_12+", DoubleType(), True)
                               , StructField("vaccidnated_18+", IntegerType(), True)
                               , StructField("vaccinated_65+", IntegerType(), True)
                               , StructField("Metro_status", StringType(), True)])
    df = spark.createDataFrame(pdf, schema=df_schema)

    return df


def validate(df):
    # Validate the Data types
    conversions = {
        'Date': lambda c: f.from_unixtime(f.unix_timestamp(c, "yyyy-MM-dd")).cast("date"),
        'State': lambda c: f.col(c).cast('string'),
        'vaccinated_total': lambda c: f.col(c).cast('int'),
        'vaccinated_12+': lambda c: f.col(c).cast('int'),
        'vaccidnated_18+': lambda c: f.col(c).cast('int'),
        'vaccinated_65+': lambda c: f.col(c).cast('int'),
        'Metro_status': lambda c: f.col(c).cast('string')
    }

    df = df.withColumn(
        "dataTypeValidationErrors",
        f.concat_ws(", ",
                    *[
                        f.when(
                            v(k).isNull() & f.col(k).isNotNull(),
                            f.lit(k + " not valid")
                        ).otherwise(f.lit(None))
                        for k, v in conversions.items()])
                    )\

    # Validate the data values
    df = df.withColumn('validMetro', f.when(f.col('Metro_status') != 'Non-metro',
                                            f.lit('NO')).otherwise(f.lit('YES')))

    df = df.withColumnRenamed('vaccidnated_18+', 'vaccinated_18+')\
        .withColumn('vaccinated_12+', col('vaccinated_12+').cast(IntegerType()))

    df_valid = df.filter(col('dataTypeValidationErrors') == '')\
        .filter(col('validMetro') == 'YES')\
        .drop('dataTypeValidationErrors').drop('validMetro')

    return df_valid


def load_data(df, path):

    my_date = datetime.date.today()  # if date is 01/01/2018
    year, week_num, day_of_week = my_date.isocalendar()
    print("********Week #" + str(week_num) + " of year " + str(year))

    """
    We can ingest the weekly data based on the current year and week
    Or we can also use partition by deriving new columns : year and week_num
    """
    df.write.mode("overwrite").parquet(path+str(year)+'/'+str(week_num)+'/')

    return df


def filter_invalid_data(df):
    df_invalid = df.filter(col('dataTypeValidationErrors') != '')
    return df_invalid

