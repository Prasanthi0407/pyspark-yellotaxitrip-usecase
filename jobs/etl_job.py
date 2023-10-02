"""
etl_job.py
~~~~~~~~~~
"""

from pyspark.sql.functions import to_date

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .parquet('resources/inputData/yellow_tripdata_2020-01.parquet'))

    return df


def transform_data(df):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param
    :return: Transformed DataFrame.
    """
    startdate = "2020-01-01"
    enddate = "2020-01-31"

    df_intermittent = df.withColumn("date_type", to_date("tpep_dropoff_datetime"))

    data_transformed = df_intermittent.filter((df_intermittent.date_type >= startdate)
                                              & (df_intermittent.date_type <= enddate))

    return data_transformed


def load_data(df):
    """Collect data locally and write to parquet in mysql db.

    :param df: DataFrame to print.
    :return: None
    """
    (df
        .write.format("jdbc")
        .partitionBy("data_type")
        .mode("overwrite")
        .option("truncate", "true")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://localhost:3306/prasaddb")
        .option("dbtable", "taxitrip")
        .option("user", "root")
        .option("password", "admin")
        .save())
    return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
