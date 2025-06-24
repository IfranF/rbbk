from pyspark.sql import SparkSession


def get_spark(app_name):
    return (SparkSession.builder
            .appName(app_name)
            .getOrCreate()
            )


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils