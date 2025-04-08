from pyspark.sql import SparkSession

def get_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.execution.useOldCommitProtocol", "true") \
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \  # ← this is key!
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \               # ← and this!
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.native.io.library", "false") \
        .getOrCreate()
    return spark
