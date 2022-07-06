import os

from datetime import datetime as dtu

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from tools.const import DATE_TIME_FORMAT
from tools.data_files import get_waiting_data_paths, move_complete_data


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("dim_reviewer_etl")\
        .getOrCreate()

    logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    keyspace = spark.conf.get("spark.keyspace.name")
    data_path = os.path.join(os.environ['PROJECT_PATH'], 'data')

    for file_path in get_waiting_data_paths(data_path, 'reviews'):

        json_df =  spark.read.json(file_path)

        result_df = json_df.select("reviewerID", "reviewerName")\
                        .withColumn("etl_processed_dttm", lit(dtu.strftime(dtu.utcnow(), DATE_TIME_FORMAT)))\
                        .withColumnRenamed("reviewerID", "reviewer_id")\
                        .withColumnRenamed("reviewerName", "reviewer_name")

        result_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="dim_reviewer", keyspace=keyspace)\
            .save()

        logger.info(f'{spark.sparkContext.appName}: File {file_path} successfully processed.')
        
        move_complete_data(file_path)

    spark.stop()
