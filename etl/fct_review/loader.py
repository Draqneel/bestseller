import os

from datetime import datetime as dtu

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, udf

from tools.const import DATE_TIME_FORMAT, DATE_FORMAT
from tools.data_files import get_waiting_data_paths, move_complete_data


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("fct_review_etl")\
        .getOrCreate()

    logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    keyspace = spark.conf.get("spark.keyspace.name")
    input_date_format = "%m %d, %Y"
    data_path = os.path.join(os.environ['PROJECT_PATH'], 'data')
    parse_dt_func = udf(lambda date: dtu.strptime(date, input_date_format).strftime(DATE_FORMAT))
    

    for file_path in get_waiting_data_paths(data_path, 'reviews'):

        json_df =  spark.read.json(file_path)

        result_df = json_df.select("reviewerID", "asin", "reviewText", "summary", "unixReviewTime", "overall",
                                   parse_dt_func(col("reviewTime")).alias("reviewTime"))\
                        .withColumn("etl_processed_dttm", lit(dtu.strftime(dtu.utcnow(), DATE_TIME_FORMAT)))\
                        .withColumnRenamed("reviewerID", "reviewer_id")\
                        .withColumnRenamed("reviewText", "review_text")\
                        .withColumnRenamed("unixReviewTime", "review_ts")\
                        .withColumnRenamed("reviewTime", "review_dt")

        result_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="fct_review", keyspace=keyspace)\
            .save()

        logger.info(f'{spark.sparkContext.appName}: File {file_path} successfully processed.')

        move_complete_data(file_path)

        
    spark.stop()
