
import os
import ast

from datetime import datetime as dtu

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

from tools.const import DATE_TIME_FORMAT
from tools.data_files import get_waiting_data_paths, move_complete_data

from input_schema import schema


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("dim_item_etl")\
        .getOrCreate()

    logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    keyspace = spark.conf.get("spark.keyspace.name")
    data_path = os.path.join(os.environ['PROJECT_PATH'], 'data')
    process_json = lambda x: ast.literal_eval(x)

    for file_path in get_waiting_data_paths(data_path, 'metadata'):
        with open(file=file_path, mode='r') as f:
            lines = f.readlines()

        # saving memory, reading file with python generator
        line_generator = map(process_json, lines)

        rdd = spark.sparkContext.parallelize(line_generator)
        df =  spark.createDataFrame(rdd, schema=schema)

        result_df = df.select("*")\
                        .withColumn("etl_processed_dttm", lit(dtu.strftime(dtu.utcnow(), DATE_TIME_FORMAT)))\
                        .withColumnRenamed("imUrl", "img_url")\
                        .withColumnRenamed("salesRank", "sales_rank")\

        result_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="dim_item", keyspace=keyspace)\
            .save()

        logger.info(f'{spark.sparkContext.appName}: File {file_path} successfully processed.')

        move_complete_data(file_path)

        
    spark.stop()
