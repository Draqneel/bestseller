from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, MapType, IntegerType

schema = StructType([
    StructField("asin", StringType(), False),
    StructField("title", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("imUrl", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("salesRank", MapType(StringType(), IntegerType()), True),
    StructField("categories", ArrayType(ArrayType(StringType())), True),
])