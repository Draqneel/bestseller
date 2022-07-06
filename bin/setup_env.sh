export AIRFLOW_HOME="~/airflow"

# TODO: add Spark master url
export AIRFLOW_CONN_SPARK_DEFAULT=<master_url>

export PYTHONPATH="$PWD"
export PROJECT_PATH="$PWD"
export PROPERTIES_PATH="$PWD/properties.conf"
export SPARK_HOME = "$PWD/spark-3.0.1-bin-hadoop2.7"

export CASSANDRA_CONNECTOR = "com.datastax.spark:spark-cassandra-connector_2.12:3.0.1"
