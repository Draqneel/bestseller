airflow db init

airflow users create \
    --username admin \
    --firstname Tony \
    --lastname Stark \
    --role Admin \
    --email ironman@avengers.com

# start the web server, default port is 8080
airflow webserver --port 8080 -D

# start scheduler
airflow scheduler
