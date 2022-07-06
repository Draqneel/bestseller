# bestseller
## intro
I chose **Clothing, Shoes and Jewelry** [from](http://jmcauley.ucsd.edu/data/amazon/links.html), because the data is as close as possible to the data of BESTSELLER. Also I used `metadata.json`.

In a project, I first used [DataStax_Astra_DB](https://auth.cloud.datastax.com/) as a cloud data service and `Cassandra` as a database (not because it is the best database for analytics, just first try).
## data architecture  
The diagram of data flows:  

![data model](https://github.com/Draqneel/bestseller/blob/main/Clothing_shoes_jewelry.drawio.png?raw=True)  

In my project data samples contains in */data* directory which separated by */waitig* (not processed data) and */complete* (processed).  
After processing, the file is moved from */waitig* to */complete* (In future, we can create backfill functionality).  

For example, the path to data looks like - *data/waiting/<DD_MM_YYYY of downloading>/<data_domain>/<file_name>.json*  

# install 

## 1. Set-up DataStax Astra

### 1.1 - Sign up for a free DataStax Astra account if you do not have one already

### 1.2 - Hit the `Create Database` button on the dashboard

### 1.3 - Hit the `Get Started` button on the dashboard
This will be a pay-as-you-go method, but they won't ask for a payment method until you exceed $25 worth of operations on your account. We won't be using nearly that amount, so it's essentially a free Cassandra database in the cloud.

### 1.4 - Define your database
- Database name: whatever you want
- Keyspace name: whatever you want
- Cloud: whichever GCP region applies to you. 
- Hit `create database` and wait a couple minutes for it to spin up and become `active`.

### 1.5 - Generate application token
- Once your database is active, connect to it. 
- Once on `dashboard/<your-db-name>`, click the `Settings` menu tab. 
- Select `Admin User` for role and hit generate token. 
- **COPY DOWN YOUR CLIENT ID AND CLIENT SECRET** as they will be used by Spark

### 1.6 - Download `Secure Bundle`
- Hit the `Connect` tab in the menu
- Click on `Python` (doesn't matter which option under `Connect using a driver`)
- Download `Secure Bundle`
- Drag-and-Drop the `Secure Bundle` into the running Gitpod container.

### 1.7 - Copy and paste the contents of `bin/setup_db_scheme.cql` into the CQLSH terminal

## 2. Set up Airflow and other stuff

```bash
bash bin/setup_requirements.sh
```

```bash
bash bin/setup_env.sh
```

```bash
bash bin/run_airflow.sh
```
(If superuser already exists just comment `airflow users create` in `*.sh` file.)

## 3. Start Spark in standalone mode

### 3.1 - Start master

```bash
./spark-3.0.1-bin-hadoop2.7/sbin/start-master.sh
```

### 3.2 - Start worker

Open port 8081 in the browser, copy the master URL, and paste in the designated spot below

```bash
./spark-3.0.1-bin-hadoop2.7/sbin/start-slave.sh <master-URL>
```

## 4. Move /dags to ~/airflow/dags

### 4.1 - Create ~/airflow/dags

```bash
mkdir ~/airflow/dags
```

### 4.2 - Move 

```bash
mv -r /dags ~/airflow/dags
```

## 5. Update the TODO's in properties.config and setup_env.sh with your specific parameters

```bash
vim properties.conf
```

```bash
vim bin/setup_env.sh

bash bin/setup_env.sh
```

## 6. Open port 8080 to see Airflow UI and check if pipelines exists. 

## 7. Run dugs in UI and check data in Astra