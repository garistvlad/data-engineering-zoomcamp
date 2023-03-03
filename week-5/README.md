## Week 5 Homework 

The artefacts related to this home assignment could be found here:
- Lecture notes Jupyter notebook [pyspark-lecture-notes.ipynb](./pyspark-lecture-notes.ipynb)
- Home assignment Jupyter notebook [pyspark-home-assignment.ipynb](./pyspark-home-assignment.ipynb)


### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
- 3.3.2
- 2.1.4
- 1.2.3
- 5.4
</br></br>


**Answer 1:**
`3.3.2`

```python
from pyspark.sql import SparkSession

# init session:
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()

spark.version
```


### Question 2:

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br> 
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>


- 2MB
- 24MB
- 100MB
- 250MB
</br></br>


**Answer 2:**
`24M`

```python
from pyspark.sql import types

# define schema
schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True),
])

# load FHV trips
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(str(fhvhv_data_filepath))

# repartition & save to .parquet
df = df.repartition(12)
df.write.parquet(
    f"{str(fhvhv_data_dir)}/parquet/2021/06",
    mode="overwrite"
)
```

```shell
ls -lh data/fhvhv_trips/parquet/2021/06/ | grep .parquet
```

### Question 3: 

**Count records**  

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- 308,164
- 12,856
- 452,470
- 50,982
</br></br>


**Answer 3:**
`452,470`

```python
# filter & count:
df \
    .withColumn("pickup_date", F.to_date("pickup_datetime")) \
    .filter("pickup_date == '2021-06-15'") \
    .count()
```


### Question 4: 

**Longest trip for each day**  

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- 66.87 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours
</br></br>


**Answer 4:**
`66.87`

```python
df \
    .withColumn("pickup_sec", F.unix_timestamp("pickup_datetime")) \
    .withColumn("dropoff_sec", F.unix_timestamp("dropoff_datetime")) \
    .withColumn("diff_in_hours", (F.col("dropoff_sec") - F.col("pickup_sec")) / 3600) \
    .groupBy() \
    .max("diff_in_hours") \
    .show()
```


### Question 5: 

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- 80
- 443
- 4040
- 8080
</br></br>


**Answer 5:**<br>
Port: `4040`

Spark Jobs could be monitored here:<br>
`http://localhost:4040/jobs/`


### Question 6: 

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- East Chelsea
- Astoria
- Union Sq
- Crown Heights North
</br></br>


**Answer 6**
`Crown Heights North`

```python
from pyspark.sql import types

# define zones schema:
zones_schema = types.StructType([
    types.StructField('LocationID', types.IntegerType(), nullable=False),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True),
])

# read zones:
df_zones = spark.read \
    .option("header", "true") \
    .schema(zones_schema) \
    .csv(str(taxi_zones_filepath))

# create Views:
df.createOrReplaceTempView("taxi_trips")
df_zones.createOrReplaceTempView("taxi_zones")

# get most grequest pickup location zone:
spark.sql("""
SELECT
    taxi_zones.Zone as zone_name,
    count(*) as trips_cnt
FROM taxi_trips
JOIN taxi_zones
    ON taxi_trips.PULocationID = taxi_zones.LocationID
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
""").show()
```
