# Databricks notebook source
#Using Spark Dataframe with Python (Pyspark) for Data Engineering Pipeline
# By Moncef Ansseti  Data Engineer - Spark Geek

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sf_open_data/fire_incidents

# COMMAND ----------

# Using Spark for Data Science Project Part 1

#Data Acquisition : Pyspark Dataframe Api
#Data Preparation & Cleansing :
#Data Exploration :
#Data Visualisation: Databricks cloud dataviz  (or Matplotlib in Jupyter)
#Machine Learning : Spark Mllib ( ML packages)

# COMMAND ----------

display(dataset.limit(3))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# COMMAND ----------

#Create a schema for a Pyspark Dataframe Api
fireSchema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),                  
                     StructField('CallDate', StringType(), True),       
                     StructField('WatchDate', StringType(), True),       
                     StructField('ReceivedDtTm', StringType(), True),       
                     StructField('EntryDtTm', StringType(), True),       
                     StructField('DispatchDtTm', StringType(), True),       
                     StructField('ResponseDtTm', StringType(), True),       
                     StructField('OnSceneDtTm', StringType(), True),       
                     StructField('TransportDtTm', StringType(), True),                  
                     StructField('HospitalDtTm', StringType(), True),       
                     StructField('CallFinalDisposition', StringType(), True),       
                     StructField('AvailableDtTm', StringType(), True),       
                     StructField('Address', StringType(), True),       
                     StructField('City', StringType(), True),       
                     StructField('ZipcodeofIncident', IntegerType(), True),       
                     StructField('Battalion', StringType(), True),                 
                     StructField('StationArea', StringType(), True),       
                     StructField('Box', StringType(), True),       
                     StructField('OriginalPriority', StringType(), True),       
                     StructField('Priority', StringType(), True),       
                     StructField('FinalPriority', IntegerType(), True),       
                     StructField('ALSUnit', BooleanType(), True),       
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumberofAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('Unitsequenceincalldispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('NeighborhoodDistrict', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True)])

# COMMAND ----------

# Data Acquisition with spark Dataframe Api 2.0
fireServiceCallsDF = spark.read
                          .csv('/mnt/sf_open_data/fire_dept_calls_for_service/Fire_Department_Calls_for_Service.csv', header=True, schema=fireSchema)

# COMMAND ----------

display(fireServiceCallsDF.limit(5))

# COMMAND ----------

fireServiceCallsDF.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *#Data Engineer -- M.Ansseti


# COMMAND ----------

#Data Engineer -- M.Ansseti

from_pattern1 = 'MM/dd/yyyy'
to_pattern1 = 'yyyy-MM-dd'

from_pattern2 = 'MM/dd/yyyy hh:mm:ss aa'
to_pattern2 = 'MM/dd/yyyy hh:mm:ss aa'


fireServiceCallsTsDF = fireServiceCallsDF \
  .withColumn('CallDateTS', unix_timestamp(fireServiceCallsDF['CallDate'], from_pattern1).cast("timestamp")) \
  .drop('CallDate') \
  .withColumn('WatchDateTS', unix_timestamp(fireServiceCallsDF['WatchDate'], from_pattern1).cast("timestamp")) \
  .drop('WatchDate') \
  .withColumn('ReceivedDtTmTS', unix_timestamp(fireServiceCallsDF['ReceivedDtTm'], from_pattern2).cast("timestamp")) \
  .drop('ReceivedDtTm') \
  .withColumn('EntryDtTmTS', unix_timestamp(fireServiceCallsDF['EntryDtTm'], from_pattern2).cast("timestamp")) \
  .drop('EntryDtTm') \
  .withColumn('DispatchDtTmTS', unix_timestamp(fireServiceCallsDF['DispatchDtTm'], from_pattern2).cast("timestamp")) \
  .drop('DispatchDtTm') \
  .withColumn('ResponseDtTmTS', unix_timestamp(fireServiceCallsDF['ResponseDtTm'], from_pattern2).cast("timestamp")) \
  .drop('ResponseDtTm') \
  .withColumn('OnSceneDtTmTS', unix_timestamp(fireServiceCallsDF['OnSceneDtTm'], from_pattern2).cast("timestamp")) \
  .drop('OnSceneDtTm') \
  .withColumn('TransportDtTmTS', unix_timestamp(fireServiceCallsDF['TransportDtTm'], from_pattern2).cast("timestamp")) \
  .drop('TransportDtTm') \
  .withColumn('HospitalDtTmTS', unix_timestamp(fireServiceCallsDF['HospitalDtTm'], from_pattern2).cast("timestamp")) \
  .drop('HospitalDtTm') \
  .withColumn('AvailableDtTmTS', unix_timestamp(fireServiceCallsDF['AvailableDtTm'], from_pattern2).cast("timestamp")) \
  .drop('AvailableDtTm')  

# COMMAND ----------

fireServiceCallsTsDF.printSchema()           #Data Engineer -- M.Ansseti


# COMMAND ----------

#Data Aggregation with Pyspark dataframe APi
fireServiceCallsTsDF
                   .filter(year('CallDateTS') == '2016')
                   .filter(dayofyear('CallDateTS') >= 180)
                   .select(dayofyear('CallDateTS'))
                   .distinct()
                   .orderBy('dayofyear(CallDateTS)')
                   .show()

# COMMAND ----------

fireServiceCallsTsDF.filter(year('CallDateTS') == '2016')
                    .filter(dayofyear('CallDateTS') >= 180)
                    .groupBy(dayofyear('CallDateTS'))
                    .count()
                    .orderBy('dayofyear(CallDateTS)')
                    .show()

# COMMAND ----------

#NumberofPartitions From a Dataframe Too an RDD
fireServiceCallsTsDF.rdd.getNumPartitions()

# COMMAND ----------

#Create a Temporary Table in Spark SQL and caching the Data 
#Data Engineer -- M.Ansseti
fireServiceCallsTsDF.repartition(6).createOrReplaceTempView("fireServiceVIEW");
spark.catalog.cacheTable("fireServiceView") # Check the Spark UI ===>  Storage 

# COMMAND ----------

#Data Engineer -- M.Ansseti

spark.table("fireServiceVIEW").count()


# COMMAND ----------

#Data Engineer -- M.Ansseti

FireServiceDF =spark.table("fireServiceVIEW")

# COMMAND ----------

display(FireServiceDF.limit(5))

# COMMAND ----------


