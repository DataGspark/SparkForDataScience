// Databricks notebook source
//Using Spark Dataframe with Scala for Data Engineering Pipeline
// By Moncef Ansseti  Data Engineer - Spark Geek

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val sparkSession = SparkSession.builder
  .master("local")
  .appName("Sparknewversion")
  .config("spark.some.config.option", "config-value")
  .getOrCreate()


// COMMAND ----------

// MAGIC %fs ls  dbfs:/tmp/hive

// COMMAND ----------

import org.apache.spark.sql.types.StringType,DoubleType

//#Data Engineer -- M.Ansseti
val path = "dbfs:/databricks-datasets/online_retail/data-001/"

val retail = StructType(Array(
                             StructField("InvoiceNO", StringType, true),
                             StructField("StockCODE",StringType,true),
                             StructField("Description", StringType, true),
                             StructField("Quantity", DoubleType, true),
                              StructField("InvoiceDate", StringType, true),
                              StructField("CodeUnitPrice", DoubleType, true),
                              StructField("CustomerID", StringType, true),
                             StructField("Country", StringType, true)
                               
                               )
                               )
val ds1= spark.read
                   .schema(retail)
                   .option("header", "true")
                   .csv(path)
                   .createOrReplaceTempView("retail")
    //Caching Table In Memory   ==== check the SPARK UI for storage             
     spark.catalog.cacheTable("retail")


/*import org.apache.spark.sql.functions.unix_timestamp
val tst = unix_timestamp($"InvoiceDate","MM/dd/yyyy HH:mm:ss").cast("double").cast("timestamp")

// COMMAND ----------

//#Data Engineer -- M.Ansseti
import org.apache.spark.sql.functions._

val analytics = spark.sql("select * from retail")



 val MLpers = analytics
             .groupBy("Country","InvoiceDate","Quantity")
             .agg(sum("Quantity").as ("TotalPrice"), count("CustomerID").as ("TotalCustomers"))
             .filter(analytics("Quantity") > 24)
   //Writing Data into Hive metastore in Parquet Format 
           MLpers.write
                   .format("Parquet")
                   .save("dbfs:/tmp/hive/datatestmoncef")

             

// COMMAND ----------

// MAGIC %fs ls  dbfs:/tmp/hive/datatestmoncef/

// COMMAND ----------

//#Data Engineer -- M.Ansseti
import org.apache.spark.sql.types
import org.apache.spark.sql.functions._
//Read Parquet Table in Hive Metastore
val  ds2 = spark.read.parquet("dbfs:/tmp/hive/datatestmoncef/")


// join data with Spark DataFrame [InnerJoin]
val path = "dbfs:/databricks-datasets/online_retail/data-001/"
val ds1= spark.read.format("com.databricks.spark.csv")
                   .schema(retail)
                   .option("header", "true")
                   .load(path)
val   matrixdata =ds1.join(ds2, ds1("InvoiceDate") === ds2("InvoiceDate"), "inner")
                     .createOrReplaceTempview("matrixdata")
                     .
                     .


                   


// COMMAND ----------

// MAGIC %python
// MAGIC #Data Engineer -- M.Ansseti
// MAGIC ds2 = spark.read.parquet("dbfs:/tmp/hive/datatestmoncef/")
// MAGIC ds2.toPandas()
// MAGIC #ds2.descripte()
// MAGIC # Using Python
// MAGIC #Converting Spark Dataframe too Pandas

// COMMAND ----------

display(ds2)

// COMMAND ----------


