# Databricks notebook source
# Read API from Kafka, Event hub, Sockets (for testing) and files . One  need to read data from API and store in file else these structures before processing by spark sreaming
# create the base directory to store csv files
dbutils.fs.rm("/FileStore/users",recurse=True)
dbutils.fs.mkdirs("/FileStore/users")
dbutils.fs.mkdirs("/FileStore/users/inprogress")  #  needed and used to fetch data streams
dbutils.fs.mkdirs("/FileStore/users/query")   # Needed for advanced implementations 

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/users/checkpoints") 

# Will track processing and in case failed will able to start processes from that point onwards


# COMMAND ----------

import schedule
import time
import requests
import datetime
import pandas as pd
from pyspark.sql.functions import lit
 
def job():
  print("calling CSV load function")
  url = "https://my.api.mockaroo.com/users_load.json?key=6af9c3e0"
  
  df = spark.createDataFrame(pd.read_csv(url))
  
  ts = time.time()
  st = datetime.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M')
  df_with_batch = df.withColumn("batch", lit(datetime.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M_%S')))
  fileName = '/FileStore/users/inprogress/'+ st + '.tmp'
  fileprefix = '/FileStore/users/inprogress/'
  df_with_batch.coalesce(1).write.format("com.databricks.spark.csv") \
    .option("header", True) \
    .option("quote", "") \
    .save(fileName)  #saved to the FileStore
    
  fileList =  dbutils.fs.ls(fileName)

  csvFileLocation = ''
  for fileInfo in fileList:   
    if ".csv" in fileInfo.path:
      print("this file is csv file.." )
      print(fileInfo.path)
      csvFileLocation = fileprefix + fileInfo.name
      
      dbutils.fs.cp(fileInfo.path,fileprefix)
      dbutils.fs.rm(fileName,recurse=True)
  
  #if (len(csvFileLocation) >0):
  #  processUserInfo(csvFileLocation)
  #  dbutills.fs.mv(csvFileLocation, '/FileStore/users/completed/')
      
schedule.every(20).seconds.do(job)
 

while True:
    schedule.run_pending()
    time.sleep(1)

# COMMAND ----------

dbutils.fs.ls("/FileStore/users/inprogress/")
