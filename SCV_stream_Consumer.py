# Databricks notebook source
# MAGIC %md
# MAGIC ## Single Customer View
# MAGIC Steps involved in creating SCV
# MAGIC ### 1. Build base UserSCV table  
# MAGIC > 1.1. Cleanse the data (Validate Email, format Phone No, Landline No). This is done by calling function on each row in dataFrame. 
# MAGIC 
# MAGIC > 1.2. An intermediate table is created to hold validated/cleanse data, before transforming original data. 
# MAGIC 
# MAGIC > 1.3. Create additional fields by combining base fields (FirstName, UserName, LastName, DOB). Some of the combinations are as follows:
# MAGIC             * Firstname_Lastname_RegIP		
# MAGIC             * Firstname_Lastname_LastIP		
# MAGIC             * Firstname_Lastname_Username		
# MAGIC             * Firstname_DOB_City				
# MAGIC             * Firstname_Postcode				
# MAGIC             * Firstname_Mobilephone			
# MAGIC             * DOB_Postcode					
# MAGIC             * Address1_Postcode				
# MAGIC             * Firstname_Lastname_Address1_City
# MAGIC > Create UserSCV hive table with base fields and additional fields.
# MAGIC 
# MAGIC 
# MAGIC ### 2. For each data load, perform check against base UserCSV. A record is considered same if it meets any one of the criteria:
# MAGIC | FirstName| Lastname | DOB  | Email | Postcode | Result   |
# MAGIC | :-------:| :-------:| :---:| :----:| :-------:| :-------:|
# MAGIC | X|X|X|X|X|**MATCH**|
# MAGIC |  |X|X|X|X|**MATCH**|
# MAGIC | X| |X|X|X|**MATCH**|
# MAGIC | X|X| |X|X|**MATCH**|
# MAGIC | X|X|X| |X|**MATCH**|
# MAGIC | X|X|X|X| |**MATCH**|
# MAGIC 
# MAGIC **Minimal conditions for match: **
# MAGIC 
# MAGIC | S.No| Criteria|
# MAGIC | :--:| :------:|
# MAGIC |1.|Firstname + IP Address|
# MAGIC |2.|Firstname + Username|
# MAGIC 
# MAGIC ### 2.1. Data is given as  csv file and converted into Table with cleansed data. Join is performed with UserSCV table and loaded data and eac criteria mentioned above is checked to determine the match with existing Master UserSCV table.  
# MAGIC ### 3. If matched records found, insert new version of user record with Related Id into UserSCV table. 
# MAGIC 
# MAGIC 
# MAGIC ####  Issues faced while building UserSCV
# MAGIC 
# MAGIC 1. **Pre-processing data**: Pre-processing and cleansing posed as main milestone when building base UserSCV table. A function is called on each row to pre-process the data.
# MAGIC 2. **Transforming data:** Transforming pre-process data before converting into UserSCV table involves adding many new fields by combining different combinations of exisitin field and assigning each row with unique Id. This unique Id will be used as "Related Id" when matching user record is found in UserSCV table.

# COMMAND ----------

# import libraries
from pyspark.sql.types import StringType, IntegerType, TimestampType, DateType, DoubleType, StructType, StructField
import requests
import json
import re
import datetime
import schedule
import time
import pandas as pd
import phonenumbers
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import  col
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

# schema for SCV User Table 
user_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("Userid", IntegerType(), True),
            StructField("SkinID", StringType(), True),
            StructField("username", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("gender", StringType(), True), 
            StructField("ip_address", StringType(), True), 
            StructField("RegDate", StringType(), True), 
            StructField("RegIP", StringType(), True), 
            StructField("LastIP", StringType(), True), 
            StructField("DOB", StringType(), True), 
            StructField("Postcode", StringType(), True), 
            StructField("MobilePhone", StringType(), True), 
            StructField("Landline", StringType(), True), 
            StructField("Address1", StringType(), True),
            StructField("City", StringType(), True),
            StructField("County", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("SelfExcludedUntil", StringType(), True),
            StructField("Status", StringType(), True)])
            

# COMMAND ----------

# schema for incoming stream 
stream_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("Userid", IntegerType(), True),
            StructField("SkinID", StringType(), True),
            StructField("username", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("gender", StringType(), True), 
            StructField("ip_address", StringType(), True), 
            StructField("RegDate", StringType(), True), 
            StructField("RegIP", StringType(), True), 
            StructField("LastIP", StringType(), True), 
            StructField("DOB", StringType(), True), 
            StructField("Postcode", StringType(), True), 
            StructField("MobilePhone", StringType(), True), 
            StructField("Landline", StringType(), True), 
            StructField("Address1", StringType(), True),
            StructField("City", StringType(), True),
            StructField("County", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("SelfExcludedUntil", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("batch", StringType(), True)])
# The batch field is added to show the batch for a record 

# COMMAND ----------

# This function cleans the user row; it cleans the fields like MobilePhone, Email
def fixUserRow(c):
    # get the Mobile field
    number = c.MobilePhone

    # initialize variables 
    is_valid_number = "N"
    clean_number = None
    number_type = None
    valid_mail = None

    p = None

    if number is not None:
        # Clean the Mobile Number first
        try:
            p = phonenumbers.parse(number, c.Country)

            if phonenumbers.is_valid_number(p):
                is_valid_number = "Y"
            elif phonenumbers.truncate_too_long_number(p):
                is_valid_number = "Y"
            else:
                is_valid_number = "N"

            clean_number = "%s%s" % (p.country_code, p.national_number)
            
        except:
            p = None

    # clean up PhoneNumber
    phone_no = c.Landline
    if phone_no is not None:
      phone_no = phone_no.replace('-', '')
      if (len(phone_no) != 10):
        phone_no = None
    
    # validate Email 
    if re.match(r"^[A-Za-z0-9\.\+_-]+@[A-Za-z0-9\._-]+\.[a-zA-Z]*$", c.email):
      valid_mail = c.email
    
    return Row( 
		id = c.id, 
        Userid = c.Userid, 			
        SkinID = c.SkinID,
        username = c.username,
        first_name = c.first_name, 		
        last_name = c.last_name,	
        email = valid_mail,			
        gender = c.gender,			
        ip_address = c.ip_address,
        RegDate = c.RegDate,
        RegIP = c.RegIP,
		LastIP = c.LastIP,			
		DOB = c.DOB,			
		Postcode = c.Postcode,		
		MobilePhone = clean_number, 	
		Landline = phone_no, 		
		Address1 = c.Address1,		
        City = c.City, 			
		County = c.County,			
		Country = c.Country, 		
        SelfExcludedUntil = c.SelfExcludedUntil,
		Status = c.Status			
    )


# COMMAND ----------

# insert matching records into UserSCV table
def insertNewVersionOfUser(tableName):
  df = spark.sql("select  * from " + tableName)
  dateTimeStr = datetime.datetime.today().strftime("%m-%d-%Y %H:%M:%S")

  # select max of id from userSCV table
  lv = sqlContext.sql("select max(ID) as lastVal from UserSCV").collect()
  lastValue = lv[0]["lastVal"]
  df_userSCV = df.select("ID", \
                         "Userid1", \
                         "SkinID1", \
                         "username1", \
                         "first_name1", \
                         "last_name1", \
                         "email1", \
                         "gender1", "ip_address1", "RegDate1", "RegIP1", \
                         "LastIP1", "DOB1", "Postcode1", "MobilePhone1", "Landline1", \
                         "Address11", "City1", "County1", "Country1", \
                         "SelfExcludedUntil1", "Status1", \
                         "EntityId", \
                         "OriginalEmail", \
                         "OriginalFirstname", \
                         "OriginalLastname", \
                         "OriginalRegDate", \
                         "OriginalDOB", \
                         "OriginalPostcode", \
                         "OriginalMobilePhone", \
                         "OriginalAddress1", \
                         "OriginalCity", \
                         "Firstname_Lastname_RegIP", \
                         "Firstname_Lastname_LastIP", \
                         "Firstname_Lastname_Username", \
                         "Firstname_DOB_City",\
                         "Firstname_Postcode", \
                         "Firstname_Mobilephone", \
                         "DOB_Postcode",  \
                         "Address1_Postcode", \
                         "Firstname_Lastname_Address1_City")
  #df_userSCV = df_userSCV.withColumnRenamed("ID", "RelatedID") 
  df_userSCV = df_userSCV.withColumn("RelatedID", col("ID"))
  df_userSCV = df_userSCV.withColumn("Load_date", lit(dateTimeStr))
  df_userSCV = df_userSCV.withColumn("LastModifiedDate", lit(dateTimeStr))
  df_userSCV = df_userSCV.withColumn("CompareStatus", lit(0))
   
  #df_userSCV = df_userSCV.withColumn("ID", monotonically_increasing_id() + lastValue)
  df_userSCV = df_userSCV.select("ID", \
                        col("Userid1").alias("Userid"), col("SkinID1").alias("SkinID"), \
                        col("username1").alias("username"), col("first_name1").alias("first_name"), \
                        col("last_name1").alias("last_name"), col("email1").alias("email"), \
                        col("gender1").alias("gender"), col("ip_address1").alias("ip_address"), \
                        col("RegDate1").alias("RegDate"), col("RegIP1").alias("RegIP"), \
                        col("LastIP1").alias("LastIP"), col("DOB1").alias("DOB"), \
                        col("Postcode1").alias("Postcode"), col("MobilePhone1").alias("MobilePhone"), \
                        col("Landline1").alias("Landline"), col("Address11").alias("Address1"), \
                        col("City1").alias("City"), col("County1").alias("County"), \
                        col("Country1").alias("Country"), col("SelfExcludedUntil1").alias("SelfExcludedUntil"), \
                        col("Status1").alias("Status"), \
                         "RelatedID", \
                         "EntityId", \
                         "OriginalEmail", \
                         "OriginalFirstname", \
                         "OriginalLastname", \
                         "OriginalRegDate", \
                         "OriginalDOB", \
                         "OriginalPostcode", \
                         "OriginalMobilePhone", \
                         "OriginalAddress1", \
                         "OriginalCity", \
                         "Firstname_Lastname_RegIP", \
                         "Firstname_Lastname_LastIP", \
                         "Firstname_Lastname_Username", \
                         "Firstname_DOB_City",\
                         "Firstname_Postcode", \
                         "Firstname_Mobilephone", \
                         "DOB_Postcode",  \
                         "Address1_Postcode", \
                         "Firstname_Lastname_Address1_City", \
                         "Load_date", \
                         "LastModifiedDate",\
                         "CompareStatus")

  df_userSCV.write.insertInto("UserSCV")
  
  

# COMMAND ----------

# This function converts the csv file to Spark Data Frame.
def getDataFrameFromStream(df, schema):
   
  df_new_load = df
  from pyspark.sql.functions import col

  
  # cleanse the data
  df_user_updated1 = df_new_load.rdd.map(lambda c: fixUserRow(c))
  # change the column type now
  df_new = sqlContext.createDataFrame(df_user_updated1, user_schema)
  df_new = df_new.select (col("ID").alias("ID1"), col("Userid").alias("Userid1"), col("SkinID").alias("SkinID1"), \
                        col("username").alias("username1"), col("first_name").alias("first_name1"), \
                        col("last_name").alias("last_name1"), col("email").alias("email1"), \
                        col("gender").alias("gender1"), col("ip_address").alias("ip_address1"), \
                        col("RegDate").alias("RegDate1"), col("RegIP").alias("RegIP1"), \
                        col("LastIP").alias("LastIP1"), col("DOB").alias("DOB1"), \
                        col("Postcode").alias("Postcode1"), col("MobilePhone").alias("MobilePhone1"), \
                        col("Landline").alias("Landline1"), col("Address1").alias("Address11"), \
                        col("City").alias("City1"), col("County").alias("County1"), \
                        col("Country").alias("Country1"), col("SelfExcludedUntil").alias("SelfExcludedUntil1"), \
                        col("Status").alias("Status1")) 
  return df_new
  

# COMMAND ----------

# This function creates and/or inserts new records to Output Table - UserSCV
def createOutputTable(tableName):
  # create output table
  df = spark.sql("select * from " + tableName)
  dateTimeStr = datetime.datetime.today().strftime("%m-%d-%Y %H:%M:%S")


  userSCV =  df.withColumn("ID", F.monotonically_increasing_id()) \
    .withColumn("RelatedID", lit(-1).cast(IntegerType())) 
  userSCV = userSCV.withColumn("EntityId", col("ID")) 

  # rename columns 
  userSCV = userSCV.withColumn("OriginalEmail", col("email")) 
  userSCV = userSCV.withColumn("OriginalFirstname", col("first_name")) 
  userSCV = userSCV.withColumn("OriginalLastname", col("last_name")) 
  userSCV = userSCV.withColumn("OriginalRegDate", col("RegDate"))
  userSCV = userSCV.withColumn("OriginalDOB", col("DOB"))
  userSCV = userSCV.withColumn("OriginalPostcode", col("Postcode"))             
  userSCV = userSCV.withColumn("OriginalMobilePhone", col("MobilePhone"))
  userSCV = userSCV.withColumn("OriginalAddress1", col("Address1"))            
  #userSCV = userSCV.withColumn("OriginalAddress2", col("Address2"))            
  userSCV = userSCV.withColumn("OriginalCity", col("City"))
  userSCV = userSCV.withColumn("Firstname_Lastname_RegIP", F.concat(col('first_name'),lit('_'), col('last_name'), lit('_'),col('RegIP') ))       
  userSCV = userSCV.withColumn("Firstname_Lastname_LastIP", \
                               F.concat(col('first_name'),lit('_'), col('last_name'), lit('_'),col('LastIP') ))
  userSCV = userSCV.withColumn("Firstname_Lastname_Username", \
                               F.concat(col('first_name'),lit('_'), col('last_name'), lit('_'),col('Username') ))
  userSCV = userSCV.withColumn("Firstname_DOB_City", F.concat(col('first_name'),lit('_'), col('DOB'), lit('_'),col('City') ))
  userSCV = userSCV.withColumn("Firstname_Postcode", F.concat(col('first_name'),lit('_'), col('Postcode')  )) 
  userSCV = userSCV.withColumn("Firstname_Mobilephone", F.concat(col('first_name'),lit('_'), col('MobilePhone')  ))          
  userSCV = userSCV.withColumn("DOB_Postcode", F.concat(col('DOB'),lit('_'), col('Postcode')  )) 
  userSCV = userSCV.withColumn("Address1_Postcode", F.concat(col('Address1'),lit('_'), col('Postcode')  ))              
  userSCV = userSCV.withColumn("Firstname_Lastname_Address1_City", \
                               F.concat(col('first_name'),lit('_'), col('last_name'), lit('_'),col('Address1'), lit('_'), col('City') ))
  userSCV = userSCV.withColumn("Load_date", lit(dateTimeStr))
  userSCV = userSCV.withColumn("LastModifiedDate", lit(dateTimeStr))
  userSCV = userSCV.withColumn("CompareStatus", lit(0))
  userSCV = userSCV.withColumn("CompareStatus", lit(None).cast(StringType()))
  # Create a HIVE table to save Data fro Dataframe 
  if (len(spark.sql("SHOW TABLES LIKE '" + "UserSCV"+ "'").collect()) == 1):
    userSCV.write.insertInto("UserSCV")
  else:
    userSCV.write.saveAsTable("UserSCV")



# COMMAND ----------

# This function converts the Existing User with new User records.
def compareData(tableName):
  spark.sql("REFRESH TABLE  " + tableName)
  df_temp = spark.sql ("select * from " + tableName)
  count = df_temp.count()
  if (count > 0):
    insertNewVersionOfUser(tableName)
  

# COMMAND ----------

# streaming starts here by reading the input file 
inputPath = "/FileStore/users/inprogress/"
streamingInputDF = (
  spark
    .readStream
    .schema(stream_schema)
    .option("maxFilesPerTrigger", "1")
    .option("header", "true")
    .csv(inputPath)
)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

query = (
  streamingInputDF
    .writeStream
    .format("memory")
    .outputMode("update")
    .queryName("users")
    .start()
)

# COMMAND ----------

def processUserInfo(df):
  # Read the csv file as example
  print("-----------------------------------------------------------------------------------")
  print("Reading streaming data")
  # cleanse the data
  # Standardise the telephone number to take away -, ) from input data 

  df_user_updated = df.rdd.map(lambda c: fixUserRow(c))
  # change the column type now
  df_user = sqlContext.createDataFrame(df_user_updated, user_schema)
  # Insert into intermediate table
  # check if table exists
  if (len(spark.sql("SHOW TABLES LIKE '" + "users_load"+ "'").collect()) == 1):
    df_user.write.insertInto("users_load")
  else:
    df_user.write.saveAsTable("users_load")

  print("After saving data to userload")
  
  # check if UserSCV table exists:
  if (len(spark.sql("SHOW TABLES LIKE '" + "UserSCV"+ "'").collect()) == 1) :
    spark.sql("REFRESH TABLE UserSCV")
  else:
    print("UserSCV table do not exist; hence creating it")
    df_new = sqlContext.createDataFrame(spark.sparkContext.emptyRDD(), user_schema)
    df_new.createOrReplaceTempView("output_user")
    createOutputTable("output_user")
    spark.sql("REFRESH TABLE UserSCV")
    
  # compare the data with existing data in UserSCV
  
  userSCV = spark.sql("select * from UserSCV")
  # 1. Rename the base columns 
  df_new =  getDataFrameFromStream(df_user, user_schema)
  # 2. compare the data
  # check for the minimal condition
  # whether firstName + IP equals
  print("1. checking for firstName + IP")
  df_criteria_min = userSCV.join(df_new, (userSCV.first_name == df_new.first_name1) & (userSCV.ip_address == df_new.ip_address1) )
  df_criteria_min.createOrReplaceTempView("c1_FN_IP") 
  compareData("c1_FN_IP")

  # This is to check  criteria: FirstName + username 
  print("2. checking for firstName + username") 
  #df_new =  getDataFrameFromCSV(csvFilePath_new, user_schema)
  df_criteria_fn_username = userSCV.join(df_new, (userSCV.first_name == df_new.first_name1) & (userSCV.username == df_new.username1) )
  df_criteria_fn_username.createOrReplaceTempView("c1_FN_username") 
  compareData("c1_FN_username")

  # check for firstName, Dob and city
  print("3. checking for firstName + DOB+ City") 
  df_criteria_fn_dob_city = userSCV.join(df_new, (userSCV.first_name == df_new.first_name1) \
                                     & (userSCV.DOB == df_new.DOB1) \
                                     & (userSCV.City == df_new.City1) )
  df_criteria_fn_dob_city.createOrReplaceTempView("c1_fn_dob_city") 
  compareData("c1_fn_dob_city") 

  # This is to check  criteria: FirstName + postcode 
  print("4. checking for firstName + PostCode") 
  df_criteria_fn_postcode = userSCV.join(df_new, (userSCV.first_name == df_new.first_name1) \
                                     & (userSCV.Postcode == df_new.Postcode1) )
  df_criteria_fn_postcode.createOrReplaceTempView("c1_fn_postcode") 
  compareData("c1_fn_postcode")

  # This is to check  criteria: DOB + postcode
  print("5. checking for DOB + PostCode") 
  df_criteria_postcode_dob = userSCV.join(df_new, (userSCV.DOB == df_new.DOB1) \
                                     & (userSCV.Postcode == df_new.Postcode1) )
  df_criteria_postcode_dob.createOrReplaceTempView("c1_postcode_dob") 
  compareData("c1_postcode_dob")

  # This is to check  criteria: Address1 + postcode 
  print("6. checking for Address1 + PostCode") 
  df_criteria_postcode_addr1 = userSCV.join(df_new, (userSCV.Address1 == df_new.Address11) \
                                     & (userSCV.Postcode == df_new.Postcode1) )
  df_criteria_postcode_addr1.createOrReplaceTempView("c1_postcode_addr1") 
  compareData("c1_postcode_addr1")

  # This is to check  criteria: FirstName + LastName + Address1 + city 
  print("7. checking for firstName + Address1 + LastName + City") 
  df_criteria_fn_ln_addr1_city = userSCV.join(df_new, (userSCV.first_name == df_new.first_name1) \
                                     & (userSCV.last_name == df_new.last_name1)
                                     & (userSCV.Address1 == df_new.Address11) \
                                     & (userSCV.City == df_new.City1) )

  df_criteria_fn_ln_addr1_city.createOrReplaceTempView("c1_fn_ln_addr1_city") 
  compareData("c1_fn_ln_addr1_city")

  # check for FirstName and MobilePhone
  print("8. checking for firstName + MobilePhone") 
  df_fn_mobile = userSCV.join(df_new, (userSCV.first_name == df_new.first_name1) & (userSCV.MobilePhone == df_new.MobilePhone1) )
  df_fn_mobile.createOrReplaceTempView("c1_fn_mobile") 
  compareData("c1_fn_mobile")

  df_user.createOrReplaceTempView("output_user")
  createOutputTable("output_user")
    
  
  print("-----------------------------------------------------------------------------------")


# COMMAND ----------

# This function filters new data
def filterUpdatedDF(df_user, tracker_table):
  if (len(spark.sql("SHOW TABLES LIKE '" + tracker_table+ "'").collect()) == 1):
    spark.sql("REFRESH TABLE " + tracker_table)
    tracker_df = spark.sql("select batch from " + tracker_table).distinct()
    old_batch = df_user.select("batch").intersect(tracker_df)
    df_filtered = df_user.select("*").where(df_user.batch != old_batch.batch)
    df_filtered.drop("batch").createOrReplaceTempView("filteredView")
    df_filtered.select("batch").distinct().write.insertInto(tracker_table)
  else:
    df_user.drop("batch").createOrReplaceTempView("filteredView")
    df_user.select("batch").distinct().write.saveAsTable(tracker_table)
  return spark.sql("select * from filteredView")


# COMMAND ----------

row_count = 0
def job():
  global row_count
  spark.sql("REFRESH TABLE users")
  stream_snap_df = spark.sql("select * from users")
  if stream_snap_df.count() > row_count:
    total = str(stream_snap_df.count() - row_count)
    print("New csv files received with {} records. Total Records received {}".format(total, str(stream_snap_df.count())))
    row_count = stream_snap_df.count()
    df = filterUpdatedDF(stream_snap_df, 'batchTracker')
    if df.count()>0:
      processUserInfo(df)
    else:
      print("No new record received ...")
  else:
    print("No new csv file received ...")

schedule.every(10).seconds.do(job)

while True:
  schedule.run_pending()
  time.sleep(1)

# COMMAND ----------

# MAGIC %sql select * from userSCV

# COMMAND ----------

# MAGIC %sql select count(*) from userSCV

# COMMAND ----------

# MAGIC %sql select count(*) from users
