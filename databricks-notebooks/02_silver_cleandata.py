from pyspark.sql.types import *
from pyspark.sql.functions import *

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  dbutils.secrets.get(scope = "<<Scope_name>>", key = "<<Secretkey_name>>")
)

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/patient_flow"
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/patient_flow"

#read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

#Define Schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("hospital_id", IntegerType()),
    StructField("bed_id", StringType()),
    StructField("activity_type", StringType()),
    StructField("activity_time", StringType())
    
])

#Parse it to dataframe
parsed_df = bronze_df.withColumn("data",from_json(col("raw_json"),schema)).select("data.*")

#convert type to Timestamp
clean_df = parsed_df.withColumn("activity_time", to_timestamp("activity_time"))

#future/null activity_times
clean_df = clean_df.withColumn(
    "activity_time",
    when(
        col("activity_time").isNull() | (col("activity_time") > current_timestamp()),
        current_timestamp()
        ).otherwise(col("activity_time")))

# Dedup + watermark
clean_df = clean_df \
    .withWatermark("activity_time", "10 minutes") \
    .dropDuplicates([
        "patient_id",
        "bed_id",
        "activity_type",
        "activity_time"
    ])

# Handle Gender
clean_df = clean_df.withColumn(
    "gender",
    when(upper(col("gender")).isin("M", "MALE"), "Male")
    .when(upper(col("gender")).isin("F", "FEMALE"), "Female")
    .otherwise("Unknown")
)
#---Flag gender
clean_df = clean_df.withColumn(
    "unknown_gender",
    col("gender") == "Unknown"
)

# Flag Invalid Age
clean_df = clean_df.withColumn(
    "invalid_age",
    (col("age") > 110) | (col("age") <= 0)
)

#--------------------
#schema evolution
expected_cols = ["patient_id", "gender", "age", "department", "hospital_id", "bed_id", "activity_type", "activity_time"]

for col_name in expected_cols:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name, lit(None))

#Write to silver table
(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema","true")
    .option("checkpointLocation", silver_path + "_checkpoint")
    .start(silver_path)
)

#Check
# display(spark.read.format("delta").load(silver_path))
