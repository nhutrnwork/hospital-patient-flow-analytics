from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "<<Namespace_hostname>>"
event_hub_name="<<Eventhub_Name>>"
event_hub_conn_str = dbutils.secrets.get(scope = "<<Scope_name>>", key = "<<Secretkey_name>>")

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}
#Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  dbutils.secrets.get(scope = "<<Scope_name>>", key = "<<Secretkey_name>>")
)

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/patient_flow"
#Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_path + "_checkpoint")
    .start(bronze_path)
)

# Check
# display(spark.read.format("delta").load(bronze_path))
