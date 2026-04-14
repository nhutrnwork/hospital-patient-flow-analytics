from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, current_timestamp, sha2, concat_ws, coalesce, row_number, to_date
from delta.tables import DeltaTable
from pyspark.sql import Window

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  dbutils.secrets.get(scope = "<<Scope_name>>", key = "<<Secretkey_name>>")
)

# Paths
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/patient_flow"
gold_dim_patient = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/dim_patient"
gold_dim_department = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/dim_department"
gold_fact = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/fact_patient_flow"

gold_open_sessions_path = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/open_sessions"
gold_bed_state_path = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/bed_state"
gold_patient_dept_state = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/patient_department_state"

watermark_path = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/metadata"

# 1. LOAD WATERMARK
if DeltaTable.isDeltaTable(spark, watermark_path):
    watermark_df = spark.read.format("delta").load(watermark_path) \
        .filter(col("pipeline_name") == "patient_flow_gold")

    if len(watermark_df.select("last_processed_ts").head(1)) > 0:
        last_ts = watermark_df.select("last_processed_ts").collect()[0][0]
    else:
        last_ts = "1900-01-01"
else:
    last_ts = "1900-01-01"

# 2. READ INCREMENTAL SILVER
silver_df = spark.read.format("delta").load(silver_path)
batch_df = silver_df.filter(col("activity_time") > last_ts)

if batch_df.limit(1).count() == 0:
    print("No new data")
    dbutils.notebook.exit("No new data")

# 3. DIM PATIENT (SCD TYPE 2)
incoming_patient = batch_df.select(
    "patient_id", "gender", "age"
).dropDuplicates(["patient_id"]) \
.withColumn("effective_from", current_timestamp()) \
.withColumn("_hash",
    sha2(concat_ws("||",
        coalesce(col("gender"), lit("NA")),
        coalesce(col("age").cast("string"), lit("NA"))
    ), 256)
)

target = DeltaTable.forPath(spark, gold_dim_patient)

current_df = spark.read.format("delta").load(gold_dim_patient) \
    .filter(col("is_current") == True) \
    .select("patient_id", "_hash")

# detect change
staged = incoming_patient.alias("s") \
    .join(current_df.alias("t"), "patient_id", "left") \
    .withColumn("is_changed", col("t._hash").isNull() | (col("s._hash") != col("t._hash")))

# Split into updates for existing records and inserts for new versions
updates = staged.filter("is_changed = true") \
    .selectExpr("patient_id as mergeKey", "*")

inserts = staged.filter("is_changed = true") \
    .selectExpr("NULL as mergeKey", "*")

final_df = updates.unionByName(inserts)

target.alias("t").merge(
    final_df.alias("s"),
    "t.patient_id = s.mergeKey AND t.is_current = true"
).whenMatchedUpdate(set={
    "is_current": "false",
    "effective_to": "current_timestamp()"
}).whenNotMatchedInsert(values={
    "patient_id": "s.patient_id",
    "gender": "s.gender",
    "age": "s.age",
    "effective_from": "s.effective_from",
    "effective_to": "NULL",
    "is_current": "true",
    "_hash": "s._hash",
    "surrogate_key": "sha2(concat_ws('||', s.patient_id, s.effective_from), 256)"
}).execute()

# 4. DIM DEPARTMENT
incoming_dept = batch_df.select("department", "hospital_id").dropDuplicates()

if not DeltaTable.isDeltaTable(spark, gold_dim_department):
    incoming_dept.withColumn(
        "surrogate_key",
        sha2(concat_ws("||", col("department"), col("hospital_id")), 256)
    ).write.format("delta").save(gold_dim_department)

else:
    DeltaTable.forPath(spark, gold_dim_department).alias("t").merge(
        incoming_dept.alias("s"),
        "t.department = s.department AND t.hospital_id = s.hospital_id"
    ).whenNotMatchedInsert(values={
        "department": "s.department",
        "hospital_id": "s.hospital_id",
        "surrogate_key": "sha2(concat_ws('||', s.department, s.hospital_id), 256)"
    }).execute()
    
# 5. STATE TABLES
def read_delta_or_empty(path, schema):
    if DeltaTable.isDeltaTable(spark, path):
        return spark.read.format("delta").load(path)
    else:
        return spark.createDataFrame([], schema)

open_schema = "patient_id string, bed_id string, admission_time timestamp"
bed_schema = "bed_id string, patient_id string, occupied_since timestamp"

open_df = read_delta_or_empty(gold_open_sessions_path, open_schema)
bed_df = read_delta_or_empty(gold_bed_state_path, bed_schema)

admission_df = batch_df.filter(F.col("activity_type") == "Admission")
discharge_df = batch_df.filter(F.col("activity_type") == "Discharge")

# ---- ADMISSION ----
valid_admission = admission_df \
    .dropDuplicates(["patient_id", "bed_id", "activity_time"]) \
    .alias("a") \
    .join(open_df.select("patient_id").alias("o"), "patient_id", "left") \
    .join(bed_df.select("bed_id").alias("b"), "bed_id", "left") \
    .filter(col("o.patient_id").isNull() & col("b.bed_id").isNull())

new_open = valid_admission.select(
    "patient_id", "bed_id",
    col("activity_time").alias("admission_time")
)

if DeltaTable.isDeltaTable(spark, gold_open_sessions_path):
    DeltaTable.forPath(spark, gold_open_sessions_path).alias("t").merge(
        new_open.alias("s"),
        "t.patient_id = s.patient_id AND t.admission_time = s.admission_time"
    ).whenNotMatchedInsertAll().execute()
else:
    new_open.write.format("delta").save(gold_open_sessions_path)

# ---- BED STATE ----
new_bed = valid_admission.select(
    "bed_id",
    "patient_id",
    F.col("activity_time").alias("occupied_since")
)

if DeltaTable.isDeltaTable(spark, gold_bed_state_path):
    DeltaTable.forPath(spark, gold_bed_state_path).alias("t").merge(
        new_bed.alias("s"),
        "t.bed_id = s.bed_id AND t.occupied_since = s.occupied_since"
    ).whenNotMatchedInsertAll().execute()
else:
    new_bed.write.format("delta").save(gold_bed_state_path)

# 6. PATIENT DEPARTMENT 
latest_updates = batch_df.select(
    "patient_id", "department", "hospital_id", "activity_time"
)

latest_updates = latest_updates.withColumn(
    "rn",
    row_number().over(
        Window.partitionBy("patient_id").orderBy(col("activity_time").desc())
    )
).filter("rn = 1").drop("rn")

if not DeltaTable.isDeltaTable(spark, gold_patient_dept_state):
    latest_updates.write.format("delta").save(gold_patient_dept_state)
else:
    DeltaTable.forPath(spark, gold_patient_dept_state).alias("t").merge(
        latest_updates.alias("s"),
        "t.patient_id = s.patient_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

latest_patient_dept = spark.read.format("delta").load(gold_patient_dept_state)

# 7. DISCHARGE
open_df = read_delta_or_empty(gold_open_sessions_path, open_schema)

valid_discharge = discharge_df.alias("d") \
    .join(open_df.alias("o"), "patient_id") \
    .filter(col("d.bed_id") == col("o.bed_id")) \
    .select(
        col("d.patient_id"),
        col("d.bed_id"),
        col("d.activity_time"),
        col("o.admission_time")
    )

# 8. FACT BUILD 
fact_new_admissions = valid_admission.select(
    "patient_id", "bed_id",
    col("activity_time").alias("admission_time")
).withColumn("discharge_time", lit(None).cast("timestamp")) \
 .withColumn("length_of_stay_hours", lit(None).cast("double")) \
 .withColumn("is_currently_admitted", lit(True)) \
 .withColumn(
    "fact_key",
    sha2(concat_ws("||", col("patient_id"), col("admission_time")), 256)
 ) \
 .withColumn("admission_date", to_date("admission_time")) \
 .withColumn("event_ingestion_time", current_timestamp())

fact_closed = valid_discharge.select(
    col("patient_id"),
    col("bed_id"),
    col("admission_time"),
    col("activity_time").alias("discharge_time")
).withColumn(
    "length_of_stay_hours",
    (col("discharge_time").cast("long") - col("admission_time").cast("long")) / 3600
).withColumn("is_currently_admitted", lit(False))

fact_closed_updates = fact_closed.withColumn(
    "fact_key",
    sha2(concat_ws("||", col("patient_id"), col("admission_time")), 256)
)

# CLEAN UP STATE TABLES (Delete handled records)
if DeltaTable.isDeltaTable(spark, gold_open_sessions_path):

    DeltaTable.forPath(spark, gold_open_sessions_path).alias("t").merge(
        valid_discharge.alias("s"),
        """
        t.patient_id = s.patient_id 
        AND t.bed_id = s.bed_id
        AND t.admission_time = s.admission_time
        """
    ).whenMatchedDelete().execute()


if DeltaTable.isDeltaTable(spark, gold_bed_state_path):

    DeltaTable.forPath(spark, gold_bed_state_path).alias("t").merge(
        valid_discharge.alias("s"),
        "t.bed_id = s.bed_id AND t.patient_id = s.patient_id"
    ).whenMatchedDelete().execute()

# JOIN DIMENSIONS FOR ENRICHMENT
dim_patient = spark.read.format("delta").load(gold_dim_patient) \
    .filter(col("is_current") == True) \
    .select("patient_id", col("surrogate_key").alias("patient_sk"))

dim_department = spark.read.format("delta").load(gold_dim_department) \
    .select("department", "hospital_id", col("surrogate_key").alias("department_sk"))

fact_new_admissions_enriched = fact_new_admissions \
    .join(latest_patient_dept, "patient_id", "left") \
    .join(dim_patient, "patient_id", "left") \
    .join(dim_department, ["department", "hospital_id"], "left")

fact_closed_updates_enriched = fact_closed_updates \
    .join(latest_patient_dept, "patient_id", "left") \
    .join(dim_patient, "patient_id", "left") \
    .join(dim_department, ["department", "hospital_id"], "left")

fact_new_final = fact_new_admissions_enriched.select(
    "fact_key", "patient_sk", "department_sk",
    "admission_time", "discharge_time", "admission_date",
    "length_of_stay_hours", "is_currently_admitted",
    "bed_id", "event_ingestion_time"
)

fact_closed_final = fact_closed_updates_enriched.select(
    "fact_key",
    "discharge_time",
    "length_of_stay_hours"
)

# MERGE FACT
if not DeltaTable.isDeltaTable(spark, gold_fact):
    fact_new_final.write.format("delta").partitionBy("admission_date").save(gold_fact)
else:
    fact_table = DeltaTable.forPath(spark, gold_fact)

    # INSERT admission
    fact_table.alias("t").merge(
        fact_new_final.alias("s"),
        "t.fact_key = s.fact_key"
    ).whenNotMatchedInsertAll().execute()

    # UPDATE discharge
    fact_table.alias("t").merge(
        fact_closed_final.alias("s"),
        "t.fact_key = s.fact_key"
    ).whenMatchedUpdate(set={
        "discharge_time": "s.discharge_time",
        "length_of_stay_hours": "s.length_of_stay_hours",
        "is_currently_admitted": "false",
        "event_ingestion_time": "current_timestamp()"
    }).execute()

# 9. UPDATE WATERMARK
new_max_ts = batch_df.agg(F.max("activity_time")).collect()[0][0]

if new_max_ts is None:
    new_max_ts = last_ts

spark.createDataFrame(
    [("patient_flow_gold", new_max_ts)],
    ["pipeline_name", "last_processed_ts"]
).write.format("delta") \
.mode("overwrite") \
.option("replaceWhere", f"pipeline_name = 'patient_flow_gold'") \
.save(watermark_path)

# Quick sanity checks
# print("Patient dim count:", spark.read.format("delta").load(gold_dim_patient).count())
# print("Department dim count:", spark.read.format("delta").load(gold_dim_department).count())
# print("Fact rows:", spark.read.format("delta").load(gold_fact).count())

#Check
# display(spark.read.format("delta").load(gold_dim_patient))
# display(spark.read.format("delta").load(gold_dim_department))
# display(spark.read.format("delta").load(gold_fact))