# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC select count(*) from uber_catalog.uber_schema.stg_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC select ride_id, count(*) as duplicate_record from uber_catalog.uber_schema.stg_rides group by ride_id having count(*)>1  

# COMMAND ----------

# DBTITLE 1,Dedup verification
# MAGIC %sql
# MAGIC -- stg_rides is a pipeline-managed streaming table (created via dp.create_streaming_table in
# MAGIC -- transformations/my_transformation.py), so it cannot be rewritten from a notebook with
# MAGIC -- CREATE OR REPLACE TABLE or any other DML - only the owning pipeline can write to it.
# MAGIC -- Deduplication is now handled in the pipeline itself: the stg_rides_dedup materialized view
# MAGIC -- keeps exactly one row per ride_id (earliest booking_timestamp wins).
# MAGIC
# MAGIC -- Verify the deduped table: should return 0 rows
# MAGIC SELECT ride_id, COUNT(*) AS duplicate_record
# MAGIC FROM uber_catalog.uber_schema.stg_rides_dedup
# MAGIC GROUP BY ride_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ride_id, COUNT(*) AS duplicate_record
# MAGIC FROM uber_catalog.uber_schema.stg_rides_dedup
# MAGIC GROUP BY ride_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC
# MAGIC show columns in uber_catalog.uber_schema.silver_obt
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select passenger_id,passenger_name,passenger_email,passenger_phone
# MAGIC from
# MAGIC uber_catalog.uber_schema.silver_obt
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Testing Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_catalog.uber_schema.fact

# COMMAND ----------

# MAGIC %sql
# MAGIC select fact.ride_id, fact.base_fare, dim.region from uber_catalog.uber_schema.fact as fact
# MAGIC LEFT JOIN
# MAGIC uber_catalog.uber_schema.dim_location as dim
# MAGIC ON
# MAGIC fact.pickup_city_id = dim.pickup_city_id
# MAGIC --AND '__END_AT' IS NULL