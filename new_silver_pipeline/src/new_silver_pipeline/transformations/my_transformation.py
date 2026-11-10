from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

rides_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', StringType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])

# Empty streaming Table
dp.create_streaming_table(name="stg_rides")

@dp.append_flow(
    target = 'stg_rides',
)

def ride_bulks():
    df = spark.readStream.option("skipChangeCommits", "true").table("uber_catalog.uber_schema.bulk_rides")
    return df 


# Streaming Load
@dp.append_flow(
  target = "stg_rides"
  ) 
def rides_stream():
    df = spark.readStream.table("uber_catalog.uber_schema.table_raw")
    df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema)).select("parsed_rides.*")
    return df_parsed


# Deduplicated view of stg_rides: exactly one row per ride_id
# (keeps the row with the earliest booking_timestamp, same logic as the exploration notebook)
@dp.materialized_view(name="stg_rides_dedup")
def stg_rides_dedup():
    return (
        spark.read.table("stg_rides")
        .withColumn("rn", row_number().over(Window.partitionBy("ride_id").orderBy("booking_timestamp")))
        .filter(col("rn") == 1)
        .drop("rn")
    )





