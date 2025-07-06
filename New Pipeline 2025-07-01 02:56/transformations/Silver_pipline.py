from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# -----------------------------bookings_pipline------------------------------
@dlt.table(name="Stage_bookings")
def Stage_bookings():
    return spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/bookings/data")

@dlt.view(name="Trans_bookings")
def Trans_bookings():
    df = spark.readStream.table("Stage_bookings")
    df = df.withColumn("amount", col("amount").cast("double"))\
           .withColumn("booking_date", to_date(col("booking_date")))\
           .withColumn("modified_Data",current_timestamp())\
           .drop("_rescued_data")
    return df

roles = {
    "role1": "booking_id IS NOT NULL",
    "role2": "passenger_id IS NOT NULL",
    "role3": "flight_id IS NOT NULL",
    "role4": "airport_id IS NOT NULL"
}

@dlt.table(name="Silver_bookings")
@dlt.expect_all(roles)
def Silver_bookings():
    return spark.readStream.table("Trans_bookings")


# -----------------------------------------flights_pipline------------------------------
@dlt.view(name="stage_flights")
def stage_flights():
    return spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data")

@dlt.view(name="transe_flights")
def transe_flights():
    return dlt.readStream("stage_flights")\
              .withColumn("flight_date", to_date(col("flight_date")))\
              .withColumn("modified_Data",current_timestamp())\
              .drop("_rescued_data")

rules = {
    "rore1": "flight_id IS NOT NULL"
}

@dlt.table(name="silver_flights")
@dlt.expect_all(rules)
def silver_flights():
    return dlt.readStream("transe_flights")


# --------------------------------------passengers_pipline------------------------------
@dlt.view(name="stage_passengers")
def stage_passengers():
    return spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/passengers/data")

@dlt.view(name="transe_passengers")
def transe_passengers():
    df2 = spark.readStream.table("stage_passengers")
    df2 = df2.withColumn("modified_Data",current_timestamp())\
           .drop("_rescued_data")
    return df2

rules = {
    "role1": "passenger_id IS NOT NULL"
}

@dlt.table(name="silver_passengers")
@dlt.expect_all(rules)
def silver_passengers():
    return dlt.readStream("transe_passengers")


# --------------------------------------------airports_pipline------------------------------
@dlt.view(name="stage_airports")
def stage_airports():
    return spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data")

@dlt.view(name="transe_airports")
def transe_airports():
    df3 = spark.readStream.table("stage_airports")
    df3 = df3.withColumn("modified_Data",current_timestamp())\
           .drop("_rescued_data")
    return df3
rules = {
    "role1": "airport_id IS NOT NULL"
}

@dlt.table(name="silver_airports")
@dlt.expect_all(rules)
def silver_airports():
    return dlt.readStream("transe_airports")


# -----------------------------Busniess_Silver_table------------------------------
@dlt.table(name="Silver_Busniess")
def Silver_Busness():
    df4 = dlt.readStream("Silver_bookings")\
            .join(dlt.readStream("silver_flights"), ["flight_id"])\
            .join(dlt.readStream("silver_passengers"), ["passenger_id"])\
            .join(dlt.readStream("silver_airports"), ["airport_id"])\
            .drop("_rescued_data")\
            .drop("modified_Data")\
            .withColumn("last_modified_Data",current_timestamp())
            

    return df4
