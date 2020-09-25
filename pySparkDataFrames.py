from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("Learn SQLContext") \
    .getOrCreate()
sqlc = SQLContext(spark)

df = spark.read.csv(
    path="hdfs://master:9000/tripdata/*.csv",
    sep=",",
    header=True
)

# switch over to SQL context so we can easily change column names
sqlc.registerDataFrameAsTable(df, "trips_data")

df2 = sqlc.sql("""SELECT 
                `01 - Rental Details Rental ID`  AS rental_id,
                `01 - Rental Details Local Start Time`  AS rental_start_time,
                `01 - Rental Details Local End Time`  AS rental_end_time,
                `01 - Rental Details Bike ID`  AS bike_id,
                `01 - Rental Details Duration In Seconds Uncapped`  AS ride_duration,
                `03 - Rental Start Station ID`  AS start_station_id,
                `03 - Rental Start Station Name`  AS start_station_name,
                `02 - Rental End Station ID`  AS end_station_id,
                `02 - Rental End Station Name`  AS end_station_name,
                `User Type`  AS user_type,
                `Member Gender`  AS member_gender,
                `05 - Member Details Member Birthday Year`  AS member_birth_year
                FROM trips_data""")

sqlc.registerDataFrameAsTable(df2, "trip_duration_data_mart")

data_mart_df = spark.sql("""SELECT 
                YEAR(to_date(rental_start_time)) as rental_year,
                MONTH(to_date(rental_start_time)) as rental_month,
                 AVG(ride_duration) as avg_duration
                FROM trip_duration_data_mart
                GROUP BY YEAR(rental_start_time), MONTH(rental_start_time)
                ORDER BY rental_month ASC
                """)

sqlc.registerDataFrameAsTable(df2, "trip_duration_by_age")

data_mart_age_df = spark.sql("""SELECT 
                (YEAR(current_date()) - member_birth_year) as age,
                 AVG(ride_duration) as avg_duration
                FROM trip_duration_data_mart
                GROUP BY (YEAR(current_date()) - member_birth_year)
                ORDER BY age ASC
                """)

data_mart_age_df.show()
