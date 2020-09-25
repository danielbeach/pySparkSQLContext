# pySparkSQLContext
Learning to use SQLContext with PySpark.
Full blog post at https://www.confessionsofadataguy.com/pyspark-sqlcontext-tired-of-your-decades-old-etl-process/


1. get CSV data from Divvy Bikes.  https://divvy-tripdata.s3.amazonaws.com/index.html
2. Put files into your storage of choice.
  - curl https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip 
  - Unzip Divvy_trips_2020_Q1.zip
  - hdfs dfs -mkdir /tripdata
  - hdfs dfs -copyFromLocal Divvy_Trips_2020_Q1.csv /tripdata
  
3. From your Spark cluster.... `/usr/local/spark/bin/spark-submit pySparkDataFrames.py`
