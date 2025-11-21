from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import psycopg2
from datetime import datetime

kafka_bootstrap = "localhost:9092,localhost:9093"
input_topic = "gps_topic"
jdbc_url = "jdbc:postgresql://localhost:5432/gpsdb"

# DBSCAN PARAMETERS
earth_radius = 6371000
eps = 50 / earth_radius
min_samples = 5

def spark_init():

    spark = (SparkSession.builder
            .appName("gps_stream_cleaner")
            .master("local[*]")
            .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Ho_Chi_Minh")
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Ho_Chi_Minh")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
            .config("spark.jars",
                    "./tools/postgresql-42.7.1.jar")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("user_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType())
    ])

    raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", kafka_bootstrap)
       .option("subscribe", input_topic)
       .option("startingOffsets", "latest")  
       .option("failOnDataLoss", "false")    
       .load())

    json_df = (raw
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*"))

    json_df = json_df.withColumn("ts", to_timestamp(col("timestamp")))

    json_df = json_df.filter(
        col("lat").isNotNull() &
        col("lon").isNotNull() &
        col("ts").isNotNull() &
        col("user_id").isNotNull()
    )

    return json_df

def create_stop_table_if_not_exists():
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="gpsdb", user="gps", password="gps123"
        )
        conn.autocommit = True
        cur = conn.cursor()

        create_sql = """
        CREATE TABLE IF NOT EXISTS stop_detection (
            user_id VARCHAR(50),
            cluster_id INTEGER,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            duration_seconds INTEGER,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_sql)
    
        cur.execute("""
            CREATE INDEX idx_stop_user_time ON stop_detection (user_id, start_ts);
        """)
        print("Table stop_detection is ready.")
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating stop table: {e}")

def detect_stops_for_user(pdf: pd.DataFrame):
    if pdf.shape[0] < 3:
        return []

    coords = pdf[['lat', 'lon']].to_numpy()
    coords = np.radians(coords)

    try:
        db = DBSCAN(eps=eps, min_samples=min_samples, metric='haversine').fit(coords)
        pdf = pdf.copy()
        pdf['cluster'] = db.labels_
    except Exception as e:
        print(f"DBSCAN error: {e}")
        return []

    stops = []

    for cid in sorted(set(db.labels_)):
        if cid == -1:
            continue

        cluster_points = pdf[pdf['cluster'] == cid]

        if cluster_points.empty:
            continue

        lat_mean = cluster_points['lat'].mean()
        lon_mean = cluster_points['lon'].mean()
        start_ts = cluster_points['ts'].min()
        end_ts = cluster_points['ts'].max()

        duration = (end_ts - start_ts).total_seconds()

        if duration >= 300:  # 300 seconds = 5 minutes
            stops.append({
                "user_id": str(cluster_points['user_id'].iloc[0]),
                "cluster_id": int(cid),
                "lat": float(lat_mean),
                "lon": float(lon_mean),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "duration_seconds": int(duration)
            })

    return stops

def process_batch(df, batch_id):
    print(f"\n=== Processing batch {batch_id} ===")
    
    try:
        df.select("user_id", "ts", "lat", "lon") \
            .write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "gps_processed") \
            .option("user", "gps") \
            .option("password", "gps123") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print("GPS written to gps_processed")

        pdf = df.toPandas()
        if pdf.empty:
            print("No data in batch")
            return

        users = pdf['user_id'].unique()
        print(f"Processing {len(users)} users in batch {batch_id}")

        stop_results = []

        for u in users:
            one_user = pdf[pdf['user_id'] == u].sort_values("ts")
            print(f"User {u}: {len(one_user)} points")
            print(f"One User Data:", one_user)
            stops = detect_stops_for_user(one_user)
            stop_results.extend(stops)
            if stops:
                print(f"User {u}: {len(stops)} stops detected")
                print(f"Stops Data: ", stops)

        if len(stop_results) == 0:
            print("No stops detected in this batch.")
            return

        print(f"Total stops detected: {len(stop_results)}")

        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="gpsdb", user="gps", password="gps123"
        )
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO stop_detection (
                user_id, cluster_id, lat, lon,
                start_ts, end_ts, duration_seconds
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for row in stop_results:
            cur.execute(insert_sql, (
                row["user_id"], row["cluster_id"],
                row["lat"], row["lon"],
                row["start_ts"], row["end_ts"],
                row["duration_seconds"]
            ))

        conn.commit()
        cur.close()
        conn.close()

        print(f"STOP detection saved: {len(stop_results)} stops")
        
    except Exception as e:
        print(f"ERROR in batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# Checkpoint path
checkpoint = "./chk_postgres" 
if __name__ == '__main__':

    print("Spark Init...")
    json_df = spark_init()
    
    create_stop_table_if_not_exists()
    
    print("Starting Spark Streaming...")

    query = (json_df.writeStream
            .foreachBatch(process_batch)
            .trigger(processingTime='1 seconds')
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .start())

    query.awaitTermination()