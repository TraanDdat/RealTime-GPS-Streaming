import time
import psycopg2
from psycopg2 import sql
import threading
import csv
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import os
import re

# CSV_PATH = "User_0.csv"
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093"
TOPIC_NAME = "gps_topic"
DATA_DIR = "./data_csv"

def create_topic():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topic = NewTopic(name=TOPIC_NAME, num_partitions=6, replication_factor=2)
    try:
        admin.create_topics(new_topics=[topic], validate_only=False)
        print(f"Created topic: {TOPIC_NAME}")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' already exists, skipping creation.")
    finally:
        admin.close()

def normalize_row(row):
    user_id = row.get('User_ID') or row.get('id') or 'user_0'
    timestamp = row.get('Timestamp') or row.get('time') or row.get('ts')
    lat = row.get('lat') or row.get('Latitude')
    lon = row.get('lon') or row.get('Longitude')

    try:
        lat = float(lat) if lat not in (None, '', 'NaN', 'null') else None
        lon = float(lon) if lon not in (None, '', 'NaN', 'null') else None
    except Exception:
        lat, lon = None, None

    out = {'user_id': user_id, 'timestamp': timestamp, 'lat': lat, 'lon': lon}
    # for k, v in row.items():
    #     if k not in ('user_id','id','timestamp','time','ts','lat','latitude','lon','longitude'):
    #         out[k] = v
    return out

def on_send_success(record_metadata):
    print(f"Sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Failed to send message: {excp}")

def stream_user(producer, file_path):
    file_name = os.path.basename(file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print("Reader", reader)
        for row in reader:
            record = normalize_row(row)
            print("Record:", record)

            key = str(record["user_id"]).encode("utf-8")
            print("Key:", key)

            producer.send(
                TOPIC_NAME,
                key=key,
                value=record
            )

            print(f"[User {record['user_id']}] Sent -> {record}")

            time.sleep(1)

    print(f"Done streaming user file: {file_name}")

# def stream_user(producer, file_path):
#     """Gửi dữ liệu 1 user theo timestamp"""
#     with open(file_path, "r", encoding="utf-8") as f:
#         reader = csv.DictReader(f)
#         rows = sorted(reader, key=lambda x: datetime.strptime(x["Timestamp"], "%Y-%m-%d %H:%M:%S"))

#         prev_time = None

#         for row in rows:
#             record = normalize_row(row)
#             key = str(record["user_id"]).encode("utf-8")

#             # Tính khoảng cách thời gian giữa các điểm GPS
#             curr_time = datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S")
#             if prev_time:
#                 delta = (curr_time - prev_time).total_seconds()
#                 if delta > 0:
#                     time.sleep(delta)  # mô phỏng realtime
#             prev_time = curr_time

#             # Gửi message
#             producer.send(TOPIC_NAME, key=key, value=record)
#             print(f"[User {record['user_id']}] Sent → {record}")

# def read_and_send(producer):
#     files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".csv")])
#     print(f"Found {len(files)} CSV files.")
#     for file_name in files:
#         file_path = os.path.join(DATA_DIR, file_name)
#         with open(file_path, 'r', encoding='utf-8') as f:
#             reader = csv.DictReader(f)
#             for row in reader:
#                 record = normalize_row(row)
#                 #producer.send(TOPIC_NAME, value=record).add_callback(on_send_success).add_errback(on_send_error)
#                 producer.send(TOPIC_NAME,key=record["user_id"].encode("utf-8"), value=record)
#                 print("Sent →", record)
#                 time.sleep(0.2)  # chỉ dùng để demo, production có thể bỏ
#     producer.flush()  # đảm bảo tất cả message đã gửi


def extract_number(filename):
    m = re.search(r'(\d+)', filename)
    return int(m.group(1)) if m else -1

def read_and_send(producer):
    
    files = sorted(
    [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")],
    key=extract_number
    )
    
    print(f"Found",files)
    print(f"Found {len(files)} CSV files.")

    threads = []

    for file_name in files:
        file_path = os.path.join(DATA_DIR, file_name)
        print("File path:", file_path)
        t = threading.Thread(target=stream_user, args=(producer, file_path))
        print("Return thread:", t)
        threads.append(t)
        t.start()
        time.sleep(0.05)

    for t in threads:
        t.join()

    producer.flush()

def create_stop_detection_table(
    host="localhost",
    port=5432,
    dbname="gpsdb",
    user="gps",
    password="gps123",
    table_name="stop_detection"
):
    """Tạo bảng stop_detection nếu chưa tồn tại"""
    
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            user_id VARCHAR(50),
            cluster_id INT,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            duration_seconds DOUBLE PRECISION
        );
    """).format(table=sql.Identifier(table_name))

    create_index_query = sql.SQL("""
        CREATE INDEX IF NOT EXISTS idx_stop_user_ts
        ON {table} (user_id, start_ts);
    """).format(table=sql.Identifier(table_name))

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(create_table_query)
        cur.execute(create_index_query)

        print(f"Table '{table_name}' is ready.")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")

def create_table_if_not_exists(
    host="localhost",
    port=5432,
    dbname="gpsdb",
    user="gps",
    password="gps123",
    table_name="gps_processed"
):
    """Tạo table PostgreSQL nếu chưa tồn tại"""
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            user_id VARCHAR(50),
            ts TIMESTAMP,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION
        );
    """).format(table=sql.Identifier(table_name))

    create_index_query = sql.SQL("""
        CREATE INDEX IF NOT EXISTS idx_user_ts ON {table} (user_id, ts);
    """).format(table=sql.Identifier(table_name))

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        cursor.execute(create_index_query)
        print(f"Table '{table_name}' is ready.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")

if __name__ == '__main__':
    create_table_if_not_exists()
    #create_stop_detection_table()
    print("Starting Kafka producer...")

    create_topic()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    read_and_send(producer)
    producer.close()
    print("Producer finished sending data.")
