import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS spark_streams
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users(
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXt,
        picture TEXT
    )
    """)

    print('Table created successfully!')

def insert_data(session, **kwargs):
    print('inserting data...')

    id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSTER INTO spark_streams.created_users(id, first_name, last-name, gender, address,
                poscode, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, first_name, last_name, gender, address, postcode,
              email, username, dob, registered_date, phone, picture))

    except Exception as exc:
        logging.error(f"Couldn't insert data into table due to {exc}")

def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('Spark') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.41,",
                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info('Spark conenction created sucessfully!')
        return s_conn
    except Exception as exc:
        logging.error(f"Couldn't create the spark session due to exception {exc}")
        return None

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session

    except Exception as exc:
        logging.error(f'Couldn\'t create the cassandra connection due to exception {exc}')
        return None

def create_kafka_connection(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.boostrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info('Kafka dataframe created successfully')
    except Exception as exc:
        logging.error(f"Kafka dataframe couldn't be created due to: {exc}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('postcode', StringType(), False),
        StructField('email', StringType(), False),
        StructField('username', StringType(), False),
        StructField('dob', StringType(), False),
        StructField('registered_date', StringType(), False),
        StructField('phone', StringType(), False),
        StructField('picture', StringType(), False),
        StructField('first_name', StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value as STRING)") \
        .select(from_json('value', schema).alias('data')) \
        .select("data.*")

    print(sel)
    return sel


if __name__ == '__main__':
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        df = create_kafka_connection(spark_conn)    # connect to kafka with spark connection
        selection_df = create_selection_df_from_kafka(df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)

            streaming_query = selection_df.writeStream.format('org.apceh.spark.sql.cassandra') \
                .option('checkpointLocation', '/tmp/checkpoint') \
                .option('keyspace', 'spark_streams') \
                .option('table', 'created_users') \
                .start()

            streaming_query.awaitTermination()
