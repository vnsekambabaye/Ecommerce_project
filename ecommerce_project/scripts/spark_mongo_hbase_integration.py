#!C:/Python310/python.exe
import pymongo
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, datediff, current_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os
import sys
import logging

os.environ["PYSPARK_PYTHON"] = "C:/Python310/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Python310/python.exe"

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_spark():
    return SparkSession.builder \
        .appName("CustomerLifetimeValue") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def get_mongo_data():
    try:
        mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
        mongo_db = mongo_client['ecommerce_db']

        users_data = list(mongo_db['users'].find({}, {'user_id': 1, 'registration_date': 1, '_id': 0}))
        transactions_data = list(mongo_db['transactions'].find({}, {'user_id': 1, 'total': 1, 'product_id': 1, '_id': 0}))
        products_data = list(mongo_db['products'].find({}, {'product_id': 1, 'category_id': 1, 'name': 1, '_id': 0}))
        categories_data = list(mongo_db['categories'].find({}, {'category_id': 1, 'name': 1, '_id': 0}))

        logging.info("MongoDB data successfully retrieved.")
        return users_data, transactions_data, products_data, categories_data, mongo_client
    except Exception as e:
        logging.error("Failed to connect to MongoDB: %s", e)
        sys.exit(1)

def get_hbase_data():
    try:
        hbase_connection = happybase.Connection('localhost')
        hbase_connection.open()
        table = hbase_connection.table('user_sessions')
        session_data = []

        for row_key, data in table.scan():
            try:
                user_id = row_key.decode().split(':')[0]
                duration = int(data.get(b'info:duration', b'0').decode())
                session_data.append({'user_id': user_id, 'duration': duration})
            except Exception as parse_error:
                logging.warning("Skipping malformed row: %s", parse_error)
        
        logging.info("HBase session data successfully retrieved.")
        return session_data, hbase_connection
    except Exception as e:
        logging.error("Failed to connect to HBase: %s", e)
        sys.exit(1)

def main():
    spark = initialize_spark()

    # --- Load MongoDB Data ---
    users_data, transactions_data, products_data, categories_data, mongo_client = get_mongo_data()

    users_schema = StructType([
        StructField('user_id', StringType(), True),
        StructField('registration_date', StringType(), True)
    ])
    transactions_schema = StructType([
        StructField('user_id', StringType(), True),
        StructField('product_id', StringType(), True),
        StructField('total', DoubleType(), True)
    ])
    products_schema = StructType([
        StructField('product_id', StringType(), True),
        StructField('category_id', StringType(), True),
        StructField('name', StringType(), True)
    ])
    categories_schema = StructType([
        StructField('category_id', StringType(), True),
        StructField('name', StringType(), True)
    ])

    users_df = spark.createDataFrame(users_data, schema=users_schema)
    transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)
    products_df = spark.createDataFrame(products_data, schema=products_schema)
    categories_df = spark.createDataFrame(categories_data, schema=categories_schema)

    # --- Enrich Transactions with Product and Category Info ---
    product_enriched_df = transactions_df.join(products_df, 'product_id', 'left') \
                                         .join(categories_df.withColumnRenamed('name', 'category_name'), 'category_id', 'left')

    transactions_agg = product_enriched_df.groupBy('user_id') \
        .agg({'total': 'sum'}) \
        .withColumnRenamed('sum(total)', 'total_spending')

    # --- Load HBase Data ---
    session_data, hbase_connection = get_hbase_data()

    sessions_schema = StructType([
        StructField('user_id', StringType(), True),
        StructField('duration', IntegerType(), True)
    ])
    sessions_df = spark.createDataFrame(session_data, schema=sessions_schema)

    sessions_agg = sessions_df.groupBy('user_id') \
        .agg(
            count('*').alias('session_count'),
            avg('duration').alias('avg_session_duration')
        )

    # --- Join and Calculate CLV ---
    clv_df = users_df.join(transactions_agg, 'user_id', 'left') \
        .join(sessions_agg, 'user_id', 'left') \
        .fillna({'total_spending': 0, 'session_count': 0, 'avg_session_duration': 0}) \
        .withColumn('registration_date', col('registration_date').cast(TimestampType())) \
        .withColumn('tenure_days', datediff(current_date(), col('registration_date'))) \
        .withColumn('sessions_per_month', col('session_count') / (col('tenure_days') / 30.0)) \
        .withColumn('engagement_score', 
            col('sessions_per_month') / 10.0 + 
            col('avg_session_duration') / 3600.0
        ) \
        .withColumn('clv', col('total_spending') * col('engagement_score'))

    clv_results = clv_df.select(
        'user_id', 'total_spending', 'session_count', 'avg_session_duration',
        'tenure_days', 'engagement_score', 'clv'
    ).orderBy(col('clv').desc())

    # --- Save Results ---
    output_path = os.path.join(os.getcwd(), 'clv_results.csv')
    clv_results.write.mode('overwrite').option('header', 'true').csv(output_path)
    logging.info(f"CLV results saved to: {output_path}")

    # --- Show Top 10 ---
    print("Top 10 Customers by Estimated CLV:")
    clv_results.show(10, truncate=False)

    # --- Cleanup ---
    mongo_client.close()
    hbase_connection.close()
    spark.stop()
    logging.info("All connections closed and Spark stopped.")

if __name__ == "__main__":
    main()


