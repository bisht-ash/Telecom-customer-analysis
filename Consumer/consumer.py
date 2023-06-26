import re
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pathlib import Path


class DataConsumer:
    def __init__(self):
        load_dotenv()
        self.bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")  # Kafka bootstrap servers
        self.topic_name = os.getenv("TOPIC_NAME")  # Kafka topic name
        self.spark = self.create_spark_session()  # Create SparkSession
        sc=self.spark.sparkContext
        sc.setLogLevel('INFO')

    def create_spark_session(self):
        """
        Creates a SparkSession with necessary configurations.

        Returns:
            SparkSession: The created SparkSession object.
        """
        return SparkSession.builder.master("local[*]") .config(
                'spark.jars.packages',  # Specifies the necessary packages and versions
                'org.mongodb.spark:mongo-spark-connector_2.12:10.1.0,'
                'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,'
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,'
                'net.snowflake:snowflake-jdbc:3.12.17,'
                'net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2,'
                'org.apache.kafka:kafka-clients:3.2.2'
            ) \
            .getOrCreate()
    
    def read_from_mongo(self, collection):
        """
        Reads data from a MongoDB collection using Spark.

        Args:
            collection (str): The name of the collection to read from.

        Returns:
            pyspark.sql.DataFrame: The DataFrame containing the data from the MongoDB collection.
        """

        # MongoDB connection URI
        uri = os.getenv('MONGO_URI')  # Get the MongoDB connection URI from environment variables
        
        # Read data from MongoDB collection using Spark
        df = self.spark.read.format("mongodb").option('spark.mongodb.read.connection.uri',
                                                    uri + collection+"?readPreference=primaryPreferred").load()
        
        # Remove the '_id' column from the DataFrame
        df = df.drop(col('_id'))
        
        # Fill any missing values in the DataFrame with 'Not available'
        df.fillna('Not available')
        
        # Return the DataFrame
        return df        


    def read_stream_from_kafka(self):
        """
        Reads data from Kafka as a streaming DataFrame.

        Returns:
            DataFrame or None: The streaming DataFrame read from Kafka, or None if an error occurs.
        """
        try:
            df = self.spark.readStream.format('kafka') \
                .option('kafka.bootstrap.servers', self.bootstrap_servers) \
                .option('startingOffsets','earliest') \
                .option('subscribe', self.topic_name) \
                .option('failOnDataLoss',False) \
                .load()  # Loads the data as a streaming DataFrame
        except Exception as e:
            print("ERROR:", str(e))  # Prints the error message if an exception occurs
            return None
        return df


    def get_value_from_kafka_dataStream(self, revenue_df):
        """
        Extracts the values from a Kafka data stream DataFrame.

        Args:
            revenue_df (pyspark.sql.streaming.DataStream): The DataStream containing the Kafka data stream.

        Returns:
            pyspark.sql.streaming.DataFrame: The DataStream with the extracted values.
        """

        # Convert the 'value' column to string type
        revenue_df = revenue_df.selectExpr('CAST(value AS STRING)')

        # Define the schema for the values
        schema = StructType() \
            .add("msisdn", StringType()) \
            .add("week_number", IntegerType()) \
            .add("revenue_usd", FloatType())

        # Extract the values using the defined schema
        revenue_df = revenue_df.select(from_json(col("value"), schema).alias('data')).select(col("data.*"))

        # Return the DataStream with the extracted values
        return revenue_df


    def decode_private_key(self, private_key_file_path, password):
        """
        Decodes a private key from a file using a password.

        Args:
            private_key_file_path (str): The path to the private key file.
            password (str): The password used to decrypt the private key.

        Returns:
            str: The decoded private key as a string.
        """
        # Open the private key file in binary mode
        with open(private_key_file_path, "rb") as key_file:
            # Load the PEM-encoded private key from the file
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=password.encode(),
                backend=default_backend()
            )

        # Serialize the private key as PEM without encryption
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Decode the serialized private key from bytes to string
        pkb = pkb.decode("UTF-8")

        # Remove any leading/trailing dashes and newlines
        pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", "")

        # Return the decoded private key as a string
        return pkb


    def write_stream_to_snowflake(self, df, sf_options):
        """
        Writes a DataFrame to Snowflake as a streaming job.

        Args:
            df (pyspark.sql.streaming.DataStream): The DataFrame to write to Snowflake.
            sf_options (dict): A dictionary containing the Snowflake connection and configuration options.

        Note:
            The `sf_options` dictionary should include the following keys and their respective values:
            - "sfUrl": Snowflake URL
            - "sfUser": Snowflake username
            - "sfPassword": Snowflake password
            - "sfDatabase": Snowflake database name
            - "sfSchema": Snowflake schema name
            - "sfWarehouse": Snowflake warehouse name
            - "pem_private_key": Decoded private key to authenticate with Snowflake

        Raises:
            Exception: If there is an error during the streaming write operation.
        """
        try:
            # Write the DataFrame to Snowflake as a streaming job
            df.writeStream.format("net.snowflake.spark.snowflake") \
                .options(**sf_options) \
                .option('dbtable', 'final_data_5') \
                .option('mode','append') \
                .option('streaming_stage', 'stream_stg_5') \
                .option("checkpointLocation", "/tmp/checkpoint") \
                .option("continue_on_error","on") \
                .start().awaitTermination()
        except Exception as e:
            # Print an error message if there is an exception during the streaming write operation
            print("ERROR:", str(e))


    def consume_data(self):
        """
        Main data consumption function.

        This function performs the following steps:
        1. Reads data from MongoDB.
        2. Performs a join operation on the data from MongoDB.
        3. Reads streaming data from Kafka.
        4. If there is streaming data available, performs the following operations:
            a. Selects the necessary columns from the streaming data.
            b. Defines the schema for the streaming data.
            c. Applies the schema to the streaming data and extracts the required fields.
            d. Performs an inner join operation between the joined MongoDB data and the streaming data.
            e. Retrieves the private key file path and password from environment variables.
            f. Decodes the private key using the provided password.
            g. Defines the Snowflake connection and configuration options.
            h. Writes the merged data to Snowflake as a streaming job.

        Note:
            The function assumes that the necessary data, environment variables, and resources are available.
        """
        crm_df= self.read_from_mongo("crm1")  # Read data from MongoDB
        device_df=self.read_from_mongo("device1")  # Read data from MongoDB
        crm_x_device_df = crm_df.join(device_df, on='msisdn', how='full')  # Perform join operation
        revenue_df = self.read_stream_from_kafka()  # Read streaming data from Kafka

        if(revenue_df is not None):
            revenue_df=self.get_value_from_kafka_dataStream(revenue_df)

            merged_data_df = crm_x_device_df.join(broadcast(revenue_df), on='msisdn', how='inner')  # Perform join operation

            private_key_file_path = "/Consumer/keys/rsa_key.p8"
            password = os.getenv('FILE_PASSWORD')
            pem_private_key = self.decode_private_key(private_key_file_path, password)

            sf_options = {
                "sfUrl": os.getenv('SF_URL'),
                "sfUser": os.getenv('SF_USER'),
                "sfPassword": os.getenv('SF_PASSWORD'),
                "sfDatabase": os.getenv('SF_DATABASE'),
                "sfSchema": os.getenv('SF_SCHEMA'),
                "sfWarehouse": os.getenv('SF_WAREHOUSE'),
                "pem_private_key": pem_private_key
            }

            self.write_stream_to_snowflake(merged_data_df, sf_options)  # Write data to Snowflake


if __name__ == "__main__":
    processor = DataConsumer()
    processor.consume_data()
