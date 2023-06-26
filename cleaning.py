import os
from os.path import join, dirname
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, row_number, last, count
from pyspark.sql.window import Window
from fuzzywuzzy import fuzz
import pymongo
import boto3
import re


# load variables from .env file
dotenv_path=join(dirname(__file__),"dags/.env")
load_dotenv(dotenv_path=dotenv_path)

# Accessing environment variables
AWS_ACCESS_KEY=os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY=os.getenv('AWS_SECRET_KEY')
CRM_PATH=os.getenv('CRM_PATH')
DEVICE_PATH=os.getenv('DEVICE_PATH')


# Creating singleton class ,so that it creates only one spark instance
class SingletonSparkSession:
    """
        Creates a SparkSession with necessary configurations.

        Returns:
            SparkSession: The created SparkSession object.
    """

    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.spark = SparkSession.builder \
                .appName("read_S3_write_mongodb") \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.12.180") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.driver.memory", "15g").config("spark.driver.maxResultSize", "4g") \
                .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)\
                .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)\
                .getOrCreate()
        return cls._instance.spark


# Writing the data to moongodb
class SingletonMongoWriter:
    """
    Singleton class for writing data to MongoDB.
    """
    _instance = None

    def __new__(cls):
        """
        Create and return a new instance of the class if it doesn't exist.

        Returns:
            SingletonMongoWriter: The instance of the class.
        """
         
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def write_to_Mongodb(self, dataframe, output_uri):
        """
        Write a DataFrame to MongoDB.

        Args:
            dataframe (pyspark.sql.DataFrame): The DataFrame containing the data to be written.
            output_uri (str): The MongoDB connection URI specifying the target database and collection.

        Returns:
            None
        """
        dataframe.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append") \
            .option("spark.mongodb.output.uri", output_uri) \
            .save()


# For validating data
class DataProcessor:
    """
    A class that validates and processes data.

    This class provides methods for checking file existence in an S3 bucket,
    performing fuzzy matching on words to determine gender, and processing
    CRM and device data.

    Attributes:
        spark (SparkSession): The singleton SparkSession object.
        mongo_writer (SingletonMongoWriter): The singleton MongoDB writer object.

    Methods:
        check_file_exists_in_S3(bucket_name, key):
            Checks if a file exists in the specified S3 bucket.
        
        fuzzy_ratio(word):
            Determines the gender based on a fuzzy matching ratio of the input word.
        
        process_crm_data(crm_s3_path):
            Processes CRM data from an S3 bucket.
        
        process_device_data(device_s3_path):
            Processes device data from an S3 bucket.
    """
    def __init__(self):
        """
        Initializes the DataProcessor object.

        It creates instances of the SingletonSparkSession and SingletonMongoWriter
        classes to access the SparkSession and MongoDB writer objects.
        """
        self.spark = SingletonSparkSession()
        self.mongo_writer = SingletonMongoWriter()


    def check_file_exists_in_S3(self, bucket_name, key):
        """
        Checks if a file exists in the specified S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            key (str): The key or path of the file in the S3 bucket.

        Returns:
            bool: True if the file exists in the S3 bucket, False otherwise.
        """

        # Create a session using AWS access key and secret key
        session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

        # Create an S3 client using the session
        s3_client = session.client('s3')
        
        try:
            # Use the head_object method to check if the file exists
            s3_client.head_object(Bucket=bucket_name, Key=key)
            print(f"The file '{key}' exists in the bucket '{bucket_name}'.")
            return True
        
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # If the file does not exist, handle the 404 error
                print(f"The file '{key}' does not exist in the bucket '{bucket_name}'.")
            else:
                # Handle other exceptions
                print(f"We got an exception: {e}")

            return False


    @staticmethod
    def fuzzy_ratio(word):
        """
        Determines the gender based on a fuzzy matching ratio of the input word with 'male' and 'female'.

        Args:
            word (str): The input word.

        Returns:
            str or None: The gender ('male' or 'female') if a match is found with a sufficient ratio, or None otherwise.
        """

        if word is None:
            return None

        male_pattern = "ma(l)*(e)*"
        female_pattern = "fe(m)*(a)*(l)*(e)*"

        # Convert the word to lowercase for case-insensitive matching
        word = word.lower()

        # Check if the word directly matches 'f' or matches the female pattern
        if word == "f" or re.search(female_pattern, word):
            return "female"

        # Check if the word directly matches 'm' or matches the male pattern
        if word == "m" or re.search(male_pattern, word):
            return "male"

        # Calculate fuzzy matching ratios between the word and 'male' and 'female'
        ratio1 = fuzz.ratio(word, "male")
        ratio2 = fuzz.ratio(word, "female")

        # Determine the gender based on the fuzzy matching ratios
        if ratio1 >= 70:
            res = "male"
        elif ratio2 >= 70:
            res = "female"
        else:
            res = None

        return res


    def process_crm_data(self, crm_s3_path):
        """
        Processes CRM data from an S3 bucket.

        Args:
            crm_s3_path (str): The S3 path of the CRM data file.

        Returns:
            DataFrame or None: The processed CRM data as a DataFrame if successful, None otherwise.
        """

        # Checking file exits or not in s3 bucket
        bucket_name = "telecom-data-source"
        key = "crm1.csv"

        file_flag = self.check_file_exists_in_S3(bucket_name=bucket_name, key=key)

        if not file_flag:
            # If the file doesn't exist, print an error message and return None
            print("Some error occured while getting data from s3 bucket")
            return None
        
        # Creating user define function to transform gender column.
        fuzzy_ratio_udf = self.spark.udf.register("fuzzy_ratio", self.fuzzy_ratio)

        #  Read CRM data from the provided s3 file
        crm_df = self.spark.read.option('header','true').option('inferSchema','true').format("csv").load(crm_s3_path)

        # # Clean and normalize 'gender' column values and replacing null values in year_of_birth with not available
        crm_df = crm_df.withColumn("gender", fuzzy_ratio_udf(col("gender")))\
                       .withColumn("year_of_birth", when(col("year_of_birth").isNull(), 'Not Available').otherwise(col("year_of_birth")))

          
        # Fill the last non-null value in the 'gender' column for each 'msisdn'
        window_spec = Window.partitionBy("msisdn").orderBy(col("year_of_birth"))
        crm_df = crm_df.withColumn("row_number1", row_number().over(window_spec))

        window_spec = Window.partitionBy("msisdn").orderBy(col("row_number1")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        crm_df = crm_df.withColumn('gender', last('gender', ignorenulls=True).over(window_spec))
        
        window_spec = Window.partitionBy("msisdn").orderBy(col("row_number1").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        crm_df = crm_df.withColumn('gender', last('gender', ignorenulls=True).over(window_spec))
        # Filling all null with Not Available in gender column
        crm_df = crm_df.drop("row_number1")
        crm_df = crm_df.withColumn("gender", when(col("gender").isNull(), "Not Available").otherwise(col("gender")))


        # Create an outer window partitioned by "msisdn"
        outer_window = Window.partitionBy("msisdn")

        # Create a window partitioned by multiple columns including "msisdn", "year_of_birth", "mobile_type", and "gender"
        window = outer_window.partitionBy("msisdn", "year_of_birth", "mobile_type", "gender")

        # Drop duplicates
        crm_df = crm_df.dropDuplicates()

        # Adding required columns to the crm_df 
        crm_df = crm_df.withColumn("outer_window", count("*").over(outer_window))\
                        .withColumn("no_of_deactive", count(when(col("system_status").isin("DEACTIVE"), True)).over(outer_window)) \
                        .withColumn("count_except_deactive", count(when(col("system_status").isin("ACTIVE", "SUSPEND", "IDLE"), True)).over(window))\
                        .withColumn("total_active", count(when((col("system_status") == "ACTIVE"), True)).over(outer_window))\
                        .withColumn("count_active", count(when(col("system_status") == 'ACTIVE', True)).over(window)) \
                        .withColumn("count_idle", count(when(col("system_status").isin("IDLE"), True)).over(window))\
                        .withColumn("total_idle", count(when((col("total_active") == 0) & (col("system_status") == "IDLE"), True)).over(outer_window))

        # Replacing the count_except_deactive columm value if it satisfy the given condition
        crm_df = crm_df.withColumn("count_except_deactive", when(col("outer_window") == col("count_except_deactive"), 1).otherwise(col("count_except_deactive")))

        # Replacing the count_except_deactive columm value if it satisfy the given conditions
        crm_df = crm_df.withColumn("count_except_deactive", when(((col("count_active") == 0) & (col("count_except_deactive") == 1) & (col("total_active") >= 1) & (col("no_of_deactive") == 0)), -1).
                                                when(((col("count_active") == 0) & (col("count_except_deactive") == 1) & (col("total_active") == 0) & (col("total_idle") >= 1) & (col("count_idle") == 0)), -1).
                                                otherwise(col("count_except_deactive")))\
                        .withColumn("taken", count(when((col("count_except_deactive") == 1) & (col("system_status") == "ACTIVE"), 1)).over(outer_window))
   
        # Filtering the based on count_except_deactive column value 
        crm_df = crm_df.filter(~(col("count_except_deactive") == -1))

        #  # Replacing the count_except_deactive columm value if it satisfy the given conditions
        crm_df = crm_df.withColumn("count_except_deactive", when((col("total_active") >= 1) & (col("count_active") >= 1) & (col("taken") == 0), 1)
                                                            .when(((col("total_active") == 0) & (col("total_idle") >= 1) & (col("system_status") == "IDLE") & (col("count_idle") >= 1)), 1)
                                                            .when(((col("total_active") == 0) & (col("total_idle") == 0) & (col("system_status") == "SUSPEND") & (col("count_idle") == 0)), 1)
                                                            .otherwise(col("count_except_deactive")))
        # Filtering the records based on conditions
        crm_df = crm_df.filter(((col("system_status") == "DEACTIVE") & (col("no_of_deactive") != 0)) |
                                            ((col("count_except_deactive") == 1) & (col("system_status") == "ACTIVE") & (col("no_of_deactive") == 0)) |
                                            ((~(col("system_status") == "ACTIVE") & (col("count_active") == 0)  & (col("count_except_deactive") == 1) & (col("system_status") == "IDLE") & (col("no_of_deactive") == 0 ) &(col("total_active") == 0))) |
                                            (((col("count_except_deactive") == 1) & (col("count_idle") == 0) & (col("count_active") == 0 ) & (col("system_status") == "SUSPEND") & (col("no_of_deactive") == 0 ))) |
                                            (((col("total_active") >= col("count_active")) & (col("system_status") == "ACTIVE") & (col("count_except_deactive") == 1) & (col("no_of_deactive") == 0) & (col("taken") == 0))) |
                                            ((col("count_active") >= 1) & (col("system_status") == 'ACTIVE') & (col("no_of_deactive") == 0) & (col("taken") == 0)))

        # Drop the unnnessary columns
        crm_df= crm_df.drop("outer_window", "no_of_deactive", "count_except_deactive", "count_active", "count_idle", "total_idle", "total_active", "taken")
    

        # Filter duplicate 'msisdn' entries based on 'system_status'
        window_spec = Window.partitionBy("msisdn").orderBy(col("system_status").asc())
        crm_df = crm_df.withColumn("row_number", row_number().over(window_spec))
        crm_df = crm_df.filter(col("row_number") == 1).drop("row_number")
        

        # Write the processed CRM data to MongoDB
        self.mongo_writer.write_to_Mongodb(crm_df, 'mongodb://127.0.0.1/ProjectDB.crm1')

        # Writing to local
        # crm_df.coalesce(1).to_csv('results/crm_validate.csv')
        return crm_df


    def process_device_data(self, device_s3_path):
        """
        Processes device data from an S3 bucket.

        Args:
            device_s3_path (str): The S3 path of the device data file.

        Returns:
            DataFrame or None: The processed device data as a DataFrame if successful, None otherwise.
        """

        # Checking if the file exists in the S3 bucket
        bucket_name = "telecom-data-source"
        key = "device1.csv"
        file_flag = self.check_file_exists_in_S3(bucket_name=bucket_name, key=key)
        if not file_flag:
            print("Some error occurred while getting data from the S3 bucket")
            return None

        # Read device data from CSV
        device_df = self.spark.read.option("header", "true").option("inferSchema", "true").format("csv").load(device_s3_path)

        # Finding the number of nulls in each row
        device_df = device_df.withColumn("no_of_nulls", sum([col(c).isNull().cast('int') for c in device_df.columns]))

        # Remove duplicate 'msisdn' entries based on 'imei_tac' and select the record with fewer null values
        window_spec = Window.partitionBy("msisdn").orderBy('no_of_nulls')
        device_df = device_df.withColumn("row_number", row_number().over(window_spec))
        device_df = device_df.filter(col('row_number') == 1)
        device_df = device_df.drop("row_number").drop("no_of_nulls")

        # Fill missing values in the dataframe with "Not Available"
        device_df = device_df.fillna('Not Available')

        # Write the processed device data to MongoDB
        self.mongo_writer.write_to_Mongodb(device_df, 'mongodb://127.0.0.1/ProjectDB.device1')

        # Writing to local
        # device_df.coalesce(1).to_csv('results/device_validate.csv')

        return device_df



def databaseValidation(db_name, collection_name):
    """
    Validates the existence of a MongoDB database and collection, and checks if a document exists in the collection.

    Args:
        db_name (str): The name of the MongoDB database.
        collection_name (str): The name of the collection within the database.

    Returns:
        int: 1 if the database is validated, 0 otherwise.
    """

    # Create a MongoClient instance
    mongo_client = pymongo.MongoClient('mongodb://127.0.0.1:27017')

    try:
        # Get the list of database names from the MongoClient instance
        db_list = mongo_client.list_database_names()

        # Check if the specified database exists
        if db_name in db_list:
            # Get the specified database
            db = mongo_client.get_database(db_name)

            # Get the list of collection names within the database
            collections_list = db.list_collection_names()

            # Check if the specified collection exists
            if collection_name in collections_list:
                # Get the specified collection
                collection = db.get_collection(collection_name)

                # Find one document in the collection
                document = collection.find_one()

                # Check if a document exists in the collection
                if document is not None:
                    print("Database is validated")
                    return 1
    
    except pymongo.errors.PyMongoError as e:
        # Handle specific exceptions raised by PyMongo
        print(f"An error occurred during database validation: {str(e)}")

    return 0



if __name__ == '__main__':
    # Creating an object for initilizing spark
    processor = DataProcessor()


    # Validating the CRM database and processing CRM data if validation fails
    if databaseValidation('ProjectDB', "crm1") == 0:
        crm_df = processor.process_crm_data(CRM_PATH)

    # Validating the CRM database and processing device data if validation fails
    if databaseValidation('ProjectDB', "device1") == 0:
        device_df = processor.process_device_data(DEVICE_PATH)
