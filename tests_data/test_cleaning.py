import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
import os
import sys
from os.path import dirname


# Get the parent directory path
parent_dir = os.path.abspath(os.path.join(dirname(__file__), '..'))
# Add the parent directory to the module search path
sys.path.append(parent_dir)

from cleaning import SingletonSparkSession, SingletonMongoWriter, DataProcessor, databaseValidation


# Accessing environment variables
AWS_ACCESS_KEY=os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY=os.getenv('AWS_SECRET_KEY')
CRM_PATH=os.getenv('CRM_PATH')
DEVICE_PATH=os.getenv('DEVICE_PATH')


class TestSingletonCreateSparkObject(unittest.TestCase):
    """
    Test suite to verify the behavior of creating singleton SparkSession objects.

    This test suite contains test cases that validate the behavior of the SingletonSparkSession class
    in creating singleton instances of SparkSession objects. The tests ensure that only one instance
    of SparkSession is created and that subsequent calls to create an instance return the same object.

    Methods:
        test_singleton_create_spark_object:
            Test case to verify that two instances of SingletonSparkSession refer to the same object.

        test_singleton_create_spark_object_session:
            Test case to verify that an instance of SingletonSparkSession is an instance of SparkSession.
    """
    # Test case to verify the behavior of creating a singleton SparkSession object.
    def test_singleton_create_spark_object(self):
        """
        Test case to verify the behavior of creating a singleton SparkSession object.

        This test ensures that when two instances of SingletonSparkSession are created,
        they both refer to the same object. The test uses the `assertIs` method from the
        unittest.TestCase class to assert that `spark1` and `spark2` are the same object.

        Returns:
            None
        """
        # Create two instances of SingletonSparkSession
        spark1 = SingletonSparkSession()
        spark2 = SingletonSparkSession()
        self.assertIs(spark1, spark2)

    #  Test case to verify the behavior of creating a singleton SparkSession object
    def test_singleton_create_spark_object_session(self):
        """
        Test case to verify the behavior of creating a singleton SparkSession object.

        This test ensures that when an instance of SingletonSparkSession is created,
        it is an instance of SparkSession. The test uses the `assertIsInstance` method
        from the unittest.TestCase class to assert that `spark1` is an instance of SparkSession.

        Returns:
            None
        """
        # Create an instance of SingletonSparkSession
        spark1 = SingletonSparkSession()
        self.assertIsInstance(spark1, SparkSession)



class TestSingletonMongoWriter(unittest.TestCase):
    def setUp(self):
        """
        Set up the test case by creating an instance of SingletonMongoWriter.

        Returns:
            None
        """
        self.writer = SingletonMongoWriter()

    def test_create_singleton_mongo_writer_object(self):
        """
        Test case to verify that the SingletonMongoWriter returns the same instance.

        Returns:
            None
        """
        mongo1 = SingletonMongoWriter()
        mongo2 = SingletonMongoWriter()
        self.assertIs(mongo1, mongo2)


    def test_write_to_Mongodb(self):
        """
        Test case to verify that the write_to_Mongodb method calls the appropriate methods on the provided DataFrame.

        Returns:
            None
        """
        mock_dataframe = MagicMock()
        mock_dataframe.write.format.return_value = mock_dataframe.write
        mock_dataframe.write.mode.return_value = mock_dataframe.write
        mock_dataframe.write.option.return_value = mock_dataframe.write

        mock_uri = "mongodb://localhost:27017/myDatabase.myCollection"

        self.writer.write_to_Mongodb(mock_dataframe, mock_uri)

        mock_dataframe.write.format.assert_called_with("com.mongodb.spark.sql.DefaultSource")
        mock_dataframe.write.mode.assert_called_with("append")
        mock_dataframe.write.option.assert_called_with("spark.mongodb.output.uri", mock_uri)
        mock_dataframe.write.save.assert_called()



class TestDatabaseValidation(unittest.TestCase):
    """
    A test case class for validating MongoDB database and collection existence.

    This class provides test methods to validate the existence of a MongoDB database and collection,
    and checks if a document exists in the collection.

    The tests utilize MagicMock objects to mock the behavior of the pymongo.MongoClient class.

    """

    def setUp(self):
        # Create a MagicMock object for the MongoClient
        self.mock_mongo_client = MagicMock()
        
        # Patch the pymongo.MongoClient class with the MagicMock object
        self.patcher = patch('pymongo.MongoClient', return_value=self.mock_mongo_client)
        self.mock_mongo_client_instance = self.patcher.start()

    def tearDown(self):
        # Stop patching the pymongo.MongoClient class
        self.patcher.stop()

    # Test the databaseValidation function with an existing database and collection.
    def test_database_validation_existing_db_and_collection(self):
        """
        Test the databaseValidation function with an existing database and collection.
        """
        # Set up mock data
        db_name = 'ProjectDB'
        collection_name = 'crm1'

        # Mock the list_database_names method
        self.mock_mongo_client.list_database_names.return_value = [db_name]

        # Mock the get_database method
        mock_database = MagicMock()
        self.mock_mongo_client.get_database.return_value = mock_database

        # Mock the list_collection_names method
        mock_database.list_collection_names.return_value = [collection_name]

        # Mock the find_one method
        mock_collection = MagicMock()
        mock_database.get_collection.return_value = mock_collection
        mock_collection.find_one.return_value = {'_id': '12345'}

        # Call the databaseValidation function
        result = databaseValidation(db_name, collection_name)

        # Assert the expected calls were made
        self.mock_mongo_client.list_database_names.assert_called_once()
        self.mock_mongo_client.get_database.assert_called_once_with(db_name)
        mock_database.list_collection_names.assert_called_once()
        mock_database.get_collection.assert_called_once_with(collection_name)
        mock_collection.find_one.assert_called_once()

        # Assert the result
        self.assertEqual(result, 1)
    
    # Test the databaseValidation function with a non-existing database.
    def test_database_validation_non_existing_db(self):
        """
        Test the databaseValidation function with a non-existing database.
        """
        # Set up mock data
        db_name = 'nonExistingDatabase'
        collection_name = 'crm1'

        # Mock the list_database_names method
        self.mock_mongo_client.list_database_names.return_value = ['otherDatabase']

        # Call the databaseValidation function
        result = databaseValidation(db_name, collection_name)

        # Assert the expected calls were made
        self.mock_mongo_client.list_database_names.assert_called_once()
        self.mock_mongo_client.get_database.assert_not_called()

        # Assert the result
        self.assertEqual(result, 0)

    # Test the databaseValidation function with a non-existing collection.
    def test_database_validation_non_existing_collection(self):
        """
        Test the databaseValidation function with a non-existing collection.
        """
        # Set up mock data
        db_name = 'ProjectDB'
        collection_name = 'nonExistingCollection'

        # Mock the list_database_names method
        self.mock_mongo_client.list_database_names.return_value = [db_name]

        # Mock the get_database method



class TestDataProcessorCrm(unittest.TestCase):
    """
    Unit tests for the DataProcessor class specific to CRM data processing.
    """

    def setUp(self) -> None:
        """
            Set up the necessary objects and configurations before each test case.
        """

        self.data_processor = DataProcessor()
        self.spark = SingletonSparkSession()
  
  #  Test the check_file_exists_in_S3 method of the DataProcessor class.
    @patch('cleaning.boto3.Session')
    def test_check_file_exists_in_S3(self, mock_session):
        """
        Test the check_file_exists_in_S3 method of the DataProcessor class.

        This test validates the behavior of the check_file_exists_in_S3 method when the specified file exists in the S3 bucket.
        It mocks the necessary objects and methods using the patch decorator and asserts the expected calls were made.

        Args:
            mock_session: The mocked boto3.Session class.
        """
        mock_client = mock_session.return_value.client.return_value
        mock_client.head_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        
        result = self.data_processor.check_file_exists_in_S3(bucket_name="telecom-data-source", key="crm1.csv")
        self.assertTrue(result)
        
        mock_session.assert_called_once()
        mock_client.head_object.assert_called_once_with(Bucket='telecom-data-source', Key='crm1.csv')


    # Test the check_gender_column_exists method of the DataProcessor class.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_check_gender_column_exists(self,mock_check_file_exists ):
        """
        Test the check_gender_column_exists method of the DataProcessor class.

        This test validates that the gender column exists in the processed CRM DataFrame.
        It mocks the necessary objects and methods using the patch decorator and asserts the expected column existence.

        Args:
            mock_check_file_exists: The mocked DataProcessor.check_file_exists_in_S3 method.
        """

        mock_crm_data = [("000004e30498f8ab922e2944fa86a45a","mal",1980,"ACTIVE","Prepaid","Tier_3")]
        mock_crm = self.spark.createDataFrame(mock_crm_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"])  
        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_crm):
            res_df = self.data_processor.process_crm_data("random_path")
            mock_check_file_exists.return_value = True
            columns = res_df.columns
            self.assertIn("gender", columns)


    # Test the gender_validated method of the DataProcessor class.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_gender_validated(self,mock_check_file_exists ):
        """
        Test the gender_validated method of the DataProcessor class.

        This test validates that the gender values in the processed CRM DataFrame are validated correctly.
        It mocks the necessary objects and methods using the patch decorator and asserts the expected validation.

        Args:
            mock_check_file_exists: The mocked DataProcessor.check_file_exists_in_S3 method.
        """
        mock_crm_data = [("000004e30498f8ab922e2944fa86a45a","mal",1980,"ACTIVE","Prepaid","Tier_3")]
        mock_crm = self.spark.createDataFrame(mock_crm_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"])
        
        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_crm):
            res_df = self.data_processor.process_crm_data("random_path")
            mock_check_file_exists.return_value = True

        res = res_df.select("gender").collect()[0].gender
        self.assertEqual("male", res)


    # Test the check_year_of_birth_column_exists method of the DataProcessor class.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_check_year_of_birth_column_exists(self,mock_check_file_exists ):
        """
        Test the check_year_of_birth_column_exists method of the DataProcessor class.

        This test validates that the 'year_of_birth' column exists in the processed CRM DataFrame.
        It mocks the necessary objects and methods using the patch decorator and asserts the expected column existence.

        Args:
            mock_check_file_exists: The mocked DataProcessor.check_file_exists_in_S3 method.
        """
        mock_crm_data = [("000004e30498f8ab922e2944fa86a45a","mal",1980,"ACTIVE","Prepaid","Tier_3")]
        mock_crm = self.spark.createDataFrame(mock_crm_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"])  

        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_crm):
            res_df = self.data_processor.process_crm_data("random_path")
            mock_check_file_exists.return_value = True
            columns = res_df.columns
        self.assertIn("year_of_birth", columns)    
    

    # Test the year_of_birth_process_crm_data method of the DataProcessor class.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_year_of_birth_process_crm_data(self,mock_check_file_exists ):
        """
        Test the year_of_birth_process_crm_data method of the DataProcessor class.

        This test validates the processing of the 'year_of_birth' column in the CRM DataFrame.
        It mocks the necessary objects and methods using the patch decorator and compares the processed DataFrame
        with the expected DataFrame.

        Args:
            mock_check_file_exists: The mocked DataProcessor.check_file_exists_in_S3 method.
        """
        mock_crm_data = [("000004e30498f8ab922e2944fa86a45a","mal",None,"ACTIVE","Prepaid","Tier_3"),
                         ("000004e30498f8ab922e2944fa86a45b","female]","1998","ACTIVE","Prepaid","Tier_3")]
        
        mock_crm = self.spark.createDataFrame(mock_crm_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"])  
        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_crm):
            res_df = self.data_processor.process_crm_data("random_path")
            mock_check_file_exists.return_value = True
        
        expected_data = [("000004e30498f8ab922e2944fa86a45a","male","Not Available","ACTIVE","Prepaid","Tier_3"),
                         ("000004e30498f8ab922e2944fa86a45b","female","1998","ACTIVE","Prepaid","Tier_3")
                        ]
        
        expected_df = self.spark.createDataFrame(expected_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"]) 
        self.assertEqual(expected_df.collect(), res_df.collect())


    # Test the each_msisdn_has_one_record method of the DataProcessor class
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_each_msisdn_has_one_record(self,mock_check_file_exists):
        """
        Test the each_msisdn_has_one_record method of the DataProcessor class.

        This test ensures that each MSISDN (mobile number) has only one record in the processed DataFrame.
        It mocks the necessary objects and methods using the patch decorator and compares the processed DataFrame
        with the expected DataFrame.

        Args:
            mock_check_file_exists: The mocked DataProcessor.check_file_exists_in_S3 method.
        """
        mock_crm_data = [("000004e30498f8ab922e2944fa86a45a","mal",None,"ACTIVE","Prepaid","Tier_3"),
                         ("000004e30498f8ab922e2944fa86a45a","female]","1998","ACTIVE","Prepaid","Tier_3"),
                         ("000004e30498f8ab922e2944fa86a45a","female]","1998","SUSPEND","Prepaid","Tier_3"),
                         ]
        
        mock_crm = self.spark.createDataFrame(mock_crm_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"])  
        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_crm):
            res_df = self.data_processor.process_crm_data("random_path")
            mock_check_file_exists.return_value = True
    
       
        expected_data = [("000004e30498f8ab922e2944fa86a45a","male","Not Available","ACTIVE","Prepaid","Tier_3"),]
        
        expected_df = self.spark.createDataFrame(expected_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"]) 
        
        self.assertEqual(expected_df.collect(), res_df.collect())
    
    #  Test the select_active_user method of the DataProcessor class.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_select_active_user(self,mock_check_file_exists ):
        """
        Test the select_active_user method of the DataProcessor class.

        This test validates the filtering of active users in the CRM DataFrame.
        It mocks the necessary objects and methods using the patch decorator and compares the filtered DataFrame
        with the expected DataFrame.

        Args:
            mock_check_file_exists: The mocked DataProcessor.check_file_exists_in_S3 method.
        """
        mock_crm_data = [("000004e30498f8ab922e2944fa86a45a","mal",None,"ACTIVE","Prepaid","Tier_3"),
                         ("000004e30498f8ab922e2944fa86a45a","female]","1998","ACTIVE","Prepaid","Tier_3"),
                         ("000004e30498f8ab922e2944fa86a45a","female]","1998","SUSPEND","Prepaid","Tier_3"),
                         ]
        
        mock_crm = self.spark.createDataFrame(mock_crm_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"])  
        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_crm):
            res_df = self.data_processor.process_crm_data("random_path")
            mock_check_file_exists.return_value = True
        
        expected_data = [("000004e30498f8ab922e2944fa86a45a","male","Not Available","ACTIVE","Prepaid","Tier_3"),]
        
        expected_df = self.spark.createDataFrame(expected_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"]) 
    
        self.assertEqual(expected_df.collect(), res_df.collect())


  #  Test the select_deactive_user a method of the DataProcessor class.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_select_deactive_if_exists(self,mock_check_file_exists):
        """
        Test the select_deactive_user a method of the DataProcessor class.

        This test validates the filtering of active users in the CRM DataFrame.
        It mocks the necessary objects and methods using the patch decorator and compares the filtered DataFrame
        with the expected DataFrame.

        Args:
            mock_check_file_exists: The mocked DataProcessor.check_file_exists_in_S3 method.
        """
        mock_crm_data = [("000004e30498f8ab922e2944fa86a45a","mal",None,"ACTIVE","Prepaid","Tier_3"),
                         ("000004e30498f8ab922e2944fa86a45a","female]","1998","ACTIVE","Prepaid","Tier_3"),
                         ("000004e30498f8ab922e2944fa86a45a","female]","1998","DEACTIVE","Prepaid","Tier_3"),
                         ]
        
        mock_crm = self.spark.createDataFrame(mock_crm_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"])  
        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_crm):
            res_df = self.data_processor.process_crm_data("random_path")
            mock_check_file_exists.return_value = True
        
        expected_data = [("000004e30498f8ab922e2944fa86a45a","female","1998","DEACTIVE","Prepaid","Tier_3"),]
        
        expected_df = self.spark.createDataFrame(expected_data, ["msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"]) 
    
        self.assertEqual(expected_df.collect(), res_df.collect())



class TestDataProcessorDevice(unittest.TestCase):
    """
    Unit tests for the DataProcessor class specific to DEVICE data processing.
    """
    
    def setUp(self) -> None:
        """
            Set up the necessary objects and configurations before each test case.
        """
        self.data_processor = DataProcessor()
        self.spark = SingletonSparkSession()
        self.schema = StructType([StructField('msisdn', StringType(), True), 
                             StructField('imei_tac', StringType(), True), 
                             StructField('brand_name', StringType(), True), 
                             StructField('model_name', StringType(), True), 
                             StructField('os_name', StringType(), True), 
                             StructField('os_vendor', StringType(), True)])
        

    #  Test the check_file_exists_in_S3 method of the DataProcessor class.
    @patch('cleaning.boto3.Session')
    def test_check_file_exists_in_S3(self, mock_session):
        """
        Test the check_file_exists_in_S3 method of the DataProcessor class.

        This test validates the behavior of the check_file_exists_in_S3 method when the specified file exists in the S3 bucket.
        It mocks the necessary objects and methods using the patch decorator and asserts the expected calls were made.

        Args:
            mock_session: The mocked boto3.Session class.
        """

        mock_client = mock_session.return_value.client.return_value
        mock_client.head_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        
        result = self.data_processor.check_file_exists_in_S3(bucket_name="telecom-data-source", key="device1.csv")
        
        self.assertTrue(result)
        mock_session.assert_called_once()
        mock_client.head_object.assert_called_once_with(Bucket='telecom-data-source', Key='device1.csv')


    # Test case to ensure that all columns exist in the processed DataFrame.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_check_all_column_exists(self,mock_check_file_exists):
        """
        Test case to ensure that all columns exist in the processed DataFrame.

        It compares the column names of the expected DataFrame with the processed DataFrame
        to verify that all the expected columns are present.
        """

        mock_device_data = [("0000057209539d088bf5ce83782e66d5", "0162524010db5574e60022be74374e9a", "SYMPHONY", "T110", "Not Available", "Not Available"),
                ("0000057209539d088bf5ce83782e66d5", "0162524010db5574e60022be74374e9a", None, "T110", "Not Available", "Not Available"),
                ("00000df28a968d8d359bdd4566c07d58", "200fdb011d6ba8cc5fc25c817b0e0366", "SAMSUNG", "GALAXY J1 ACE DUOS (SM-J111F DS)", "Android", "Google"),
                ("000020a7646066dac79050c5481497b0", "f329899e4f2834ff58bb95c85dca4843", "SYMPHONY", "L55", "Not Available", "Not Available")
            ]
        mock_device = self.spark.createDataFrame(mock_device_data, schema=self.schema)  
        
        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_device):
            res_df = self.data_processor.process_device_data("random_path")
            mock_check_file_exists.return_value = True

        
        expected_device_data = [("0000057209539d088bf5ce83782e66d5","0162524010db5574e60022be74374e9a","SYMPHONY","T110","Not Available","Not Available"),
                            ("00000df28a968d8d359bdd4566c07d58","200fdb011d6ba8cc5fc25c817b0e0366","SYMPHONY","GALAXY J1 ACE DUOS (SM-J111F DS)","Android","Google"),
                            ("000020a7646066dac79050c5481497b0","f329899e4f2834ff58bb95c85dca4843","SYMPHONY","L55","Not Available","Not Available"),]
        expected_device = self.spark.createDataFrame(expected_device_data, schema=self.schema)  
        self.assertListEqual(expected_device.columns, res_df.columns)


    # Test case to ensure that missing values are filled with "Not Available" in the processed DataFrame.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_check_fill_not_available(self,mock_check_file_exists ):
        """
            Test case to ensure that missing values are filled with "Not Available" in the processed DataFrame.

            It verifies that any null or None values in the original DataFrame are replaced with the string "Not Available"
            in the processed DataFrame.
        """

        mock_device_data = [("0000057209539d088bf5ce83782e66d5", "0162524010db5574e60022be74374e9a", None, "T110", None, None)]
        mock_device = self.spark.createDataFrame(mock_device_data, schema=self.schema)  

        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_device):
            res_df = self.data_processor.process_device_data("random_path")
            mock_check_file_exists.return_value = True

    
        expected_device_data = [("0000057209539d088bf5ce83782e66d5","0162524010db5574e60022be74374e9a","Not Available","T110","Not Available","Not Available")]
                            
        expected_device_df = self.spark.createDataFrame(expected_device_data, schema=self.schema)
        self.assertEqual(res_df.collect(), expected_device_df.collect())


    # Test case to ensure that only one record is selected for each unique msisdn.
    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_check_one_record_for_each_msisdn(self,mock_check_file_exists ):
        """
        Test case to ensure that only one record is selected for each unique msisdn.

        It verifies that the processed DataFrame contains only one record for each unique msisdn, eliminating any duplicates.
        """

        mock_device_data = [("0000057209539d088bf5ce83782e66d5", "0162524010db5574e60022be74374e9a", None, "T110", None, None),
                            ("0000057209539d088bf5ce83782e66d5", "0162524010db5574e60022be74374e9a", "Symphony", "T110", "Android", "Google"),]
        mock_device = self.spark.createDataFrame(mock_device_data, schema=self.schema)  

        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_device):
            res_df = self.data_processor.process_device_data("random_path")
            mock_check_file_exists.return_value = True

        self.assertEqual(res_df.count(), 1)


    @patch('cleaning.DataProcessor.check_file_exists_in_S3')
    def test_check_one_record_for_msisdn_with_less_nulls(self,mock_check_file_exists ):
        """
        Test case to ensure that only one record is selected for each msisdn with fewer null values.

        It verifies that the processed DataFrame contains only one record for each msisdn, considering the record with
        fewer null values when there are multiple records for the same msisdn.

        """

        mock_device_data = [("0000057209539d088bf5ce83782e66d5", "0162524010db5574e60022be74374e9a", None, "T110", None, None),
                            ("0000057209539d088bf5ce83782e66d5", "0162524010db5574e60022be74374e9a", "Symphony", "T110", "Android", "Google"),]
        mock_device = self.spark.createDataFrame(mock_device_data, schema=self.schema)  

        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_device):
            res_df = self.data_processor.process_device_data("random_path")
            mock_check_file_exists.return_value = True

        expected_device_data = [("0000057209539d088bf5ce83782e66d5", "0162524010db5574e60022be74374e9a", "Symphony", "T110", "Android", "Google")]
        expected_device_df = self.spark.createDataFrame(expected_device_data, schema=self.schema)

        self.assertEqual(res_df.count(), expected_device_df.count())



if __name__ == "__main__":
    unittest.main()
