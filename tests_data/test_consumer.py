import pytest
from unittest.mock import patch, mock_open
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from datetime import datetime
import os
import sys
from os.path import dirname

parent_dir = os.path.abspath(os.path.join(dirname(__file__), '..'))
# Add the parent directory to the module search path
sys.path.append(parent_dir)

from Consumer.consumer import DataConsumer

class TestDataConsumer:
    
    @pytest.fixture
    def object(self):
        obj = DataConsumer()
        return obj
    
    @pytest.fixture
    def spark(self):
        obj = DataConsumer()
        return obj.create_spark_session()
        
    def test_read_from_mongo(self, object, spark):
        # Mocked CRM data
        mock_crm_data = [("99939hshjshudbbjha73h388939i","000004e30498f8ab922e2944fa86a45a","male",1980,"ACTIVE","Prepaid","Tier_3")]
        mock_crm = spark.createDataFrame(mock_crm_data, ["_id", "msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"])  
        
        # Define the expected schema
        expected_schema = StructType([
            StructField('msisdn', StringType(), nullable=True),
            StructField('gender', StringType(), nullable=True),
            StructField('year_of_birth', LongType(), nullable=True),
            StructField('system_status', StringType(), nullable=True),
            StructField('mobile_type', StringType(), nullable=True),
            StructField('value_segment', StringType(), nullable=True)
        ])
        
        with patch("pyspark.sql.DataFrameReader.load", return_value=mock_crm):
            crm = object.read_from_mongo("crm1")
            
            # Assert that the returned value is a DataFrame
            assert isinstance(crm, DataFrame)
            
            # Assert that the "_id" column is not present in the DataFrame
            assert "_id" not in crm.columns
            
            # Assert that the schema of the DataFrame matches the expected schema
            assert crm.schema == expected_schema
    
    def test_read_from_kafka(self, object, spark):
        # Mocked Kafka data
        mock_data = [
            (b'key1', b'value1', 'topic1', 0, 12345, datetime.strptime('2023-06-19 10:30:00', '%Y-%m-%d %H:%M:%S'), 0),
            (b'key2', b'value2', 'topic1', 1, 54321, datetime.strptime('2023-06-19 11:30:00', '%Y-%m-%d %H:%M:%S'), 1)
        ]
        
        # Define the expected schema
        expected_schema = StructType([
            StructField('key', BinaryType(), True), 
            StructField('value', BinaryType(), True),
            StructField('topic', StringType(), True), 
            StructField('partition', IntegerType(), True), 
            StructField('offset', LongType(), True), 
            StructField('timestamp', TimestampType(), True), 
            StructField('timestampType', IntegerType(), True)
        ])
        
        mock_df = spark.createDataFrame(mock_data, schema=expected_schema)
        
        with patch("pyspark.sql.streaming.DataStreamReader", return_value=mock_df):
            data = object.read_stream_from_kafka()
            
            # Assert that the schema of the returned DataFrame matches the expected schema
            assert data.schema == expected_schema
    
    def test_get_value_from_kafka_dataStream(self, object, spark):
        # Mocked Kafka data
        mock_data = [
            (b'key1', b'{msisdn:000004e30498f8ab922e2944fa86a45a,week_number:22,revenue_usd:17.3994904}', 'topic', 0, 12345, datetime.strptime('2023-06-19 10:30:00', '%Y-%m-%d %H:%M:%S'), 0)
        ]
        
        schema = StructType([
            StructField('key', BinaryType(), True), 
            StructField('value', BinaryType(), True),
            StructField('topic', StringType(), True), 
            StructField('partition', IntegerType(), True), 
            StructField('offset', LongType(), True), 
            StructField('timestamp', TimestampType(), True), 
            StructField('timestampType', IntegerType(), True)
        ])
        
        # Define the expected schema for extracting values from the "value" column
        expected_schema = StructType() \
            .add("msisdn", StringType()) \
            .add("week_number", IntegerType()) \
            .add("revenue_usd", FloatType())
            
        mock_df = spark.createDataFrame(mock_data, schema=schema)
        rev = object.get_value_from_kafka_dataStream(mock_df)
        
        # Assert that the schema of the extracted values DataFrame matches the expected schema
        assert rev.schema == expected_schema
