import unittest
from unittest.mock import MagicMock, patch
import os
import sys
from os.path import dirname
parent_dir = os.path.abspath(os.path.join(dirname(__file__), '..'))
# Add the parent directory to the module search path
sys.path.append(parent_dir)
print(parent_dir)

from dags.kafka_producer import get_current_week_number, producer_function
from dags.analysis import connect_to_snowflake


class TestKafkaProducer(unittest.TestCase):

    # Testing for getting the current week number
    def test_get_current_week_number(self):
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = None

        # Call the function under test
        get_current_week_number(mock_ti)

        # Assert the expected XCom method calls
        mock_ti.xcom_pull.assert_called_once_with(
            key='current_week_number',
            task_ids='produce_to_topic',
            include_prior_dates=True
        )


    # Testing for broker available
    @patch('dags.kafka_producer.KafkaProducer')
    def test_broker_available(self, mock_producer):
        # Set up the mock behavior for the dependencies
        mock_ti = MagicMock()
        bootstrap_servers = 'broker:9092'

        # Call the function under test
        producer_function(mock_ti)

        # Assert the expected behavior of the dependencies
        mock_producer.assert_called_once_with(bootstrap_servers=bootstrap_servers)


    # Testing for updated week number
    @patch('dags.kafka_producer.KafkaProducer')
    def test_updated_week_number(self, mock_producer):
        # Set up the mock behavior for the dependencies
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = 22
        bootstrap_servers = 'broker:9092'

        # Call the function under test
        producer_function(mock_ti)

        mock_producer.assert_called_once_with(bootstrap_servers=bootstrap_servers)
        mock_ti.xcom_push.assert_called_once_with(key='current_week_number', value=23)


    # Testing for the correct bucket
    @patch('dags.kafka_producer.boto3')
    @patch('dags.kafka_producer.open')
    @patch('dags.kafka_producer.KafkaProducer')
    def test_s3_bucket(self, mock_producer, mock_open, mock_boto3):
        # Set up the mock behavior for the dependencies
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = 10

        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_file = MagicMock()

        mock_open.return_value = mock_file
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_client

        # Set up the test data
        mock_dataframe = MagicMock()
        mock_df = MagicMock()
        mock_dataframe.__getitem__.return_value = mock_df

        # Call the function under test
        with patch('dags.kafka_producer.pd.read_csv', return_value=mock_dataframe):
            producer_function(mock_ti)

        # Assert the expected behavior of the dependencies
        mock_open.assert_called_once_with('s3://telecom-data-source/rev1.csv', 'r', transport_params={'client': mock_client})


    @patch('snowflake.connector.connect')
    def test_connect_to_snowflake_success(self, mock_connect):
        # Mock the Snowflake connector's connect() method
        mock_conn = mock_connect.return_value
        mock_curs = mock_conn.cursor.return_value

        # Mock any necessary environment variables
        with patch.dict('os.environ', {"USER": "test_user", "PASSWORD": "test_password",
                                       "ACCOUNT": "test_account", "WAREHOUSE": "test_warehouse",
                                       "DATABASE": "test_database", "SCHEMA": "test_schema"}):
            # Call the function to be tested
            connect_to_snowflake()

        # Assert that the Snowflake connector's connect() method was called with the expected arguments
        mock_connect.assert_called_once_with(
            user="test_user",
            password="test_password",
            account="test_account",
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema"
        )

        # Assert that the cursor's execute() method was called
        mock_curs.execute.assert_called_once_with("")

        # Assert that the cursor's close() method was called
        mock_curs.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()

