import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from smart_open import open
from airflow import DAG
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email


# load variables from .env file
load_dotenv()

args = {
    'retries': 5
}

# kafka cluster details 
bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
topic_name = os.getenv('TOPIC_NAME')
num_of_partitions = os.getenv('NUMBER_OF_PARTITIONS')
replication_factor = os.getenv('REPLICATION_FACTOR')

# sends mail on task failure
def failure_function(context):
    dag_run = context.get('dag_run')
    msg = "Task Failed"
    subject = f"DAG {dag_run} Failed"
    send_email(to='jasveens129@gmail.com', subject=subject, html_content=msg)


# define the function to get current week number
def get_current_week_number(ti):
    """
    Fetches the current week number from XCom.
    
    Args:
        ti (airflow.models.TaskInstance): The TaskInstance object.

    Returns:
        int: The current week number. Defaults to 22 if not available or an error occurs.
    """
    try:
        current_week_number = ti.xcom_pull(key='current_week_number', task_ids='produce_to_topic', include_prior_dates=True)
        print(current_week_number)
        if current_week_number is None:
            current_week_number = 22

        return current_week_number

    except Exception as e:
        # Handle the exception here
        print("An error occurred while fetching the current week number:", str(e))
        return 22  


# define the function to create the topic
def topic_function():
    """
    Creates a Kafka topic with the specified parameters.

    Args:
        bootstrap_servers (str): The list of Kafka broker addresses.
        topic_name (str): The name of the topic to create.
        num_of_partitions (int): The number of partitions for the topic.
        replication_factor (int): The replication factor for the topic.

    Returns:
        None
    """

    # Create a Kafka admin client
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    # Create a new topic
    topic = NewTopic(name=topic_name, num_partitions=int(num_of_partitions), replication_factor=int(replication_factor))
    topic_config = {'cleanup.policy': 'delete', 'retention.ms': '3600000'}
    topic.config_entries = topic_config

    try:
        admin_client.create_topics([topic])
        print(f'Topic "{topic_name}" created successfully.')
    except TopicAlreadyExistsError:
        print(f'Topic "{topic_name}" already exists.')

    admin_client.close()    
     

# define the function to produce data to topic
def producer_function(ti):
    """
    Fetches data from an S3 file based on the current week number and sends it to a Kafka topic.

    Args:
        ti (airflow.models.TaskInstance): The TaskInstance object.

    Returns:
        None
    """

    # get the current week number from the previous task using XCom
    current_week_number = ti.xcom_pull(key='return_value', task_ids='current_week_number')
    print(current_week_number)

    try:         
        s3_url = os.getenv('REV_PATH')
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
        )
    except Exception as e:
        # Handle the exception here
        print("An error occurred while creating the S3 session:", str(e)) 


    # create a Kafka Producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    with open(s3_url, 'r', transport_params={'client': session.client('s3')}) as file:
        dataframe = pd.read_csv(file)
        df = dataframe[dataframe['week_number']==current_week_number]
    
        for _, row in df.iterrows():
            data=row.to_json()
            #print(data)
            
            # sending data to producer
            producer.send(topic_name, value=data.encode('utf-8'))

        # close the Kafka producer and admin client
        producer.close()       

    # update the current week number if the data is processed
    updated_week_number = current_week_number + 1
    ti.xcom_push(key='current_week_number', value=updated_week_number)            

                        

with DAG(
    dag_id="kafka_producer",
    start_date=datetime(2022, 11, 1),
    schedule='*/10 * * * *',
    catchup=False,
    default_args=args
):
    
    # define the task to get the current week number
    current_week_number = PythonOperator(
        task_id='current_week_number',
        python_callable=get_current_week_number,
        provide_context=True,
        on_failure_callback=failure_function
    )    


    # define the task to create topic
    create_topic = PythonOperator(
        task_id='create_topic',
        python_callable=topic_function,
        provide_context=True,
        on_failure_callback=failure_function
    )    
                

    # define the producer task
    produce_to_topic = PythonOperator( 
        task_id="produce_to_topic",
        python_callable=producer_function,
        provide_context=True,
        on_failure_callback=failure_function
    )



# dependencies between tasks
current_week_number >> create_topic >> produce_to_topic
