# Telecom-project

## SECTION 1.

### Prerequisites
- Ensure that you have MongoDB installed on your local device. If not, you can follow the official MongoDB documentation [here](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-os-x/) for installation instructions.
- Make sure your local MongoDB server is running before proceeding with the steps below.


### Getting Started
#### Step 1: Configuration of Environment Variables
Make sure to set the following environment variables:

- `AWS_ACCESS_KEY`: Your AWS access key.
- `AWS_SECRET_KEY`: Your AWS secret key.
- `CRM_PATH`: The path to the CRM data.
- `REV_PATH`: The path to the revenue data.
- `DEVICE_PATH`: The path to the device data.
- `BOOTSTRAP_SERVERS`: The Kafka bootstrap servers.
- `TOPIC_NAME`: The Kafka topic name.
- `NUMBER_OF_PARTITIONS`: The number of Kafka partitions.
- `REPLICATION_FACTOR`: The Kafka replication factor.
- `SF_URL`: The Snowflake URL.
- `SF_USER`: Your Snowflake username.
- `SF_PASSWORD`: Your Snowflake password.
- `SF_DATABASE`: The Snowflake database name.
- `SF_SCHEMA`: The Snowflake schema name.
- `SF_WAREHOUSE`: The Snowflake warehouse name.
- `FILE_PASSWORD`: The password for accessing files.
- `MONGO_URI`: The MongoDB connection URI.


#### Step 2: Cleaning Data
1. Open your terminal or command prompt.
2. Navigate to the project directory.
3. Run the following command: `python3 cleaning.py`
   - This script will perform the necessary data cleaning operations.

#### Step 3: Running with Docker Compose
1. Make sure you have Docker and Docker Compose installed on your local device.
2. Open your terminal or command prompt.
3. Navigate to the project directory.
4. Run the following command: `docker-compose up`
   - This command will start the necessary containers and services defined in the Docker Compose file.
   - Ensure that Docker Compose retrieves the required dependencies and builds the environment accordingly.
   - You will see the logs and status of each container as they start up.

   Note: If you encounter any issues during the Docker Compose process, please refer to the official Docker documentation for troubleshooting steps.

### Additional Notes
- Remember to customize the configuration files or environment variables according to your specific setup and requirements.
- Feel free to explore and modify the code to fit your use case or extend the functionality.
- Don't forget to stop the Docker containers when you're done using the project. You can do this by running `docker-compose down`.

That's it! You're now ready to run the project. If you have any further questions or need assistance, please don't hesitate to reach out.
