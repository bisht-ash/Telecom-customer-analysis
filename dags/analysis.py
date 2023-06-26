import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
from airflow.utils.email import send_email
from dotenv import load_dotenv


# load variables from .env file
load_dotenv()

args = {
    'retries': 5
}

# sends mail on task failure
def failure_function(context):
    dag_run = context.get('dag_run')
    msg = "Task Failed"
    subject = f"DAG {dag_run} Failed"
    send_email(to='jasveens129@gmail.com', subject=subject, html_content=msg)


def connect_to_snowflake():
    """
    Function to establish a connection to Snowflake.

    Arguments:
        None

    Returns:
        None
    """

    try:
        conn=snowflake.connector.connect(
            user=os.getenv("SF_USER"),
            password=os.getenv('SF_PASSWORD'),
            account=os.getenv('SF_URL'),
            warehouse=os.getenv('SF_WAREHOUSE'),
            database=os.getenv('SF_DATABASE'),
            schema=os.getenv('SF_SCHEMA')
        )

        curs=conn.cursor()
        curs.execute("")
        curs.close()

    except Exception as e:
        # Handle the exception here
        print("An error occurred while connecting to snowflake:", str(e))     


def analysis():
    """
        Function to do analysis of snowflake queries.

        Arguments:
            None

        Returns:
            None
    """

    try:

        conn=snowflake.connector.connect(
            user=os.getenv("SF_USER"),
            password=os.getenv('SF_PASSWORD'),
            account=os.getenv('SF_URL'),
            warehouse=os.getenv('SF_WAREHOUSE'),
            database=os.getenv('SF_DATABASE'),
            schema=os.getenv('SF_SCHEMA')
        )

        #create cursor
        curs=conn.cursor()

        # query for total number of devices
        curs.execute("""create 
                        or replace table TOTAL_DEVICES as 
                    select 
                        count(distinct "imei_tac") as total_devices 
                    from 
                        FINAL_TELECOM_TABLE;""")
        
        # query for total devices weekwise
        curs.execute("""create
                        or replace table TOTAL_DEVICES_WEEKWISE as
                    select
                        "week_number",
                        count(*) as total_devices
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number"
                    order by
                        "week_number";""")

        # query for total active devices
        curs.execute("""create
                        or replace table TOTAL_ACTIVE_DEVICES as
                    select
                        count(distinct "imei_tac") as total_active_devices
                    from
                        FINAL_TELECOM_TABLE
                    where
                        "system_status" = 'ACTIVE';""")
    
        # query for total active devices weekwise
        curs.execute("""create
                        or replace table TOTAL_ACTIVE_DEVICES_WEEKWISE as
                    select
                        "week_number",
                        count(*) as total_active_devices
                    from
                        FINAL_TELECOM_TABLE
                    where
                        "system_status" = 'ACTIVE'
                    group by
                        "week_number"
                    order by
                        "week_number";""")
    
        # query from total customers weekwise
        curs.execute("""create
                        or replace table TOTAL_CUSTOMERS_WEEKWISE as
                    select
                        "week_number",
                        count(*) as total_customers
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number"
                    order by
                        "week_number";""")
    
        # query for total customers active
        curs.execute("""create
                    or replace table TOTAL_CUSTOMERS_ACTIVE as
                select
                    count(*) as total_active_customers
                from
                    FINAL_TELECOM_TABLE
                where
                    "system_status" = 'ACTIVE';""")
    
        # query for total customers active weekwise
        curs.execute("""create
                        or replace table TOTAL_ACTIVE_CUSTOMERS_WEEKWISE as
                    select
                        "week_number",
                        count(*) as total_active_customers
                    from
                        FINAL_TELECOM_TABLE
                    where
                        "system_status" = 'ACTIVE'
                    group by
                        "week_number"
                    order by
                        "week_number";""")
    
        # query for query for total revenue male vs female
        curs.execute("""create
                        or replace table TOTAL_REVENUE_MALE_FEMALE as with transform1 as (
                            select
                                case
                                    when "gender" = 'male' then round("revenue_usd",2)
                                    else 0
                                end as male,
                                case
                                    when "gender" = 'female' then round("revenue_usd",2)
                                    else 0
                                end as female
                            from
                                FINAL_TELECOM_TABLE
                            )
                    select
                        sum(male) as male_revenue,
                        sum(female) as female_revenue
                    from
                        transform1;""")
    
        # query for query for total revenue male vs female weekwise
        curs.execute("""create
                        or replace table TOTAL_REVENUE_MALE_FEMALE_WEEKWISE as with transform1 as (
                            select
                                "week_number",
                                case
                                    when "gender" = 'male' then round("revenue_usd",2)
                                    else 0
                                end as male,
                                case
                                    when "gender" = 'female' then round("revenue_usd",2)
                                    else 0
                                end as female
                                from
                                    FINAL_TELECOM_TABLE
                            )
                    select
                        "week_number",
                        sum(male) as male_revenue,
                        sum(female) as female_revenue
                    from
                        transform1
                    group by
                        "week_number";""")
    
        # query for total revenue age
        curs.execute("""create
                        or replace table TOTAL_REVENUE_AGE as with transform1 as (
                            select
                                (YEAR(CURRENT_DATE)-"year_of_birth")::INT as age,
                                "revenue_usd"
                            from
                                FINAL_TELECOM_TABLE
                            ),
                            transform2 as (
                                select
                                    ( age - age % 10 )::INT as start_dat,
                                    sum(round("revenue_usd",2)) as revenue
                                from
                                    transform1
                                group by
                                    age
                            )
                    select
                        concat(start_dat, '-', start_dat + 9) as age_group,
                        revenue
                    from
                        transform2;""")
    
        # query for total revenue age weekwise
        curs.execute("""create
                        or replace table TOTAL_REVENUE_AGE_WEEKWISE as with transform1 as (
                            select
                                (YEAR(CURRENT_DATE)-"year_of_birth")::INT as age,
                                "revenue_usd",
                                "week_number"
                            from
                                FINAL_TELECOM_TABLE
                            ),
                            transform2 as (
                                select
                                    "week_number",
                                    (age - age % 10 )::INT as start_dat,
                                    sum(round("revenue_usd",2)) as revenue
                                from
                                    transform1
                                group by
                                    "week_number",
                                    age
                            )
                    select
                        "week_number",
                        concat(start_dat, '-', start_dat + 9) as age_group,
                        revenue
                    from
                        transform2;""")
    
        # query for total revenue by age value segment
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_AGE_VALUE_SEGMENT as with transform1 as (
                            select
                                *,
                                (YEAR(CURRENT_DATE)-"year_of_birth")::INT, "revenue_usd" as age
                            from
                                FINAL_TELECOM_TABLE
                            )
                    select
                        "value_segment",
                        concat((age - age % 10), '-',(age - age % 10) + 9) as age_group,
                        sum(round("revenue_usd",2)) as total
                    from
                        transform1
                    group by
                        "value_segment",
                        age;""")

        # query for total revenue by age value segment weekwise
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_AGE_VALUE_SEGMENT_WEEKWISE as with transform1 as (
                            select
                                *,
                                (YEAR(CURRENT_DATE)-"year_of_birth")::INT as age
                            from
                                FINAL_TELECOM_TABLE
                            )
                    select
                        "value_segment",
                        "week_number",
                        concat((age - age % 10), '-',(age - age % 10) + 9) as age_group,
                        sum(round("revenue_usd",2)) as total
                    from
                        transform1
                    group by
                        "value_segment",
                        "week_number",
                        age;""")

        # query for total revenue by mobile type
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_MOBILE_TYPE as
                    select
                        "mobile_type",
                        sum(round("revenue_usd", 2)) as revenue
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "mobile_type";""")
                 
        # query for total revenue by mobile type weekwise
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_MOBILE_TYPE_WEEKWISE as
                    select
                        "week_number",
                        "mobile_type",
                        sum(round("revenue_usd", 2)) as revenue
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number",
                        "mobile_type";""")

        # query for total revenue by brand name
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_BRAND_NAME as
                    select
                        "brand_name",
                        sum(round("revenue_usd",2)) as revenue
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "brand_name";""")   

        # query for total revenue by brand name weekwise
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_BRAND_NAME_WEEKWISE as
                    select
                        "week_number",
                        "brand_name",
                        sum(round("revenue_usd",2)) as revenue
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number",
                        "brand_name";""")

        # query for total revenue by os name
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_OS_NAME as
                    select
                        "os_name",
                        sum(round("revenue_usd", 2)) as revenue
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "os_name";""")

        # query for total revenue by os name weekwise
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_OS_NAME_WEEKWISE as
                    select
                        "week_number",
                        "os_name",
                        sum(round("revenue_usd", 2)) as revenue
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number",
                        "os_name";""")

        # query for total revenue by os vender
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_OS_VENDER as
                    select
                        "os_vendor",
                        sum(round("revenue_usd",2)) as revenue
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "os_vendor";""")

        # query for total revenue by os vender weekwise
        curs.execute("""create
                        or replace table TOTAL_REVENUE_BY_OS_VENDER_WEEKWISE as
                    select
                        "week_number",
                        "os_vendor",
                        sum(round("revenue_usd", 2)) as revenue
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number",
                        "os_vendor";""") 

        # query for total customers by os name
        curs.execute("""create
                        or replace table TOTAL_CUSTOMERS_BY_OS_NAME as
                    select
                        "os_name",
                        count(*) as total_customers
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "os_name";""")

        # query for total customers by os name weekwise
        curs.execute("""create
                        or replace table TOTAL_CUSTOMERS_BY_OS_NAME_WEEKWISE as
                    select
                        "week_number",
                        "os_name",
                        count(*) as total_customers
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number",
                        "os_name"; """)

        # query for total customers by brand name
        curs.execute("""create
                        or replace table TOTAL_CUSTOMERS_BY_BRAND_NAME as
                    select
                        "brand_name",
                        count(*) as total_customers
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "brand_name";""")

        # query for total customers by brand name weekwise
        curs.execute("""create
                        or replace table TOTAL_CUSTOMERS_BY_BRAND_NAME_WEEKWISE as
                    select
                        "week_number",
                        "brand_name",
                        count(*) as total_customers
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number",
                        "brand_name";""")

        # query for total customers by mobile type
        curs.execute("""create
                        or replace table TOTAL_CUSTOMERS_BY_MOBILE_TYPE as
                    select
                        "mobile_type",
                        count(*) as total_customers
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "mobile_type";""")

        # query for total customers by mobile type weekwise
        curs.execute("""create
                        or replace table TOTAL_CUSTOMERS_BY_MOBILE_TYPE_WEEKWISE as
                    select
                        "week_number",
                        "mobile_type",
                        count(*) as total_customers
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number",
                        "mobile_type";""")

        # query for highest and lowest revenue by brand name weekwise
        curs.execute("""create
                        or replace table HIGHEST_LOWEST_REVENUE_BY_BRAND_NAME_WEEKWISE as
                    select
                        "week_number",
                        "brand_name",
                        max(round("revenue_usd",2)) as highest_revenue_generated,
                        min(round("revenue_usd",2)) as lowest_revenue_generated
                    from
                        FINAL_TELECOM_TABLE
                    group by
                        "week_number",
                        "brand_name"; """)

        # query for distribution of brand name by age
        curs.execute("""create
                        or replace table DISTRBUTION_BRAND_NAME_BY_AGE as with transform1 as (
                            select
                                *,
                                (YEAR(CURRENT_DATE)-"year_of_birth")::INT as age
                            from
                                FINAL_TELECOM_TABLE
                            ),
                            transform2 as (
                                select
                                    *,
                                    ( age - age % 10 )::INT as start_dat
                                from
                                    transform1
                                )
                    select
                        concat(start_dat, '-', start_dat + 9) as age_group,
                        "brand_name",
                        count(*) as total_customers
                    from
                        transform2
                    group by
                        age_group,
                        "brand_name";""")

        # query for distribution of os name by age
        curs.execute("""create
                        or replace table DISTRBUTION_OS_NAME_BY_AGE as with transform1 as (
                            select
                                *,
                                (YEAR(CURRENT_DATE)-"year_of_birth")::INT as age
                            from
                                FINAL_TELECOM_TABLE
                            ),
                            transform2 as (
                                select
                                    *,
                                    ( age - age % 10 )::INT as start_dat
                                from
                                    transform1
                            )
                    select
                        concat(start_dat, '-', start_dat + 9) as age_group,
                        "os_name",
                        count(*) as total_customers
                    from
                        transform2
                    group by
                        age_group,
                        "os_name";""")
    except Exception as e:
        # Handle the exception here
        print("An error occurred while doing analysis:", str(e))    


def to_s3():
    """
        Function to export data from Snowflake to S3.

        Arguments:
            None

        Returns:
            None
    """
        
    try:

        conn=snowflake.connector.connect(
            user=os.getenv("SF_USER"),
            password=os.getenv('SF_PASSWORD'),
            account=os.getenv('SF_URL'),
            warehouse=os.getenv('SF_WAREHOUSE'),
            database=os.getenv('SF_DATABASE'),
            schema=os.getenv('SF_SCHEMA')
        )

        tables = ["TOTAL_DEVICES", "TOTAL_ACTIVE_DEVICES", "TOTAL_ACTIVE_DEVICES_WEEKWISE", "TOTAL_CUSTOMERS", "TOTAL_CUSTOMERS_WEEKWISE",
                    "TOTAL_CUSTOMERS_ACTIVE", "TOTAL_ACTIVE_CUSTOMERS_WEEKWISE", "TOTAL_REVENUE_MALE_FEMALE", "TOTAL_REVENUE_MALE_FEMALE_WEEKWISE", 
                    "TOTAL_REVENUE_AGE", "TOTAL_REVENUE_AGE_WEEKWISE", "TOTAL_REVENUE_BY_AGE_VALUE_SEGMENT", "TOTAL_REVENUE_BY_AGE_VALUE_SEGMENT_WEEKWISE", 
                    "TOTAL_REVENUE_BY_MOBILE_TYPE", "TOTAL_REVENUE_BY_MOBILE_TYPE_WEEKWISE", "TOTAL_REVENUE_BY_BRAND_NAME", "TOTAL_REVENUE_BY_BRAND_NAME_WEEKWISE", 
                    "TOTAL_REVENUE_BY_OS_NAME", "TOTAL_REVENUE_BY_OS_NAME_WEEKWISE", "TOTAL_REVENUE_BY_OS_VENDER", "TOTAL_REVENUE_BY_OS_VENDER_WEEKWISE", 
                    "TOTAL_CUSTOMERS_BY_OS_NAME", "TOTAL_CUSTOMERS_BY_OS_NAME_WEEKWISE", "TOTAL_CUSTOMERS_BY_BRAND_NAME", "TOTAL_CUSTOMERS_BY_BRAND_NAME_WEEKWISE", 
                    "TOTAL_CUSTOMERS_BY_MOBILE_TYPE", "TOTAL_CUSTOMERS_BY_MOBILE_TYPE_WEEKWISE", "HIGHEST_LOWEST_REVENUE_BY_BRAND_NAME_WEEKWISE", 
                    "HIGHEST_LOWEST_REVENUE_BY_OS_NAME_WEEKWISE", "DISTRBUTION_BRAND_NAME_BY_AGE", "DISTRBUTION_OS_NAME_BY_AGE"]

        # Execute Snowflake query and export to Snowflake internal stage
        cursor = conn.cursor()
        
        for table in tables:

            cursor.execute(f""" COPY INTO '@"TELECOM_DATA"."PUBLIC"."FINAL_OUTPUT_STAGE"/{table}'
                            FROM TOTAL_DEVICES
                            SINGLE = FALSE 
                            OVERWRITE=TRUE
                        """)

        conn.close()

    except Exception as e:
        # Handle the exception here
        print("An error occurred while sending data to s3 bucket:", str(e))      
        

with DAG(
    dag_id="analysis",
    start_date=datetime(2023, 1, 1),
    schedule="0 0 * * MON",
    catchup=False,
    default_args=args
):
    

    snowflake_connection = PythonOperator(
        task_id = 'snowflake_connection',
        python_callable=connect_to_snowflake,
        on_failure_callback=failure_function
    )

    query_analysis = PythonOperator(
        task_id = 'query_analysis',
        python_callable=analysis,
        on_failure_callback=failure_function
    )

    data_to_s3 = PythonOperator(
        task_id='data_to_s3',
        python_callable=to_s3,
        on_failure_callback=failure_function
    )

snowflake_connection >> query_analysis >> data_to_s3