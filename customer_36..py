from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
from textwrap import dedent

# ✅ Define the response check function
def check_response(response):
    return response.status_code == 200

default_args = {
    'start_date': datetime(2024, 4, 19),
    'depends_on_past': False,
}

with DAG('customer_360_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # 1. Watch for orders file in S3
    sensor = HttpSensor(
        task_id='watch_for_orders',
        http_conn_id='s3_orders',
        endpoint='orders.csv',
        poke_interval=60,
        timeout=600,
        retry_delay=timedelta(minutes=5),
        retries=12,
        response_check=check_response
    )

    # 2. Export customers data from MySQL to CSV
    export_mysql = BashOperator(
        task_id='export_mysql_to_csv',
        bash_command='mysql -u root -pMicrosoft@1998 -e "SELECT * FROM customers" customer_db > /tmp/customers.csv'
    )

    # 3. Transfer customers.csv to edge node
    upload_customers_to_edge = BashOperator(
        task_id='upload_customers_to_edge_node',
        bash_command='scp /tmp/customers.csv itv018688@g01.itversity.com:/home/itv018688/'
    )

    # 4. Download orders.csv to edge node from S3
    download_orders_to_edge = BashOperator(
        task_id='download_orders_to_edge_node',
        bash_command='ssh itv018688@g01.itversity.com "wget -O /home/itv018688/orders.csv https://gowtam-bucket1.s3.ap-southeast-2.amazonaws.com/orders.csv"'
    )

    # 5. Move both files to HDFS
    move_customers_to_hdfs = BashOperator(
        task_id='move_customers_to_hdfs',
        bash_command='ssh itv018688@g01.itversity.com "hdfs dfs -mkdir -p /user/itv018688 && hdfs dfs -put -f /home/itv018688/customers.csv /user/itv018688/"'
    )

    move_orders_to_hdfs = BashOperator(
        task_id='move_orders_to_hdfs',
        bash_command='ssh itv018688@g01.itversity.com "hdfs dfs -mkdir -p /user/itv018688 && hdfs dfs -put -f /home/itv018688/orders.csv /user/itv018688/"'
    )

    # 6. Create HDFS directories for customers and orders if they don't exist
    create_hive_dirs = BashOperator(
        task_id='create_hive_directories',
        bash_command='ssh itv018688@g01.itversity.com "hdfs dfs -mkdir -p /user/itv018688/hive/customers && hdfs dfs -mkdir -p /user/itv018688/hive/orders"'
    )

    # 7. Load into Hive using hive -e command
    load_customers_hive = BashOperator(
        task_id='load_customers_into_hive',
        bash_command=dedent('''\
            ssh itv018688@g01.itversity.com "
            set -e
            echo 'Creating customers table in Hive...'
            hive -e \\" 
            CREATE EXTERNAL TABLE IF NOT EXISTS customers (
                id INT, name STRING, gender STRING, po_box STRING, email STRING,
                cnic STRING, dob STRING, registered_age INT, mobile STRING, religion STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\\t' 
            STORED AS TEXTFILE
            LOCATION '/user/itv018688/hive/customers';

            LOAD DATA INPATH '/user/itv018688/customers.csv'
            OVERWRITE INTO TABLE customers;
            \\""
        ''')
    )

    load_orders_hive = BashOperator(
        task_id='load_orders_into_hive',
        bash_command=dedent('''\
            ssh itv018688@g01.itversity.com "
            set -e
            echo 'Creating orders table in Hive...'
            hive -e \\" 
            CREATE EXTERNAL TABLE IF NOT EXISTS orders (
                order_id INT, order_date STRING, customer_id INT, status STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ',' 
            STORED AS TEXTFILE
            LOCATION '/user/itv018688/hive/orders';

            LOAD DATA INPATH '/user/itv018688/orders.csv'
            OVERWRITE INTO TABLE orders;
            \\""
        ''')
    )

    dummy = DummyOperator(task_id='dummy_end')

    # DAG flow
    sensor >> download_orders_to_edge >> move_orders_to_hdfs >> create_hive_dirs >> load_orders_hive
    export_mysql >> upload_customers_to_edge >> move_customers_to_hdfs >> create_hive_dirs >> load_customers_hive

    [load_orders_hive, load_customers_hive] >> dummy
