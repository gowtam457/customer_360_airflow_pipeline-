# 🧩 Customer 360 Airflow Pipeline

A mini ETL orchestration pipeline using **Apache Airflow**, **MySQL**, **Amazon S3**, **HDFS**, and **Hive**.  
This project simulates a real-world data flow to build a unified customer view using various data sources.

---

## 🔧 Technologies Used

- Apache Airflow (Orchestration)
- MySQL (Customer data source)
- Amazon S3 (Order data source)
- HDFS (Storage layer)
- Hive (Data warehouse)

---

## ⚙️ Pipeline Steps

1. 🧠 Watch for `orders.csv` file in S3 using an Airflow `HttpSensor`
2. 📤 Export customer data from MySQL as `customers.csv`
3. 🔁 Move both `orders.csv` and `customers.csv` to an Edge Node
4. 📂 Copy files to HDFS
5. 🏗️ Create Hive tables and load data into:
   - `customers`
   - `orders`

---

## 🚀 DAG Flow Overview

```text
         +---------------------+
         | Watch S3 for Orders |
         +----------+----------+
                    |
                    v
     +--------------+---------------+
     | Download orders.csv to Edge |
     +--------------+---------------+
                    |
                    v
        +-----------+-----------+
        | Move orders.csv to HDFS |
        +-----------+-----------+
                    |
                    v
          +---------+---------+
          | Create Hive Dirs  |
          +---------+---------+
                    |
                    v
          +---------+----------+
          | Load Orders into Hive |
          +---------------------+

        (Meanwhile in Parallel)

+----------------------------+
| Export Customers from MySQL|
+-------------+--------------+
              |
              v
  +-----------+----------+
  | Upload customers.csv  |
  +-----------+----------+
              |
              v
   +----------+---------+
   | Move to HDFS       |
   +----------+---------+
              |
              v
   +----------+----------+
   | Load Customers into Hive |
   +--------------------------+


