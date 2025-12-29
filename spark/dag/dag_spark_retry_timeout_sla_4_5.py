from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
   "owner"      : "data-engineer",
   "retries"    : 2,
   "retry_delay": timedelta(minutes=10),
   "sla"        : timedelta(hours=7),      # pipeline harus selesai sebelum jam 07:00
}

with DAG(
   dag_id            = "spark_retry_timeout_sla_4_5",
   start_date        = datetime(2025, 1, 1),
   schedule_interval = "0 2 * * *",                     # jalan setiap jam 02:00
   catchup           = False,
   default_args      = default_args,
) as dag:

   spark_job = SparkSubmitOperator(
       task_id          = "spark_daily_job",
       application      = "dags/spark_jobs/daily_gmv.py",
       conn_id          = "spark_default",
       application_args = ["--process_date", "{{ ds }}"],
       conf             = {
           "spark.executor.instances": "2",
           "spark.executor.memory"   : "2g",
       },
       execution_timeout=timedelta(hours=1),  # job tidak boleh > 1 jam
   )

   spark_job


