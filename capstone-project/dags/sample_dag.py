from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import os
import pyspark

args={
    'owner': 'username',
    'start_date': days_ago(1),
}

dag = DAG(dag_id='sample_dag', default_args=args, schedule_interval=None)

def run_this_func(**context):
    print('hi')

def create_file(**context):
    f = open("output_test.txt","w+")

    for i in range(10):
        f.write("This is line %d\r\n" % (i+1))

    f.close()

def spark_dag(**context):
    sc = pyspark.SparkContext('local[*]')
    txt = sc.textFile('output_test.txt')
    print(txt.count())

    python_lines = txt.filter(lambda line: '8' in line.lower())
    print(python_lines.count())

with dag:
    run_this_task = PythonOperator(
        task_id='run_this',
        python_callable=run_this_func,
        provide_context=True
    )

    run_this_task2 = PythonOperator(
        task_id='run_this2',
        python_callable=run_this_func,
        provide_context=True
    )

    run_this_task3 = PythonOperator(
        task_id='run_this3',
        python_callable=create_file,
        provide_context=True
    )

    run_this_task4 = PythonOperator(
        task_id='run_this4',
        python_callable=spark_dag,
        provide_context=True
    )

    run_this_task5 = PythonOperator(
        task_id='run_this5',
        python_callable=run_this_func,
        provide_context=True
    )

    run_this_task >> run_this_task2
    run_this_task >> run_this_task5
    run_this_task2 >> run_this_task3
    run_this_task3 >> run_this_task4
