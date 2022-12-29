from datetime import datetime

from airflow.decorators import dag, task



@dag(
    schedule_interval="@weekly",
    start_date=datetime(2022, 11, 11),
    catchup=False,
    default_args={
        "retries": 3,
    },
    tags=["Nextflow Tower Metrics"],
)
def taskflow_test_dag():
    @task
    def no_return():
        print("this is a task")

    @task
    def returns_string(nothing):
        print("---------------------")
        print(nothing)     
        print("---------------------")
        return "string"

    @task
    def returns_dict_from_string(string):
        return {"string": "string"}

    nothing = no_return()
    string = returns_string(nothing)
    dict = returns_dict_from_string(string)

    nothing >> string >> dict

taskflow_test_dag = taskflow_test_dag()
    

