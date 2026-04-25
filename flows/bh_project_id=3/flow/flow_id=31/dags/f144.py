
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='f144', dag_params={})
lineage_task = LineageTask(dag_id='f144', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='f144',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=[]
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 31,
            'flow_name': 'f144',
            'flow_key': 'f144',
            'bh_project_id': 3,
            'project_name': 'varadha-local-azure',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 2,
            'flow_status': 'In Progress',
        }
    )


    from airflow.operators.bash import BashOperator
    custom_8f747c114 = BashOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='custom_8f747c114',
        bash_command='test',
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )



    from airflow.operators.python import PythonOperator
    end_flow_task = PythonOperator(
        task_id='end_flow_task',
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_failure_callback=common_task.failure_callback
    )

    start_flow_task >> custom_8f747c114
    custom_8f747c114 >> end_flow_task
