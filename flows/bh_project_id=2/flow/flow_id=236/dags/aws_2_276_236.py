
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask
import airflow_plugins.dag_task_definitions.feed_control_callbacks as feed_control_callbacks

common_task = CommonTask(dag_id='aws_2_276_236', dag_params={})
lineage_task = LineageTask(dag_id='aws_2_276_236', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='aws_2_276_236',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['12346', 'dev'],
    params={
    "bh_environment_name": "sv-aws-env-2",
    "bh_environment_id": 7,
    "bh_kc_secret_url": "/bh/app/local/aws/keycloak/7/v4/secrets",
    "engine_version": "3.2.6",
    "driver_jars": "postgresql-42.7.12.jar",
    "customer_root_base": "s3://bh-dev-utils"
}
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 236,
            'flow_name': 'aws-2_276',
            'flow_key': 'aws_2_276',
            'bh_project_id': 2,
            'project_name': 'local',
            'flow_tags': [{'key': 'drn', 'value': '12346'}, {'key': 'environment', 'value': 'dev'}],
            'flow_type': 'DATA_PIPELINE',
            'tenant_id': 1,
            'flow_status': 'In Progress',
        }
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.emr import acquire_emr_compute_pool

    _acquire_params = {
        "compute_config_id": 16,
        "airflow_connection_id": "aws_default",
        "ingestion_group_id": 276,
        "flow_id": 236,
        "pool_namespace": "default"
    }
    create_compute = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='create_compute',
        python_callable=acquire_emr_compute_pool,
        params=_acquire_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.compute_pool.emr import submit_job_to_emr_cluster

    _submit_params = {
        "compute_task_id": "create_compute",
        "step_config": {
            "name": "{{ dag.dag_id }}_run_job_{{ ts_nodash }}",
            "job_type": "pyspark",
            "python_file": "s3://bh-dev-utils/pipelines/main.py",
            "main_python_file": "s3://bh-dev-utils/pipelines/main.py",
            "parameters": [
                "s3://bh-dev-utils/pipelines/bh_project_id=2/pipeline/pipeline_id=48/input_276.json",
                "aws",
                "s3://bh-dev-utils/schemas"
            ],
            "args": [
                "s3://bh-dev-utils/pipelines/bh_project_id=2/pipeline/pipeline_id=48/input_276.json",
                "aws",
                "s3://bh-dev-utils/schemas"
            ],
            "action_on_failure": "CONTINUE"
        },
        "pool_enabled": True,
        "airflow_connection_id": "aws_default",
        "ingestion_group_id": 276,
        "flow_id": 236,
        "pipeline_id": 48,
        "pipeline_name": "input_276",
        "pipeline_key": "input_276",
        "pool_heartbeat_interval_seconds": 120,
        "pool_release_lease": True,
        "compute_xcom_key": "return_value",
        "facts_source": "emr"
    }
    run_job = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_job',
        python_callable=submit_job_to_emr_cluster,
        params=_submit_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )


    from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator    
    delete_compute = EmrTerminateJobFlowOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='delete_compute',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_compute', key='return_value') }}",
        aws_conn_id='None',
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )



    from airflow.operators.python import PythonOperator
    end_flow_task = PythonOperator(
        task_id='end_flow_task',
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_success_callback=common_task.flow_success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    start_flow_task >> create_compute
    create_compute >> run_job
    run_job >> delete_compute
    create_compute >> delete_compute
    delete_compute >> end_flow_task
