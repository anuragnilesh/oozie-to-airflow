import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

# Task params
params = {
    'common_args': '--prod_mode true',
    'run_as_user': 'user',
    'host': 'abc-123.abc.net',
    'bash_script': '/home/user/test.sh',
    
}
args = {
    'owner': 'team_name',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['team@abc.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    dag_id='oozie_workflow_1',
    default_args=args,
    schedule_interval=None)

task1_task = BashOperator(
    task_id='task1',
    bash_command='ssh -l {{params.run_as_user}} {{ params.host }} {{ '
                 'params.bash_script }} --tasks task1 {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task2_task = BashOperator(
    task_id='task2',
    bash_command='ssh -l {{params.run_as_user}} {{ params.host }} {{ '
                 'params.bash_script }} --tasks task2 {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task3_task = BashOperator(
    task_id='task3',
    bash_command='ssh -l {{params.run_as_user}} {{ params.host }} {{ '
                 'params.bash_script }} --tasks task3 {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task4_task = BashOperator(
    task_id='task4',
    bash_command='ssh -l {{params.run_as_user}} {{ params.host }} {{ '
                 'params.bash_script }} --tasks task4 {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task2_task.set_upstream(task1_task)
task3_task.set_upstream(task2_task)
task4_task.set_upstream(task3_task)
