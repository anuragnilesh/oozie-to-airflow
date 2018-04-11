import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

# Task params
params = {
    'common_args': '--prod_mode false --print_mode true --bootstrap_conf lsv --check_marker true --create_marker false --package_base /srv/app/autodeploy/data.vostok/modeling.pipelines.ag_offline.ag_offline_pkg-latest --data_date 20170725 --run_id 1 --model_configs demo_eval_world_atomic --model_dates 20170307 --model_ids 3295 --demo_configs atomic_segments.txt --backfill_days 1 --online_hbase_dcs lax-arp,eqv-arp,ewr-arp,sjc-arp',
    'ag_offline_script': '/srv/app/autodeploy/data.vostok/modeling.pipelines.ag_offline.ag_offline_pkg-latest/modeling/pipelines/ag_offline/driver_bin',
    'HDFS_WORKFLOW_PATH': '/tmp/tmp9lg59e',
    'oozie.wf.application.path': '/tmp/tmp9lg59e',
    'mapreduce.job.user.name': 'ag',
    'HOST_NAME': 'lsv-203.rfiserve.net',
    'VOSTOK_DEPLOY_BASE_DIR': '/srv/app/autodeploy/data.vostok',
    'user.name': 'ag',
    
}
args = {
    'owner': 'team_name',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['team@rocketfuelinc.com'],
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
    dag_id='ag_offline_scoring',
    default_args=args,
    schedule_interval=None)

create_tables_task = BashOperator(
    task_id='create_tables',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'create_tables {{ params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_active_users_task = BashOperator(
    task_id='get_active_users',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'check_ag_actions_users check_bt_offline '
                 'get_active_users {{ params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

download_model_coefficients_task = BashOperator(
    task_id='download_model_coefficients',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'download_model_coefficients {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

read_and_score_task = BashOperator(
    task_id='read_and_score',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'read_and_score {{ params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

add_hive_partition_online_agprofile_task = BashOperator(
    task_id='add_hive_partition_online_agprofile',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'add_hive_partition_online_agprofile {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

replicate_online_agprofile_to_inw_task = BashOperator(
    task_id='replicate_online_agprofile_to_inw',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'replicate_online_agprofile_to_inw {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

add_hive_partition_score_histogram_task = BashOperator(
    task_id='add_hive_partition_score_histogram',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'add_hive_partition_score_histogram {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

import_hive_to_vertica_task = BashOperator(
    task_id='import_hive_to_vertica',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'import_hive_to_vertica {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

upload_to_online_hbase_task = BashOperator(
    task_id='upload_to_online_hbase',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.ag_offline_script }} --tasks '
                 'upload_to_online_hbase {{ '
                 'params.common_args }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_active_users_task.set_upstream(create_tables_task)
download_model_coefficients_task.set_upstream(get_active_users_task)
read_and_score_task.set_upstream(download_model_coefficients_task)
add_hive_partition_online_agprofile_task.set_upstream(read_and_score_task)
replicate_online_agprofile_to_inw_task.set_upstream(add_hive_partition_online_agprofile_task)
add_hive_partition_score_histogram_task.set_upstream(replicate_online_agprofile_to_inw_task)
import_hive_to_vertica_task.set_upstream(add_hive_partition_score_histogram_task)
upload_to_online_hbase_task.set_upstream(import_hive_to_vertica_task)
