import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

# Task params
params = {
    'prod_mode': 'true',
    'end_date': '20180117',
    'bootstrap_conf': 'lsv',
    'HDFS_WORKFLOW_PATH': '/tmp/tmpmWWoD6',
    'oozie.wf.application.path': '/tmp/tmpmWWoD6',
    'model_feature_count_script': '/srv/app/autodeploy/data.vostok/modeling.p'
                                  'ipeline'
                                  's.model'
                                  '_featur'
                                  'e_count'
                                  '.model_'
                                  'feature'
                                  '_count_'
                                  'pkg-lat'
                                  'est/mod'
                                  'eling/p'
                                  'ipeline'
                                  's/model'
                                  '_featur'
                                  'e_count'
                                  '/driver'
                                  '_bin',
    'check_marker': 'true',
    'mapreduce.job.user.name': 'modeling',
    'print_mode': 'false',
    'HOST_NAME': 'lsv-203.rfiserve.net',
    'package_base': '/srv/app/autodeploy/data.vostok/modeling.pipelines.model'
                    '_feature_count'
                    '.model_feature_count_pkg-latest',
    'start_date': '20180111',
    'VOSTOK_DEPLOY_BASE_DIR': '/srv/app/autodeploy/data.vostok',
    'user.name': 'modeling',
    
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
    dag_id='conversion_funnel',
    default_args=args,
    schedule_interval=None)

create_tables_task = BashOperator(
    task_id='create_tables',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks create_tables'
                 ' --data_date {{ params.data_date }} '
                 '--package_base {{ params.package_base }} '
                 '--bootstrap_conf {{ params.bootstrap_conf'
                 ' }} --prod_mode {{ params.prod_mode }} '
                 '--print_mode {{ params.print_mode }} '
                 '--check_marker {{ params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_conversion_actions_users_task = BashOperator(
    task_id='get_conversion_actions_users',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks '
                 'get_conversion_actions_users --data_date '
                 '{{ params.data_date }} --package_base {{ '
                 'params.package_base }} --bootstrap_conf '
                 '{{ params.bootstrap_conf }} --prod_mode '
                 '{{ params.prod_mode }} --print_mode {{ '
                 'params.print_mode }} --check_marker {{ '
                 'params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_conversion_action_to_campaign_task = BashOperator(
    task_id='get_conversion_action_to_campaign',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks '
                 'get_conversion_action_to_campaign '
                 '--data_date {{ params.data_date }} '
                 '--package_base {{ params.package_base }} '
                 '--bootstrap_conf {{ params.bootstrap_conf'
                 ' }} --prod_mode {{ params.prod_mode }} '
                 '--print_mode {{ params.print_mode }} '
                 '--check_marker {{ params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_tracking_action_to_campaign_task = BashOperator(
    task_id='get_tracking_action_to_campaign',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks '
                 'get_tracking_action_to_campaign '
                 '--data_date {{ params.data_date }} '
                 '--package_base {{ params.package_base }} '
                 '--bootstrap_conf {{ params.bootstrap_conf'
                 ' }} --prod_mode {{ params.prod_mode }} '
                 '--print_mode {{ params.print_mode }} '
                 '--check_marker {{ params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_campaign_hierarchy_task = BashOperator(
    task_id='get_campaign_hierarchy',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks '
                 'get_campaign_hierarchy --data_date {{ '
                 'params.data_date }} --package_base {{ '
                 'params.package_base }} --bootstrap_conf '
                 '{{ params.bootstrap_conf }} --prod_mode '
                 '{{ params.prod_mode }} --print_mode {{ '
                 'params.print_mode }} --check_marker {{ '
                 'params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_restrictions_task = BashOperator(
    task_id='get_restrictions',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks '
                 'get_restrictions --data_date {{ '
                 'params.data_date }} --package_base {{ '
                 'params.package_base }} --bootstrap_conf '
                 '{{ params.bootstrap_conf }} --prod_mode '
                 '{{ params.prod_mode }} --print_mode {{ '
                 'params.print_mode }} --check_marker {{ '
                 'params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

read_rtbids_task = BashOperator(
    task_id='read_rtbids',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks read_rtbids '
                 '--data_date {{ params.data_date }} '
                 '--package_base {{ params.package_base }} '
                 '--bootstrap_conf {{ params.bootstrap_conf'
                 ' }} --prod_mode {{ params.prod_mode }} '
                 '--print_mode {{ params.print_mode }} '
                 '--check_marker {{ params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

read_stats_task = BashOperator(
    task_id='read_stats',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks read_stats '
                 '--data_date {{ params.data_date }} '
                 '--package_base {{ params.package_base }} '
                 '--bootstrap_conf {{ params.bootstrap_conf'
                 ' }} --prod_mode {{ params.prod_mode }} '
                 '--print_mode {{ params.print_mode }} '
                 '--check_marker {{ params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

add_hive_partition_task = BashOperator(
    task_id='add_hive_partition',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks '
                 'add_hive_partition --data_date {{ '
                 'params.data_date }} --package_base {{ '
                 'params.package_base }} --bootstrap_conf '
                 '{{ params.bootstrap_conf }} --prod_mode '
                 '{{ params.prod_mode }} --print_mode {{ '
                 'params.print_mode }} --check_marker {{ '
                 'params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

import_hive_to_vertica_task = BashOperator(
    task_id='import_hive_to_vertica',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.cf_script }} --tasks '
                 'import_hive_to_vertica --data_date {{ '
                 'params.data_date }} --package_base {{ '
                 'params.package_base }} --bootstrap_conf '
                 '{{ params.bootstrap_conf }} --prod_mode '
                 '{{ params.prod_mode }} --print_mode {{ '
                 'params.print_mode }} --check_marker {{ '
                 'params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_conversion_actions_users_task.set_upstream(create_tables_task)
get_conversion_action_to_campaign_task.set_upstream(get_conversion_actions_users_task)
get_tracking_action_to_campaign_task.set_upstream(get_conversion_action_to_campaign_task)
get_campaign_hierarchy_task.set_upstream(get_tracking_action_to_campaign_task)
get_restrictions_task.set_upstream(get_campaign_hierarchy_task)
read_rtbids_task.set_upstream(get_restrictions_task)
read_stats_task.set_upstream(read_rtbids_task)
add_hive_partition_task.set_upstream(read_stats_task)
import_hive_to_vertica_task.set_upstream(add_hive_partition_task)
