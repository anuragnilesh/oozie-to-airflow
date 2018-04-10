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
    dag_id='model_feature_count',
    default_args=args,
    schedule_interval=None)

get_model_feature_count_task = BashOperator(
    task_id='get_model_feature_count',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.model_feature_count_script }} '
                 '--tasks get_model_feature_count '
                 'get_rt_model_feature_count '
                 'combine_feature_count_files --start_date '
                 '{{ params.start_date }} --end_date {{ '
                 'params.end_date }} --bootstrap_conf {{ '
                 'params.bootstrap_conf }} --prod_mode {{ '
                 'params.prod_mode }} --print_mode {{ '
                 'params.print_mode }} --check_marker {{ '
                 'params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

upload_model_feature_count_task = BashOperator(
    task_id='upload_model_feature_count',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.model_feature_count_script }} '
                 '--tasks upload_model_feature_count '
                 '--start_date {{ params.start_date }} '
                 '--end_date {{ params.end_date }} '
                 '--bootstrap_conf {{ params.bootstrap_conf'
                 ' }} --prod_mode {{ params.prod_mode }} '
                 '--print_mode {{ params.print_mode }} '
                 '--check_marker {{ params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

get_model_coefficients_task = BashOperator(
    task_id='get_model_coefficients',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.model_feature_count_script }} '
                 '--tasks get_model_coefficients '
                 '--start_date {{ params.start_date }} '
                 '--end_date {{ params.end_date }} '
                 '--bootstrap_conf {{ params.bootstrap_conf'
                 ' }} --prod_mode {{ params.prod_mode }} '
                 '--print_mode {{ params.print_mode }} '
                 '--check_marker {{ params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

upload_model_coefficients_task = BashOperator(
    task_id='upload_model_coefficients',
    bash_command='ssh -l {{params.owner}} {{ params.HOST_NAME }} {{ '
                 'params.model_feature_count_script }} '
                 '--tasks upload_model_coefficients '
                 '--start_date {{ params.start_date }} '
                 '--end_date {{ params.end_date }} '
                 '--bootstrap_conf {{ params.bootstrap_conf'
                 ' }} --prod_mode {{ params.prod_mode }} '
                 '--print_mode {{ params.print_mode }} '
                 '--check_marker {{ params.check_marker }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

upload_model_feature_count_task.set_upstream(get_model_feature_count_task)
get_model_coefficients_task.set_upstream(upload_model_feature_count_task)
upload_model_coefficients_task.set_upstream(get_model_coefficients_task)
