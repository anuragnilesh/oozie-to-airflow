import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

# Task params
params = {
    '_': '/usr/local/bin/cronhelper.rb',
    'HBASE_SSH_OPTS': '-o StrictHostKeyChecking=no',
    'rfi.externalreport.report.mysql.host': 'master-reportsdb.global.rfiserve.net',
    'reports_db_ro_url': 'master-reportsdb.global.rfiserve.net',
    'CODE_BASE': '/data/804eeab8-d007-4a8d-b3db-092fd1575fbb/app/autodeploy/data.vostok/grid.externalreport-1.0.53',
    'WORKFLOW_NAME': 'er_spark',
    'conversion_ready_done_marker_path': '/user/grid/warehouse/donemarkers/external-report/er-hourly-conversion-ready',
    'reports_db_schema': 'rfi_meta_data',
    'load_adv_xd_conv_data_to_hive_script': '/data/804eeab8-d007-4a8d-b3db-092fd1575fbb/app/autodeploy/data.vostok/grid.externalreport-1.0.53/scripts/hive/load-adv-xd-conv-data-to-hive',
    'ENVIRONMENT': 'production',
    'rfi.externalreport.report.vertica.host': 'vertica-reportsdb-prod-01.rfiserve.net',
    'rfi.externalreport.report.vertica.dbname': 'rfi_vertica',
    'LOGNAME': 'grid',
    'USER': 'grid',
    'HOME': '/home/grid',
    'rfi_recent_data_ready_marker_path': '/user/grid/warehouse/donemarkers/external-report/rfi-recent-data-ready',
    'PATH': '/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin',
    'er_rfi_agg_daily_conversion_action_marker_path': '/user/grid/warehouse/donemarkers/external-report/er-rfi-agg-daily-conversion-action',
    'utc_timestamp': '20170727T190002Z',
    'reports_db_username': 'grid',
    'rfi.externalreport.report.vertica.ropwd': 'fd50d82a',
    'LANG': 'en_US.UTF-8',
    'rfi_recent_conv_data_ready_marker_path': '/user/grid/warehouse/donemarkers/external-report/rfi-recent-conv-data-ready',
    'SHELL': '/bin/sh',
    'rfi.externalreport.load.advertiser.conversion.data.inclusion.days': '70',
    'VAR_BASE': '/srv/var/grid.externalreport',
    'SHLVL': '1',
    'hdfs_workflow_temp_path': '/tmp/tmp0CHRTw',
    'publisher_param_data_inclusion_days': '70',
    'EXTERNAL_REPORT_DONEMARKER_PATH': '/user/grid/warehouse/donemarkers/external-report',
    'mapreduce.job.user.name': 'grid',
    'user.name': 'grid',
    'environment': 'production',
    'er_cron_date_unified': '20170727',
    'check_and_mark_max_done_file_script': '/data/804eeab8-d007-4a8d-b3db-092fd1575fbb/app/autodeploy/data.vostok/grid.externalreport-1.0.53/scripts/common/check_and_mark_max_done_file',
    'HBASE_VERSION': '0.92.2',
    'HADOOP_SSH_OPTS': '-o StrictHostKeyChecking=no',
    'rfi.externalreport.report.mysql.user': 'grid',
    'HIVE_HOME': '/usr/lib/hive',
    'rfi.externalreport.load.advertiser.param.data.inclusion.days': '70',
    'JAVA_HOME': '/usr/java/jdk1.7.0_55',
    'etl_daily_aggregate_jobs_done_marker_path': '/dp/donemarkers/etl-daily-aggregate-jobs/impressions',
    'HOST_NAME': 'lsv-197.rfiserve.net',
    'vertica_reports_db_ro_schema': 'rfi_vertica',
    'rfi.externalreport.report.mysql.dbname': 'rfi_meta_data',
    'load_adv_data_to_hive_script': '/data/804eeab8-d007-4a8d-b3db-092fd1575fbb/app/autodeploy/data.vostok/grid.externalreport-1.0.53/scripts/hive/load-adv-data-to-hive',
    'HIVE_LIB': '/usr/lib/hive/lib/',
    'workflow_name': 'er_spark',
    'vertica_reports_db_ro_url': 'vertica-reportsdb-prod-01.rfiserve.net',
    'HBASE_HOME': '/usr/share/hbase',
    'ETL_DAILY_AGGREGATE_IMPRESSIONS_DONEMARKER_PATH': '/dp/donemarkers/etl-daily-aggregate-jobs/impressions',
    'HDFS_WORKFLOW_PATH': '/tmp/tmp0CHRTw',
    'oozie.wf.application.path': '/tmp/tmp0CHRTw',
    'impression_ready_done_marker_path': '/user/grid/warehouse/donemarkers/external-report/er-hourly-impression-ready',
    'epiphany_daily_attribution_jobs_done_marker_path': '/dp/donemarkers/epiphany/prod_credited_conversions_day_ready',
    'reports_db_ro_schema': 'rfi_meta_data',
    'reports_db_ro_username': 'dp_ro',
    'HIVE_AUX_JARS_PATH': '/usr/lib/hive/auxlib',
    'rfi.externalreport.load.publisher.param.data.inclusion.days': '70',
    'reports_db_url': 'master-reportsdb.global.rfiserve.net',
    'EPIPHANY_DAILY_ATTRIBUTION_JOBS_DONEMARKER_PATH': '/dp/donemarkers/epiphany/prod_credited_conversions_day_ready',
    'vertica_reports_db_ro_username': 'grid_r',
    'vertica_reports_db_ro_pwd': 'fd50d82a',
    'load_adv_conv_data_to_hive_script': '/data/804eeab8-d007-4a8d-b3db-092fd1575fbb/app/autodeploy/data.vostok/grid.externalreport-1.0.53/scripts/hive/load-adv-conv-data-to-hive',
    'rfi.externalreport.report.mysql.ropwd': '6f7f40a',
    'ER_CRON_DATE_UNIFIED': '20170727',
    'reports_db_ro_pwd': '6f7f40a',
    'rfi.externalreport.load.rfi.consolidated.data.lookback.days': '70',
    'HOSTNAME': 'lsv-197.rfiserve.net',
    'load_pub_data_to_hive_script': '/data/804eeab8-d007-4a8d-b3db-092fd1575fbb/app/autodeploy/data.vostok/grid.externalreport-1.0.53/scripts/hive/load-pub-data-to-hive',
    'UTC_TIMESTAMP': '20170727T190002Z',
    'JOBNAME': 'external-report-hourly-cron',
    'reports_db_pwd': 'grid',
    'rfi.externalreport.report.vertica.rouser': 'grid_r',
    'PWD': '/home/grid',
    'er_cron_host': 'lsv-197.rfiserve.net',
    'rfi.externalreport.report.mysql.pwd': 'grid',
    'load_hive_table_to_mysql_script': '/data/804eeab8-d007-4a8d-b3db-092fd1575fbb/app/autodeploy/data.vostok/grid.externalreport-1.0.53/scripts/hive/load_hive_table_to_mysql',
    'advertiser_conversion_data_inclusion_days': '70',
    'rfi.externalreport.report.mysql.rouser': 'dp_ro',
    'rfi_agg_data_lookback_days': '70',
    'VOSTOK_DEPLOY_BASE_DIR': '/srv/app/autodeploy/data.vostok',
    'advertiser_param_data_inclusion_days': '70',
    
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
    dag_id='workflow_2',
    default_args=args,
    schedule_interval=None)

task1_task = BashOperator(
    task_id='task1',
    bash_command='ssh -l {{params.run_as_user}} {{ params.er_cron_host }} '
                 '/bin/bash {{ params.check_and_mark_max_do'
                 'ne_file_script }} {{ params.etl_daily_agg'
                 'regate_jobs_done_marker_path }} {{ '
                 'params.rfi_recent_data_ready_marker_path '
                 '}}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task2_task = BashOperator(
    task_id='task2',
    bash_command='ssh -l {{params.run_as_user}} {{ params.er_cron_host }} '
                 '/bin/bash {{ params.check_and_mark_max_do'
                 'ne_file_script }} {{ params.epiphany_dail'
                 'y_attribution_jobs_done_marker_path }} {{'
                 ' params.rfi_recent_conv_data_ready_marker'
                 '_path }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task4_task = BashOperator(
    task_id='task4',
    bash_command='ssh -l {{params.run_as_user}} {{ params.er_cron_host }} {{ '
                 'params.CODE_BASE '
                 '}}/dp/externalreport/whitelist_info_bin '
                 '--type conversion --output {{ '
                 'params.VAR_BASE '
                 '}}/config_conversion_filter.yml',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task5_task = BashOperator(
    task_id='task5',
    bash_command='ssh -l {{params.run_as_user}} {{ params.er_cron_host }} '
                 '/bin/bash {{ params.CODE_BASE }}/scripts/'
                 'hive/load_rfi_aggregate_impressions {{ '
                 'params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task6_task = BashOperator(
    task_id='task6',
    bash_command='ssh -l {{params.run_as_user}} {{ params.er_cron_host }} '
                 '/bin/bash {{ params.CODE_BASE }}/scripts/'
                 'hive/load_rfi_aggregate_daily_conversion_'
                 'action {{ params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }} {{ p'
                 'arams.rfi_recent_conv_data_ready_marker_p'
                 'ath }} {{ params.er_rfi_agg_daily_convers'
                 'ion_action_marker_path }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task10_task = BashOperator(
    task_id='task10',
    bash_command='ssh -l {{params.run_as_user}} {{ params.er_cron_host }} '
                 '/bin/bash {{ params.CODE_BASE }}/scripts/'
                 'hive/load_rfi_aggregate_conversion_action'
                 ' {{ params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task11_task = BashOperator(
    task_id='task11',
    bash_command='ssh -l {{params.run_as_user}} {{ params.er_cron_host }} '
                 '/bin/bash {{ params.CODE_BASE }}/scripts/'
                 'hive/load_rfi_xd_aggregate_conversion_act'
                 'ion {{ params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

task8_task = DummyOperator(
    task_id='task8',
    dag=dag)

task3_task = DummyOperator(
    task_id='task3',
    dag=dag)

task9_task = DummyOperator(
    task_id='task9',
    dag=dag)

task7_task = DummyOperator(
    task_id='task7',
    dag=dag)

task12_task = DummyOperator(
    task_id='task12',
    dag=dag)

task2_task.set_upstream(task1_task)
task3_task.set_upstream(task2_task)
task7_task.set_upstream(task4_task)
task7_task.set_upstream(task5_task)
task9_task.set_upstream(task6_task)
task7_task.set_upstream(task6_task)
task12_task.set_upstream(task10_task)
task12_task.set_upstream(task11_task)
task4_task.set_upstream(task3_task)
task5_task.set_upstream(task3_task)
task6_task.set_upstream(task3_task)
task10_task.set_upstream(task9_task)
task11_task.set_upstream(task9_task)
task8_task.set_upstream(task7_task)
task7_task.set_upstream(task12_task)
