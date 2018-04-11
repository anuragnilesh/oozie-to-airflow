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
    dag_id='er_spark_20170727',
    default_args=args,
    schedule_interval=None)

CheckAndMarkRfiRecentDataReadyFile_task = BashOperator(
    task_id='CheckAndMarkRfiRecentDataReadyFile',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.check_and_mark_max_done_file_s'
                 'cript }} {{ params.etl_daily_aggregate_jo'
                 'bs_done_marker_path }} {{ '
                 'params.rfi_recent_data_ready_marker_path '
                 '}}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

CheckAndMarkRfiRecentConvDataReadyFile_task = BashOperator(
    task_id='CheckAndMarkRfiRecentConvDataReadyFile',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.check_and_mark_max_done_file_s'
                 'cript }} {{ params.epiphany_daily_attribu'
                 'tion_jobs_done_marker_path }} {{ params.r'
                 'fi_recent_conv_data_ready_marker_path }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadAdvertiserDataToHive_task = BashOperator(
    task_id='LoadAdvertiserDataToHive',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.load_adv_data_to_hive_script '
                 '}} {{ params.environment }} {{ '
                 'params.vertica_reports_db_ro_url }} {{ '
                 'params.vertica_reports_db_ro_username }} '
                 '{{ params.vertica_reports_db_ro_pwd }} {{'
                 ' params.vertica_reports_db_ro_schema }} '
                 '{{ params.er_cron_date_unified }} {{ para'
                 'ms.advertiser_param_data_inclusion_days '
                 '}} {{ '
                 'params.rfi_recent_data_ready_marker_path '
                 '}}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadAdvertiserFilterReducerConfig_task = BashOperator(
    task_id='LoadAdvertiserFilterReducerConfig',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} {{ '
                 'params.CODE_BASE '
                 '}}/dp/externalreport/whitelist_info_bin '
                 '--type advertiser --output {{ '
                 'params.VAR_BASE '
                 '}}/config_advertiser_filter.yml',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadAdvertiserAdMapping_task = BashOperator(
    task_id='LoadAdvertiserAdMapping',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE }}/scripts/hive'
                 '/load-advertiser-ad-mapping {{ '
                 'params.environment }} {{ '
                 'params.reports_db_ro_url }} {{ '
                 'params.reports_db_ro_username }} {{ '
                 'params.reports_db_ro_pwd }} {{ '
                 'params.reports_db_ro_schema }} {{ '
                 'params.er_cron_date_unified }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadPublisherDataToHive_task = BashOperator(
    task_id='LoadPublisherDataToHive',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.load_pub_data_to_hive_script '
                 '}} {{ params.environment }} {{ '
                 'params.reports_db_ro_url }} {{ '
                 'params.reports_db_ro_username }} {{ '
                 'params.reports_db_ro_pwd }} {{ '
                 'params.reports_db_ro_schema }} {{ '
                 'params.er_cron_date_unified }} {{ params.'
                 'publisher_param_data_inclusion_days }} {{'
                 ' params.rfi_recent_data_ready_marker_path'
                 ' }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadPublisherFilterReducerConfig_task = BashOperator(
    task_id='LoadPublisherFilterReducerConfig',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} {{ '
                 'params.CODE_BASE '
                 '}}/dp/externalreport/whitelist_info_bin '
                 '--type publisher --output {{ '
                 'params.VAR_BASE '
                 '}}/config_publisher_filter.yml',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadAdvertiserConversionDataToHive_task = BashOperator(
    task_id='LoadAdvertiserConversionDataToHive',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ '
                 'params.load_adv_conv_data_to_hive_script '
                 '}} {{ params.environment }} {{ '
                 'params.vertica_reports_db_ro_url }} {{ '
                 'params.vertica_reports_db_ro_username }} '
                 '{{ params.vertica_reports_db_ro_pwd }} {{'
                 ' params.vertica_reports_db_ro_schema }} '
                 '{{ params.er_cron_date_unified }} {{ para'
                 'ms.advertiser_conversion_data_inclusion_d'
                 'ays }} {{ params.rfi_recent_conv_data_rea'
                 'dy_marker_path }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadAdvertiserXdConversionDataToHive_task = BashOperator(
    task_id='LoadAdvertiserXdConversionDataToHive',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.load_adv_xd_conv_data_to_hive_'
                 'script }} {{ params.environment }} {{ '
                 'params.vertica_reports_db_ro_url }} {{ '
                 'params.vertica_reports_db_ro_username }} '
                 '{{ params.vertica_reports_db_ro_pwd }} {{'
                 ' params.vertica_reports_db_ro_schema }} '
                 '{{ params.er_cron_date_unified }} {{ para'
                 'ms.advertiser_conversion_data_inclusion_d'
                 'ays }} {{ params.rfi_recent_conv_data_rea'
                 'dy_marker_path }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadConversionFilterReducerConfig_task = BashOperator(
    task_id='LoadConversionFilterReducerConfig',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} {{ '
                 'params.CODE_BASE '
                 '}}/dp/externalreport/whitelist_info_bin '
                 '--type conversion --output {{ '
                 'params.VAR_BASE '
                 '}}/config_conversion_filter.yml',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadRfiAggregateImpressions_task = BashOperator(
    task_id='LoadRfiAggregateImpressions',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE }}/scripts/hive/load'
                 '_rfi_aggregate_impressions {{ '
                 'params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadRfiAggregateDailyConversionAction_task = BashOperator(
    task_id='LoadRfiAggregateDailyConversionAction',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE }}/scripts/hive/load'
                 '_rfi_aggregate_daily_conversion_action {{'
                 ' params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }} {{ p'
                 'arams.rfi_recent_conv_data_ready_marker_p'
                 'ath }} {{ params.er_rfi_agg_daily_convers'
                 'ion_action_marker_path }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadRfiAggregateConversionAction_task = BashOperator(
    task_id='LoadRfiAggregateConversionAction',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE }}/scripts/hive/load'
                 '_rfi_aggregate_conversion_action {{ '
                 'params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadRfiXdAggregateConversionAction_task = BashOperator(
    task_id='LoadRfiXdAggregateConversionAction',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE }}/scripts/hive/load'
                 '_rfi_xd_aggregate_conversion_action {{ '
                 'params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

AdvertiserReport_task = BashOperator(
    task_id='AdvertiserReport',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE '
                 '}}/scripts/spark/advertiser_report {{ '
                 'params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadAdvertiserRatioToVertica_task = BashOperator(
    task_id='LoadAdvertiserRatioToVertica',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} {{ '
                 'params.CODE_BASE }}/dp/externalreport/hiv'
                 'e_to_vertica_loader_bin --environment {{ '
                 'params.environment }} --run_date {{ '
                 'params.er_cron_date_unified }} '
                 '--look_back_days {{ params.advertiser_par'
                 'am_data_inclusion_days }} --table '
                 'advertiser_external_ratio',
    params=params,
    trigger_rule='all_success',
    dag=dag)

PublisherReport_task = BashOperator(
    task_id='PublisherReport',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE '
                 '}}/scripts/spark/publisher_report {{ '
                 'params.environment }} {{ '
                 'params.er_cron_date_unified }} {{ '
                 'params.rfi_agg_data_lookback_days }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadPublisherRatioToVertica_task = BashOperator(
    task_id='LoadPublisherRatioToVertica',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} {{ '
                 'params.CODE_BASE }}/dp/externalreport/hiv'
                 'e_to_vertica_loader_bin --environment {{ '
                 'params.environment }} --run_date {{ '
                 'params.er_cron_date_unified }} '
                 '--look_back_days {{ params.publisher_para'
                 'm_data_inclusion_days }} --table '
                 'publisher_external_ratio',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadAggregateAdLogsAndPublisherDataToVertica_task = BashOperator(
    task_id='LoadAggregateAdLogsAndPublisherDataToVertica',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} {{ '
                 'params.CODE_BASE }}/dp/externalreport/hiv'
                 'e_to_vertica_loader_bin --environment {{ '
                 'params.environment }} --run_date {{ '
                 'params.er_cron_date_unified }} '
                 '--look_back_days {{ params.publisher_para'
                 'm_data_inclusion_days }} --table '
                 'ad_logs_and_publisher_data',
    params=params,
    trigger_rule='all_success',
    dag=dag)

ConversionReport_task = BashOperator(
    task_id='ConversionReport',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE '
                 '}}/scripts/spark/conversion_report {{ '
                 'params.environment }} {{ '
                 'params.er_cron_date_unified }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

LoadConversionRatioToVertica_task = BashOperator(
    task_id='LoadConversionRatioToVertica',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} {{ '
                 'params.CODE_BASE }}/dp/externalreport/hiv'
                 'e_to_vertica_loader_bin --environment {{ '
                 'params.environment }} --run_date {{ '
                 'params.er_cron_date_unified }} '
                 '--look_back_days {{ params.advertiser_con'
                 'version_data_inclusion_days }} --table '
                 'advertiser_external_conversion_ratio',
    params=params,
    trigger_rule='all_success',
    dag=dag)

XdConversionReport_task = BashOperator(
    task_id='XdConversionReport',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.CODE_BASE '
                 '}}/scripts/spark/xd_conversion_report {{ '
                 'params.environment }} {{ '
                 'params.er_cron_date_unified }}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

CheckAndMarkImpressionReadyDoneFile_task = BashOperator(
    task_id='CheckAndMarkImpressionReadyDoneFile',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.check_and_mark_max_done_file_s'
                 'cript }} {{ '
                 'params.rfi_recent_data_ready_marker_path '
                 '}} {{ '
                 'params.impression_ready_done_marker_path '
                 '}}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

CheckAndMarkConversionReadyDoneFile_task = BashOperator(
    task_id='CheckAndMarkConversionReadyDoneFile',
    bash_command='ssh -l {{params.owner}} {{ params.er_cron_host }} /bin/bash'
                 ' {{ params.check_and_mark_max_done_file_s'
                 'cript }} {{ params.rfi_recent_conv_data_r'
                 'eady_marker_path }} {{ '
                 'params.conversion_ready_done_marker_path '
                 '}}',
    params=params,
    trigger_rule='all_success',
    dag=dag)

CheckLoadDataError_task = DummyOperator(
    task_id='CheckLoadDataError',
    dag=dag)

CheckErrors_task = DummyOperator(
    task_id='CheckErrors',
    dag=dag)

fork_load_data_task = DummyOperator(
    task_id='fork_load_data',
    dag=dag)

fork_load_conv_data_task = DummyOperator(
    task_id='fork_load_conv_data',
    dag=dag)

fork_1_task = DummyOperator(
    task_id='fork_1',
    dag=dag)

join_load_data_task = DummyOperator(
    task_id='join_load_data',
    dag=dag)

join_load_conv_data_task = DummyOperator(
    task_id='join_load_conv_data',
    dag=dag)

join_1_task = DummyOperator(
    task_id='join_1',
    dag=dag)

CheckAndMarkRfiRecentConvDataReadyFile_task.set_upstream(CheckAndMarkRfiRecentDataReadyFile_task)
fork_load_data_task.set_upstream(CheckAndMarkRfiRecentConvDataReadyFile_task)
join_load_data_task.set_upstream(LoadAdvertiserDataToHive_task)
join_load_data_task.set_upstream(LoadAdvertiserFilterReducerConfig_task)
join_load_data_task.set_upstream(LoadAdvertiserAdMapping_task)
join_load_data_task.set_upstream(LoadPublisherDataToHive_task)
join_load_data_task.set_upstream(LoadPublisherFilterReducerConfig_task)
join_load_data_task.set_upstream(LoadAdvertiserConversionDataToHive_task)
join_load_data_task.set_upstream(LoadAdvertiserXdConversionDataToHive_task)
join_load_data_task.set_upstream(LoadConversionFilterReducerConfig_task)
join_load_data_task.set_upstream(LoadRfiAggregateImpressions_task)
fork_load_conv_data_task.set_upstream(LoadRfiAggregateDailyConversionAction_task)
join_load_data_task.set_upstream(LoadRfiAggregateDailyConversionAction_task)
join_load_conv_data_task.set_upstream(LoadRfiAggregateConversionAction_task)
join_load_conv_data_task.set_upstream(LoadRfiXdAggregateConversionAction_task)
join_1_task.set_upstream(AdvertiserReport_task)
LoadAdvertiserRatioToVertica_task.set_upstream(AdvertiserReport_task)
join_1_task.set_upstream(LoadAdvertiserRatioToVertica_task)
join_1_task.set_upstream(PublisherReport_task)
LoadPublisherRatioToVertica_task.set_upstream(PublisherReport_task)
LoadAggregateAdLogsAndPublisherDataToVertica_task.set_upstream(LoadPublisherRatioToVertica_task)
join_1_task.set_upstream(LoadPublisherRatioToVertica_task)
join_1_task.set_upstream(LoadAggregateAdLogsAndPublisherDataToVertica_task)
LoadConversionRatioToVertica_task.set_upstream(ConversionReport_task)
join_1_task.set_upstream(ConversionReport_task)
join_1_task.set_upstream(LoadConversionRatioToVertica_task)
join_1_task.set_upstream(XdConversionReport_task)
CheckAndMarkConversionReadyDoneFile_task.set_upstream(CheckAndMarkImpressionReadyDoneFile_task)
fork_1_task.set_upstream(CheckLoadDataError_task)
CheckAndMarkImpressionReadyDoneFile_task.set_upstream(CheckErrors_task)
LoadAdvertiserDataToHive_task.set_upstream(fork_load_data_task)
LoadAdvertiserFilterReducerConfig_task.set_upstream(fork_load_data_task)
LoadAdvertiserAdMapping_task.set_upstream(fork_load_data_task)
LoadPublisherDataToHive_task.set_upstream(fork_load_data_task)
LoadPublisherFilterReducerConfig_task.set_upstream(fork_load_data_task)
LoadAdvertiserConversionDataToHive_task.set_upstream(fork_load_data_task)
LoadAdvertiserXdConversionDataToHive_task.set_upstream(fork_load_data_task)
LoadConversionFilterReducerConfig_task.set_upstream(fork_load_data_task)
LoadRfiAggregateImpressions_task.set_upstream(fork_load_data_task)
LoadRfiAggregateDailyConversionAction_task.set_upstream(fork_load_data_task)
LoadRfiAggregateConversionAction_task.set_upstream(fork_load_conv_data_task)
LoadRfiXdAggregateConversionAction_task.set_upstream(fork_load_conv_data_task)
AdvertiserReport_task.set_upstream(fork_1_task)
PublisherReport_task.set_upstream(fork_1_task)
ConversionReport_task.set_upstream(fork_1_task)
XdConversionReport_task.set_upstream(fork_1_task)
CheckLoadDataError_task.set_upstream(join_load_data_task)
join_load_data_task.set_upstream(join_load_conv_data_task)
CheckErrors_task.set_upstream(join_1_task)
