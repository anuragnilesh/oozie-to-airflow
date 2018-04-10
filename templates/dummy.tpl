{{ task_id }}_task = DummyOperator(
    task_id='{{ task_id }}',
    dag=dag)

