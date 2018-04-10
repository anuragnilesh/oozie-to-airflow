{{ task_id }}_task = BashOperator(
    task_id='{{ task_id }}',
    bash_command='echo {{ message }}',
    trigger_rule='{{ trigger_rule }}',
    params="",
    dag=dag)

