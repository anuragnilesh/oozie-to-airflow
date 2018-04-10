{{ task_id }}_task = BashOperator(
    task_id='{{ task_id }}',
    bash_command='{{ textwrap_func("bash_command=", bash_command, 4) }}',
    params={{ params }},
    trigger_rule='{{ trigger_rule }}',
    dag=dag)

