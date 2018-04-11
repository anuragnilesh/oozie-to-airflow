# Oozie Workflow to Airflow DAG

This projects helps the teams who want to migrate existing Oozie workflows to Airflow DAGs. It was developed as 1-day
effort at internal hacakthon held at Rocketfuel. This doesn't cover all the possible Oozie worklows.
So, feel free to use it and add enhancements as needed.

## Examples
### Example 1: Simple Workflow
python2.7 oozie_to_airflow.py -w examples/example1/workflow.xml -p examples/example1/properties.xml -o out/example1_dag.py

### Example 2: Workflow with decision/fork/join node
python2.7 oozie_to_airflow.py -w examples/example2/workflow_with_switch.xml -p examples/example2/properties.xml -o out/example2_dag.py