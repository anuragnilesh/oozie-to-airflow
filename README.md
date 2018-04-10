# Example 1: Success all workflow
## Generating middleware file
python2.7 wf_to_json.py --workflow_path example1/workflow.xml --middleware_file example1/dag.json

## Generating propeties file
python2.7 wf_to_json.py --properties_file example1/properties.xml --properties_json_file example1/properties.json

## Generating dag file
python json_to_dag.py --middleware_file example1/dag.json --output_file example1_dag.py --property_file example1/properties.json

# =================
# Example 2: Failed the one of workflow
## Generating middleware file
python2.7 wf_to_json.py --workflow_path example2/workflow.xml --middleware_file example2/dag.json

## Generating propeties file
python2.7 wf_to_json.py --properties_file example2/properties.xml --properties_json_file example2/properties.json

## Generating dag file
python json_to_dag.py --middleware_file example2/dag.json --output_file example2_dag.py --property_file example2/properties.json

# =================
# Example 3: Workflow with decision/fork/join node
## Generating middleware file
python2.7 wf_to_json.py --workflow_path example3/workflow_with_switch.xml --middleware_file example3/dag.json

## Generating propeties file
python2.7 wf_to_json.py --properties_file example3/properties.xml --properties_json_file example3/properties.json

## Generating dag file
python json_to_dag.py --middleware_file example3/dag.json --output_file example3_dag.py --property_file example3/properties.json
