"""Oozie workflow to Airflow dag config."""

from collections import OrderedDict

import argparse
import json
import re
import xmltodict


class Workflow(object):

    def __init__(self, workflow_json):
        self.workflow_json = workflow_json
        self.name = self.workflow_json['@name']
        self.start_node = self.get_nodes_for('start')[0]['@to']
        self.kill_node = self.get_node_name_for('kill')
        self.end_node = self.get_node_name_for('end')

    def get_node_name_for(self, element):
        if element in self.workflow_json:
            return self.workflow_json[element]['@name']

    def get_nodes_for(self, element):
        return Workflow.get_nodes_in(self.workflow_json, element)

    @staticmethod
    def get_nodes_in(node, element):
        if element in node:
            nodes = node[element]
            return [nodes] if type(nodes) != list else nodes
        else:
            return list()

    def is_kill_or_end_node(self, task_id):
        return task_id in (self.kill_node, self.end_node)


def _parsed_args():
    parser = argparse.ArgumentParser(
        description='Workflow xml to Airflow dag json converter')

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--workflow_path', help='Workflow xml')
    input_group.add_argument('--properties_file', help='Properties file')

    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument('--middleware_file',
                              help='Airflow dag output file')
    output_group.add_argument('--properties_json_file',
                              help='Properties json output file')

    return parser.parse_args()


def _replace_with_jinja(command):
    variables = re.findall(r'\${(\w+)}', command)
    replaced_cmd = command
    for var in variables:
        replaced_cmd = replaced_cmd.replace(
            '${' + var + '}', '{{ params.' + var + ' }}')
    return replaced_cmd


def convert_properties_file(properties_file, properties_output_file):
    output_json = OrderedDict()
    with open(properties_file) as fd:
        properties = xmltodict.parse(fd.read())['configuration']['property']
        for p in sorted(properties):
            output_json[p['name']] = p['value']

    with open(properties_output_file, 'w') as fd:
        fd.write(json.dumps(output_json, indent=4))


def _extract_task_name(case_text):
    task_names = re.findall(
        r'\${wf:actionExternalStatus\("(\w+)"\) != "OK"}', case_text)
    if task_names:
        return task_names[0]


def convert_oozie_xml(workflow_xml, json_output_file):
    print "Reading from ", workflow_xml
    with open(workflow_xml) as fd:
        doc = xmltodict.parse(fd.read())
        workflow = Workflow(doc['workflow-app'])

        af_operators = list()
        af_dag_json = OrderedDict([
            ('dag_id', _replace_with_jinja(workflow.name)),
            ('start', workflow.start_node),
            ('end', workflow.end_node),
            ('operators', af_operators)
        ])
        af_dag_json['operators'] = af_operators
        _handle_action_nodes(af_operators, workflow)
        # _handle_kill_node(af_operators, workflow)
        # _handle_end_node(af_operators, workflow)
        _handle_decision_nodes(af_operators, workflow)
        _handle_fork_nodes(af_operators, workflow)
        _handle_join_nodes(af_operators, workflow)

        for operator in af_operators:
            if 'post_task' in operator:
                if workflow.end_node in operator['post_task']:
                    operator['post_task'].remove(workflow.end_node)
                if workflow.kill_node in operator['post_task']:
                    operator['post_task'].remove(workflow.kill_node)

        print "Writing to ", json_output_file
        with open(json_output_file, 'w') as fd:
            fd.write(json.dumps(af_dag_json, indent=4))


def _handle_action_nodes(af_operators, workflow):
    for action in workflow.get_nodes_for('action'):
        ssh_json = action['ssh']
        command = 'ssh -l {{params.run_as_user}} ' + ssh_json['host']
        for key, value in ssh_json.iteritems():
            if key == 'host' or not value:
                continue
            if type(value) != list:
                command += ' ' + value
            else:
                command += ' ' + ' '.join(value)
        af_operators.append(OrderedDict([
                ('task_id', action['@name']),
                ('operator_type', 'bash'),
                ('bash_command', _replace_with_jinja(command)),
                ('post_task',
                 list(set([action['ok']['@to'], action['error']['@to']])))
            ]))


def _handle_kill_node(af_operators, workflow):
    if not workflow.kill_node:
        return
    af_operators.append(
        OrderedDict([
            ('task_id', workflow.kill_node),
            ('operator_type', 'bash'),
            ('bash_command', 'exit 1'),
            ('trigger_rule', 'one_failed')
        ]))


def _handle_end_node(af_operators, workflow):
    if not workflow.end_node:
        return
    af_operators.append(
        OrderedDict([
            ('task_id', workflow.end_node),
            ('operator_type', 'dummy')
        ]))


def _handle_decision_nodes(af_operators, workflow):
    for decision_node in workflow.get_nodes_for('decision'):
        switch_cases = list()
        switch_nodes = Workflow.get_nodes_in(decision_node, 'switch')
        for switch_node in switch_nodes:
            case_nodes = Workflow.get_nodes_in(switch_node, 'case')
            switch_cases.extend(case_nodes)
        case_transitions = set()
        case_tasks = set()
        for case in switch_cases:
            case_transition = case['@to']
            case_transitions.add(case_transition)
            task_name = _extract_task_name(case['#text'])
            if not task_name:
                raise RuntimeError("Switch case format not satisfied")
            case_tasks.add(task_name)
        case_transitions = list(case_transitions)
        assert len(
            case_transitions) == 1, "Transitions to more/less than one state"
        assert workflow.is_kill_or_end_node(case_transitions[0])

        # The decision node shall transition only to
        # the default case.
        af_operators.append(
            OrderedDict([
                ('task_id', decision_node['@name']),
                ('operator_type', 'dummy'),
                ('post_task', [decision_node['switch']['default']['@to']])
            ]))


def _handle_fork_nodes(af_operators, workflow):
    for fork_node in workflow.get_nodes_for('fork'):
        af_operators.append(
            OrderedDict([
                ('task_id', fork_node['@name']),
                ('operator_type', 'dummy'),
                ('post_task', [path['@start'] for path in fork_node['path']])
            ]))


def _handle_join_nodes(af_operators, workflow):
    for join_node in workflow.get_nodes_for('join'):
        af_operators.append(
            OrderedDict([
                ('task_id', join_node['@name']),
                ('operator_type', 'dummy'),
                ('post_task', [join_node['@to']])
            ]))


def main():
    args = _parsed_args()
    if args.workflow_path:
        convert_oozie_xml(args.workflow_path, args.middleware_file)
    else:
        convert_properties_file(args.properties_file,
                                args.properties_json_file)


if __name__ == "__main__":
    main()
