"""Airflow DAG config to Airflow python DAG code."""
from jinja2 import Environment, FileSystemLoader
import os
import argparse
import json
import textwrap

LINE_LENGTH = 79
TAB_LENGTH = 4
TEMPLATE_DIR='templates'


def textwrap_func(key, value, extra_indent_length):
    # Indent for [tab, single quotes around key,
    # colon after key, space before value],
    indent=' ' * (len(key) + extra_indent_length) + '\''
    # Available width = LINE_LENGTH - indent - 2 characters for
    # single quotes for value
    width = LINE_LENGTH - len(indent) - 2
    return textwrap.fill(
            value,
            width=width,
            drop_whitespace=False,
            subsequent_indent=indent).replace('\n', '\'\n')


class DagGenerator(object):
    OPERATOR_TYPE = {
        "BASH": "bash",
        "LOG": "log",
        "DUMMY": "dummy"
    }

    def __init__(self, middleware_file, property_file, output_file):
        self.middleware_file = middleware_file
        self.output_file = output_file
        self.property_file = property_file
        self.env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

    def run(self):
        file_path = os.path.join(self.output_file)
        if os.path.isfile(file_path):
            print "Found file : %s, removed." % file_path
            os.remove(file_path)

        self._get_python_lib()

        middleware_flow = self._load_middleware_file(
            middleware_file=self.middleware_file)

        property_dict = self._read_property_file(
            property_file=self.property_file)

        self._get_default_dag(middleware_flow=middleware_flow,
                              params=property_dict['dict'])

        self._parse_middleware_file(middleware_flow=middleware_flow,
                                    property_dict=property_dict)

    def _load_middleware_file(self, middleware_file):
        file = open(middleware_file, "r")
        middleware_flow = json.loads(file.read())
        return middleware_flow

    def _get_default_dag(self, middleware_flow, params):
        """get default dag"""
        template = self.env.get_template('dag.tpl')
        dag_id = self.env.from_string(middleware_flow['dag_id']).render(
            params=params)
        output_from_parsed_template = template.render(dag_id=dag_id)
        self.write_dag_to_file(template=output_from_parsed_template)

    def _get_python_lib(self):
        """get python library"""
        template = self.env.get_template('lib.tpl')
        output_from_parsed_template = template.render()
        self.write_dag_to_file(template=output_from_parsed_template)

    def _operator_handler(self, operator, workflow_tasks, template_list, property_dict):
        task_id = operator['task_id']
        operator_type = operator['operator_type']
        post_task_list = []

        if operator_type == self.OPERATOR_TYPE["BASH"]:
            template = self.env.get_template('bash.tpl')
            bash_command = operator['bash_command']

            # multiple success pipe to support fork case
            if 'post_task' in operator:
                post_task_list = operator['post_task']
            indent=' ' * len('\tbash_command='.expandtabs(4)) + '\''
            template_list.append(template.render(
                task_id=task_id,
                bash_command=bash_command,
                textwrap_func=textwrap_func,
                params=property_dict['name'],
                trigger_rule=operator.get('trigger_rule', 'all_success')))

        elif operator_type == self.OPERATOR_TYPE['DUMMY']:
            template = self.env.get_template('dummy.tpl')
            # multiple success pipe to support fork case
            if 'post_task' in operator:
                post_task_list = operator['post_task']
            template_list.append(template.render(task_id=task_id))

        else:
            raise Exception("Unsupported operator type: %s" % operator_type)

        if len(post_task_list) > 0:
            for post_task in post_task_list:
                workflow_tasks.append({
                    "task": task_id,
                    "post_task": post_task
                    })

        return template_list, workflow_tasks

    def _parse_middleware_file(self, middleware_flow, property_dict):
        """parse middleware file"""
        workflow_tasks = []
        template_list = []
        for operator in middleware_flow['operators']:
            template_list, workflow_tasks = self._operator_handler(
                operator=operator,
                workflow_tasks=workflow_tasks,
                template_list=template_list,
                property_dict=property_dict)

        # write templates
        for tpl in template_list:
            self.write_dag_to_file(template=tpl)

        # write workflows
        for workflow_task in workflow_tasks:
            template = self.env.get_template('flow.tpl')
            # Success pipe
            output_from_parsed_template = template.render(
                task=workflow_task['post_task'],
                dependency=workflow_task['task'])

            self.write_dag_to_file(template=output_from_parsed_template)

    def _read_property_file(self, property_file):
        """read the property file"""
        file = open(property_file, "r")
        property_dict = json.loads(file.read())
        property_name = "params"

        template = self.env.get_template('properties.tpl')
        output_from_parsed_template = template.render(
            property_name=property_name,
            property_dict=property_dict)
        self.write_dag_to_file(template=output_from_parsed_template)

        return {
            "name": property_name,
            "dict": property_dict
        }

    def write_dag_to_file(self, template):
        """save the results to the file"""
        with open(os.path.join(self.output_file), "a") as fh:
            fh.write(template)
            fh.write("\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--middleware_file',
                        help='Json file created from oozie workflow',
                        required=True)
    parser.add_argument('--property_file',
                        help='Json file created from oozie properties.xml',
                        required=True)
    parser.add_argument('--output_file',
                        help='Output file containing the Airflow DAG code.',
                        required=True)
    args = parser.parse_args()

    dagGenerator = DagGenerator(middleware_file=args.middleware_file,
                                property_file=args.property_file,
                                output_file=args.output_file)
    dagGenerator.run()
