"""Main module to convert Oozie workflow to Airflow DAG."""
import argparse
import os
import tempfile

import wf_to_json
from json_to_dag import DagGenerator


def _parsed_args():
    """Returns parsed args."""
    parser = argparse.ArgumentParser(
        description='Workflow xml to Airflow dag code')
    parser.add_argument('-w', '--oozie_workflow_xml',
                        help='Oozie Workflow xml file', required=True)
    parser.add_argument('-p', '--oozie_properties_xml',
                        help='Oozie Properties XML file', required=True)
    parser.add_argument('-o', '--airflow_dag',
                        help='Output file containng Airflow dag code',
                        required=True)
    return parser.parse_args()


def _get_temp_file(suffix='', prefix='tmp', directory='.'):
    """Returns a temp file."""
    handle, file_path = tempfile.mkstemp(suffix=suffix, prefix=prefix,
                                         dir=directory)
    os.close(handle)
    return file_path


def _exists(file_path):
    """Returns True if file exists or False otherwise."""
    return file_path and os.path.exists(file_path)


def _delete_file(file_path):
    """
    Deletes the given file if it exists.
    :param file_path: file to be deleted
    """
    if _exists(file_path) and os.path.isfile(file_path):
        os.remove(file_path)


def main():
    """Module entry point."""
    args = _parsed_args()
    tmp_dag_json = None
    tmp_properties_json = None
    try:
        tmp_dag_json = _get_temp_file()
        wf_to_json.convert_oozie_xml(args.oozie_workflow_xml,
                                     tmp_dag_json)
        tmp_properties_json = _get_temp_file()
        wf_to_json.convert_properties_file(args.oozie_properties_xml,
                                           tmp_properties_json)
        dagGenerator = DagGenerator(middleware_file=tmp_dag_json,
                                    property_file=tmp_properties_json,
                                    output_file=args.airflow_dag)
        dagGenerator.run()
    finally:
        _delete_file(tmp_dag_json)
        _delete_file(tmp_properties_json)


if __name__ == '__main__':
    main()
