from datetime import datetime
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
import yaml

args = {
    'owner': 'root',
    'start_date': datetime.utcnow(),
    'provide_context': True
}

def read_config():
    with open('config.yaml','r') as stream:
        return (yaml.safe_load(stream))

my_config = read_config()
dag = DAG(
    my_config['dag_id'],
    schedule_interval='@once',
    default_args=args)

analyze_log = HiveOperator(
    hql='hive_task/EC_data_analysis.hql',
    hive_cli_conn_id=my_config['hive_connection'],
    schema= my_config['hive_schema'],
    hiveconf_jinja_translate=True,
    task_id='analyze_log',
    params = { 'input_path': my_config['input_path'], 'output_path': my_config['output_path'] },
    dag=dag)

merge_files = BashOperator(
    task_id = 'merge_files',
    dag = dag,
    bash_command = '{{ params.project_home }}/shell_task/table_to_csv_one.sh {{ params.project_home }}/shell_task/table_to_csv_one.sh {{ params.tmp_dir }} {{ params.output_path }} {{ params.hive_schema }}',
    params = {'project_home': my_config['project_home'], 'tmp_dir': my_config['tmp_dir'], 'output_path': my_config['output_path'], 'hive_schema':my_config['hive_schema']}
  )

analyze_log >> merge_files

