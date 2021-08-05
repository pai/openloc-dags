# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DAG running in response to a Cloud Storage bucket change."""

# [START composer_trigger_response_twostep_dag]
import datetime
import time
import random
import os



import airflow
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators import dummy_operator


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

DAGS_FOLDER = os.environ["DAGS_FOLDER"]
jar_file = f"{DAGS_FOLDER}/okapi-genesis-0.36-jar-with-dependencies.jar"


default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': yesterday,
}

with models.DAG(
        'composer_sample_trigger_response_twostep_dag',
        default_args=default_args,
        # Not scheduled, trigger only
        schedule_interval=None) as dag:

    def greeting():
        print('Greetings from openloc!.')
        return 'Greeting successfully printed.' 

    def makeBranchChoice():
        x = random.randint(1, 5)

        if(x <= 2):
            return 'implement_feedback'

        else:
            return 'generate_output'  


    # Print the dag_run's configuration, which includes information about the
    # Cloud Storage object change.
    start = bash_operator.BashOperator(
        task_id='start', bash_command='echo init...')

    print_gcs_info = bash_operator.BashOperator(
        task_id='print_gcs_info', bash_command='echo {{ dag_run.conf }}')

    

    segment_asset = bash_operator.BashOperator(
        task_id='segment_asset', 
        bash_command = 'java -jar ' + jar_file + ' -f gs://assetmanager-321903/geronimo.properties -a e -il en -ol fr',
        params = {'-f': 'gs://assetmanager-321903/geronimo.properties', '-a': 'e', 'il': 'en', 'ol': 'fr'}
    )

    analyse_tm = bash_operator.BashOperator(
        task_id='analyse_tm', bash_command='echo analysing...')

    translate = bash_operator.BashOperator(
        task_id='translate', bash_command='echo translating...')

    review = bash_operator.BashOperator(
        task_id='review', bash_command='echo reviewing...')

    branching = python_operator.BranchPythonOperator(
        task_id='branching',
        python_callable=makeBranchChoice
    )

    start >> print_gcs_info >> segment_asset >> analyse_tm >> translate >> review >> branching

    implement_feedback = dummy_operator.DummyOperator(
        task_id='implement_feedback')

    generate_output = bash_operator.BashOperator(
        task_id='generate_output', bash_command='echo output generated')

    branching >> generate_output
    branching >> implement_feedback >> generate_output
# [END composer_trigger_response_twostep_dag]