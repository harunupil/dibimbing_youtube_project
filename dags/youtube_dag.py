import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.youtube_pipeline import youtube_pipeline

default_args = {
    'owner': 'Luthfi Arif',
    'start_date': datetime(2026, 1, 1)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_youtube_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['youtube', 'etl', 'pipeline']
)

extract = PythonOperator(
    task_id='youtube_extraction',
    python_callable=youtube_pipeline,
    op_kwargs={
        'file_name': f'youtube_data_{file_postfix}',
        'queries': [
            'Data Engineer',
            'Finance and Accounting',
            'DevOps Engineer',
            'FullStack Developer',
            'Supply Chain Management',
            'UI/UX'
        ], #List of Subject to Queries
        'max_results': 10, #How Many Videos per Queries
        'max_comments': 50 #How Many Sampled Comments
    },
    dag=dag
)

# upload to s3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

extract >> upload_s3
