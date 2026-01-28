from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import boto3
from io import StringIO, BytesIO
import os
import json

default_args = {
    'owner': 'layman_ai',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def backup_to_csv(**context):
    """Backup database tables to CSV files"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    tables = [
        'youtube_channels_daily',
        'youtube_videos_info', 
        'youtube_videos_daily'
    ]
    
    backup_dir = '/tmp/youtube_backups'
    os.makedirs(backup_dir, exist_ok=True)
    
    backup_files = {}
    
    for table in tables:
        query = f"SELECT * FROM {table}"
        df = hook.get_pandas_df(query)
        
        if not df.empty:
            # Add backup metadata
            df['_backup_timestamp'] = datetime.now().isoformat()
            df['_backup_type'] = 'full'
            
            # Save to CSV
            file_path = f"{backup_dir}/{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(file_path, index=False)
            
            backup_files[table] = {
                'file_path': file_path,
                'row_count': len(df),
                'columns': list(df.columns)
            }
            
            context['ti'].log.info(f"Backed up {len(df)} rows from {table} to {file_path}")
    
    context['ti'].xcom_push(key='backup_files', value=backup_files)
    return backup_files

def backup_to_s3(**context):
    """Upload backup files to AWS S3 (optional)"""
    # This requires AWS credentials setup
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    if not aws_access_key or not aws_secret_key:
        context['ti'].log.info("AWS credentials not found, skipping S3 backup")
        return "Skipped - No AWS credentials"
    
    backup_files = context['ti'].xcom_pull(task_ids='backup_to_csv', key='backup_files')
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name='ap-south-1'  # Mumbai region
    )
    
    bucket_name = 'layman-ai-youtube-backups'
    
    uploaded_files = []
    for table, info in backup_files.items():
        file_path = info['file_path']
        s3_key = f"youtube_analytics/{datetime.now().strftime('%Y/%m/%d')}/{os.path.basename(file_path)}"
        
        try:
            s3_client.upload_file(file_path, bucket_name, s3_key)
            uploaded_files.append({
                'table': table,
                's3_key': s3_key,
                'size': os.path.getsize(file_path)
            })
            context['ti'].log.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            context['ti'].log.error(f"Failed to upload {file_path} to S3: {str(e)}")
    
    return uploaded_files

def cleanup_old_backups(**context):
    """Clean up old backup files (keep last 7 days)"""
    import glob
    import os
    from datetime import datetime, timedelta
    
    backup_dir = '/tmp/youtube_backups'
    
    if not os.path.exists(backup_dir):
        return "No backup directory found"
    
    # Find all CSV files
    csv_files = glob.glob(f"{backup_dir}/*.csv")
    
    cutoff_date = datetime.now() - timedelta(days=7)
    deleted_files = []
    
    for file_path in csv_files:
        file_time = datetime.fromtimestamp(os.path.getctime(file_path))
        
        if file_time < cutoff_date:
            os.remove(file_path)
            deleted_files.append(os.path.basename(file_path))
    
    context['ti'].log.info(f"Deleted {len(deleted_files)} old backup files")
    return f"Deleted {len(deleted_files)} files: {', '.join(deleted_files[:5])}"

def generate_backup_report(**context):
    """Generate backup report"""
    backup_files = context['ti'].xcom_pull(task_ids='backup_to_csv', key='backup_files')
    s3_backup = context['ti'].xcom_pull(task_ids='backup_to_s3')
    cleanup = context['ti'].xcom_pull(task_ids='cleanup_old_backups')
    
    report = {
        'backup_timestamp': datetime.now().isoformat(),
        'local_backup': {
            'tables_backed_up': list(backup_files.keys()) if backup_files else [],
            'total_files': len(backup_files) if backup_files else 0,
            'total_rows': sum(info['row_count'] for info in backup_files.values()) if backup_files else 0
        },
        's3_backup': s3_backup if s3_backup else "Skipped",
        'cleanup': cleanup,
        'status': 'SUCCESS'
    }
    
    # Save report to file
    report_dir = '/tmp/youtube_backups/reports'
    os.makedirs(report_dir, exist_ok=True)
    
    report_file = f"{report_dir}/backup_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    context['ti'].xcom_push(key='backup_report', value=report)
    return report

with DAG(
    dag_id='youtube_data_backup',
    default_args=default_args,
    description='Backup YouTube analytics data',
    schedule_interval='0 0 * * 0',  # Run every Sunday at midnight
    catchup=False,
    tags=['youtube', 'backup', 'layman_ai'],
) as dag:

    start_backup = DummyOperator(
        task_id='start_backup_process',
    )
    
    backup_csv = PythonOperator(
        task_id='backup_to_csv',
        python_callable=backup_to_csv,
        provide_context=True,
    )
    
    backup_s3 = PythonOperator(
        task_id='backup_to_s3',
        python_callable=backup_to_s3,
        provide_context=True,
    )
    
    cleanup = PythonOperator(
        task_id='cleanup_old_backups',
        python_callable=cleanup_old_backups,
        provide_context=True,
    )
    
    generate_report = PythonOperator(
        task_id='generate_backup_report',
        python_callable=generate_backup_report,
        provide_context=True,
    )
    
    end_backup = DummyOperator(
        task_id='end_backup_process',
    )
    
    # Task dependencies
    start_backup >> backup_csv
    backup_csv >> [backup_s3, cleanup]
    [backup_s3, cleanup] >> generate_report >> end_backup