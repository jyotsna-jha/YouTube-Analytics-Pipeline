from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'layman_ai',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def check_data_completeness(**context):
    """Check if all expected data is present"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    checks = []
    
    # Check channel data for today
    query = """
        SELECT COUNT(*) as count 
        FROM youtube_channels_daily 
        WHERE date = CURRENT_DATE
        AND channel_id = %s
    """
    
    result = hook.get_first(query, parameters=(context['params']['channel_id'],))
    if result and result[0] > 0:
        checks.append({'check': 'channel_data_today', 'status': 'PASSED', 'count': result[0]})
    else:
        checks.append({'check': 'channel_data_today', 'status': 'FAILED', 'message': 'No channel data for today'})
    
    # Check video data for today
    query = """
        SELECT COUNT(DISTINCT video_id) as count 
        FROM youtube_videos_daily 
        WHERE date = CURRENT_DATE
        AND video_id IN (
            SELECT video_id FROM youtube_videos_info 
            WHERE channel_id = %s
        )
    """
    
    result = hook.get_first(query, parameters=(context['params']['channel_id'],))
    if result and result[0] > 0:
        checks.append({'check': 'video_data_today', 'status': 'PASSED', 'count': result[0]})
    else:
        checks.append({'check': 'video_data_today', 'status': 'FAILED', 'message': 'No video data for today'})
    
    # Check for null values in critical columns
    query = """
        SELECT 
            SUM(CASE WHEN subscriber_count IS NULL THEN 1 ELSE 0 END) as null_subscribers,
            SUM(CASE WHEN view_count IS NULL THEN 1 ELSE 0 END) as null_views
        FROM youtube_channels_daily 
        WHERE channel_id = %s
        AND date >= CURRENT_DATE - INTERVAL '7 days'
    """
    
    result = hook.get_first(query, parameters=(context['params']['channel_id'],))
    if result and result[0] == 0 and result[1] == 0:
        checks.append({'check': 'no_null_critical_values', 'status': 'PASSED'})
    else:
        checks.append({
            'check': 'no_null_critical_values', 
            'status': 'FAILED', 
            'message': f'Found {result[0]} null subscribers, {result[1]} null views in last 7 days'
        })
    
    context['ti'].xcom_push(key='completeness_checks', value=checks)
    
    # Determine overall status
    failed_checks = [c for c in checks if c['status'] == 'FAILED']
    return {
        'total_checks': len(checks),
        'passed_checks': len(checks) - len(failed_checks),
        'failed_checks': len(failed_checks),
        'checks': checks
    }

def check_data_consistency(**context):
    """Check data consistency across tables"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    checks = []
    
    # Check if video count matches between tables
    query = """
        SELECT 
            (SELECT video_count FROM youtube_channels_daily 
             WHERE channel_id = %s AND date = CURRENT_DATE 
             ORDER BY date DESC LIMIT 1) as channel_video_count,
            (SELECT COUNT(*) FROM youtube_videos_info 
             WHERE channel_id = %s) as actual_video_count
    """
    
    result = hook.get_first(query, parameters=(
        context['params']['channel_id'], 
        context['params']['channel_id']
    ))
    
    if result and result[0] and result[1]:
        channel_count = result[0] or 0
        actual_count = result[1] or 0
        
        # Allow 10% variance
        if abs(channel_count - actual_count) / max(channel_count, 1) <= 0.1:
            checks.append({
                'check': 'video_count_consistency', 
                'status': 'PASSED',
                'channel_count': channel_count,
                'actual_count': actual_count
            })
        else:
            checks.append({
                'check': 'video_count_consistency', 
                'status': 'WARNING',
                'message': f'Video count mismatch: channel says {channel_count}, actual is {actual_count}'
            })
    
    # Check for decreasing subscriber count (shouldn't happen)
    query = """
        SELECT date, subscriber_count
        FROM youtube_channels_daily 
        WHERE channel_id = %s
        ORDER BY date DESC
        LIMIT 2
    """
    
    results = hook.get_records(query, parameters=(context['params']['channel_id'],))
    if len(results) >= 2:
        today_count = results[0][1]
        yesterday_count = results[1][1]
        
        if today_count >= yesterday_count:
            checks.append({
                'check': 'subscriber_growth_consistent',
                'status': 'PASSED',
                'growth': today_count - yesterday_count
            })
        else:
            checks.append({
                'check': 'subscriber_growth_consistent',
                'status': 'FAILED',
                'message': f'Subscribers decreased from {yesterday_count} to {today_count}'
            })
    
    context['ti'].xcom_push(key='consistency_checks', value=checks)
    return checks

def generate_quality_report(**context):
    """Generate comprehensive data quality report"""
    completeness = context['ti'].xcom_pull(task_ids='check_data_completeness', key='completeness_checks')
    consistency = context['ti'].xcom_pull(task_ids='check_data_consistency', key='consistency_checks')
    
    all_checks = (completeness or []) + (consistency or [])
    
    report = {
        'report_date': datetime.now().isoformat(),
        'channel_id': context['params']['channel_id'],
        'total_checks': len(all_checks),
        'passed_checks': len([c for c in all_checks if c['status'] == 'PASSED']),
        'failed_checks': len([c for c in all_checks if c['status'] == 'FAILED']),
        'warning_checks': len([c for c in all_checks if c['status'] == 'WARNING']),
        'checks': all_checks,
        'overall_status': 'PASSED' if len([c for c in all_checks if c['status'] == 'FAILED']) == 0 else 'FAILED'
    }
    
    context['ti'].xcom_push(key='quality_report', value=report)
    return report

with DAG(
    dag_id='youtube_data_quality_checks',
    default_args=default_args,
    description='Data quality checks for YouTube analytics',
    schedule_interval='0 7 * * *',  # Run at 7 AM, after main ETL
    catchup=False,
    tags=['youtube', 'data-quality', 'layman_ai'],
    params={'channel_id': CHANNEL_ID},
) as dag:

    start_checks = DummyOperator(
        task_id='start_data_quality_checks',
    )
    
    check_completeness = PythonOperator(
        task_id='check_data_completeness',
        python_callable=check_data_completeness,
        provide_context=True,
    )
    
    check_consistency = PythonOperator(
        task_id='check_data_consistency',
        python_callable=check_data_consistency,
        provide_context=True,
    )
    
    generate_report = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
        provide_context=True,
    )
    
    send_quality_alert = EmailOperator(
        task_id='send_quality_alert_if_needed',
        to=['data-team@laymanai.com'],
        subject='YouTube Data Quality Report - {{ ds }}',
        html_content="""
            <h2>🔍 YouTube Data Quality Report</h2>
            <p><strong>Date:</strong> {{ ds }}</p>
            <p><strong>Overall Status:</strong> {{ task_instance.xcom_pull(task_ids='generate_quality_report').overall_status }}</p>
            
            <h3>Check Summary</h3>
            <ul>
                <li>Total Checks: {{ task_instance.xcom_pull(task_ids='generate_quality_report').total_checks }}</li>
                <li>Passed: {{ task_instance.xcom_pull(task_ids='generate_quality_report').passed_checks }}</li>
                <li>Failed: {{ task_instance.xcom_pull(task_ids='generate_quality_report').failed_checks }}</li>
                <li>Warnings: {{ task_instance.xcom_pull(task_ids='generate_quality_report').warning_checks }}</li>
            </ul>
            
            <h3>Failed Checks:</h3>
            {% set report = task_instance.xcom_pull(task_ids='generate_quality_report') %}
            {% if report.failed_checks > 0 %}
                <ul>
                {% for check in report.checks %}
                    {% if check.status == 'FAILED' %}
                        <li><strong>{{ check.check }}:</strong> {{ check.message }}</li>
                    {% endif %}
                {% endfor %}
                </ul>
            {% else %}
                <p>No failed checks! ✅</p>
            {% endif %}
            
            <p>Check Airflow logs for more details.</p>
        """,
        trigger_rule='all_done',  # Send email regardless of success/failure
    )
    
    end_checks = DummyOperator(
        task_id='end_data_quality_checks',
    )
    
    # Task dependencies
    start_checks >> [check_completeness, check_consistency]
    [check_completeness, check_consistency] >> generate_report
    generate_report >> send_quality_alert >> end_checks