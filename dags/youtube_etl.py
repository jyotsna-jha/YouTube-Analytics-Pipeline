from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from plugins.custom_operators.youtube_extractor import YouTubeExtractOperator
from plugins.custom_operators.data_validator import DataQualityOperator
import pandas as pd
import json
import os

default_args = {
    'owner': 'layman_ai',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['team@laymanai.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Get configuration
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '../scripts/config/api_config.yaml')
    with open(config_path, 'r') as file:
        import yaml
        return yaml.safe_load(file)

config = load_config()
CHANNEL_ID = config['youtube']['channel_id']

with DAG(
    dag_id='youtube_analytics_etl',
    default_args=default_args,
    description='ETL pipeline for YouTube analytics of The Layman AI',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    catchup=False,
    tags=['youtube', 'analytics', 'layman_ai', 'etl'],
    max_active_runs=1,
) as dag:

    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag,
    )

    # ========== EXTRACTION TASKS ==========
    
    extract_channel_stats = YouTubeExtractOperator(
        task_id='extract_channel_statistics',
        task_type='channel_stats',
        channel_id=CHANNEL_ID,
        youtube_conn_id='youtube_default',
        dag=dag,
    )
    
    extract_channel_videos = YouTubeExtractOperator(
        task_id='extract_channel_videos',
        task_type='channel_videos',
        channel_id=CHANNEL_ID,
        max_results=100,
        youtube_conn_id='youtube_default',
        dag=dag,
    )
    
    extract_trending_videos = YouTubeExtractOperator(
        task_id='extract_trending_videos',
        task_type='trending',
        region_code='IN',
        max_results=50,
        youtube_conn_id='youtube_default',
        dag=dag,
    )
    
    # ========== DATA QUALITY CHECKS ==========
    
    check_channel_data_quality = DataQualityOperator(
        task_id='check_channel_data_quality',
        data_key='youtube_channel_stats',
        checks=[
            {
                'name': 'channel_id_not_null',
                'type': 'not_null',
                'columns': ['channel_id', 'channel_name'],
                'severity': 'critical'
            },
            {
                'name': 'counts_non_negative',
                'type': 'value_range',
                'column': 'subscriber_count',
                'min': 0,
                'severity': 'critical'
            },
            {
                'name': 'row_count_check',
                'type': 'row_count',
                'min_rows': 1,
                'max_rows': 1,
                'severity': 'critical'
            }
        ],
        dag=dag,
    )
    
    check_videos_data_quality = DataQualityOperator(
        task_id='check_videos_data_quality',
        data_key='youtube_channel_videos',
        checks=[
            {
                'name': 'video_id_not_null',
                'type': 'not_null',
                'columns': ['video_id', 'video_title'],
                'severity': 'critical'
            },
            {
                'name': 'view_count_non_negative',
                'type': 'value_range',
                'column': 'view_count',
                'min': 0,
                'severity': 'warning'
            },
            {
                'name': 'row_count_reasonable',
                'type': 'row_count',
                'min_rows': 1,
                'severity': 'critical'
            }
        ],
        dag=dag,
    )
    
    # ========== TRANSFORMATION TASKS ==========
    
    def transform_channel_data(**context):
        """Transform channel data for database loading"""
        channel_data = context['ti'].xcom_pull(task_ids='extract_channel_statistics', key='youtube_channel_stats')
        
        # Add calculated fields
        channel_data['growth_rate'] = None  # Will be calculated in SQL
        channel_data['extraction_date'] = datetime.now().date().isoformat()
        
        # Prepare for database insertion
        transformed = {
            'channel_id': channel_data['channel_id'],
            'channel_name': channel_data['channel_name'],
            'subscriber_count': channel_data['subscriber_count'],
            'view_count': channel_data['view_count'],
            'video_count': channel_data['video_count'],
            'date': datetime.now().date().isoformat()
        }
        
        context['ti'].xcom_push(key='transformed_channel_data', value=transformed)
        return transformed
    
    transform_channel = PythonOperator(
        task_id='transform_channel_data',
        python_callable=transform_channel_data,
        dag=dag,
    )
    
    def transform_videos_data(**context):
        """Transform videos data for database loading"""
        videos_data = context['ti'].xcom_pull(task_ids='extract_channel_videos', key='youtube_channel_videos')
        
        transformed_videos = []
        for video in videos_data:
            transformed = {
                'video_id': video['video_id'],
                'channel_id': video['channel_id'],
                'video_title': video['video_title'].replace("'", "''")[:500],  # Clean for SQL
                'description': video['description'][:1000] if video['description'] else '',
                'published_at': video['published_at'],
                'duration': video['duration'],
                'category_id': video['category_id'],
                'tags': video['tags'][:5] if video['tags'] else [],  # Limit tags
                'date': datetime.now().date().isoformat()
            }
            
            # Daily statistics
            stats = {
                'video_id': video['video_id'],
                'date': datetime.now().date().isoformat(),
                'view_count': video['view_count'],
                'like_count': video['like_count'],
                'comment_count': video['comment_count'],
                'estimated_minutes_watched': video.get('view_count', 0) * 5  # Estimate: 5 min per view
            }
            
            transformed_videos.append({
                'video_info': transformed,
                'video_stats': stats
            })
        
        context['ti'].xcom_push(key='transformed_videos_data', value=transformed_videos)
        return transformed_videos
    
    transform_videos = PythonOperator(
        task_id='transform_videos_data',
        python_callable=transform_videos_data,
        dag=dag,
    )
    
    # ========== DATABASE LOADING TASKS ==========
    
    create_tables_if_not_exists = PostgresOperator(
        task_id='create_tables_if_not_exists',
        postgres_conn_id='postgres_default',
        sql='''
            CREATE TABLE IF NOT EXISTS youtube_channels_daily (
                channel_id VARCHAR(50),
                date DATE,
                subscriber_count INTEGER,
                view_count BIGINT,
                video_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (channel_id, date)
            );
            
            CREATE TABLE IF NOT EXISTS youtube_videos_info (
                video_id VARCHAR(50) PRIMARY KEY,
                channel_id VARCHAR(50),
                video_title TEXT,
                description TEXT,
                published_at TIMESTAMP,
                duration VARCHAR(20),
                category_id INTEGER,
                tags TEXT[],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS youtube_videos_daily (
                id SERIAL PRIMARY KEY,
                video_id VARCHAR(50),
                date DATE,
                view_count INTEGER,
                like_count INTEGER,
                comment_count INTEGER,
                estimated_minutes_watched BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (video_id) REFERENCES youtube_videos_info(video_id),
                UNIQUE(video_id, date)
            );
        ''',
        dag=dag,
    )
    
    def load_channel_data_to_db(**context):
        """Load channel data to PostgreSQL"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        channel_data = context['ti'].xcom_pull(task_ids='transform_channel_data', key='transformed_channel_data')
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        insert_query = """
            INSERT INTO youtube_channels_daily 
            (channel_id, date, subscriber_count, view_count, video_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (channel_id, date) 
            DO UPDATE SET 
                subscriber_count = EXCLUDED.subscriber_count,
                view_count = EXCLUDED.view_count,
                video_count = EXCLUDED.video_count;
        """
        
        cursor.execute(insert_query, (
            channel_data['channel_id'],
            channel_data['date'],
            channel_data['subscriber_count'],
            channel_data['view_count'],
            channel_data['video_count']
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return f"Loaded channel data for {channel_data['channel_id']}"
    
    load_channel_db = PythonOperator(
        task_id='load_channel_data_to_db',
        python_callable=load_channel_data_to_db,
        dag=dag,
    )
    
    def load_videos_data_to_db(**context):
        """Load videos data to PostgreSQL"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        videos_data = context['ti'].xcom_pull(task_ids='transform_videos_data', key='transformed_videos_data')
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        # Load video info
        video_info_query = """
            INSERT INTO youtube_videos_info 
            (video_id, channel_id, video_title, description, published_at, duration, category_id, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) 
            DO UPDATE SET 
                video_title = EXCLUDED.video_title,
                description = EXCLUDED.description,
                duration = EXCLUDED.duration,
                category_id = EXCLUDED.category_id,
                tags = EXCLUDED.tags,
                updated_at = CURRENT_TIMESTAMP;
        """
        
        # Load daily statistics
        video_stats_query = """
            INSERT INTO youtube_videos_daily 
            (video_id, date, view_count, like_count, comment_count, estimated_minutes_watched)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id, date) 
            DO UPDATE SET 
                view_count = EXCLUDED.view_count,
                like_count = EXCLUDED.like_count,
                comment_count = EXCLUDED.comment_count,
                estimated_minutes_watched = EXCLUDED.estimated_minutes_watched;
        """
        
        for video_data in videos_data:
            video_info = video_data['video_info']
            video_stats = video_data['video_stats']
            
            # Insert video info
            cursor.execute(video_info_query, (
                video_info['video_id'],
                video_info['channel_id'],
                video_info['video_title'],
                video_info['description'],
                video_info['published_at'],
                video_info['duration'],
                video_info['category_id'],
                video_info['tags']
            ))
            
            # Insert daily stats
            cursor.execute(video_stats_query, (
                video_stats['video_id'],
                video_stats['date'],
                video_stats['view_count'],
                video_stats['like_count'],
                video_stats['comment_count'],
                video_stats['estimated_minutes_watched']
            ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return f"Loaded {len(videos_data)} videos"
    
    load_videos_db = PythonOperator(
        task_id='load_videos_data_to_db',
        python_callable=load_videos_data_to_db,
        dag=dag,
    )
    
    # ========== ANALYTICS & REPORTING ==========
    
    generate_daily_report = PostgresOperator(
        task_id='generate_daily_report',
        postgres_conn_id='postgres_default',
        sql='''
            -- Calculate daily growth
            WITH daily_growth AS (
                SELECT 
                    date,
                    subscriber_count,
                    subscriber_count - LAG(subscriber_count) OVER (ORDER BY date) as new_subscribers_today,
                    ROUND((subscriber_count::DECIMAL / LAG(subscriber_count) OVER (ORDER BY date) - 1) * 100, 2) as growth_rate_percent
                FROM youtube_channels_daily 
                WHERE channel_id = '{{ params.channel_id }}'
                ORDER BY date DESC
                LIMIT 7
            ),
            -- Top videos
            top_videos AS (
                SELECT 
                    v.video_title,
                    SUM(vd.view_count) as total_views,
                    SUM(vd.like_count) as total_likes,
                    ROUND(SUM(vd.like_count)::DECIMAL / NULLIF(SUM(vd.view_count), 0) * 100, 2) as engagement_rate
                FROM youtube_videos_daily vd
                JOIN youtube_videos_info v ON vd.video_id = v.video_id
                WHERE v.channel_id = '{{ params.channel_id }}'
                AND vd.date = CURRENT_DATE - INTERVAL '1 day'
                GROUP BY v.video_id, v.video_title
                ORDER BY total_views DESC
                LIMIT 5
            )
            SELECT 
                'Daily Report - The Layman AI' as report_title,
                CURRENT_DATE as report_date,
                (SELECT subscriber_count FROM daily_growth ORDER BY date DESC LIMIT 1) as current_subscribers,
                (SELECT new_subscribers_today FROM daily_growth ORDER BY date DESC LIMIT 1) as new_subscribers_today,
                (SELECT growth_rate_percent FROM daily_growth ORDER BY date DESC LIMIT 1) as growth_rate,
                (SELECT json_agg(row_to_json(top_videos)) FROM top_videos) as top_performing_videos;
        ''',
        params={'channel_id': CHANNEL_ID},
        dag=dag,
    )
    
    send_daily_email = EmailOperator(
        task_id='send_daily_email_report',
        to=['team@laymanai.com'],
        subject='YouTube Analytics Daily Report - The Layman AI',
        html_content="""
            <h2>📊 YouTube Analytics Daily Report</h2>
            <p>Date: {{ ds }}</p>
            
            <h3>Channel Performance</h3>
            <p>The daily ETL pipeline has completed successfully.</p>
            
            <h4>Next Steps:</h4>
            <ul>
                <li>Check Airflow UI for detailed logs</li>
                <li>View dashboards for visual analytics</li>
                <li>Review data quality reports</li>
            </ul>
            
            <p>Best regards,<br>
            Airflow Analytics Team<br>
            The Layman AI</p>
        """,
        dag=dag,
    )
    
    # ========== PIPELINE COMPLETION ==========
    
    end_pipeline = DummyOperator(
        task_id='end_pipeline',
        dag=dag,
    )
    
    # ========== TASK DEPENDENCIES ==========
    
    start_pipeline >> [extract_channel_stats, extract_channel_videos, extract_trending_videos]
    
    extract_channel_stats >> check_channel_data_quality >> transform_channel
    extract_channel_videos >> check_videos_data_quality >> transform_videos
    
    [transform_channel, transform_videos] >> create_tables_if_not_exists
    
    create_tables_if_not_exists >> [load_channel_db, load_videos_db]
    
    [load_channel_db, load_videos_db] >> generate_daily_report >> send_daily_email >> end_pipeline