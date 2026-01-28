import pytest
import os
import sys
from datetime import datetime
from airflow.models import DagBag

# Add project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def test_dag_import():
    """Test that DAG files can be imported without errors"""
    dag_files = [
        'dags/youtube_etl.py',
        'dags/data_quality_dag.py',
        'dags/backup_dag.py'
    ]
    
    for dag_file in dag_files:
        dagbag = DagBag(dag_folder=os.path.dirname(dag_file), include_examples=False)
        assert dagbag.import_errors == {}, f"Import errors in {dag_file}: {dagbag.import_errors}"
        
        # Check each DAG has at least one task
        for dag_id, dag in dagbag.dags.items():
            assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"
            assert dag.schedule_interval is not None, f"DAG {dag_id} has no schedule_interval"
            assert dag.default_args is not None, f"DAG {dag_id} has no default_args"

def test_dag_structure():
    """Test DAG structure and task dependencies"""
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    
    # Test main ETL DAG
    etl_dag = dagbag.get_dag('youtube_analytics_etl')
    assert etl_dag is not None, "youtube_analytics_etl DAG not found"
    
    # Check required tasks exist
    required_tasks = [
        'start_pipeline',
        'extract_channel_statistics',
        'extract_channel_videos',
        'transform_channel_data',
        'load_channel_data_to_db',
        'end_pipeline'
    ]
    
    for task_id in required_tasks:
        assert task_id in etl_dag.task_ids, f"Task {task_id} not found in ETL DAG"
    
    # Test data quality DAG
    quality_dag = dagbag.get_dag('youtube_data_quality_checks')
    assert quality_dag is not None, "youtube_data_quality_checks DAG not found"
    
    # Test backup DAG
    backup_dag = dagbag.get_dag('youtube_data_backup')
    assert backup_dag is not None, "youtube_data_backup DAG not found"

def test_dag_default_args():
    """Test DAG default arguments"""
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    
    etl_dag = dagbag.get_dag('youtube_analytics_etl')
    
    # Check default args
    assert 'owner' in etl_dag.default_args
    assert 'retries' in etl_dag.default_args
    assert 'start_date' in etl_dag.default_args
    assert etl_dag.default_args['retries'] == 3
    
    # Check tags
    assert 'layman_ai' in etl_dag.tags
    assert 'youtube' in etl_dag.tags

def test_dag_schedule():
    """Test DAG schedules"""
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    
    etl_dag = dagbag.get_dag('youtube_analytics_etl')
    quality_dag = dagbag.get_dag('youtube_data_quality_checks')
    backup_dag = dagbag.get_dag('youtube_data_backup')
    
    # Check schedules
    assert etl_dag.schedule_interval == '0 6 * * *'  # Daily at 6 AM
    assert quality_dag.schedule_interval == '0 7 * * *'  # Daily at 7 AM
    assert backup_dag.schedule_interval == '0 0 * * 0'  # Weekly on Sunday

def test_no_import_errors():
    """Test that there are no import errors in any DAG"""
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    
    assert len(dagbag.import_errors) == 0, \
        f"DAG import errors: {dagbag.import_errors}"
    
    # Check all DAGs are loaded
    expected_dags = [
        'youtube_analytics_etl',
        'youtube_data_quality_checks',
        'youtube_data_backup'
    ]
    
    for dag_id in expected_dags:
        assert dag_id in dagbag.dags, f"DAG {dag_id} not loaded"

if __name__ == '__main__':
    # Run tests
    test_dag_import()
    test_dag_structure()
    test_dag_default_args()
    test_dag_schedule()
    test_no_import_errors()
    print("All tests passed! ✅")