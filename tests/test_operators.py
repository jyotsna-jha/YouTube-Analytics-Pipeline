import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../plugins/custom_operators'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../plugins/hooks'))

from youtube_extractor import YouTubeExtractOperator
from data_validator import DataQualityOperator
from youtube_hook import YouTubeHook

class TestYouTubeHook:
    
    @patch('youtube_hook.requests.get')
    def test_make_request_success(self, mock_get):
        """Test successful API request"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'items': [{'id': 'test'}]}
        mock_get.return_value = mock_response
        
        hook = YouTubeHook()
        hook.api_key = 'test_key'
        
        result = hook._make_request('/test', {})
        
        assert result == {'items': [{'id': 'test'}]}
        mock_get.assert_called_once()
    
    @patch('youtube_hook.requests.get')
    def test_make_request_rate_limit(self, mock_get):
        """Test handling of rate limiting"""
        mock_response = Mock()
        mock_response.status_code = 429  # Rate limit
        mock_get.return_value = mock_response
        
        hook = YouTubeHook()
        hook.api_key = 'test_key'
        
        with pytest.raises(Exception):
            hook._make_request('/test', {})

class TestYouTubeExtractOperator:
    
    def setup_method(self):
        """Setup test environment"""
        self.operator = YouTubeExtractOperator(
            task_id='test_task',
            task_type='channel_stats',
            channel_id='test_channel'
        )
    
    @patch('youtube_extractor.YouTubeHook')
    def test_extract_channel_stats(self, mock_hook):
        """Test channel stats extraction"""
        mock_instance = Mock()
        mock_instance.get_channel_statistics.return_value = {
            'id': 'test_channel',
            'snippet': {'title': 'Test Channel'},
            'statistics': {'subscriberCount': '1000'}
        }
        mock_instance.channel_id = 'test_channel'
        mock_hook.return_value = mock_instance
        
        # Mock context
        context = {'ti': Mock()}
        context['ti'].xcom_push = Mock()
        
        result = self.operator.execute(context)
        
        assert 'channel_id' in result
        assert result['channel_id'] == 'test_channel'
        assert result['subscriber_count'] == 1000
        
        mock_instance.get_channel_statistics.assert_called_once_with('test_channel')
        context['ti'].xcom_push.assert_called_once()

class TestDataQualityOperator:
    
    def setup_method(self):
        """Setup test environment"""
        self.operator = DataQualityOperator(
            task_id='test_task',
            data_key='test_data',
            checks=[
                {
                    'name': 'test_check',
                    'type': 'not_null',
                    'columns': ['id']
                }
            ]
        )
    
    def test_check_not_null_pass(self):
        """Test not null check passing"""
        data = [{'id': 1, 'name': 'test'}]
        
        result = self.operator._check_not_null(data, ['id', 'name'])
        
        assert result['status'] == 'PASSED'
    
    def test_check_not_null_fail(self):
        """Test not null check failing"""
        data = [{'id': 1, 'name': None}]
        
        result = self.operator._check_not_null(data, ['id', 'name'])
        
        assert result['status'] == 'FAILED'
        assert 'null values' in result['message']
    
    def test_check_unique_pass(self):
        """Test unique check passing"""
        data = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]
        
        result = self.operator._check_unique(data, ['id'])
        
        assert result['status'] == 'PASSED'
    
    def test_check_unique_fail(self):
        """Test unique check failing"""
        data = [
            {'id': 1, 'name': 'test1'},
            {'id': 1, 'name': 'test2'}  # Duplicate ID
        ]
        
        result = self.operator._check_unique(data, ['id'])
        
        assert result['status'] == 'FAILED'
        assert 'duplicate' in result['message']
    
    def test_check_value_range_pass(self):
        """Test value range check passing"""
        data = [{'value': 5}, {'value': 10}, {'value': 15}]
        
        result = self.operator._check_value_range(data, 'value', 0, 20)
        
        assert result['status'] == 'PASSED'
    
    def test_check_value_range_fail(self):
        """Test value range check failing"""
        data = [{'value': -5}, {'value': 25}]
        
        result = self.operator._check_value_range(data, 'value', 0, 20)
        
        assert result['status'] == 'FAILED'
    
    def test_check_row_count_pass(self):
        """Test row count check passing"""
        data = [{'id': i} for i in range(10)]
        
        result = self.operator._check_row_count(data, min_rows=5, max_rows=15)
        
        assert result['status'] == 'PASSED'
    
    def test_check_row_count_fail_min(self):
        """Test row count check failing on minimum"""
        data = [{'id': i} for i in range(3)]
        
        result = self.operator._check_row_count(data, min_rows=5)
        
        assert result['status'] == 'FAILED'
    
    def test_check_row_count_fail_max(self):
        """Test row count check failing on maximum"""
        data = [{'id': i} for i in range(20)]
        
        result = self.operator._check_row_count(data, max_rows=15)
        
        assert result['status'] == 'FAILED'

def test_operator_execution():
    """Test operator execution with mock context"""
    operator = DataQualityOperator(
        task_id='test_task',
        data_key='test_data',
        checks=[
            {
                'name': 'basic_check',
                'type': 'row_count',
                'min_rows': 1
            }
        ]
    )
    
    # Mock context
    context = {'ti': Mock()}
    context['ti'].xcom_pull.return_value = [{'id': 1}]
    context['ti'].xcom_push = Mock()
    
    # Mock the parent execute method to test our checks directly
    with patch.object(DataQualityOperator, '_check_row_count') as mock_check:
        mock_check.return_value = {'status': 'PASSED', 'message': 'OK'}
        
        result = operator.execute(context)
        
        assert result is not None
        context['ti'].xcom_push.assert_called()

if __name__ == '__main__':
    # Run tests
    test_operator_execution()
    print("Operator tests passed! ✅")