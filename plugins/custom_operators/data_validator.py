from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime

class DataQualityOperator(BaseOperator):
    """
    Custom operator for data quality checks
    """
    
    @apply_defaults
    def __init__(
        self,
        data_key: str,  # XCom key containing data
        checks: List[Dict[str, Any]],
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data_key = data_key
        self.checks = checks
        
    def execute(self, context):
        self.log.info(f"Running data quality checks for {self.data_key}")
        
        # Get data from XCom
        data = context['ti'].xcom_pull(key=self.data_key)
        
        if not data:
            self.log.warning(f"No data found for key: {self.data_key}")
            return []
        
        results = []
        
        for check in self.checks:
            check_type = check.get('type')
            check_name = check.get('name', check_type)
            
            try:
                if check_type == 'not_null':
                    result = self._check_not_null(data, check.get('columns', []))
                elif check_type == 'unique':
                    result = self._check_unique(data, check.get('columns', []))
                elif check_type == 'value_range':
                    result = self._check_value_range(data, check.get('column'), 
                                                    check.get('min'), check.get('max'))
                elif check_type == 'data_type':
                    result = self._check_data_type(data, check.get('column'), 
                                                  check.get('expected_type'))
                elif check_type == 'row_count':
                    result = self._check_row_count(data, check.get('min_rows'), 
                                                  check.get('max_rows'))
                else:
                    result = {'status': 'ERROR', 'message': f'Unknown check type: {check_type}'}
                
                result['check_name'] = check_name
                results.append(result)
                
                if result['status'] == 'FAILED':
                    self.log.error(f"Check failed: {check_name} - {result.get('message')}")
                elif result['status'] == 'PASSED':
                    self.log.info(f"Check passed: {check_name}")
                    
            except Exception as e:
                results.append({
                    'check_name': check_name,
                    'status': 'ERROR',
                    'message': f'Check execution error: {str(e)}'
                })
        
        # Push results to XCom
        context['ti'].xcom_push(key=f'data_quality_{self.data_key}', value=results)
        
        # Check if any critical failures
        critical_failures = [r for r in results if r['status'] == 'FAILED' 
                           and r.get('severity') == 'critical']
        
        if critical_failures:
            raise AirflowException(f"Critical data quality checks failed: {critical_failures}")
        
        return results
    
    def _check_not_null(self, data: List[Dict], columns: List[str]) -> Dict:
        """Check for null values in specified columns"""
        if not columns:
            return {'status': 'SKIPPED', 'message': 'No columns specified'}
        
        df = pd.DataFrame(data)
        failures = []
        
        for column in columns:
            if column not in df.columns:
                failures.append(f"Column '{column}' not found in data")
                continue
                
            null_count = df[column].isnull().sum()
            if null_count > 0:
                failures.append(f"Column '{column}' has {null_count} null values")
        
        if failures:
            return {'status': 'FAILED', 'message': '; '.join(failures)}
        return {'status': 'PASSED', 'message': 'All columns have no null values'}
    
    def _check_unique(self, data: List[Dict], columns: List[str]) -> Dict:
        """Check uniqueness of specified columns"""
        if not columns:
            return {'status': 'SKIPPED', 'message': 'No columns specified'}
        
        df = pd.DataFrame(data)
        
        for column in columns:
            if column not in df.columns:
                return {'status': 'FAILED', 'message': f"Column '{column}' not found"}
        
        duplicates = df[columns].duplicated().sum()
        if duplicates > 0:
            return {'status': 'FAILED', 'message': f'Found {duplicates} duplicate rows'}
        
        return {'status': 'PASSED', 'message': 'All rows are unique'}
    
    def _check_value_range(self, data: List[Dict], column: str, min_val: Any, max_val: Any) -> Dict:
        """Check if column values are within range"""
        df = pd.DataFrame(data)
        
        if column not in df.columns:
            return {'status': 'FAILED', 'message': f"Column '{column}' not found"}
        
        if min_val is not None:
            below_min = (df[column] < min_val).sum()
        else:
            below_min = 0
        
        if max_val is not None:
            above_max = (df[column] > max_val).sum()
        else:
            above_max = 0
        
        if below_min > 0 or above_max > 0:
            message = []
            if below_min > 0:
                message.append(f"{below_min} values below minimum ({min_val})")
            if above_max > 0:
                message.append(f"{above_max} values above maximum ({max_val})")
            return {'status': 'FAILED', 'message': '; '.join(message)}
        
        return {'status': 'PASSED', 'message': 'All values within range'}
    
    def _check_data_type(self, data: List[Dict], column: str, expected_type: str) -> Dict:
        """Check data type of column"""
        df = pd.DataFrame(data)
        
        if column not in df.columns:
            return {'status': 'FAILED', 'message': f"Column '{column}' not found"}
        
        # Simple type checking
        if expected_type == 'numeric':
            non_numeric = pd.to_numeric(df[column], errors='coerce').isnull().sum()
            if non_numeric > 0:
                return {'status': 'FAILED', 'message': f'{non_numeric} non-numeric values found'}
        
        return {'status': 'PASSED', 'message': f'Column type matches {expected_type}'}
    
    def _check_row_count(self, data: List[Dict], min_rows: int = None, max_rows: int = None) -> Dict:
        """Check row count"""
        row_count = len(data)
        
        if min_rows is not None and row_count < min_rows:
            return {'status': 'FAILED', 'message': f'Row count ({row_count}) below minimum ({min_rows})'}
        
        if max_rows is not None and row_count > max_rows:
            return {'status': 'FAILED', 'message': f'Row count ({row_count}) above maximum ({max_rows})'}
        
        return {'status': 'PASSED', 'message': f'Row count ({row_count}) within limits'}