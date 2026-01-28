from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import requests
import time
import logging
from typing import Dict, List, Optional
import yaml
import os

class YouTubeHook(BaseHook):
    """
    Custom Hook for YouTube Data API v3
    """
    
    def __init__(self, youtube_conn_id: str = 'youtube_default'):
        super().__init__()
        self.conn_id = youtube_conn_id
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self._load_config()
        
    def _load_config(self):
        """Load configuration from YAML file"""
        config_path = os.path.join(os.path.dirname(__file__), '../../scripts/config/api_config.yaml')
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Try to get API key from connection first, then config, then env
        try:
            conn = self.get_connection(self.conn_id)
            self.api_key = conn.extra_dejson.get('api_key')
        except:
            self.api_key = self.config.get('youtube', {}).get('api_key') or os.environ.get('YOUTUBE_API_KEY')
            
        if not self.api_key:
            raise AirflowException("YouTube API key not found in connection, config, or environment")
        
        self.channel_id = self.config.get('youtube', {}).get('channel_id') or os.environ.get('LAYMAN_AI_CHANNEL_ID')
        
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """Make API request with error handling and rate limiting"""
        url = f"{self.base_url}{endpoint}"
        params['key'] = self.api_key
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Rate limit
                    wait_time = 2 ** attempt  # Exponential backoff
                    self.log.info(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 403:
                    error_msg = response.json().get('error', {}).get('message', 'Unknown error')
                    raise AirflowException(f"YouTube API Error (403): {error_msg}")
                else:
                    raise AirflowException(f"HTTP Error {response.status_code}: {str(e)}")
                    
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise AirflowException(f"Failed after {max_retries} attempts: {str(e)}")
                time.sleep(1)
                
        raise AirflowException("Max retries exceeded")
    
    def get_channel_statistics(self, channel_id: str = None) -> Dict:
        """Get channel statistics"""
        channel_id = channel_id or self.channel_id
        if not channel_id:
            raise AirflowException("Channel ID not provided")
            
        params = {
            'part': 'snippet,statistics,contentDetails,brandingSettings',
            'id': channel_id
        }
        
        data = self._make_request('/channels', params)
        return data.get('items', [{}])[0] if data.get('items') else {}
    
    def get_channel_videos(self, channel_id: str = None, max_results: int = 50) -> List[Dict]:
        """Get all videos from a channel"""
        channel_id = channel_id or self.channel_id
        if not channel_id:
            raise AirflowException("Channel ID not provided")
            
        # First get uploads playlist ID
        channel_data = self.get_channel_statistics(channel_id)
        uploads_playlist_id = channel_data.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads')
        
        if not uploads_playlist_id:
            return []
        
        # Get videos from uploads playlist
        params = {
            'part': 'snippet,contentDetails',
            'playlistId': uploads_playlist_id,
            'maxResults': max_results
        }
        
        data = self._make_request('/playlistItems', params)
        return data.get('items', [])
    
    def get_video_statistics(self, video_ids: List[str]) -> List[Dict]:
        """Get statistics for specific videos"""
        if not video_ids:
            return []
            
        params = {
            'part': 'snippet,statistics,contentDetails',
            'id': ','.join(video_ids[:50])  # API limit: 50 videos per request
        }
        
        data = self._make_request('/videos', params)
        return data.get('items', [])
    
    def search_videos(self, query: str, max_results: int = 10) -> List[Dict]:
        """Search for videos"""
        params = {
            'part': 'snippet',
            'q': query,
            'type': 'video',
            'maxResults': max_results,
            'order': 'viewCount'  # Most viewed first
        }
        
        data = self._make_request('/search', params)
        return data.get('items', [])
    
    def get_trending_videos(self, region_code: str = 'IN', max_results: int = 20) -> List[Dict]:
        """Get trending videos for a region"""
        params = {
            'part': 'snippet,statistics,contentDetails',
            'chart': 'mostPopular',
            'regionCode': region_code,
            'maxResults': max_results
        }
        
        data = self._make_request('/videos', params)
        return data.get('items', [])