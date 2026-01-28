from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.youtube_hook import YouTubeHook
from typing import Dict, List, Optional
import json
import pandas as pd
from datetime import datetime, timedelta

class YouTubeExtractOperator(BaseOperator):
    """
    Custom operator to extract YouTube data
    """
    
    @apply_defaults
    def __init__(
        self,
        task_type: str = 'channel_stats',  # 'channel_stats', 'videos', 'trending', 'search'
        channel_id: Optional[str] = None,
        query: Optional[str] = None,
        region_code: str = 'IN',
        max_results: int = 50,
        youtube_conn_id: str = 'youtube_default',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.task_type = task_type
        self.channel_id = channel_id
        self.query = query
        self.region_code = region_code
        self.max_results = max_results
        self.youtube_conn_id = youtube_conn_id
        
    def execute(self, context):
        self.log.info(f"Starting YouTube extraction for task type: {self.task_type}")
        
        hook = YouTubeHook(youtube_conn_id=self.youtube_conn_id)
        
        if self.task_type == 'channel_stats':
            data = self._extract_channel_stats(hook)
        elif self.task_type == 'channel_videos':
            data = self._extract_channel_videos(hook)
        elif self.task_type == 'trending':
            data = self._extract_trending_videos(hook)
        elif self.task_type == 'search':
            data = self._extract_search_results(hook)
        else:
            raise ValueError(f"Unknown task type: {self.task_type}")
        
        # Push data to XCom for next tasks
        context['ti'].xcom_push(key=f'youtube_{self.task_type}', value=data)
        
        self.log.info(f"Extracted {len(data) if isinstance(data, list) else 1} records")
        return data
    
    def _extract_channel_stats(self, hook: YouTubeHook) -> Dict:
        """Extract channel statistics"""
        channel_id = self.channel_id or hook.channel_id
        data = hook.get_channel_statistics(channel_id)
        
        # Transform to flat structure
        transformed = {
            'channel_id': data.get('id'),
            'channel_name': data.get('snippet', {}).get('title'),
            'description': data.get('snippet', {}).get('description'),
            'published_at': data.get('snippet', {}).get('publishedAt'),
            'country': data.get('snippet', {}).get('country'),
            'subscriber_count': int(data.get('statistics', {}).get('subscriberCount', 0)),
            'view_count': int(data.get('statistics', {}).get('viewCount', 0)),
            'video_count': int(data.get('statistics', {}).get('videoCount', 0)),
            'uploads_playlist_id': data.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads'),
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        return transformed
    
    def _extract_channel_videos(self, hook: YouTubeHook) -> List[Dict]:
        """Extract all videos from channel"""
        channel_id = self.channel_id or hook.channel_id
        videos = hook.get_channel_videos(channel_id, self.max_results)
        
        # Get video IDs for detailed stats
        video_ids = [video['snippet']['resourceId']['videoId'] for video in videos]
        video_details = hook.get_video_statistics(video_ids)
        
        # Create mapping of video ID to details
        details_map = {video['id']: video for video in video_details}
        
        transformed_videos = []
        for video in videos:
            video_id = video['snippet']['resourceId']['videoId']
            details = details_map.get(video_id, {})
            
            transformed = {
                'video_id': video_id,
                'channel_id': channel_id,
                'video_title': video['snippet'].get('title'),
                'description': video['snippet'].get('description'),
                'published_at': video['snippet'].get('publishedAt'),
                'thumbnails': json.dumps(video['snippet'].get('thumbnails', {})),
                'duration': details.get('contentDetails', {}).get('duration'),
                'category_id': int(details.get('snippet', {}).get('categoryId', 0)),
                'tags': details.get('snippet', {}).get('tags', []),
                'view_count': int(details.get('statistics', {}).get('viewCount', 0)),
                'like_count': int(details.get('statistics', {}).get('likeCount', 0)),
                'comment_count': int(details.get('statistics', {}).get('commentCount', 0)),
                'extraction_timestamp': datetime.now().isoformat()
            }
            transformed_videos.append(transformed)
        
        return transformed_videos
    
    def _extract_trending_videos(self, hook: YouTubeHook) -> List[Dict]:
        """Extract trending videos"""
        trending = hook.get_trending_videos(self.region_code, self.max_results)
        
        transformed = []
        for video in trending:
            transformed.append({
                'video_id': video.get('id'),
                'video_title': video.get('snippet', {}).get('title'),
                'channel_id': video.get('snippet', {}).get('channelId'),
                'channel_title': video.get('snippet', {}).get('channelTitle'),
                'published_at': video.get('snippet', {}).get('publishedAt'),
                'category_id': int(video.get('snippet', {}).get('categoryId', 0)),
                'tags': video.get('snippet', {}).get('tags', []),
                'duration': video.get('contentDetails', {}).get('duration'),
                'view_count': int(video.get('statistics', {}).get('viewCount', 0)),
                'like_count': int(video.get('statistics', {}).get('likeCount', 0)),
                'comment_count': int(video.get('statistics', {}).get('commentCount', 0)),
                'region_code': self.region_code,
                'extraction_timestamp': datetime.now().isoformat()
            })
        
        return transformed
    
    def _extract_search_results(self, hook: YouTubeHook) -> List[Dict]:
        """Extract search results"""
        if not self.query:
            raise ValueError("Query parameter required for search task type")
        
        results = hook.search_videos(self.query, self.max_results)
        
        transformed = []
        for item in results:
            transformed.append({
                'video_id': item['id']['videoId'],
                'video_title': item['snippet'].get('title'),
                'description': item['snippet'].get('description'),
                'channel_id': item['snippet'].get('channelId'),
                'channel_title': item['snippet'].get('channelTitle'),
                'published_at': item['snippet'].get('publishedAt'),
                'thumbnails': json.dumps(item['snippet'].get('thumbnails', {})),
                'query': self.query,
                'extraction_timestamp': datetime.now().isoformat()
            })
        
        return transformed