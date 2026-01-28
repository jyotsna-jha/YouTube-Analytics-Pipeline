-- Daily Growth Metrics
SELECT 
    date,
    subscriber_count,
    subscriber_count - LAG(subscriber_count) OVER (ORDER BY date) as new_subscribers,
    ROUND((subscriber_count::DECIMAL / LAG(subscriber_count) OVER (ORDER BY date) - 1) * 100, 2) as growth_rate_percent
FROM channel_statistics_daily 
WHERE channel_id = '${channel_id}'
ORDER BY date DESC;

-- Top Performing Videos
SELECT 
    v.video_title,
    SUM(vs.view_count) as total_views,
    SUM(vs.like_count) as total_likes,
    SUM(vs.comment_count) as total_comments,
    ROUND(SUM(vs.like_count)::DECIMAL / NULLIF(SUM(vs.view_count), 0) * 100, 2) as engagement_rate
FROM video_statistics_daily vs
JOIN youtube_videos v ON vs.video_id = v.video_id
WHERE v.channel_id = '${channel_id}'
GROUP BY v.video_id, v.video_title
ORDER BY total_views DESC
LIMIT 10;

-- Weekly Performance Trends
SELECT 
    DATE_TRUNC('week', date) as week_start,
    COUNT(DISTINCT video_id) as videos_published,
    SUM(view_count) as total_views,
    SUM(like_count) as total_likes,
    AVG(like_count::DECIMAL / NULLIF(view_count, 0)) * 100 as avg_engagement_rate
FROM video_statistics_daily vs
JOIN youtube_videos v ON vs.video_id = v.video_id
WHERE v.channel_id = '${channel_id}'
GROUP BY week_start
ORDER BY week_start DESC;

-- Best Time to Post Analysis
SELECT 
    EXTRACT(HOUR FROM published_at) as hour_of_day,
    COUNT(*) as videos_count,
    AVG(vs.view_count) as avg_views,
    AVG(vs.like_count) as avg_likes
FROM youtube_videos v
JOIN video_statistics_daily vs ON v.video_id = vs.video_id
WHERE v.channel_id = '${channel_id}'
GROUP BY hour_of_day
ORDER BY avg_views DESC;

-- Content Category Performance
SELECT 
    category_name,
    COUNT(*) as video_count,
    SUM(vs.view_count) as total_views,
    AVG(vs.like_count) as avg_likes,
    ROUND(AVG(vs.like_count::DECIMAL / NULLIF(vs.view_count, 0)) * 100, 2) as avg_engagement
FROM youtube_videos v
JOIN video_statistics_daily vs ON v.video_id = vs.video_id
WHERE v.channel_id = '${channel_id}'
GROUP BY category_name
ORDER BY total_views DESC;