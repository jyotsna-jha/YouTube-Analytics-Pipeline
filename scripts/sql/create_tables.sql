-- Create tables for YouTube analytics
CREATE TABLE IF NOT EXISTS youtube_channels (
    channel_id VARCHAR(50) PRIMARY KEY,
    channel_name VARCHAR(255),
    subscriber_count INTEGER,
    view_count BIGINT,
    video_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id VARCHAR(50) PRIMARY KEY,
    channel_id VARCHAR(50),
    video_title TEXT,
    description TEXT,
    published_at TIMESTAMP,
    duration VARCHAR(20),
    category_id INTEGER,
    category_name VARCHAR(100),
    tags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (channel_id) REFERENCES youtube_channels(channel_id)
);

CREATE TABLE IF NOT EXISTS video_statistics_daily (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(50),
    date DATE,
    view_count INTEGER,
    like_count INTEGER,
    dislike_count INTEGER,
    favorite_count INTEGER,
    comment_count INTEGER,
    estimated_minutes_watched BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (video_id) REFERENCES youtube_videos(video_id),
    UNIQUE(video_id, date)
);

CREATE TABLE IF NOT EXISTS channel_statistics_daily (
    id SERIAL PRIMARY KEY,
    channel_id VARCHAR(50),
    date DATE,
    subscriber_count INTEGER,
    view_count BIGINT,
    video_count INTEGER,
    estimated_revenue DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (channel_id) REFERENCES youtube_channels(channel_id),
    UNIQUE(channel_id, date)
);

CREATE TABLE IF NOT EXISTS youtube_analytics_insights (
    id SERIAL PRIMARY KEY,
    channel_id VARCHAR(50),
    insight_date DATE,
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,4),
    insight_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_videos_channel_id ON youtube_videos(channel_id);
CREATE INDEX idx_video_stats_date ON video_statistics_daily(date);
CREATE INDEX idx_channel_stats_date ON channel_statistics_daily(date);
CREATE INDEX idx_insights_date ON youtube_analytics_insights(insight_date);