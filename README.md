# YouTube Analytics Pipeline - The Layman AI

> Automated ETL pipeline for YouTube channel analytics using Apache Airflow

## Quick Start

### Prerequisites

- Docker & Docker Compose
- YouTube API Key
- Channel ID

### Installation

```bash
# Clone and setup
git clone <repository-url>
cd youtube_analytics_pipeline
cp .env.example .env

# Edit .env with your API key and channel ID
nano .env

# Start services
docker-compose up airflow-init
docker-compose up -d

# Check status
docker-compose ps
```

### Access Airflow

* **URL** : [http://localhost:8080](http://localhost:8080/)
* **Username** : admin
* **Password** : admin

## 📁 Project Structure

```
youtube_analytics_pipeline/
├── dags/                    # Airflow DAGs
├── plugins/                 # Custom operators & hooks
├── scripts/                 # SQL & configs
├── tests/                   # Unit tests
├── docker-compose.yml      # Docker setup
├── .env                    # Environment vars
├── requirements.txt        # Dependencies
└── README.md              # This file
```

## 🔧 Configuration

### .env File

```
YOUTUBE_API_KEY=your_api_key_here
LAYMAN_AI_CHANNEL_ID=your_channel_id_here
AIRFLOW_UID=50000
```

### DAG Schedule

* **Main ETL** : Daily at 6 AM (0 6 * * *)
* **Quality Checks** : Daily at 7 AM (0 7 * * *)
* **Backup** : Weekly on Sunday (0 0 * * 0)

## 📊 Pipeline Flow

1. **Extract** → YouTube API data
2. **Transform** → Clean & calculate metrics
3. **Load** → PostgreSQL database
4. **Monitor** → Quality checks & alerts


### Debug Commands

```
# View logs
docker-compose logs -f

# Check database
docker-compose exec postgres psql -U airflow -d airflow

# Trigger DAG manually
docker-compose exec airflow-webserver airflow dags trigger youtube_analytics_etl
```



## 📞 Support

1. Check logs: `docker-compose logs`
2. Verify .env configuration
3. Test API connection
