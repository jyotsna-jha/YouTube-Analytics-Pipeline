# 🎥 YouTube Analytics Pipeline

> **Automated, production‑ready ETL pipeline for YouTube channel analytics powered by Apache Airflow 🚀**

---

## ✨ What is this?

This project builds a **fully automated ETL pipeline** that pulls analytics data from the **YouTube Data API**, transforms it into meaningful metrics, and stores it reliably in **PostgreSQL** — all orchestrated by **Apache Airflow** and running smoothly inside **Docker**.

Perfect for:

* 📊 Tracking channel growth
* 📈 Monitoring performance trends
* 🧪 Practicing real‑world data engineering
* ⚙️ Learning Airflow + Docker the right way

---

## ⚡ Quick Start

### 🔑 Prerequisites

Make sure you have the following installed:

* 🐳 Docker & Docker Compose
* 🔐 YouTube API Key
* 📺 YouTube Channel ID

---

### 🛠 Installation

```bash
# Clone the repository
git clone <repository-url>
cd youtube_analytics_pipeline

# Setup environment variables
cp .env.example .env
nano .env

# Initialize Airflow
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Verify running containers
docker-compose ps
```

---

## 🌐 Access Airflow UI

Once everything is running, open Airflow in your browser:

* 🔗 **URL**: [http://localhost:8080](http://localhost:8080)
* 👤 **Username**: `admin`
* 🔑 **Password**: `admin`

✨ You’re now ready to manage pipelines!

---

## 📁 Project Structure

```text
youtube_analytics_pipeline/
├── dags/                 # 🛫 Airflow DAG definitions
├── plugins/              # 🔌 Custom operators & hooks
├── scripts/              # 🧠 SQL scripts & configs
├── tests/                # 🧪 Unit & integration tests
├── docker-compose.yml    # 🐳 Docker services
├── .env                  # 🔐 Environment variables
├── requirements.txt      # 📦 Python dependencies
└── README.md             # 📘 Documentation
```

---

## 🔧 Configuration

### 📄 Environment Variables (`.env`)

```env
YOUTUBE_API_KEY=your_api_key_here
LAYMAN_AI_CHANNEL_ID=your_channel_id_here
AIRFLOW_UID=50000
```

🔒 **Tip:** Never commit your real API keys to GitHub.

---

## ⏰ DAG Schedules

| Pipeline         | Schedule    | Description                  |
| ---------------- | ----------- | ---------------------------- |
| 📊 Main ETL      | `0 6 * * *` | Daily data extraction & load |
| ✅ Quality Checks | `0 7 * * *` | Validate data accuracy       |
| 💾 Backup        | `0 0 * * 0` | Weekly database backup       |

---

## 🔄 Pipeline Flow

```text
YouTube API
    ↓
Extract ──► Transform ──► Load
    ↓            ↓          ↓
 Logging     Metrics     PostgreSQL
```

### 🧩 Steps Explained

1. **Extract** 📡
   Fetch raw analytics data using the YouTube Data API

2. **Transform** 🧹
   Clean data, compute KPIs, and prepare structured tables

3. **Load** 🗄️
   Store processed data in PostgreSQL

4. **Monitor** 🚨
   Run quality checks and alert on failures

---

## 🐞 Debug & Maintenance

Useful commands while developing or debugging:

```bash
# View service logs
docker-compose logs -f

# Access PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# Trigger DAG manually
docker-compose exec airflow-webserver airflow dags trigger youtube_analytics_etl
```

---

## 📞 Need Help?

If something isn’t working:

1. 🔍 Check logs: `docker-compose logs`
2. 🧪 Verify `.env` values
3. 🌐 Test YouTube API connectivity
4. 🔄 Restart services if needed

---

## 💡 Future Enhancements

* 📈 Add dashboard (Metabase / Superset)
* ☁️ Cloud deployment (AWS / GCP)
* 🧠 Incremental loads
* 🔔 Slack / Email alerts

---

### ❤️ Built for learning, scaling, and real‑world data engineering

Happy piping! 🚀
