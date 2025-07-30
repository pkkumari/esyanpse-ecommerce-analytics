# ESynapse: End-to-End E-commerce Data Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white" alt="Google Cloud"/>
  <img src="https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=google-bigquery&logoColor=white" alt="BigQuery"/>
  <img src="https://img.shields.io/badge/dbt-FF694A?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt"/>
  <img src="https://img.shields.io/badge/Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white" alt="Tableau"/>
</p>

This repository contains the complete source code and documentation for **ESynapse**, an end-to-end data engineering project that simulates, ingests, transforms, and analyzes e-commerce data in real-time. The project demonstrates a modern data stack and best practices for building an automated, scalable data pipeline.

---

## Key Features

* **Realistic Synthetic Data:** A Python-based data generator that simulates complex user journeys, including cart abandonment, repeat customers, product returns, and out-of-stock scenarios.
* **Real-Time Ingestion:** A scalable, serverless ingestion pipeline using **Google Cloud Pub/Sub** to stream events into **BigQuery**.
* **Automated Transformation:** An hourly **dbt Cloud** job transforms raw event data into clean, analysis-ready tables, separating raw storage from production analytics.
* **Predictive Analytics:** An automated **BigQuery ML** model that retrains daily to provide a 30-day sales forecast.
* **Interactive Visualization:** A comprehensive **Tableau** dashboard for business users to explore KPIs, track performance, and view forecasts.

---

## Architecture
<img width="1333" height="1613" alt="ESynapse Architecture Diagram drawio (4)" src="https://github.com/user-attachments/assets/12df2a69-7420-4499-955f-2e6b92e28eb0" />

---

## Project Structure

```
esyanpse-data-pipeline/
├── .gitignore
├── README.md
├── requirements.txt
├── data_generation/
│   ├── create_catalog.py
│   ├── stream_events.py
│   └── backfill_events.py
├── dbt_project/
│   └── models/
│       ├── sources.yml
│       ├── daily_revenue.sql
│       ├── hourly_revenue.sql
|       ├── product_kpis.sql
|       ├── product_health_score
|       └── trending_score
└── bigquery_ml/
    ├── create_daily_forecast_model.sql
    └── create_hourly_forecast_model.sql
```

---

## Setup and Usage

Follow these steps to set up and run the project in your own Google Cloud environment.

### 1. Prerequisites
* A Google Cloud Platform account with a billing account enabled.
* Python 3.8+ installed locally.
* The `gcloud` command-line tool installed and configured.

### 2. Clone the Repository
```bash
git clone [https://github.com/your-username/esyanpse-data-pipeline.git](https://github.com/your-username/esyanpse-data-pipeline.git)
cd esyanpse-data-pipeline
```

### 3. Configure Google Cloud
First, set your Project ID. Replace `"your-gcp-project-id"` with your actual GCP Project ID.
```bash
export GCP_PROJECT_ID="your-gcp-project-id"
gcloud config set project $GCP_PROJECT_ID
gcloud auth application-default login
```
Next, run the following commands to create all the necessary cloud infrastructure.
```bash
# Enable APIs
gcloud services enable bigquery.googleapis.com pubsub.googleapis.com

# Create BigQuery Dataset
bq mk --dataset ecommerce

# Create BigQuery Tables
bq mk --table ecommerce.products product_id:STRING,product_name:STRING,category:STRING,regular_price:FLOAT,avg_rating:FLOAT,review_count:INTEGER,in_stock:BOOLEAN
bq mk --table ecommerce.live_events event_id:STRING,event_timestamp:TIMESTAMP,event_type:STRING,user_id:STRING,session_id:STRING,product_id:STRING,on_sale:BOOLEAN,sale_price:FLOAT,quantity:INTEGER,rating:INTEGER,source:STRING

# Create Pub/Sub Topic
gcloud pubsub topics create ecommerce-events

# Create Pub/Sub Subscription
gcloud pubsub subscriptions create write-to-bigquery --topic=ecommerce-events --bigquery-table=$GCP_PROJECT_ID:ecommerce.live_events --use-table-schema

# Grant Permissions to Pub/Sub
SERVICE_ACCOUNT=$(gcloud pubsub subscriptions describe write-to-bigquery --format='value(bigqueryConfig.serviceAccountEmail)')
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID --member="serviceAccount:$SERVICE_ACCOUNT" --role="roles/bigquery.dataEditor"
```

### 4. Generate and Load Data
Install the required Python libraries and run the data generation scripts.
```bash
# Install dependencies from the requirements.txt file
pip install -r requirements.txt

# Generate the product catalog and upload it to BigQuery
python data_generation/create_catalog.py
bq load --source_format=CSV --autodetect ecommerce.products ./products.csv

# Backfill 90 days of historical event data
python data_generation/backfill_events.py
```

### 5. Set up and Run dbt
1.  Create a new project in **dbt Cloud**.
2.  Connect it to your BigQuery `ecommerce` dataset using a Service Account or OAuth.
3.  Upload the models from the `dbt_project/models` directory.
4.  Run a full refresh to build all the tables from your historical data.
    ```bash
    dbt run --full-refresh
    ```
5.  Set up a scheduled job in dbt Cloud to run `dbt run` hourly.

### 6. Create and Schedule ML Models
Run the SQL scripts located in the `bigquery_ml/` directory directly in the BigQuery Console to create your forecasting models. Then, use the BigQuery UI to schedule these queries to run daily.

### 7. Run the Live Stream (Optional)
To add new data in real-time, run the live streaming script.
```bash
python data_generation/stream_events.py
```

---
## 📊 Tableau Dashboard Walkthrough

The Tableau dashboard offers an interactive view into shopper behavior and sales performance derived from real-time and historical data processed through the ESynapse pipeline.

**Key insights covered in the video:**
- Product health scores and conversion rates
- Dynamic filtering by product category, discount period, and metrics
- Price drop detection and discount effectiveness
- Sales funnel KPIs (Viewed → Carted → Purchased)
- Forecasted vs. actual sales trends

🎥 **[Watch the interactive demo here](https://drive.google.com/file/d/1-CqvO-FaNCvKegAKOxIJZzGsK27Lpciz/view?usp=drive_link)**  

🖼️ *Preview Screenshot:*
https://drive.google.com/file/d/1WivHE1heEBg_-hgtXTrUjoD_ajgv-Uzd/view?usp=drive_link
