Esyanpse: End-to-End E-commerce Data PipelineThis repository contains the complete source code and documentation for Esyanpse, an end-to-end data engineering project that simulates, ingests, transforms, and analyzes e-commerce data in real-time. The project demonstrates a modern data stack and best practices for building an automated, scalable data pipeline.Key FeaturesRealistic Synthetic Data: A Python-based data generator that simulates complex user journeys, including cart abandonment, repeat customers, product returns, and out-of-stock scenarios.Real-Time Ingestion: A scalable, serverless ingestion pipeline using Google Cloud Pub/Sub to stream events into BigQuery.Automated Transformation: An hourly dbt Cloud job transforms raw event data into clean, analysis-ready tables, separating raw storage from production analytics.Predictive Analytics: An automated BigQuery ML model that retrains daily to provide a 30-day sales forecast.Interactive Visualization: A comprehensive Tableau dashboard for business users to explore KPIs, track performance, and view forecasts.Architecture Diagram(Here you can paste your draw.io image or use the Mermaid code below)

Technology StackData Generation: PythonCloud Provider: Google Cloud Platform (GCP)Data Ingestion: Pub/SubData Warehouse: BigQueryTransformation: dbt (Data Build Tool)Machine Learning: BigQuery ML (BQML)Visualization: TableauProject Structureesyanpse-data-pipeline/
├── .gitignore                # Specifies files for Git to ignore
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── data_generation/          # Python scripts for data generation
│   ├── create_catalog.py
│   ├── stream_events.py
│   └── backfill_events.py
├── dbt_project/              # dbt models for data transformation
│   └── models/
│       ├── sources.yml
│       ├── daily_revenue.sql
│       └── ... (and all other models)
└── bigquery_ml/              # SQL scripts for creating BQML models
    ├── create_daily_forecast_model.sql
    └── create_hourly_forecast_model.sql
Setup and UsageFollow these steps to set up and run the project in your own Google Cloud environment.1. PrerequisitesA Google Cloud Platform account with a billing account enabled.Python 3.8+ installed locally.The gcloud command-line tool installed and configured.2. Clone the Repositorygit clone https://github.com/your-username/esyanpse-data-pipeline.git
cd esyanpse-data-pipeline
3. Configure Your Cloud EnvironmentFirst, set your Project ID as an environment variable. Replace "your-gcp-project-id" with your actual GCP Project ID.export GCP_PROJECT_ID="your-gcp-project-id"
gcloud config set project $GCP_PROJECT_ID
Next, run the setup script to create all the necessary cloud resources.# This script is not provided, you would create it from the instructions
# in the "How to Run This Project" section we discussed previously.
# Or, you can run the gcloud and bq commands manually.
./setup_cloud_resources.sh 
4. Generate and Load DataInstall the required Python libraries and run the data generation scripts.# Install dependencies
pip install -r requirements.txt

# Generate the product catalog and upload it to BigQuery
python data_generation/create_catalog.py
bq load --source_format=CSV --autodetect ecommerce.products ./products.csv

# Backfill 90 days of historical event data
python data_generation/backfill_events.py
5. Set Up and Run dbtCreate a new project in dbt Cloud.Connect it to your BigQuery ecommerce dataset using a Service Account or OAuth.Upload the models from the dbt_project/models directory.Run a full refresh to build all the tables from your historical data.dbt run --full-refresh
Set up a scheduled job in dbt Cloud to run dbt run hourly.6. Create and Schedule ML ModelsRun the SQL scripts located in the bigquery_ml/ directory directly in the BigQuery Console to create your forecasting models. Then, use the BigQuery UI to schedule these queries to run daily.7. Run the Live Stream (Optional)To add new data in real-time, run the live streaming script.python data_generation/stream_events.py
Dashboard Preview(This is a great place to add a screenshot of your final Tableau dashboard!)