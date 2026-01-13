# Final Project Data Engineering Bootcamp
# Youtube Trending Video Analytics & Sentiment Pipeline

## 📌 Project Overview
Project ini bertujuan untuk membangun end-to-end data engineering pipeline yang memproses dan menganalisis YouTube Trending Videos beserta sentimen audiens berdasarkan berbagai subject profesional seperti Data Engineer, DevOps, Finance & Accounting, UI/UX, dan lainnya.

Pipeline ini menggunakan daily batch ingestion dari YouTube Data API untuk setiap subject query. Data yang diambil mencakup metadata video serta komentar teratas, kemudian dilakukan sentiment analysis dengan menghitung average sentiment score dari komentar tersebut.

Hasil akhir data disimpan dalam format Parquet dan divisualisasikan melalui AWS QuickSight dashboard untuk analisis engagement dan sentimen.

## 📌 Flow Pipeline:
Youtube Data API -> Python ETL -> Airflow Orchestration -> Amazon S3 Raw Data Upload -> AWS Glue ETL -> AWS Glue Data Catalog -> Quick Sight Dashboard

![Pipeline Architecture](visual/pipeline_flow.png)

## 📌 Key Features
- Daily batch ingestion menggunakan Apache Airflow
- Sentiment analysis berbasis komentar teratas video
- Multi-topic analysis berdasarkan subject profesional
- Schema enforcement menggunakan AWS Glue
- Interactive dashboard untuk analisis engagement & sentiment

## Author
**Luthfi Arif Radriyantomo**


