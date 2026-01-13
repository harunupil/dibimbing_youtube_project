# Final Project Data Engineering Bootcamp
# Youtube Trending Video Analytics & Sentiment Pipeline

## 📌 Project Overview
Project ini bertujuan untuk membuat end-to-end data pipeline dengan tujuan memproses dan menganalisa Youtube Trending videos dan sentiment berdasarkan subject professional seperti Data Engineer, DevOps, Finance, dst.

Data pipeline ini melakukan data ingestion daily batching berasal dari Yotube API di tiap query subjectnya, melakukan average sentiment processing berdasarkan top comments tiap video tersebut, transformasi pertama data menggunakan python ETL, mengupload data ke AWS S3, trasnformasi kedua schema dengan AWS Glue, dan yang terakhir hasil transformasi data ditampilkan melalui AWS Quicksight dashboard.

## 📌 Flow Pipeline:
Youtube Data API -> Python ETL -> Airflow Orchestration -> Amazon S3 Raw Data Upload -> AWS Glue ETL -> AWS Glue Data Catalog -> Quick Sight Dashboard


