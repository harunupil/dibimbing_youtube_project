# dibimbing_project
# Project ini bertujuan untuk membuat data pipeline youtube data dengan menggunakan AWS sebagai platform utamanya

# Main Data Source : https://www.kaggle.com/datasets/datasnaek/youtube-new

#The main flow is:
Data Source -> AWS S3 Bucket -> AWS Crawler into Database -> Data Cleaning using AWS Lambda and AWS Glue -> Join Table using AWS Glue -> Dashboard update using AWS Quick Sight
