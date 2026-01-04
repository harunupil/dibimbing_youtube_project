import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import col

# ---------------------------------------------------
# Job init
# ---------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------------------------------------------------
# INPUT PATHS (PARTITIONED BY region=*)
# ---------------------------------------------------
BASE_PATH = "s3://de-project-youtube-bucket/youtube/raw_statistics/"
INPUT_PATH = BASE_PATH + "region=*/"

# ---------------------------------------------------
# READ CSV SAFELY (PERMISSIVE + MULTILINE)
# ---------------------------------------------------
df_raw = spark.read \
    .option("header", "true") \
    .option("sep", ",") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .option("encoding", "UTF-8") \
    .option("mode", "PERMISSIVE") \
    .option("basePath", BASE_PATH) \
    .csv(INPUT_PATH)

# ---------------------------------------------------
# OPTIONAL CORRUPT RECORD HANDLING (DEFENSIVE)
# ---------------------------------------------------
if "_corrupt_record" in df_raw.columns:
    df_valid = df_raw.filter(col("_corrupt_record").isNull())
else:
    df_valid = df_raw

# ---------------------------------------------------
# CLEAN + CAST (MATCHES YOUR FINAL SCHEMA)
# ---------------------------------------------------
df_clean = df_valid.select(
    col("video_id").cast("string"),
    col("trending_date").cast("string"),
    col("title").cast("string"),
    col("channel_title").cast("string"),
    col("category_id").cast("long"),
    col("publish_time").cast("string"),
    col("tags").cast("string"),
    col("views").cast("long"),
    col("likes").cast("long"),
    col("dislikes").cast("long"),
    col("comment_count").cast("long"),
    col("thumbnail_link").cast("string"),
    col("comments_disabled").cast("boolean"),
    col("ratings_disabled").cast("boolean"),
    col("video_error_or_removed").cast("boolean"),
    col("description").cast("string"),
    col("region").cast("string")
)

# ---------------------------------------------------
# DATA QUALITY FILTERS
# ---------------------------------------------------
df_clean = df_clean.dropna(
    subset=["video_id", "views", "category_id", "region"]
)

# ---------------------------------------------------
# DEDUPLICATION
# BUSINESS KEY: video_id + trending_date + region
# ---------------------------------------------------
df_dedup = df_clean.dropDuplicates(
    ["video_id", "trending_date", "region"]
)

# ---------------------------------------------------
# WRITE PARQUET (PARTITIONED BY region)
# ---------------------------------------------------
df_final = DynamicFrame.fromDF(df_dedup, glueContext, "df_final")

glueContext.write_dynamic_frame.from_options(
    frame=df_final,
    connection_type="s3",
    connection_options={
        "path": "s3://de-project-youtube-cleaned-bucket/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    },
    format="parquet"
)

job.commit()
