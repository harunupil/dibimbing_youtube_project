import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1768272570141 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ",",
        "quoteChar": "\"",
        "escapeChar": "\"",
        "multiline": True,
        "encoding": "UTF-8"
    },
    connection_options={
        "paths": [
            "s3://de-youtube-project-luthfi/raw/"
        ],
        "recurse": True
    },
    transformation_ctx="AmazonS3_node1768272570141"
)

# Script generated for node Change Schema
ChangeSchema_node1768272615866 = ApplyMapping.apply(
    frame=AmazonS3_node1768272570141,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("title", "string", "title", "string"),
        ("published_at", "string", "published_at", "date"),
        ("channel_title", "string", "channel_title", "string"),
        ("view_count", "string", "view_count", "int"),
        ("like_count", "string", "like_count", "int"),
        ("comment_count", "string", "comment_count", "int"),
        (
            "avg_top_comments_sentiment",
            "string",
            "avg_top_comments_sentiment",
            "decimal(5,3)"
        ),
        ("subject", "string", "subject", "string"),
        ("fetch_date", "string", "fetch_date", "date")
    ],
    transformation_ctx="ChangeSchema_node1768272615866"
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1768272615866, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768272502894", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1768272697828 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1768272615866, connection_type="s3", format="glueparquet", connection_options={"path": "s3://de-youtube-project-luthfi/transformed/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1768272697828")

job.commit()