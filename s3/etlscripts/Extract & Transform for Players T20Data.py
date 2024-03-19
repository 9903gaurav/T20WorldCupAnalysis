import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Player Summary
PlayerSummary_node1710701309205 = glueContext.create_dynamic_frame.from_catalog(database="staged_t20data", table_name="staged_players", transformation_ctx="PlayerSummary_node1710701309205")

# Script generated for node Amazon S3
AmazonS3_node1710701524796 = glueContext.write_dynamic_frame.from_options(frame=PlayerSummary_node1710701309205, connection_type="s3", format="glueparquet", connection_options={"path": "s3://t20worldcupdata/output_staged/output_staged_players/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1710701524796")

job.commit()