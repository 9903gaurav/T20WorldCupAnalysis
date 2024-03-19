import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1710770133702 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://t20worldcupdata/output_staged/output_staged_matchSummary/"], "recurse": True}, transformation_ctx="AmazonS3_node1710770133702")

# Script generated for node Change Schema
ChangeSchema_node1710770240724 = ApplyMapping.apply(frame=AmazonS3_node1710770133702, mappings=[("team_1#0", "string", "team1", "string"), ("team_2#1", "string", "team2", "string"), ("winner", "string", "winner", "string"), ("margin", "string", "margin", "string"), ("ground", "string", "ground", "string"), ("match_date#2", "date", "match_date", "date"), ("scorecard", "string", "match_id", "string"), ("stage", "string", "stage", "string")], transformation_ctx="ChangeSchema_node1710770240724")

# Script generated for node Amazon Redshift
AmazonRedshift_node1710770297183 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1710770240724, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-730335539331-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.master_match_summary", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.master_match_summary (team1 VARCHAR, team2 VARCHAR, winner VARCHAR, margin VARCHAR, ground VARCHAR, match_date DATE, match_id VARCHAR, stage VARCHAR);"}, transformation_ctx="AmazonRedshift_node1710770297183")

job.commit()