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
AmazonS3_node1710769806088 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://t20worldcupdata/output_staged/output_staged_bowling/"], "recurse": True}, transformation_ctx="AmazonS3_node1710769806088")

# Script generated for node Change Schema
ChangeSchema_node1710769838629 = ApplyMapping.apply(frame=AmazonS3_node1710769806088, mappings=[("match", "string", "match", "string"), ("bowlingteam", "string", "bowlingteam", "string"), ("bowlername", "string", "bowlername", "string"), ("maiden", "int", "maiden", "int"), ("runs", "int", "runs", "int"), ("wickets", "int", "wickets", "int"), ("economy", "double", "economy", "float"), ("0s", "int", "zeros", "int"), ("4s", "int", "fours", "int"), ("6s", "int", "sixes", "int"), ("wides", "int", "wides", "int"), ("noballs", "int", "noballs", "int"), ("match_id", "string", "match_id", "string"), ("overs1", "int", "overs1", "int"), ("overs2", "int", "overs2", "int"), ("balls", "int", "balls", "int")], transformation_ctx="ChangeSchema_node1710769838629")

# Script generated for node Amazon Redshift
AmazonRedshift_node1710770057677 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1710769838629, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-730335539331-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.master_match_bowling_summary", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.master_match_bowling_summary (match VARCHAR, bowlingteam VARCHAR, bowlername VARCHAR, maiden INTEGER, runs INTEGER, wickets INTEGER, economy REAL, zeros INTEGER, fours INTEGER, sixes INTEGER, wides INTEGER, noballs INTEGER, match_id VARCHAR, overs1 INTEGER, overs2 INTEGER, balls INTEGER);"}, transformation_ctx="AmazonRedshift_node1710770057677")

job.commit()