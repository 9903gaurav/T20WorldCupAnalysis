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
AmazonS3_node1710766781666 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://t20worldcupdata/output_staged/output_staged_batting/"], "recurse": True}, transformation_ctx="AmazonS3_node1710766781666")

# Script generated for node Change Schema
ChangeSchema_node1710766803808 = ApplyMapping.apply(frame=AmazonS3_node1710766781666, mappings=[("match", "string", "match", "string"), ("teaminnings", "string", "teaminnings", "string"), ("battingpos", "int", "battingpos", "int"), ("batsmanname", "string", "batsmanname", "string"), ("runs", "int", "runs", "int"), ("balls", "int", "balls", "int"), ("4s", "int", "fours", "int"), ("6s", "int", "sixes", "int"), ("sr", "double", "sr", "float"), ("out", "int", "out", "int"), ("match_id", "string", "match_id", "string"), ("boundary_runs#0", "int", "boundaryruns", "int")], transformation_ctx="ChangeSchema_node1710766803808")

# Script generated for node Amazon Redshift
AmazonRedshift_node1710766978118 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1710766803808, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-730335539331-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.master_match_batting_summary", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.master_match_batting_summary (match VARCHAR, teaminnings VARCHAR, battingpos INTEGER, batsmanname VARCHAR, runs INTEGER, balls INTEGER, fours INTEGER, sixes INTEGER, sr REAL, out INTEGER, match_id VARCHAR, boundaryruns INTEGER);"}, transformation_ctx="AmazonRedshift_node1710766978118")

job.commit()