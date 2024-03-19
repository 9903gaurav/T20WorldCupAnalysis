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
AmazonS3_node1710770337726 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://t20worldcupdata/output_staged/output_staged_players/"], "recurse": True}, transformation_ctx="AmazonS3_node1710770337726")

# Script generated for node Change Schema
ChangeSchema_node1710770403082 = ApplyMapping.apply(frame=AmazonS3_node1710770337726, mappings=[("name", "string", "name", "string"), ("team", "string", "team", "string"), ("image", "string", "image", "string"), ("battingstyle", "string", "battingstyle", "string"), ("bowlingstyle", "string", "bowlingstyle", "string"), ("playingrole", "string", "playingrole", "string"), ("description", "string", "description", "string")], transformation_ctx="ChangeSchema_node1710770403082")

# Script generated for node Amazon Redshift
AmazonRedshift_node1710770423470 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1710770403082, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-730335539331-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.master_players", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.master_players (name VARCHAR, team VARCHAR, image VARCHAR, battingstyle VARCHAR, bowlingstyle VARCHAR, playingrole VARCHAR, description VARCHAR);"}, transformation_ctx="AmazonRedshift_node1710770423470")

job.commit()