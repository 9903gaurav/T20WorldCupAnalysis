import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, when, split
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df.createOrReplaceTempView("inputTable")
    df = df.withColumn("over_parts", split(df["overs"], "\\."))
    df = df.withColumn("over_parts", col("over_parts").cast("array<integer>"))

    # Extract the first and second parts of the "over" column and create "over1" and "over2" columns
    df = df.withColumn("overs1", col("over_parts")[0])
    df = df.withColumn("overs2", when(col("over_parts")[1] != 0, col("over_parts")[1] ).otherwise(0))

    df = df.drop("over_parts")
    df = df.drop("overs")

    df = df.withColumn("Balls", (6 * col("overs1")) + (col("overs2")))

    dyf_transformed = DynamicFrame.fromDF(df, glueContext, "result0")

    # Return the transformed DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform0": dyf_transformed}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1710704125786 = glueContext.create_dynamic_frame.from_catalog(database="staged_t20data", table_name="staged_bowling", transformation_ctx="AmazonS3_node1710704125786")

# Script generated for node Change Schema
ChangeSchema_node1710704149160 = ApplyMapping.apply(frame=AmazonS3_node1710704125786, mappings=[("match", "string", "match", "string"), ("bowlingteam", "string", "bowlingteam", "string"), ("bowlername", "string", "bowlername", "string"), ("overs", "string", "overs", "string"), ("maiden", "string", "maiden", "int"), ("runs", "string", "runs", "int"), ("wickets", "string", "wickets", "int"), ("economy", "string", "economy", "double"), ("0s", "string", "0s", "int"), ("4s", "string", "4s", "int"), ("6s", "string", "6s", "int"), ("wides", "string", "wides", "int"), ("noballs", "string", "noballs", "int"), ("match_id", "string", "match_id", "string")], transformation_ctx="ChangeSchema_node1710704149160")

# Script generated for node Custom Transform
CustomTransform_node1710704289916 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1710704149160": ChangeSchema_node1710704149160}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1710706347888 = SelectFromCollection.apply(dfc=CustomTransform_node1710704289916, key=list(CustomTransform_node1710704289916.keys())[0], transformation_ctx="SelectFromCollection_node1710706347888")

# Script generated for node Amazon S3
AmazonS3_node1710706358558 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1710706347888, connection_type="s3", format="glueparquet", connection_options={"path": "s3://t20worldcupdata/output_staged/output_staged_bowling/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1710706358558")

job.commit()