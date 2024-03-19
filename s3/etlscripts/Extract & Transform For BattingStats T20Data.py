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
    from pyspark.sql.functions import col, when
    # Convert the input DynamicFrame to a DataFrame
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Create a temporary view for the DataFrame
    df.createOrReplaceTempView("inputTable")

    # Apply the transformations using Spark SQL
    df = df.withColumn("sr", when(col("sr") == "null", 0).otherwise(col("sr")))
    df = df.withColumn("out", when(col("out") == "out", 1).otherwise(0))
    df = df.withColumn("out", df["out"].cast("int"))
    df = df.withColumn("Boundary Runs", (4 * col("4s")) + (6 * col("6s")))
    # Convert the transformed DataFrame to a DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df, glueContext, "result0")

    # Return the transformed DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform0": dyf_transformed}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Batting Stats
BattingStats_node1710702406149 = glueContext.create_dynamic_frame.from_catalog(database="staged_t20data", table_name="staged_batting", transformation_ctx="BattingStats_node1710702406149")

# Script generated for node Change Schema
ChangeSchema_node1710702438024 = ApplyMapping.apply(frame=BattingStats_node1710702406149, mappings=[("match", "string", "match", "string"), ("teaminnings", "string", "teaminnings", "string"), ("battingpos", "int", "battingpos", "int"), ("batsmanname", "string", "batsmanname", "string"), ("runs", "string", "runs", "int"), ("balls", "string", "balls", "int"), ("4s", "string", "4s", "int"), ("6s", "string", "6s", "int"), ("sr", "string", "sr", "double"), ("out/not_out", "string", "out", "string"), ("match_id", "string", "match_id", "string")], transformation_ctx="ChangeSchema_node1710702438024")

# Script generated for node Custom Transform
CustomTransform_node1710702483479 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1710702438024": ChangeSchema_node1710702438024}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1710702600796 = SelectFromCollection.apply(dfc=CustomTransform_node1710702483479, key=list(CustomTransform_node1710702483479.keys())[0], transformation_ctx="SelectFromCollection_node1710702600796")

# Script generated for node Amazon S3
AmazonS3_node1710702612629 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1710702600796, connection_type="s3", format="glueparquet", connection_options={"path": "s3://t20worldcupdata/output_staged/output_staged_batting/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1710702612629")

job.commit()