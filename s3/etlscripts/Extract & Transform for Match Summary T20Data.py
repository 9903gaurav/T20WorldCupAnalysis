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
    from pyspark.sql.functions import col, when, to_date
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Convert the input DynamicFrame to a DataFrame
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Create a temporary view for the DataFrame
    df.createOrReplaceTempView("inputTable")
    df = df.withColumn("match date", to_date(df["match date"], "MMM dd, yyyy"))
    df = df.withColumn("Stage", when(col("match date") < "2022-10-22", "Qualifier").otherwise("Super 12"))
    dyf_transformed = DynamicFrame.fromDF(df, glueContext, "result0")

    return DynamicFrameCollection({"CustomTransform0": dyf_transformed}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Match Summary
MatchSummary_node1710702907759 = glueContext.create_dynamic_frame.from_catalog(database="staged_t20data", table_name="staged_matchsummary", transformation_ctx="MatchSummary_node1710702907759")

# Script generated for node Custom Transform
CustomTransform_node1710703045068 = MyTransform(glueContext, DynamicFrameCollection({"MatchSummary_node1710702907759": MatchSummary_node1710702907759}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1710703653212 = SelectFromCollection.apply(dfc=CustomTransform_node1710703045068, key=list(CustomTransform_node1710703045068.keys())[0], transformation_ctx="SelectFromCollection_node1710703653212")

# Script generated for node Amazon S3
AmazonS3_node1710703663803 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1710703653212, connection_type="s3", format="glueparquet", connection_options={"path": "s3://t20worldcupdata/output_staged/output_staged_matchSummary/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1710703663803")

job.commit()