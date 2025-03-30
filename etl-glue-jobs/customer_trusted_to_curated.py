import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Customer Trusted
CustomerTrusted_node1743238247373 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1743238247373")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1743238266370 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1743238266370")

# Script generated for node Join
Join_node1743238286746 = Join.apply(frame1=CustomerTrusted_node1743238247373, frame2=AccelerometerLanding_node1743238266370, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1743238286746")

# Script generated for node Drop Fields and Duplicates
SqlQuery2134 = '''
select distinct customername, email, phone, birthday,
serialnumber, registrationdate, lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
DropFieldsandDuplicates_node1743238289222 = sparkSqlQuery(glueContext, query = SqlQuery2134, mapping = {"myDataSource":Join_node1743238286746}, transformation_ctx = "DropFieldsandDuplicates_node1743238289222")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1743238289222, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743238231682", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1743238783191 = glueContext.getSink(path="s3://aws-datalake-dimi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1743238783191")
CustomerCurated_node1743238783191.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1743238783191.setFormat("json")
CustomerCurated_node1743238783191.writeFrame(DropFieldsandDuplicates_node1743238289222)
job.commit()