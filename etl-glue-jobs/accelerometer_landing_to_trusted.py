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
CustomerTrusted_node1742193344232 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1742193344232")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1742193270499 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1742193270499")

# Script generated for node Join
SqlQuery2247 = '''
select * from ct 
join al on
ct.email = al.user;

'''
Join_node1743330333733 = sparkSqlQuery(glueContext, query = SqlQuery2247, mapping = {"ct":CustomerTrusted_node1742193344232, "al":AccelerometerLanding_node1742193270499}, transformation_ctx = "Join_node1743330333733")

# Script generated for node Drop Fields
SqlQuery2246 = '''
select user, timestamp, x, y, z from myDataSource
where timestamp >= sharewithresearchasofdate;
'''
DropFields_node1742194563172 = sparkSqlQuery(glueContext, query = SqlQuery2246, mapping = {"myDataSource":Join_node1743330333733}, transformation_ctx = "DropFields_node1742194563172")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1742194563172, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742193080688", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1742194320547 = glueContext.getSink(path="s3://aws-datalake-dimi/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1742194320547")
AccelerometerTrusted_node1742194320547.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1742194320547.setFormat("json")
AccelerometerTrusted_node1742194320547.writeFrame(DropFields_node1742194563172)
job.commit()