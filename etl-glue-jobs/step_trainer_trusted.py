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

# Script generated for node Customer Curated
CustomerCurated_node1743269306383 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1743269306383")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1743275746367 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1743275746367")

# Script generated for node Join
SqlQuery2123 = '''
select sensorreadingtime, st.serialnumber, distancefromobject
from cc 
inner join st 
on cc.serialnumber = st.serialnumber;
'''
Join_node1743276460008 = sparkSqlQuery(glueContext, query = SqlQuery2123, mapping = {"cc":CustomerCurated_node1743269306383, "st":StepTrainerLanding_node1743275746367}, transformation_ctx = "Join_node1743276460008")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1743276460008, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743274304963", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1743274364756 = glueContext.getSink(path="s3://aws-datalake-dimi/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1743274364756")
StepTrainerTrusted_node1743274364756.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1743274364756.setFormat("json")
StepTrainerTrusted_node1743274364756.writeFrame(Join_node1743276460008)
job.commit()