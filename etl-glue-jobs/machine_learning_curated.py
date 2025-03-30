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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1743328440147 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1743328440147")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1743329988848 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1743329988848")

# Script generated for node Join
SqlQuery2492 = '''
select * from st
join at on st.sensorreadingtime = at.timestamp;
'''
Join_node1743328449170 = sparkSqlQuery(glueContext, query = SqlQuery2492, mapping = {"st":StepTrainerTrusted_node1743328440147, "at":AccelerometerTrusted_node1743329988848}, transformation_ctx = "Join_node1743328449170")

# Script generated for node Drop Fields
SqlQuery2491 = '''
select sensorreadingtime, serialnumber, distancefromobject, timestamp, x, y, z from myDataSource
'''
DropFields_node1743332519298 = sparkSqlQuery(glueContext, query = SqlQuery2491, mapping = {"myDataSource":Join_node1743328449170}, transformation_ctx = "DropFields_node1743332519298")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=DropFields_node1743332519298, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743327605452", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1743328451825 = glueContext.getSink(path="s3://aws-datalake-dimi/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1743328451825")
MachineLearningCurated_node1743328451825.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1743328451825.setFormat("json")
MachineLearningCurated_node1743328451825.writeFrame(DropFields_node1743332519298)
job.commit()