%additional_python_modules botocore>=1.20.12,boto3>=1.26.69

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


def read_object_mapping(bucket_name,object_name):
    object_mappings = {}
    mapping_path = "s3://{}/salesforce_mappings/{}_schema.csv".format(bucket_name,object_name)
    df = spark.read\
              .format("csv")\
              .option("header",True)\
              .load(mapping_path)
    df_mappings = df.collect()
    for row in df_mappings:
        object_mappings[row["FIELD LABEL"]] = row["FIELD NAME"]

    return object_mappings



def get_all_flows(appflow_client):
    all_flows = []
    response = appflow_client.list_flows()
    if "flows" in list(response.keys()):
        all_flows += [element["flowName"] for element in response["flows"]]
    return all_flows



def create_tasks(object_mappings):
    tasks = []
    source_fields = list(object_mappings.keys())
    tasks.append({
         "sourceFields": source_fields,
         "connectorOperator":{
            "S3":"PROJECTION"
         },
         "taskType":"Filter"
      })
    for source_field in source_fields:
            tasks.append({
             "sourceFields":[
                source_field
             ],
             "connectorOperator":{
                "S3":"NO_OP"
             },
             "destinationField": object_mappings[source_field],
             "taskType":"Map"
          })
    return tasks



import boto3

def create_flow(flow_name,bucket_name,bucket_prefix,upsert_fields,object_name):

    appflow_client = boto3.client("appflow")

    if flow_name not in get_all_flows(appflow_client):

        object_mappings = read_object_mapping(bucket_name,object_name.replace(" ","_").lower())
        mapped_upsert_fields = [object_mappings[field] for field in upsert_fields]
        flow_tasks = create_tasks(object_mappings)

        response = appflow_client.create_flow(

           flowName=flow_name,
           sourceFlowConfig={
              "connectorType":"S3",
              "sourceConnectorProperties":{
                 "S3":{
                    "bucketName":bucket_name,
                    "bucketPrefix":bucket_prefix,
                    "s3InputFormatConfig": {
                        "s3InputFileType": "JSON"
                }
                 }
              }
           },
           destinationFlowConfigList=[
              {
                 "connectorType":"Salesforce",
                 "connectorProfileName":"SalesForceConnection",
                 "destinationConnectorProperties":{
                    "Salesforce":{
                       "object": "{}2".format(object_name),
                       "idFieldNames": mapped_upsert_fields,
                       "errorHandlingConfig":{
                          "failOnFirstDestinationError":True
                       },
                       "writeOperationType":"UPSERT"
                    }
                 }
              }
           ],

           triggerConfig={
              "triggerType":"OnDemand"
           },
           tasks=flow_tasks
        )
    return response

def trigger_flow(flow_name):
  appflow_client = boto3.client("appflow")
  appflow_client.start_flow(flowName=flow_name)


def rename_cols_csv(df):
    new_df = df
    prev_cols = list(df.columns)
    new_cols = [col.replace("_", " ") for col in prev_cols]
    for index,prev_col in enumerate(prev_cols):
        new_df = new_df.withColumnRenamed(prev_col,new_cols[index])
    return new_df


def parquet_to_json(bucket_name,table_name):
    parquet_loc = "s3://{}/curated/{}".format(bucket_name,table_name)
    json_loc = parquet_loc.replace("curated","curated-json").replace(table_name,"{}.json".format(table_name))
    parquet_df = spark.read.parquet(parquet_loc)
    renamed_parquet_df = rename_cols_csv(parquet_df)
    renamed_parquet_df.write.mode("overwrite").json(json_loc)



flow_name = "BotoTestingAhmadFlow"
bucket_name = "c24-salesforce-appflow-test-bucket"
bucket_prefix = "curated-json/Product-Dummy.json"
upsert_fields = ["Product Name"]
object_name = "Product"


parquet_to_json(bucket_name,object_name)
create_flow(flow_name,bucket_name,bucket_prefix,upsert_fields,object_name)
trigger_flow(flow_name)