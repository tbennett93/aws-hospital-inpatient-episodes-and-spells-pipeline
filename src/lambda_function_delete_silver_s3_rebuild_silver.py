import json
import boto3

def drop_table(athena, sql, query_output_path):
    athena.start_query_execution(
        QueryString= sql ,
        ResultConfiguration = {
            "OutputLocation": query_output_path
        }
    )

def remove_table_files(s3, bucket:str, prefix: str):
    files_to_delete = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files_to_delete = files_to_delete["Contents"]
    for file in files_to_delete:
        if file["Key"] == prefix:
            continue    
        s3.delete_object(
            Bucket=bucket,
            Key = file["Key"]
        )

def rebuild_table(athena, sql_query):
    athena.start_query_execution(
        QueryString= sql_query,
        ResultConfiguration = {
            "OutputLocation": 's3://hospital-inpatients-pipeline/athena-query-results'
        }
    )


def lambda_handler(event, context):

    BUCKET = 'hospital-inpatients-pipeline'
    PREFIX = "silver/episodes/"

    athena = boto3.client("athena")
    s3 = boto3.client("s3")

        
    REBUILD_TABLE_SQL = """
                            create table hospital_data.silver_episodes

                            with (
                                external_location = 's3://hospital-inpatients-pipeline/silver/episodes',
                                format = 'parquet'
                            )
                            as

                            select *
                            from hospital_data.vw_silver_episodes"""
    
    QUERY_OUTPUT_PATH = 's3://hospital-inpatients-pipeline/athena-query-results'

    DROP_TABLE_SQL = "drop table if exists hospital_data.silver_episodes"


    # drop_table(athena, DROP_TABLE_SQL, QUERY_OUTPUT_PATH)

    #step functions will run a 'drop table if exists' so if we're here, the silver table has been dropped. We now delete the associated files and rebuild silver

    remove_table_files(s3, BUCKET, PREFIX)    

    rebuild_table(athena, REBUILD_TABLE_SQL)
   
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



