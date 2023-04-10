import boto3
import botocore
from dotenv import dotenv_values
from fastapi import FastAPI
from model import (ErrorResponse, ExceptionResponse, Glue_connection,
                   SuccessResponse)

config = dotenv_values(".env")
ACCESS_ID = config.get("aws_access_key_id")
ACCESS_KEY = config.get("aws_secret_access_key")
REGION = config.get("aws_region")


client = boto3.client('glue', aws_access_key_id=ACCESS_ID,
                      aws_secret_access_key=ACCESS_KEY, region_name=REGION)

app = FastAPI(openapi_url="/connection/openapi.json",
              docs_url="/connection/docs")


@app.post("/connection/create_connection")
async def create_connection(glue: Glue_connection):
    """
    This endpoint creates a Glue connection.
    """
    try:
        response = client.create_connection(
            ConnectionInput={
                'Name': glue.Name,
                'ConnectionType': glue.ConnectionType,
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': glue.JDBC_CONNECTION_URL,
                    'USERNAME': glue.USERNAME,
                    'PASSWORD': glue.PASSWORD
                }
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'AlreadyExistsException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNumberLimitExceededException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'GlueEncryptionException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            print('eeeee111', error)
            return ExceptionResponse()
    except Exception as e:
        print('eeeee', e)
        return ExceptionResponse()


@app.get('/connection/get_connections')
async def get_connections():
    """
    This endpoint return the all Glue connection.
    """
    try:
        response = client.get_connections(
            # CatalogId='string',
            # Filter={
            #     'MatchCriteria': [
            #         'string',
            #     ],
            #     'ConnectionType': 'JDBC'|'SFTP'|'MONGODB'|'KAFKA'|'NETWORK'|'MARKETPLACE'|'CUSTOM'
            # },
            # HidePassword=True|False,
            # NextToken='string',
            # MaxResults=123
        )
        # connections = [connection['Name']
        #                for connection in response['ConnectionList']]
        return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response['ConnectionList'])
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'GlueEncryptionException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        return ExceptionResponse()


@app.get('/connection/get_connection/{connection_name}')
async def get_connection(connection_name: str):
    """
    This endpoint return the Glue connection.
    """
    try:
        response = client.get_connection(
            Name=connection_name)
        return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response['Connection'])
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'GlueEncryptionException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        return ExceptionResponse()


@app.put("/connection/update_connection")
async def update_connection(glue: Glue_connection):
    """
    This endpoint update a Glue connection.
    """
    try:
        response = client.update_connection(
            Name=glue.Name,
            ConnectionInput={
                'Name': glue.Name,
                'ConnectionType': glue.ConnectionType,
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': glue.JDBC_CONNECTION_URL,
                    'USERNAME': glue.USERNAME,
                    'PASSWORD': glue.PASSWORD
                }
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'GlueEncryptionException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            print('eeeee111', error)
            return ExceptionResponse()
    except Exception as e:
        print('eeeee', e)
        return ExceptionResponse()


@app.delete("/connection/delete/{connection}")
async def connection_delete(connection: str):
    """
    This endpoint delete a Glue connection .
    """
    try:
        response = client.delete_connection(
            ConnectionName=connection
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        return ExceptionResponse()
