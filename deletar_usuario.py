import os
import boto3
from boto3.dynamodb.conditions import Key

USER_ID = os.getenv('USER_ID')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME')
REGION = os.getenv('AWS_DEFAULT_REGION')
PK_VALUE = f"USER#{USER_ID}"

dynamodb = boto3.resource(
    "dynamodb",
    region_name=REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
table = dynamodb.Table(TABLE_NAME)

response = table.query(KeyConditionExpression=Key("PK").eq(PK_VALUE))
items = response['Items']

with table.batch_writer() as batch:
    for item in items:
        batch.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})

print(f"Dados do usuário {USER_ID} removidos com sucesso.")
