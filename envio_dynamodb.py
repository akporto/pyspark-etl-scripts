import os
import uuid
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# CONFIGURAÇÃO (via variáveis de ambiente)
REGION = os.getenv('AWS_DEFAULT_REGION')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME')
USER_ID = os.getenv('USER_ID')

spark = SparkSession.builder.appName("ExportToDynamoDB").getOrCreate()

df = spark.read.option("header", True).option("delimiter", ";").csv("amostragem.csv")
df_filtered = df.filter((col("Usuário") == "Jeferson Klau") & col("Status").isin("1", "2"))
df_classificado = df_filtered.withColumn(
    "Status Descrição", when(col("Status Descrição") == "Concluído", "DONE").otherwise("TODO")
)
df_final = df_classificado.drop("Tipo da Tarefa ID", "Status", "Usuário")
df_final = df_final.withColumn("user_id", lit(USER_ID))

df_simplificado = df_final.select(
    col("`Nome da Tarefa`").alias("name"),
    col("`Tipo da Tarefa`").alias("task_type"),
    col("`Data de Criação`").alias("created_at"),
    col("`Data de Conclusão`").alias("completed_at"),
    col("Status Descrição").alias("status"),
    col("user_id")
)

records = df_simplificado.toJSON().map(lambda x: eval(x)).collect()

def build_dynamodb_item(row):
    created_date_str = row["created_at"][:10].replace("-", "")
    item_id = str(uuid.uuid4())
    item = {
        "PK": f"USER#{row['user_id']}",
        "SK": f"LIST#{created_date_str}#ITEM#{item_id}",
        "name": row["name"],
        "task_type": row["task_type"],
        "status": row["status"].upper(),
        "created_at": row["created_at"],
        "item_id": item_id
    }
    if row.get("completed_at"):
        item["completed_at"] = row["completed_at"]
    if row.get("scheduled_for"):
        item["scheduled_for"] = row["scheduled_for"]
    return item

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

with table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as batch:
    for row in records:
        try:
            item = build_dynamodb_item(row)
            batch.put_item(Item=item)
        except Exception as e:
            print(f"Erro ao inserir item: {row.get('name', '')} — {str(e)}")

print(" Dados enviados para o DynamoDB com sucesso!")
