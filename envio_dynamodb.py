import os
import uuid
import boto3
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from dotenv import load_dotenv

# Carrega variáveis de ambiente do .env 
load_dotenv()

# CREDENCIAIS SEGURAS VIA VARIÁVEIS DE AMBIENTE
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'sa-east-1')

# CONFIGURAÇÃO
REGION = AWS_DEFAULT_REGION
TABLE_NAME = "hello-terraform-dev-market-list"
USER_ID = "834c6aea-10a1-70e0-e798-1fd684bcbdcd"

BATCH_SIZE = 20
DELAY_BETWEEN_BATCHES_SECONDS = 0.2

spark = SparkSession.builder.appName("ExportToDynamoDB").getOrCreate()

# ETL - TRANSFORMAÇÃO
df = spark.read.option("header", True).option("delimiter", ";").csv("dados_importar.csv")

df_filtered = df.filter((col("Usuário") == "João João") & col("Status").isin("1", "2"))

df_classificado = df_filtered.withColumn(
    "Status Descrição",
    when(col("Status Descrição") == "Concluído", "DONE").otherwise("TODO")
)

df_final = df_classificado.drop("Tipo da Tarefa ID", "Status", "Usuário")
df_final = df_final.withColumn("user_id", lit(USER_ID))

df_simplificado = df_final.select(
    col("Nome da Tarefa").alias("name"),
    col("Tipo da Tarefa").alias("task_type"),
    col("Data de Criação").alias("created_at"),
    col("Data de Conclusão").alias("completed_at"),
    col("Status Descrição").alias("status"),
    col("user_id")
)

# Converte DataFrame para lista de dicionários
records = df_simplificado.toJSON().map(lambda x: eval(x)).collect()

# FUNÇÃO PARA MONTAR ITEM DYNAMODB
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

# ENVIO PARA DYNAMODB COM BATCHES
dynamodb = boto3.resource(
    "dynamodb",
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

table = dynamodb.Table(TABLE_NAME)

total_records = len(records)
print(f"Total de registros a serem enviados: {total_records}")

for i in range(0, total_records, BATCH_SIZE):
    batch_records = records[i:i + BATCH_SIZE]
    current_batch_number = (i // BATCH_SIZE) + 1
    print(f"Enviando lote {current_batch_number} com {len(batch_records)} registros...")

    with table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as batch:
        for row in batch_records:
            try:
                item = build_dynamodb_item(row)
                batch.put_item(Item=item)
            except Exception as e:
                print(f"Erro ao inserir item '{row.get('name', '')}': {str(e)}")

    print(f"Lote {current_batch_number} enviado com sucesso.")
    time.sleep(DELAY_BETWEEN_BATCHES_SECONDS)

print("Todos os dados foram enviados para o DynamoDB com sucesso!")
