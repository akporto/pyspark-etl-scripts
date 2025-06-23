import os
import uuid
import boto3
import shutil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, to_date, current_date, datediff, month, year,
    concat_ws, lit, when, sum as spark_sum
)
from functools import reduce
from dotenv import load_dotenv

# Carrega variáveis de ambiente de .env 
load_dotenv()

# CREDENCIAIS SEGURAS
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'sa-east-1')

# CONFIGURAÇÃO
REGION = AWS_DEFAULT_REGION
TABLE_NAME = "hello-terraform-dev-market-list"
USER_ID = "834c6aea-10a1-70e0-e798-1fd684bcbdcd"
PK_VALUE = f"USER#{USER_ID}"

# Inicializa Spark
print("Inicializando a sessão Spark...")
spark = SparkSession.builder.appName("DynamoDBReport").getOrCreate()

# CONEXÃO COM DYNAMODB
print(f"Conectando à tabela {TABLE_NAME} na região {REGION}...")
dynamodb = boto3.resource(
    "dynamodb",
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
table = dynamodb.Table(TABLE_NAME)

# Consulta paginada
all_items = []
last_evaluated_key = None
print(f"Iniciando consulta para o usuário: {USER_ID}...")

while True:
    query_args = {
        "KeyConditionExpression": boto3.dynamodb.conditions.Key("PK").eq(PK_VALUE)
    }
    if last_evaluated_key:
        query_args["ExclusiveStartKey"] = last_evaluated_key

    response = table.query(**query_args)
    all_items.extend(response.get("Items", []))
    last_evaluated_key = response.get("LastEvaluatedKey")
    if not last_evaluated_key:
        break

dados = all_items
print(f"{len(dados)} registros recuperados.")

if not dados:
    print("Nenhum dado retornado.")
else:
    df = spark.createDataFrame(dados)
    df = df.withColumn("created_at", to_date("created_at"))
    df = df.withColumn("dias_ate_hoje", datediff(current_date(), col("created_at")))

    hoje = datetime.today()
    inicio_periodo = hoje - timedelta(days=183)
    df = df.filter(col("created_at") >= lit(inicio_periodo.strftime('%Y-%m-%d')))

    df_abandonadas = df.filter(
        ((col("task_type") == "Tarefa a Ser Feita") & (col("status") == "TODO") & (col("dias_ate_hoje") > 15)) |
        ((col("task_type") == "Item de Compra") & (col("status") == "TODO") & (col("dias_ate_hoje") > 30))
    )

    df_abandonadas = df_abandonadas.withColumn("mes_abandono", concat_ws("-", year("created_at"), month("created_at")))
    meses = [(hoje - relativedelta(months=i)).strftime("%Y-%-m") for i in range(5, -1, -1)]
    df_filtrado = df_abandonadas.filter(col("mes_abandono").isin(meses))

    df_grouped = df_filtrado.groupBy("task_type", "mes_abandono").count()
    df_pivot = df_grouped.groupBy("task_type").pivot("mes_abandono", meses).sum("count")

    for m in meses:
        df_pivot = df_pivot.withColumn(m, when(col(m).isNull(), 0).otherwise(col(m).cast("int")))

    sum_expression = reduce(lambda a, b: a + col(b), meses[1:], col(meses[0]))
    df_pivot = df_pivot.withColumn("Total Geral", sum_expression)

    tipos_presentes = [row['task_type'] for row in df_pivot.select("task_type").distinct().collect()]
    tipos_esperados = ["Tarefa a Ser Feita", "Item de Compra"]
    faltantes = [t for t in tipos_esperados if t not in tipos_presentes]
    zeros = {m: 0 for m in meses}

    for tipo in faltantes:
        linha_zerada = Row(task_type=tipo, **zeros, **{"Total Geral": 0})
        df_pivot = df_pivot.union(spark.createDataFrame([linha_zerada]))

    df_pivot = df_pivot.select(["task_type"] + meses + ["Total Geral"])
    df_pivot.show()

    output_dir = "relatorio_tarefas_transposto"
    output_file = "relatorio_tarefas_final.csv"

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    df_pivot.coalesce(1).write.option("header", True).mode("overwrite").csv(output_dir)

    for file in os.listdir(output_dir):
        if file.endswith(".csv"):
            shutil.move(os.path.join(output_dir, file), output_file)
            break

    shutil.rmtree(output_dir)

    try:
        from google.colab import files
        files.download(output_file)
    except ImportError:
        print("Execução local: arquivo gerado.")

print("Relatório concluído.")
