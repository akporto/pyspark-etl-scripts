import os
import boto3
import shutil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_date, current_date, datediff, month, year, concat_ws, lit, when, sum as spark_sum

REGION = os.getenv('AWS_DEFAULT_REGION')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME')
USER_ID = os.getenv('USER_ID')
PK_VALUE = f"USER#{USER_ID}"

spark = SparkSession.builder.appName("DynamoDBReport").getOrCreate()

dynamodb = boto3.resource(
    "dynamodb",
    region_name=REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
table = dynamodb.Table(TABLE_NAME)

hoje = datetime.today()
inicio_periodo = hoje - timedelta(days=183)
inicio_sk = f"LIST#{inicio_periodo.strftime('%Y%m%d')}"
fim_sk = f"LIST#{hoje.strftime('%Y%m%d')}"

response = table.query(
    KeyConditionExpression=boto3.dynamodb.conditions.Key("PK").eq(PK_VALUE) & boto3.dynamodb.conditions.Key("SK").between(inicio_sk, fim_sk)
)
dados = response['Items']

if not dados:
    print("Nenhum dado retornado.")
else:
    df = spark.createDataFrame(dados)
    df = df.withColumn("created_at", to_date("created_at"))
    df = df.withColumn("dias_ate_hoje", datediff(current_date(), col("created_at")))

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

    df_pivot = df_pivot.withColumn("Total Geral", sum([col(m) for m in meses]))

    tipos_esperados = ["Tarefa a Ser Feita", "Item de Compra"]
    tipos_presentes = [row["task_type"] for row in df_pivot.select("task_type").distinct().collect()]
    faltantes = [t for t in tipos_esperados if t not in tipos_presentes]

    if faltantes:
        zeros = {m: 0 for m in meses}
        for tipo in faltantes:
            linha_zerada = Row(task_type=tipo, **zeros, **{"Total Geral": 0})
            df_pivot = df_pivot.union(spark.createDataFrame([linha_zerada]))

    df_pivot = df_pivot.select(["task_type"] + meses + ["Total Geral"])

    output_dir = "relatorio_tarefas_transposto"
    output_file = "relatorio_tarefas_final.csv"

    df_pivot.coalesce(1).write.option("header", True).mode("overwrite").csv(output_dir)

    for file in os.listdir(output_dir):
        if file.endswith(".csv"):
            shutil.move(os.path.join(output_dir, file), output_file)
            break

    shutil.rmtree(output_dir)
    print(f"Relatório salvo como: {output_file}")

    from google.colab import files
    files.download(output_file)
