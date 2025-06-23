# PySpark ETL Scripts com DynamoDB

Este repositório contém três scripts desenvolvidos em PySpark integrados ao AWS DynamoDB, voltados para operações de ETL, análise de dados e manutenção de registros por `user_id`.

🔗 **Este repositório faz parte do projeto:** [akporto/aws-terraform-labs](https://github.com/akporto/aws-terraform-labs)

---

## 🧩 Visão Geral dos Scripts

### 1. `envio_dynamodb.py`

Realiza a leitura de um arquivo `.csv`, filtra e transforma os dados, e envia os registros para uma tabela DynamoDB com chaves compostas (`PK` e `SK`).

- **Entrada esperada:** [`dados_importar.csv`](./dados_importar.csv) com dados de tarefas fictícias.
- **Lógica de envio:** Para evitar sobrecarga e eventuais erros de throughput no DynamoDB, o script utiliza envio por lotes com atraso controlado:
  ```python
  BATCH_SIZE = 20
  DELAY_BETWEEN_BATCHES_SECONDS = 0.2
  ```
  Isso reduz a chance de throttling e melhora a confiabilidade ao lidar com grandes volumes de dados.
- **Pré-requisitos:**
  - Tabela DynamoDB com chave primária composta (`PK`, `SK`)
  - PySpark e Boto3 configurados
- **Variáveis de ambiente necessárias:**
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_DEFAULT_REGION`
  - `USER_ID`
  - `DYNAMODB_TABLE_NAME`

---

### 2. `analise_abandono.py`

Consulta os registros de um `user_id` no DynamoDB e gera um relatório de tarefas/itens abandonados nos últimos 6 meses.

- **Critérios de abandono:**
  - Tarefa a Ser Feita: `status = TODO` e mais de 15 dias de criação
  - Item de Compra: `status = TODO` e mais de 30 dias de criação
- **Saída gerada:** relatório transposto em `.csv` com contagem mensal por tipo de tarefa.
- **Exemplo de relatório gerado:**

```
+------------------+------+------+------+------+------+------+-----------+
|         task_type|2025-1|2025-2|2025-3|2025-4|2025-5|2025-6|Total Geral|
+------------------+------+------+------+------+------+------+-----------+
|    Item de Compra|     8|     4|     6|     7|     3|     0|         28|
|Tarefa a Ser Feita|     7|    19|    12|    13|    11|     0|         62|
+------------------+------+------+------+------+------+------+-----------+
```

---

### 3. `deletar_usuario.py`

Deleta todos os registros de um usuário (`user_id`) da tabela DynamoDB com base na `PK`.

---

## 🔧 Como Usar

### 1. Configure suas credenciais como variáveis de ambiente:

```python
import os

os.environ["AWS_ACCESS_KEY_ID"] = "sua_access_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "sua_secret_key"
os.environ["AWS_DEFAULT_REGION"] = "sa-east-1"
os.environ["USER_ID"] = "uuid-do-usuario"
os.environ["DYNAMODB_TABLE_NAME"] = "nome-da-tabela"
```

> Ou use um arquivo `.env` com suporte do `python-dotenv`.

---

### 2. Execute os scripts em ambiente PySpark:

- Google Colab (recomendado para protótipos)
- Amazon EMR
- Databricks

---

## 📁 Estrutura do Projeto

```
.
├── dados_importar.csv             # Arquivo de amostragem com dados fictícios
├── envio_dynamodb.py             # Envio para DynamoDB com BATCH e delay
├── analise_abandono.py           # Geração de relatório de tarefas abandonadas
├── deletar_usuario.py            # Exclusão de registros por usuário
└── README.md
```

