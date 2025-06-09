# PySpark ETL Scripts com DynamoDB

Este repositório contém três scripts desenvolvidos em PySpark integrados ao AWS DynamoDB, voltados para operações de ETL, análise de dados e manutenção de registros por `user_id`.

🔗 **Este repositório faz parte do projeto:** [akporto/aws-terraform-labs](https://github.com/akporto/aws-terraform-labs)

## 🧩 Visão Geral dos Scripts

### 1. `envio_dynamodb.py`
Realiza a leitura de um arquivo `.csv`, filtra e transforma os dados, e envia para uma tabela DynamoDB com chaves compostas (`PK` e `SK`).

- Pré-requisitos:
  - Um CSV chamado `amostragem.csv`
  - Tabela DynamoDB com chave primária composta
- Variáveis esperadas via ambiente:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_DEFAULT_REGION`
  - `USER_ID`
  - `DYNAMODB_TABLE_NAME`

---

### 2. `analise_abandono.py`
Consulta dados de um usuário no DynamoDB, identifica tarefas abandonadas nos últimos 6 meses e gera um relatório transposto `.csv`.

- Critérios de abandono:
  - "Tarefa a Ser Feita" com mais de 15 dias e status "TODO"
  - "Item de Compra" com mais de 30 dias e status "TODO"
- Exporta relatório com contagem mensal e total
- Faz download automático no Colab

---

### 3. `deletar_usuario.py`
Deleta todos os registros de um usuário (`user_id`) da tabela DynamoDB.

---

## 🔧 Como Usar

### 1. Configure suas credenciais (no ambiente ou no Colab):

```python
import os
os.environ["AWS_ACCESS_KEY_ID"] = "sua_access_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "sua_secret_key"
os.environ["AWS_DEFAULT_REGION"] = "sa-east-1"
os.environ["USER_ID"] = "uuid-do-usuario"
os.environ["DYNAMODB_TABLE_NAME"] = "nome-da-tabela"
```

### 2. Execute os scripts no ambiente PySpark (Colab, EMR, Databricks).

##📁 Estrutura

.
├── envio_dynamodb.py
├── analise_abandono.py
├── deletar_usuario.py
└── README.md

