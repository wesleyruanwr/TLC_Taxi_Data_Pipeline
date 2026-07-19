# Urban Mobility Analytics - TLC Taxi Data Pipeline

Este projeto implementa um pipeline de dados de ponta a ponta utilizando a **arquitetura medalhão** para ingestão, limpeza e modelagem analítica dos dados de corridas de táxi da TLC de Nova York (Yellow Taxi) de 2025.

---

## Tecnologias Utilizadas e Justificativas

### Python
Linguagem principal do projeto. Escolhida por ser o padrão da indústria para engenharia de dados, possuir vasto ecossistema de bibliotecas (requests, pandas, psycopg2) e ser a linguagem nativa do Airflow e do PySpark. Utilizada no módulo de ingestão da camada Bronze para download dos arquivos Parquet da TLC

### PySpark (Apache Spark 3.5.1)
Utilizado no processamento da camada Silver. O PySpark foi escolhido por oferecer processamento distribuído e escalável para grandes volumes de dados. Os arquivos da TLC possuem milhões de registros por mês, e o Spark lida com esse volume de forma eficiente através de lazy evaluation e paralelismo

### PostgreSQL 15
DB relacional utilizado como DW do projeto. Escolhido por ser robusto, open source, suportar schemas (essencial para a separação das camadas medalhão), materialized views nativas e ter excelente integração com o dbt

### dbt (Data Build Tool) 1.7.4
Utilizado para a modelagem analítica da camada Gold. O dbt foi escolhido por permitir transformações SQL versionadas, modularizadas e testáveis. Ele transforma os dados da Silver em tabelas fato, dimensão e views materializadas de forma declarativa, com rastreabilidade (lineage) e testes de qualidade integrados

### Apache Airflow 2.9.2
Orquestrador de workflows utilizado para agendar e executar o pipeline. Escolhido por ser o padrão de mercado para orquestração de pipelines de dados, oferecendo interface web para monitoramento, suporte a DAGs em Python, gestão de dependências entre tasks, retries automáticos e logs detalhados. A execução semanal e incremental pedida é nativamente suportada

### Docker & Docker Compose
Utilizados para containerizar todos os serviços (PostgreSQL, Airflow webserver, scheduler). Garantem reprodutibilidade do ambiente, eliminando problemas de "funciona na minha maquina" e facilitando o deploy e avaliação do projeto

---

## Estrutura de Pastas do Projeto

```
proj_aquila/
├── dags/
│   └── taxi_pipeline.py          # DAG do Airflow para orquestração
├── scripts/
│   ├── download_data.py          # Módulo Python - ingestão Bronze
│   └── process_silver.py         # Job PySpark - processamento Silver
├── dbt_project/
│   ├── dbt_project.yml           # Configuração do projeto dbt
│   ├── profiles.yml              # Conexão do dbt com PostgreSQL
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/gold/
│       ├── schema.yml            # Sources, testes de qualidade
│       ├── fct_trips.sql         # Tabela fato de corridas
│       ├── dim_payment_types.sql # Tabela dimensão de pagamentos
│       ├── dim_vendors.sql       # Tabela dimensão de provedores
│       ├── dim_rate_codes.sql    # Tabela dimensão de tipos de tarifa
│       └── mv_monthly_indicators.sql # View materializada de indicadores
├── sql/
│   └── init.sql                  # DDL: schemas, tabela payment_types, permissões
├── data/
│   └── bronze/                   # Arquivos Parquet da TLC (gerados pelo pipeline)
├── docker-compose.yml            # Serviços: PostgreSQL, Airflow
├── Dockerfile                    # Imagem customizada do Airflow com Java, PySpark, dbt
├── init-db.sh                    # Script de inicialização dos bancos no PostgreSQL
├── notebooks/
│   └── analise_ny_taxi.ipynb     # Jupyter Notebook para análise exploratória
└── README.md                     # Este arquivo
```

---

## Instruções de Execução

### Pré-requisitos
- Docker & Docker Compose instalados
- Docker Desktop rodando

## Ajuste de permissões em Linux
Em ambiente Linux antes de subir os containers garanta que o usuario airflow consiga escrever no dbt no diretorio montado:

```bash
sudo chgrp -R 0 dbt_project
sudo chmod -R g+rwX dbt_project
```

Esse ajuste evita erro de permissão no dbt_run logs e target

### Iniciando o ambiente

1. **Configure as variáveis de ambiente:**
   ```bash
   cp .env.example .env
   ```

   Ajuste o arquivo `.env` se quiser usar outros usuários, senhas ou nomes de banco

2. **Suba os containers:**
   ```bash
   docker compose up --build -d
   ```

3. **Acesse o Airflow:**
   - **URL**: [http://localhost:8080](http://localhost:8080)
   - **Usuário**: valor de `_AIRFLOW_WWW_USER_USERNAME` no `.env` (padrão: `admin`)
   - **Senha**: valor de `_AIRFLOW_WWW_USER_PASSWORD` no `.env` (padrão: `admin`)

4. **Ative a DAG** `tlc_yellow_taxi_pipeline`

5. **Execute o pipeline para pelo menos 6 meses:**
   - Clique em **"Trigger DAG w/ config"**
   - Use os parâmetros padrão ou informe:
     ```json
     {"year": "2025", "start_month": "1", "end_month": "6"}
     ```
   - Com esses parâmetros, uma única execução baixa e processa os meses de janeiro a junho de 2025.
   - Para outra faixa, altere `start_month` e `end_month`.

   A DAG também fica agendada semanalmente (`@weekly`) as recargas são idempotentes por lote mensal (`source_year_month`), evitando duplicidade quando a mesma faixa for executada novamente.

6. **Acesse o banco de dados** pelo DBeaver ou terminal:
   - **Host**: `localhost` | **Porta**: `5432`
   - **Banco**: valor de `NY_TAXI_DB_NAME` no `.env` (padrão: `ny_taxi`)
   - **Usuário**: valor de `POSTGRES_USER` no `.env` (padrão: `postgres`)
   - **Senha**: valor de `POSTGRES_PASSWORD` no `.env` (padrão: `postgres`)
   ```bash
   docker compose exec postgres psql -U postgres -d ny_taxi
   ```
   Se você alterou `POSTGRES_USER` ou `NY_TAXI_DB_NAME` no `.env`, use esses mesmos valores no comando acima.

### Parando o ambiente
```bash
docker compose down
```

### Parando e removendo todos os dados
```bash
docker compose down -v
```

---

## Análise Exploratória (Jupyter Notebook)

O projeto tem um notebook de análise exploratória (`analise_ny_taxi.ipynb`) localizado em `notebooks/analise_ny_taxi.ipynb` que se conecta diretamente ao banco PostgreSQL para gerar gráficos e análises estatísticas baseadas nos dados consolidados das camadas **Silver** e **Gold**.

Abaixo está o passo a passo completo para executar a análise localmente:

### Passo 1: Subir a Infra (Docker)
Antes de abrir o notebook, garanta que o banco de dados PostgreSQL esteja ativo e com a porta 5432 liberada para a máquina local:
```bash
docker compose up -d
```
*Certifique-se de que a DAG do Airflow `tlc_yellow_taxi_pipeline` já tenha sido executada pelo menos uma vez para que as tabelas nas camadas `silver` e `gold` contenham dados.*

### Passo 2: Configurar o Ambiente Python na Máquina Local
Abra o terminal na pasta raiz do projeto e instale as dependências listadas no `requirements.txt`:
```bash
pip install -r requirements.txt
```

### Passo 3: Credenciais de Acesso
O notebook lê automaticamente o arquivo `.env` da raiz do projeto e usa as variáveis:

```text
POSTGRES_USER
POSTGRES_PASSWORD
NY_TAXI_DB_NAME
```

Se não tiver `.env`  o notebook usa os valores padrão do `.env.example`:

```text
postgresql://postgres:postgres@localhost:5432/ny_taxi
```

* **Host**: `localhost`
* **Porta**: `5432`

> **Execução local dos scripts (fora do Docker):** `download_data.py` e `process_silver.py` conectam por padrão ao host `postgres` (nome do serviço no Docker Compose). Para rodá-los direto na máquina tem q exportar `DB_HOST=localhost` e se preciso o `DB_PORT`) antes de executar.

### Passo 4: Executar o Servidor Jupyter
Inicie o Jupyter Notebook executando o comando a partir do terminal na raiz do projeto:
```bash
jupyter notebook notebooks/analise_ny_taxi.ipynb
```
Ou
```bash
python -m jupyter notebook notebooks/analise_ny_taxi.ipynb
```
O servidor Jupyter será iniciado e uma nova janela do seu navegador será aberta automaticamente com o notebook aberto. Se não abrir automaticamente copie o link gerado no terminal (geralmente contendo `http://127.0.0.1:8888/?token=...`) e cole no  navegador

### Passo 5: Executar as Células do Notebook
No menu do Jupyter clicar em **Cell** -> **Run All** (ou `Shift + Enter`) para:
1. Conectar ao PostgreSQL.
2. Gerar volumetria comparativa entre viagens válidas e rejeitadas.
3. Exibir os motivos de invalidação mais frequentes.
4. Listar e plotar os tipos de pagamento mais usados.
5. Renderizar gráficos de distribuição de distância e ticket médio.

---

## Dicionário de Dados

### Camada Bronze — `data/bronze/`

Arquivos Parquet brutos baixados diretamente da TLC, sem nenhuma transformação

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| VendorID | int | Código do provedor de tecnologia (1 = CMT, 2 = Curb/VeriFone, 6 = Myle, 7 = Helix) |
| tpep_pickup_datetime | timestamp | Data e hora do início da corrida |
| tpep_dropoff_datetime | timestamp | Data e hora do fim da corrida |
| passenger_count | long | Número de passageiros informado pelo motorista |
| trip_distance | double | Distância da viagem em milhas, registrada pelo taxímetro |
| RatecodeID | long | Código da tarifa (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group) |
| store_and_fwd_flag | string | Se a viagem foi armazenada antes de enviar ao servidor (Y/N) |
| PULocationID | int | Código da zona de embarque (TLC Taxi Zone) |
| DOLocationID | int | Código da zona de desembarque (TLC Taxi Zone) |
| payment_type | long | Tipo de pagamento (1=Cartão, 2=Dinheiro, 3=Sem cobrança, 4=Disputa, 5=Desconhecido, 6=Viagem cancelada) |
| fare_amount | double | Valor da tarifa calculada pelo taxímetro |
| extra | double | Extras diversos (ex: rush hour, overnight) |
| mta_tax | double | Taxa MTA de $0.50 |
| tip_amount | double | Gorjeta (preenchida automaticamente para pagamentos em cartão) |
| tolls_amount | double | Total de pedágios da viagem |
| improvement_surcharge | double | Sobretaxa de melhoria de $0.30 |
| total_amount | double | Valor total cobrado ao passageiro (não inclui gorjeta em dinheiro) |
| congestion_surcharge | double | Sobretaxa de congestionamento |
| Airport_fee | double | Taxa de aeroporto ($1.25 para embarque em LaGuardia/JFK) |

---

### Camada Silver — `silver.trips`

Dados tratados pelo PySpark com colunas calculadas de qualidade e temporais. **Todas** as corridas da competência são mantidas (não há descarte): as corridas que falham em alguma regra permanecem na tabela, sinalizadas via `is_valid_trip = false` e `invalid_reason`, permitindo auditoria e análise de causa raiz sem perder a volumetria total do mês.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| VendorID | int | Código do provedor de tecnologia |
| tpep_pickup_datetime | timestamp | Data e hora do início da corrida |
| tpep_dropoff_datetime | timestamp | Data e hora do fim da corrida |
| passenger_count | long | Número de passageiros |
| trip_distance | double | Distância em milhas |
| RatecodeID | long | Código da tarifa |
| store_and_fwd_flag | string | Flag de armazenamento offline |
| PULocationID | int | Zona de embarque |
| DOLocationID | int | Zona de desembarque |
| payment_type | long | Tipo de pagamento |
| fare_amount | double | Valor da tarifa |
| extra | double | Extras |
| mta_tax | double | Taxa MTA |
| tip_amount | double | Gorjeta |
| tolls_amount | double | Pedágios |
| improvement_surcharge | double | Sobretaxa de melhoria |
| total_amount | double | Valor total |
| congestion_surcharge | double | Sobretaxa de congestionamento |
| Airport_fee | double | Taxa de aeroporto |
| **source_year_month** | **string** | **Lote de origem do arquivo processado no formato yyyyMM** |
| **pickup_date** | **date** | **Data do embarque (derivada de tpep_pickup_datetime)** |
| **pickup_year_month** | **string** | **Competência mensal no formato yyyyMM** |
| **trip_duration_minutes** | **double** | **Duração da viagem em minutos** |
| **is_valid_trip** | **boolean** | **Flag indicando se a viagem passou em todas as regras de qualidade** |
| **invalid_reason** | **string** | **Motivos de invalidação concatenados por "; " (null se válida)** |

**Regras de qualidade aplicadas (9 regras):**

| Regra | Flag gerada | Descrição |
|-------|-------------|-----------|
| 1 | `anomaly_duration_gt_6h` | Duração superior a 6 horas (360 min) |
| 2 | `anomaly_distance_gt_100mi` | Distância superior a 100 milhas |
| 3 | `invalid_dates_dropoff_before_pickup` | Data de desembarque anterior ou igual à de embarque |
| 4 | `negative_distance` | Distância negativa |
| 5 | `negative_total_amount` | Valor total negativo |
| 6 | `invalid_passenger_count` | Quantidade de passageiros menor/igual a 0 ou maior que 8 (anômalo) |
| 7 | `invalid_ratecode_id` | Código de tarifa fora do escopo válido (1 a 6) |
| 8 | `invalid_pickup_location` | ID do local de embarque fora do mapeamento da TLC (1 a 265) |
| 9 | `invalid_dropoff_location` | ID do local de desembarque fora do mapeamento da TLC (1 a 265) |

---

### Camada Silver — `silver.payment_types`

Tabela de referência de tipos de pagamento, criada manualmente conforme especificação.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| payment_type | int (PK) | Código do tipo de pagamento |
| payment_description | varchar(50) | Descrição textual do tipo |
| is_valid_payment | boolean | Se o pagamento é considerado receita válida |

**Dados:**

| payment_type | payment_description | is_valid_payment |
|---|---|---|
| 1 | Credit card | true |
| 2 | Cash | true |
| 3 | No charge | false |
| 4 | Dispute | false |
| 5 | Unknown | false |
| 6 | Voided trip | false |

---

### Camada Gold — `gold.fct_trips`

Tabela fato principal gerada pelo dbt a partir da `silver.trips`.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| vendor_id | int | Código do provedor (renomeado de VendorID) |
| pickup_datetime | timestamp | Data/hora do embarque |
| dropoff_datetime | timestamp | Data/hora do desembarque |
| passenger_count | long | Número de passageiros |
| trip_distance | double | Distância em milhas |
| rate_code_id | long | Código da tarifa (renomeado de RatecodeID) |
| store_and_fwd_flag | string | Flag de armazenamento offline |
| pickup_location_id | int | Zona de embarque (renomeado de PULocationID) |
| dropoff_location_id | int | Zona de desembarque (renomeado de DOLocationID) |
| payment_type | long | Tipo de pagamento (FK para dim_payment_types) |
| fare_amount | double | Valor da tarifa |
| extra | double | Extras |
| mta_tax | double | Taxa MTA |
| tip_amount | double | Gorjeta |
| tolls_amount | double | Pedágios |
| improvement_surcharge | double | Sobretaxa de melhoria |
| total_amount | double | Valor total |
| congestion_surcharge | double | Sobretaxa de congestionamento |
| airport_fee | double | Taxa de aeroporto (renomeado de Airport_fee) |
| source_year_month | string | Lote de origem do arquivo processado no formato yyyyMM |
| pickup_date | date | Data do embarque |
| pickup_year_month | string | Competência mensal (yyyyMM) |
| trip_duration_minutes | double | Duração em minutos |
| is_valid_trip | boolean | Flag de validade |
| invalid_reason | string | Motivos de invalidação |

---

### Camada Gold — `gold.dim_payment_types`

Tabela dimensão de tipos de pagamento gerada pelo dbt a partir da `silver.payment_types`.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| payment_type | int | Código do tipo de pagamento |
| payment_description | varchar | Descrição textual |
| is_valid_payment | boolean | Se é considerado receita válida |

---

### Camada Gold — `gold.dim_vendors`

Tabela dimensão dos provedores de tecnologia dos táxis.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| vendor_id | int | Código do provedor |
| vendor_description | varchar | Nome do provedor (CMT ou VeriFone) |

**Dados:**

| vendor_id | vendor_description |
|---|---|
| 1 | Creative Mobile Technologies, LLC (CMT) |
| 2 | Curb Mobility, LLC (ex-VeriFone/VTS) |
| 6 | Myle Technologies Inc |
| 7 | Helix |

---

### Camada Gold — `gold.dim_rate_codes`

Tabela dimensão dos tipos de tarifa das corridas.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| rate_code_id | int | Código da tarifa |
| rate_code_description | varchar | Descrição do tipo de tarifa |

**Dados:**

| rate_code_id | rate_code_description |
|---|---|
| 1 | Standard rate |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau/Westchester |
| 5 | Negotiated fare |
| 6 | Group ride |

---

### Camada Gold — `gold.mv_monthly_indicators`


View materializada com indicadores mensais agregados.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| month_yyyymm | string | Competência mensal no formato yyyyMM |
| vendor_id | int | Código do provedor |
| total_rides | long | Total de corridas no mês (todas as corridas da competência) |
| total_valid_amount | double | Valor total apenas de pagamentos válidos (`is_valid_payment = true` ou sej cartão/dinheiro) |
| avg_ticket_amount | double | Ticket médio, considerando apenas corridas com pagamento válido |
| avg_distance | double | Distância média das corridas |
