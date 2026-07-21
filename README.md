# Urban Mobility Analytics

Pipeline de dados para análise de corridas de táxi de Nova York (NYC TLC Yellow Taxi), construído em arquitetura medalhão (bronze/silver/gold), orquestrado semanalmente pelo Airflow.

O código do pipeline está em [`urban-mobility-analytics/`](urban-mobility-analytics/).

## Arquitetura

`NYC TLC → BRONZE → SILVER → GOLD`

- **Fonte (NYC TLC)** — arquivos parquet públicos, um por competência mensal (`yellow_tripdata_YYYY-MM.parquet`).
- **Bronze** — dado bruto, sem transformação, mais colunas de linhagem (`source_file`, `source_year_month`, `ingested_at`) que permitem rastrear origem e reprocessar uma competência sem duplicar. Vive como parquet em `data/bronze/` e como `bronze.yellow_tripdata` no Postgres, junto da tabela de referência `bronze.payment_type_reference`.
- **Silver** — dado tratado pelo job PySpark: regras de qualidade, colunas derivadas (`pickup_date`, `pickup_year_month`, `trip_duration_minutes`), flags de anomalia e de receita válida (ver [Regras de negócio](#regras-de-negócio--camada-silver)). Vive como parquet particionado por mês em `data/silver/` e como `silver.trips` no Postgres.
- **Gold** — modelagem dimensional feita pelo dbt a partir da silver: dimensões (`dim_date`, `dim_vendor`, `dim_payment_type`), fato (`fct_trips`) e a materialized view de indicadores (`mv_monthly_indicators`) — todas no schema `gold` do Postgres (ver [Indicadores](#indicadores--camada-gold)).

Cada camada tem seu próprio schema no Postgres (`bronze`, `silver`, `gold`), e a bronze/silver também existem como parquet no disco (o data lake de fato), servindo o Postgres como camada de consulta/curadoria.

## Por que cada ferramenta

- **Python** — orquestra a ingestão: download incremental por competência, controle de idempotência (não duplica ao reprocessar um mês), carga na bronze.
- **PySpark** — processa bronze → silver. Volume mensal na casa dos milhões de linhas justifica um motor de processamento distribuído em vez de pandas puro; roda em modo local (sem cluster) já que o volume cabe confortavelmente numa única máquina.
- **PostgreSQL** — repositório analítico único para as três camadas, com suporte nativo a materialized view (requisito da camada gold) e um driver JDBC maduro para o Spark escrever nele.
- **dbt** — modelagem declarativa de silver → gold (dimensões, fato, materialized view), com testes de qualidade e lineage versionados junto do código, em vez de scripts SQL soltos.
- **Airflow** — orquestração agendada (semanal), com retries, resolução de competência configurável e histórico de execuções.

## Estrutura do projeto

```
urban-mobility-analytics/
├── docker-compose.yml       # Postgres + Airflow (com PySpark, dbt, driver JDBC)
├── airflow/                 # orquestração
│   ├── Dockerfile
│   └── dags/urban_mobility_dag.py
├── sql/00_create_schemas.sql  # único DDL cross-camada (cria bronze/silver/gold)
├── src/
│   ├── bronze/              # ingestão: download_tlc_data.py + sql/ da bronze
│   ├── silver/               # tratamento: silver_job.py (PySpark) + sql/ da silver
│   └── common/               # settings.py, engine do Postgres, bootstrap do banco
├── dbt/urban_mobility/models/
│   ├── staging/              # views 1:1 sobre as fontes (stg_trips)
│   ├── dim/                   # dim_date, dim_vendor, dim_payment_type
│   ├── fact/                  # fct_trips
│   └── view/                  # mv_monthly_indicators (materialized view)
├── tests/                    # pytest (regras de qualidade da silver)
├── data/                     # data lake local (bronze/silver, ignorado pelo git)
└── docs/
    ├── execution.md          # passo a passo de execução
    └── results.md            # amostra real do resultado da MV
```

Cada tabela de `dim/`, `fact/` e `view/` tem seu próprio arquivo `.yml` de testes/documentação ao lado do `.sql` (ex.: `_dim_date.yml`, `_fct_trips.yml`), em vez de um arquivo único cobrindo todas — mais fácil de achar o teste de uma tabela específica e de revisar em PRs pequenos.

### Por que existe uma camada de staging

`stg_trips` é uma view fina (1:1) sobre `silver.trips`: só seleciona e nomeia colunas, sem join, agregação ou regra de negócio. Os modelos de `dim/` e `fact/` referenciam essa view via `ref('stg_trips')` em vez de consultar `silver.trips` diretamente. Isso isola a modelagem gold de mudanças na origem — se uma coluna da silver for renomeada ou o formato mudar, o ajuste é feito só no staging, sem tocar em cada dimensão/fato que depende dela.

## Quick start

```bash
cd urban-mobility-analytics
cp .env.example .env
docker compose up -d postgres
python -m venv .venv && .venv/bin/pip install SQLAlchemy==1.4.52 psycopg2-binary==2.9.9 python-dotenv==1.0.1
.venv/bin/python -m src.common.bootstrap_db

docker compose build airflow
docker compose run --rm airflow db migrate

docker compose run --rm --workdir /opt/airflow/project airflow \
  python -m src.bronze.download_tlc_data --year 2025 --month 1
docker compose run --rm --workdir /opt/airflow/project airflow \
  python -m src.silver.silver_job --year 2025 --month 1
docker compose run --rm --workdir /opt/airflow/project/dbt/urban_mobility --entrypoint bash airflow \
  -c "dbt deps && dbt run && dbt test"
```

Detalhes, backfill do período e troubleshooting em [`urban-mobility-analytics/docs/execution.md`](urban-mobility-analytics/docs/execution.md).

## Regras de negócio — camada silver

A partir de `bronze.yellow_tripdata`, o job PySpark:

- filtra a competência mensal pela data de embarque (`tpep_pickup_datetime`);
- calcula `pickup_date`, `pickup_year_month`, `trip_duration_minutes`;
- aplica 5 regras de qualidade — datas de embarque/desembarque presentes, desembarque não anterior ao embarque, distância não-negativa, valor total não-negativo, tipo de tarifa dentro do dicionário da TLC — resultando em `is_valid_trip`/`invalid_reason`;
- sinaliza (sem invalidar) 3 condições de anomalia — duração acima de 6h, distância acima de 100 milhas, corrida sem passageiro registrado ou com distância zero e tarifa cobrada — em `is_anomaly`/`anomaly_reason`;
- calcula `valid_revenue` como o valor total apenas quando o tipo de pagamento é considerado válido pela tabela de referência.

## Indicadores — camada gold

`gold.mv_monthly_indicators`, por `vendor_id` e mês (`yyyymm`), entre corridas com `is_valid_trip = true`:

- total de corridas;
- valor total das corridas com pagamento válido;
- ticket médio (valor total cobrado por corrida);
- distância média.

Amostra real do resultado (dado de banco, não versionado no git) em [`urban-mobility-analytics/docs/results.md`](urban-mobility-analytics/docs/results.md).
