# Execução

## Pré-requisitos

- Docker e Docker Compose.
- Um `.env` local (`cp .env.example .env`); ajustar `AIRFLOW_UID` para o resultado de `id -u` no host — necessário para o container do Airflow escrever em `./data` via bind mount sem erro de permissão.

## 1. Subir o Postgres e criar os schemas

```bash
docker compose up -d postgres
```

O `bootstrap_db` aplica os SQLs de `sql/`, `src/bronze/sql/` e `src/silver/sql/`, nessa ordem (schemas, depois tabelas de cada camada). Pode rodar num venv local, já que só precisa de `SQLAlchemy`/`psycopg2`/`python-dotenv`:

```bash
python -m venv .venv
.venv/bin/pip install SQLAlchemy==1.4.52 psycopg2-binary python-dotenv
.venv/bin/python -m src.common.bootstrap_db
```

Confirma os schemas:

```bash
docker exec uma_postgres psql -U urban_mobility -d urban_mobility -c "\dn"
```

## 2. Buildar a imagem do Airflow e inicializar o metadado

```bash
docker compose build airflow
docker compose run --rm airflow db migrate
```

A imagem já inclui Java (para o PySpark), o driver JDBC do Postgres e as dependências do `requirements.txt` (dbt, pyspark, etc).

## 3. Rodar uma competência manualmente

Ingestão (bronze):

```bash
docker compose run --rm --workdir /opt/airflow/project airflow \
  python -m src.bronze.download_tlc_data --year 2025 --month 1
```

Processamento (silver):

```bash
docker compose run --rm --workdir /opt/airflow/project airflow \
  python -m src.silver.silver_job --year 2025 --month 1
```

Modelagem (gold):

```bash
docker compose run --rm --workdir /opt/airflow/project/dbt/urban_mobility --entrypoint bash airflow \
  -c "dbt deps && dbt run --vars '{\"year_month\": \"2025-01\"}' && dbt test"
```

Omitir `--month` em qualquer um dos dois primeiros comandos processa o período completo (janeiro a setembro/2025) numa única chamada.

## 4. Conferências no Postgres

```sql
-- linhas por competência em cada camada
select source_year_month, count(*) from bronze.yellow_tripdata group by 1 order by 1;
select pickup_year_month, count(*) from silver.trips group by 1 order by 1;
select pickup_year_month, count(*) from gold.fct_trips group by 1 order by 1;

-- indicadores
select * from gold.mv_monthly_indicators order by vendor_id, year_month;
```

## 5. Rodar pela DAG do Airflow

Sem precisar do scheduler/webserver de pé, o Airflow tem um modo de teste que executa a DAG inteira num único processo:

```bash
docker compose run --rm airflow dags test urban_mobility_pipeline 2025-01-01 -c '{"year": 2025, "month": 1}'
```

Sem o `-c`, a DAG resolve a competência sozinha (variáveis `uma_year`/`uma_month`, ou o mês anterior à data de execução, nessa ordem de prioridade).

## 6. Backfill do período

Ingestão e silver aceitam o período completo (janeiro a setembro/2025) numa chamada só (sem `--month`); a `fct_trips` é incremental, então um `dbt run` sem `--vars` reprocessa todas as competências presentes em `silver.trips` de uma vez:

```bash
docker compose run --rm --workdir /opt/airflow/project airflow \
  python -m src.bronze.download_tlc_data --year 2025
docker compose run --rm --workdir /opt/airflow/project airflow \
  python -m src.silver.silver_job --year 2025
docker compose run --rm --workdir /opt/airflow/project/dbt/urban_mobility --entrypoint bash airflow \
  -c "dbt run && dbt test"
```

Antes de um backfill grande, vale um `VACUUM (ANALYZE)` nas tabelas de silver/gold se elas já tiverem passado por vários reprocessamentos — o Postgres não recupera o espaço de linhas deletadas sozinho no mesmo instante, e isso pode deixar consultas e cargas seguintes mais lentas.

## Testes

```bash
docker compose run --rm --workdir /opt/airflow/project airflow python -m pytest tests/ -v
```

Cobre as regras de qualidade e anomalia da silver com um dataset sintético (não depende de dados reais nem do banco).

## Troubleshooting

| Sintoma | Causa | Solução |
|---|---|---|
| `Mkdirs failed to create file` ao escrever parquet | UID do container (padrão 50000) sem permissão de escrita no bind mount `./data`, que pertence ao UID do host | Definir `AIRFLOW_UID` no `.env` com o `id -u` do host — já configurado em `docker-compose.yml` via `user: "${AIRFLOW_UID:-50000}:0"` |
| `sqlalchemy.exc.ArgumentError` ao rodar qualquer comando `airflow` | `SQLAlchemy` mais recente que o exigido pelo Airflow (`<2.0`) instalado por cima da imagem | `requirements.txt` fixa `SQLAlchemy==1.4.52` |
| `Error from git --help` no `dbt deps` | `git` não instalado na imagem (necessário para baixar pacotes dbt do GitHub) | Já incluído no `airflow/Dockerfile` |
| `getpwuid(): uid not found` rodando `airflow <comando>` | Comando executado com `--entrypoint bash`, pulando o script de entrypoint da imagem que registra o UID atual em `/etc/passwd` | Rodar sem `--entrypoint` (o entrypoint reconhece `bash`/`python` como primeiro argumento e aplica a correção antes de executar) |
| `could not resize shared memory segment` num `VACUUM`/`ANALYZE` | `/dev/shm` do container do Postgres limitado ao padrão do Docker (64MB) | `shm_size: '1gb'` no serviço `postgres` do `docker-compose.yml` |
| Materialized view some depois de um `dbt run --select fct_trips --full-refresh` | O full-refresh recria a tabela por baixo (`fct_trips` é incremental), derrubando em cascata quem depende dela | Rodar `dbt run` completo (todos os modelos), não `--select` isolado, ao fazer full-refresh |
