-- Roda só na 1a inicialização do volume (docker-entrypoint-initdb.d).
-- Metadados do Airflow ficam separados do banco analítico (urban_mobility).
CREATE DATABASE airflow;
