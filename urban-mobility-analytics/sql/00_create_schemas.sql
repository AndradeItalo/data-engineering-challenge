-- Camadas da arquitetura medalhão, como schemas separados no banco analítico.
CREATE SCHEMA IF NOT EXISTS bronze;
COMMENT ON SCHEMA bronze IS 'Dados brutos, recebidos da fonte (NYC TLC), com colunas de linhagem.';

CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS 'Dados consolidados e tratados a partir da camada bronze (regras de qualidade aplicadas).';

CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS 'Dados modelados (fatos/dimensões) e indicadores, disponibilizados para as análises.';
