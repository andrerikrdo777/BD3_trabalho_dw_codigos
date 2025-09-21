------ TRIGGERS ------

--  TRIGGER PARA LOG DE EXECUÇÃO DO ETL
CREATE TABLE IF NOT EXISTS dw.etl_log (
    id_log SERIAL PRIMARY KEY,
    data_execucao TIMESTAMP DEFAULT NOW(),
    tipo_operacao VARCHAR(50),
    status VARCHAR(20),
    mensagem TEXT,
    duracao INTERVAL,
    erro_detalhado TEXT,
    linha_erro INTEGER
);

CREATE OR REPLACE FUNCTION dw.fn_log_etl()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem)
    VALUES (TG_ARGV[0], TG_ARGV[1], TG_ARGV[2]);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- drop table dw.etl_log;


