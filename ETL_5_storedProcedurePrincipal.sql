CREATE OR REPLACE PROCEDURE dw.sp_etl_completo()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    -- Verificar se já está em execução
    IF EXISTS (SELECT 1 FROM dw.etl_log 
               WHERE status = 'INICIADO' 
               AND data_execucao > NOW() - INTERVAL '1 hour') THEN
        RAISE EXCEPTION 'ETL já está em execução. Aguarde a conclusão.';
    END IF;

    -- Log de início (INSERIR e capturar o ID para depois ATUALIZAR)
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('ETL_COMPLETO', 'INICIADO', 'Processo iniciado', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Iniciando processo ETL completo em %', NOW();
    
    -- Executar as procedures (elas também devem usar a mesma lógica de UPDATE)
    CALL dw.sp_limpar_staging();
    CALL dw.sp_carregar_staging();
    CALL dw.sp_limpar_dw();
    CALL dw.sp_carregar_dw();
    CALL dw.sp_atualizar_estatisticas();
    
    -- ATUALIZAR o registro existente para SUCESSO
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Processo concluído com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Processo ETL completo finalizado com sucesso em %', NOW();
    
EXCEPTION
    WHEN OTHERS THEN
        UPDATE dw.etl_log 
        SET status = 'ERRO',
            mensagem = 'Erro: ' || SQLERRM,
            duracao = NOW() - inicio_time
        WHERE id_log = log_id;
        
        RAISE EXCEPTION 'Erro durante o ETL: %', SQLERRM;
END;
$$;


-- X. SCRIPT PARA VERIFICAR STATUS DO ETL
DROP FUNCTION IF EXISTS dw.fn_verificar_status_etl();

CREATE OR REPLACE FUNCTION dw.fn_verificar_status_etl()
RETURNS TABLE (
    ultima_execucao TIMESTAMP,
    status VARCHAR(20),
    total_vendas BIGINT,
    total_clientes BIGINT,
    total_produtos BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        MAX(log.data_execucao),
        MAX(log.status)::VARCHAR(20),  -- CAST explícito aqui!
        (SELECT COUNT(*) FROM dw.Fato_Vendas),
        (SELECT COUNT(*) FROM dw.Dim_Cliente),
        (SELECT COUNT(*) FROM dw.Dim_Produto)
    FROM dw.etl_log log
    WHERE log.data_execucao = (SELECT MAX(data_execucao) FROM dw.etl_log);
END;
$$ LANGUAGE plpgsql;


