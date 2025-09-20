CREATE OR REPLACE PROCEDURE dw.sp_etl_completo()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
BEGIN
    /** [#@#] CONTROLE DE FREQUENCIA */
    /** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
    -- Verificar se já está em execução
    IF EXISTS (SELECT 1 FROM dw.etl_log 
               WHERE status IN ('INICIADO', 'EM_ANDAMENTO') 
               AND data_execucao > NOW() - INTERVAL '1 hour') THEN
        RAISE EXCEPTION 'ETL já está em execução. Aguarde a conclusão.';
    END IF;	
    /** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
    /** [#@#] CONTROLE DE FREQUENCIA  */

    -- Log de início
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem)
    VALUES ('ETL_COMPLETO', 'INICIADO', 'Processo iniciado em ' || NOW());
    
    RAISE NOTICE 'Iniciando processo ETL completo em %', NOW();
    
    -- 1. LIMPEZA DAS STAGING AREAS
    CALL dw.sp_limpar_staging();
    
    -- 2. CARREGAMENTO PARA STAGING
    CALL dw.sp_carregar_staging();
    
    -- 3. LIMPEZA DO DW
    CALL dw.sp_limpar_dw();
    
    -- 4. CARREGAMENTO PARA DW
    CALL dw.sp_carregar_dw();
    
    -- 5. ATUALIZAR ESTATÍSTICAS
    CALL dw.sp_atualizar_estatisticas();
    
    -- Log de conclusão
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, duracao)
    VALUES ('ETL_COMPLETO', 'SUCESSO', 'Processo concluído com sucesso', NOW() - inicio_time);
    
    RAISE NOTICE 'Processo ETL completo finalizado com sucesso em %', NOW();
    
    /** [#@#] TRATAMENTO DE ERROS DETALHADO */
    /** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
EXCEPTION
    WHEN OTHERS THEN
        -- Log de erro detalhado
        INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, duracao)
        VALUES ('ETL_COMPLETO', 'ERRO', 
                'Erro: ' || SQLERRM || ' - Linha: ' || SQLSTATE, 
                NOW() - inicio_time);
        
        RAISE EXCEPTION 'Erro durante o ETL: %', SQLERRM;
    /** ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
    /** [#@#] TRATAMENTO DE ERROS DETALHADO */
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


