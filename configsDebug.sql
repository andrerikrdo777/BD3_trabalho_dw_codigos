-- EXECUTA PROCEDURE ETL COMPLETA
CALL dw.sp_etl_completo();



-- Limpando registros presos 
	DELETE FROM dw.etl_log WHERE status = 'INICIADO';

	DELETE FROM dw.etl_log 
	WHERE status = 'INICIADO' 
	AND data_execucao < NOW() - INTERVAL '2 minutes';
	
-- Verificando registros mais recentes no log
	SELECT * FROM dw.etl_log;
	SELECT tipo_operacao, status, data_execucao from dw.etl_log where tipo_operacao = 'ETL_COMPLETO' 
	SELECT * FROM dw.etl_log ORDER BY data_execucao DESC LIMIT 10;
	SELECT * FROM dw.etl_log ORDER BY data_execucao DESC LIMIT 5;
	-- Select etl_log contando por operacao e status
	SELECT tipo_operacao, status, COUNT(*) 
	FROM dw.etl_log 
	GROUP BY tipo_operacao, status 
	ORDER BY tipo_operacao;


