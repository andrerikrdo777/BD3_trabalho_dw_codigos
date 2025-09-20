SELECT datname FROM pg_database;

SELECT current_user;

SELECT datname, datistemplate 
FROM pg_database 
WHERE datistemplate = false 
ORDER BY datname;

SET search_path TO ads;
	
SELECT datname, datistemplate 
FROM pg_database 
WHERE datname = 'ads';

SELECT table_name 
FROM information_schema.tables 
WHERE table_catalog = 'ads' 
AND table_schema = 'public';

-- C
call dw.sp_etl_completo();

SELECT * FROM dw.fn_verificar_status_etl();
--
SELECT 
    NOW() as monitoramento,
    (SELECT status FROM dw.etl_log ORDER BY data_execucao DESC LIMIT 1) as status_atual,
    (SELECT mensagem FROM dw.etl_log ORDER BY data_execucao DESC LIMIT 1) as ultima_mensagem,
    (SELECT COUNT(*) FROM dw.Fato_Vendas) as vendas_processadas
\watch interval=2
--
-- Verifique se há locks ou processos travados
SELECT * FROM pg_locks WHERE granted = false;
SELECT * FROM pg_stat_activity WHERE state = 'active';

--
-- Verifica se ETL ainda está rodando
-- Se aparecer como 'active' e tempo razoável, é normal
SELECT state, now() - query_start as tempo 
FROM pg_stat_activity 
WHERE query LIKE '%sp_etl%';

--
-- Se tempo > 5 minutos, pode estar travado
SELECT now() - query_start as tempo 
FROM pg_stat_activity 
WHERE query LIKE '%sp_etl%' 
AND now() - query_start > INTERVAL '5 minutes';

-- Verificar se há processo finalizado mas log antigo
SELECT *
FROM pg_stat_activity 
WHERE query LIKE '%sp_etl%' 
AND state = 'idle';

-- Primeiro encontre o PID
SELECT pid, query FROM pg_stat_activity WHERE query LIKE '%sp_etl%';

-- Depois cancele (substitua 1234 pelo PID real)
SELECT pg_cancel_backend(28,372);

-- Se não funcionar, force terminar
SELECT pg_terminate_backend(8776);

-- Limpe logs antigos que podem estar causando confusão
DELETE FROM dw.etl_log 
WHERE status = 'INICIADO' 
AND now() - data_execucao < INTERVAL '10 minutes';


--
SELECT 
    pid,
    query, 
    now() - query_start as tempo_execucao
FROM pg_stat_activity 
WHERE query LIKE '%sp_etl%' 
AND state = 'active';

-- Depois de resolver, execute novamente
CALL dw.sp_etl_completo();


-- Checa tabela etl_log
	SELECT 
	    tipo_operacao,
	    status,
	    mensagem,
	    duracao,
	    data_execucao
	FROM dw.etl_log 
	ORDER BY data_execucao DESC;

--
-- Primeiro liste os PIDs para confirmar
-- Verificar locks ativos
SELECT 
    relation::regclass as tabela,
    mode as tipo_lock,
    granted as concedido,
    query
FROM pg_locks
JOIN pg_stat_activity USING (pid)
WHERE relation::regclass IN ('tb010_012_vendas', 'tb012_017_compras', 'stg.vendas', 'dw.fato_vendas');

-- Verificar se há muitos conflitos no ON CONFLICT
SELECT 
    schemaname,
    relname,
    n_conflict
FROM pg_stat_all_tables 
WHERE n_conflict > 0;

-- Verificar se as tabelas fonte não têm índices
SELECT 
    tablename,
    COUNT(indexname) as total_indices
FROM pg_indexes 
WHERE schemaname = 'public'
AND tablename IN ('tb010_012_vendas', 'tb012_017_compras', 'tb005_funcionarios')
GROUP BY tablename;


-- Depois mate APENAS o PID específico (ex: 1234)
SELECT pg_terminate_backend(1234);
--


\timing on
CALL dw.sp_limpar_staging();   
CALL dw.sp_carregar_staging();  
CALL dw.sp_limpar_dw();        
CALL dw.sp_carregar_dw();       


-- 1. Primeiro mate processos travados
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity 
WHERE query LIKE '%sp_etl%' 
AND state = 'active';

-- 2. Limpe logs antigos se necessário  
DELETE FROM dw.etl_log 
WHERE status = 'INICIADO' 
AND now() - data_execucao > INTERVAL '10 minutes';

-- 3. Execute
CALL dw.sp_etl_completo();


SELECT COUNT(*) FROM pg_stat_activity WHERE query LIKE '%sp_etl%' AND state = 'active';

-- 2. Se houver mais de 1, cancele os extras
SELECT pg_cancel_backend(pid) FROM pg_stat_activity 
WHERE query LIKE '%sp_etl%' AND state = 'active'
AND pid != (SELECT MIN(pid) FROM pg_stat_activity WHERE query LIKE '%sp_etl%');

-- 3. Monitore o que restou
SELECT pid, query, state FROM pg_stat_activity WHERE query LIKE '%sp_etl%';

-- Monitorar logs enquanto o ETL roda (execute em outra aba/terminal)
SELECT 
    data_execucao,
    tipo_operacao,
    status,
    mensagem,
    duracao
FROM dw.etl_log 
ORDER BY data_execucao DESC 
LIMIT 10;
--
-- Contagem de registros em cada tabela
SELECT 'Dim_Tempo' as tabela, COUNT(*) as total FROM dw.Dim_Tempo
UNION ALL
SELECT 'Dim_Cliente', COUNT(*) FROM dw.Dim_Cliente
UNION ALL  
SELECT 'Dim_Funcionario', COUNT(*) FROM dw.Dim_Funcionario
UNION ALL
SELECT 'Dim_Produto', COUNT(*) FROM dw.Dim_Produto
UNION ALL
SELECT 'Dim_Loja', COUNT(*) FROM dw.Dim_Loja
UNION ALL
SELECT 'Fato_Vendas', COUNT(*) FROM dw.Fato_Vendas;

-- Verificar se há vendas sem correspondência nas dimensões
SELECT 
    (SELECT COUNT(*) FROM dw.Fato_Vendas f WHERE NOT EXISTS 
        (SELECT 1 FROM dw.Dim_Cliente c WHERE c.idCliente = f.idCliente)) as clientes_faltantes,
    (SELECT COUNT(*) FROM dw.Fato_Vendas f WHERE NOT EXISTS 
        (SELECT 1 FROM dw.Dim_Funcionario fu WHERE fu.idFuncionario = f.idFuncionario)) as funcionarios_faltantes,
    (SELECT COUNT(*) FROM dw.Fato_Vendas f WHERE NOT EXISTS 
        (SELECT 1 FROM dw.Dim_Produto p WHERE p.idProduto = f.idProduto)) as produtos_faltantes,
    (SELECT COUNT(*) FROM dw.Fato_Vendas f WHERE NOT EXISTS 
        (SELECT 1 FROM dw.Dim_Loja l WHERE l.idLoja = f.idLoja)) as lojas_faltantes;

-- Estatísticas financeiras das vendas
SELECT
    COUNT(*) as total_vendas,
    SUM(quantidade) as total_itens_vendidos,
    SUM(valorTotal) as receita_total,
    SUM(custoTotal) as custo_total,
    SUM(lucroTotal) as lucro_total,
    ROUND(SUM(lucroTotal) / SUM(valorTotal) * 100, 2) as margem_lucro_percentual
FROM dw.Fato_Vendas;

-- Vendas por mês/ano
SELECT
    EXTRACT(YEAR FROM idData) as ano,
    EXTRACT(MONTH FROM idData) as mes,
    COUNT(*) as total_vendas,
    SUM(valorTotal) as receita_mensal
FROM dw.Fato_Vendas
GROUP BY ano, mes
ORDER BY ano, mes;

-- Desempenho por loja
SELECT
    l.nomeLoja,
    l.cidade,
    l.uf,
    COUNT(*) as total_vendas,
    SUM(f.valorTotal) as receita_total,
    SUM(f.lucroTotal) as lucro_total
FROM dw.Fato_Vendas f
JOIN dw.Dim_Loja l ON f.idLoja = l.idLoja
GROUP BY l.nomeLoja, l.cidade, l.uf
ORDER BY receita_total DESC;




---- Verificar se há valores negativos ou inconsistentes
SELECT
    (SELECT COUNT(*) FROM dw.Fato_Vendas WHERE valorTotal < 0) as valor_total_negativo,
    (SELECT COUNT(*) FROM dw.Fato_Vendas WHERE custoTotal < 0) as custo_total_negativo,
    (SELECT COUNT(*) FROM dw.Fato_Vendas WHERE lucroTotal < 0) as lucro_total_negativo,
    (SELECT COUNT(*) FROM dw.Fato_Vendas WHERE quantidade < 0) as quantidade_negativa;