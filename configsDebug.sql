---- EXECUTA PROCEDURE ETL COMPLETA
CALL dw.sp_etl_completo();

---- Tabela etl log
select * from dw.etl_log

---- Funcao que retorna total_clientes e total_produtos
select * from dw.fn_verificar_status_etl()

---- Conta registros da tabela fato vendas (32 agregações OLAP)
select count(*) from dw.fato_vendas

---- Conta registros da tabela vendas do relacional
select count(*) from public.tb010_012_vendas -- Deve resultar 110

---- Conta registros da tabela clientes do relacional
select count(*) from public.tb010_clientes -- Deve resultar 51

---- Conta registros da tabela clientes do relacional
select count(*) from public.tb012_produtos  -- Deve resultar 68

select count(*) from dw.fato_vendas -- Deve resultar 110
where idFuncionario = -1
and idLoja = -1
and idProduto = -1
and idCliente != -1
and idData != -1
----
-- Tempo de cada etapa do último ETL
SELECT 
    tipo_operacao,
    data_execucao,
    duracao,
    status
FROM dw.etl_log 
WHERE data_execucao >= (
    SELECT MAX(data_execucao) 
    FROM dw.etl_log 
    WHERE tipo_operacao = 'ETL_COMPLETO' AND status = 'SUCESSO'
)
ORDER BY data_execucao;
---- 
select count(*) from dw.fato_vendas -- Deve resultar 110
where idFuncionario = -1
and idLoja = -1
and idProduto = -1
and idCliente != -1
and idData != -1



delete from dw.fato_vendas;
delete from dw.dim_cliente;
delete from dw.dim_loja;
delete from dw.dim_funcionario;
delete from dw.dim_tempo;
delete from dw.dim_produto;

--// Conta registros da tabela Fato
select count(*) from dw.fato_vendas;
--//

-- Tabela log



-- Limpando registros presos 
	DELETE FROM dw.etl_log WHERE status = 'INICIADO';

	DELETE FROM dw.etl_log 
	WHERE status = 'INICIADO' 
	AND data_execucao < NOW() - INTERVAL '2 minutes';
	
-- Verificando registros mais recentes no log
	
	SELECT tipo_operacao, status, data_execucao from dw.etl_log where tipo_operacao = 'ETL_COMPLETO' 
	SELECT * FROM dw.etl_log ORDER BY data_execucao DESC LIMIT 10;
	SELECT * FROM dw.etl_log ORDER BY data_execucao DESC LIMIT 5;
	-- Select etl_log contando por operacao e status
	SELECT tipo_operacao, status, COUNT(*) 
	FROM dw.etl_log 
	GROUP BY tipo_operacao, status 
	ORDER BY tipo_operacao;

SET datestyle TO 'MDY';




ALTER DATABASE ads_testee SET datestyle = 'ISO, MDY';

SET datestyle = 'ISO, MDY';

SHOW lc_time;
SHOW datestyle;

SET datestyle = 'SQL, MDY';

select * from dw.dim_loja

select * from public.tb004_lojas

delete from public.tb004_lojas where tb004_cod_loja > 3



/**            */
SELECT * FROM dw.Dim_Cliente WHERE cpf = 44422000044; -- Nada
SELECT COUNT(*) as total_registros_fato FROM dw.Fato_Vendas; -- 114
/**          
-- Inserir um novo cliente (opcional)
INSERT INTO public.tb010_clientes (tb010_cpf, tb010_nome, tb010_fone_residencial, tb010_fone_celular)
VALUES (44422000044, 'nOVO CLIente de agora', '(41) 3333-9999', '(41) 98888-9999');

-- Inserir novas vendas (essenciais para o teste)
INSERT INTO public.tb010_012_vendas (tb010_cpf, tb012_cod_produto, tb005_matricula, tb010_012_data, tb010_012_quantidade, tb010_012_valor_unitario)
VALUES
-- Venda para o novo cliente
(30022000055, 10, 4, CURRENT_TIMESTAMP, 5, 1.50),
(30022000055, 10, 4, CURRENT_TIMESTAMP, 5, 1.50),
(30022000055, 10, 4, CURRENT_TIMESTAMP, 5, 1.50),
(30022000055, 10, 4, CURRENT_TIMESTAMP, 5, 1.50),
(30022000055, 10, 4, CURRENT_TIMESTAMP, 5, 1.50),
(30022000055, 10, 4, CURRENT_TIMESTAMP, 5, 1.50),
(30022000055, 10, 4, CURRENT_TIMESTAMP, 5, 1.50);
-- Vendas para clientes existentes
           */

-- 1 
SELECT COUNT(*) 
FROM dw.Fato_Vendas 
WHERE idFuncionario = -1 
  AND idLoja = -1 
  AND idProduto = -1
  AND idCliente != -1
  AND idData != -1;
-- 2
SELECT 
    'Total de Vendas' as descricao,
    fv.quantidade as "Quantidade"
FROM dw.Fato_Vendas fv
WHERE fv.idData = -1 
  AND fv.idCliente = -1 
  AND fv.idFuncionario = -1 
  AND fv.idLoja = -1 
  AND fv.idProduto = -1;  -- Nível mais agregado