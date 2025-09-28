------ TRIGGERS E LOG ------

-- TABELA DE LOG (MANTIDA)
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

-- FUNÇÃO CALCULAR LUCRO (NOVA)
CREATE OR REPLACE FUNCTION dw.calcular_lucro(cod_produto INT, valor_unitario NUMERIC, quantidade INT)
RETURNS NUMERIC AS $$
DECLARE
    custo_medio NUMERIC;
BEGIN
    SELECT AVG(c.tb012_017_valor_unitario) INTO custo_medio
    FROM stg.compras c 
    WHERE c.tb012_cod_produto = cod_produto;
    
    RETURN (valor_unitario - COALESCE(custo_medio, 0)) * quantidade;
END;
$$ LANGUAGE plpgsql;

-- PROCEDURE PRINCIPAL (REFATORADA)
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

    -- Log de início
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('ETL_COMPLETO', 'INICIADO', 'Processo ETL OLAP iniciado', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Iniciando processo ETL OLAP completo em %', NOW();
    
    -- Executar as procedures
    CALL dw.sp_limpar_staging();
    CALL dw.sp_carregar_staging();
    CALL dw.sp_limpar_dw();
    CALL dw.sp_carregar_dimensoes();
    CALL dw.sp_carregar_fato_olap();  -- NOVA: Carrega agregações OLAP
    CALL dw.sp_atualizar_estatisticas();
    
    -- ATUALIZAR para SUCESSO
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Processo ETL OLAP concluído com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Processo ETL OLAP finalizado com sucesso em %', NOW();
    
EXCEPTION
    WHEN OTHERS THEN
        UPDATE dw.etl_log 
        SET status = 'ERRO',
            mensagem = 'Erro: ' || SQLERRM,
            duracao = NOW() - inicio_time,
            erro_detalhado = SQLSTATE
        WHERE id_log = log_id;
        
        RAISE EXCEPTION 'Erro durante o ETL: %', SQLERRM;
END;
$$;

-- 1. PROCEDURE PARA LIMPAR STAGING (MANTIDA)
CREATE OR REPLACE PROCEDURE dw.sp_limpar_staging()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('LIMPAR_STAGING', 'INICIADO', 'Iniciando limpeza da staging', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Limpando staging area...';
    
    DROP TABLE IF EXISTS
        stg.vendas,
        stg.compras,
        stg.clientes,
        stg.funcionarios,
        stg.cargos_mais_recentes,
        stg.produtos,
        stg.produtos_detalhes,
        stg.lojas;
    
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Staging limpa com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Staging area limpa com sucesso';
END;
$$;

-- 2. PROCEDURE PARA CARREGAR STAGING (CORRIGIDA)
CREATE OR REPLACE PROCEDURE dw.sp_carregar_staging()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    -- Log de início (CAPTURAR O ID)
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('CARREGAR_STAGING', 'INICIADO', 'Carregando dados para staging area', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Carregando dados para staging...';
    
    -- Tabela principal de Vendas
    CREATE TABLE IF NOT EXISTS stg.vendas AS
    SELECT
        tb010_012_cod_venda,
        tb010_cpf,
        tb012_cod_produto,
        tb005_matricula,
        tb010_012_data,
        tb010_012_quantidade,
        tb010_012_valor_unitario
    FROM public.tb010_012_vendas;

    -- Tabela de Compras
    CREATE TABLE IF NOT EXISTS stg.compras AS
    SELECT
        tb012_cod_produto,
        tb012_017_valor_unitario
    FROM public.tb012_017_compras;

    -- Tabela de Clientes
    CREATE TABLE IF NOT EXISTS stg.clientes AS
    SELECT
        tb010_cpf,
        tb010_nome
    FROM public.tb010_clientes;

    -- Tabela de Funcionários
    CREATE TABLE IF NOT EXISTS stg.funcionarios AS
    SELECT
        tb005_matricula,
        tb005_CPF,
        tb005_nome_completo,
        tb004_cod_loja
    FROM public.tb005_funcionarios;

    -- Tabela de Cargos mais recentes
    CREATE TABLE IF NOT EXISTS stg.cargos_mais_recentes AS
    WITH CargosRecentes AS (
        SELECT
            tb005_matricula,
            tb006_cod_cargo,
            tb005_006_data_promocao,
            ROW_NUMBER() OVER (PARTITION BY tb005_matricula ORDER BY tb005_006_data_promocao DESC) as row_num
        FROM public.tb005_006_funcionarios_cargos
    )
    SELECT
        cr.tb005_matricula,
        c.tb006_nome_cargo
    FROM CargosRecentes cr
    JOIN public.tb006_cargos c ON cr.tb006_cod_cargo = c.tb006_cod_cargo
    WHERE cr.row_num = 1;

    -- Tabela de Produtos e Categorias
    CREATE TABLE IF NOT EXISTS stg.produtos AS
    SELECT
        p.tb012_cod_produto,
        p.tb012_descricao,
        cat.tb013_descricao as categoria
    FROM public.tb012_produtos p
    JOIN public.tb013_categorias cat ON p.tb013_cod_categoria = cat.tb013_cod_categoria;

    -- Tabela de Descrição Detalhada dos Produtos
    CREATE TABLE IF NOT EXISTS stg.produtos_detalhes AS
    SELECT
        p.tb012_cod_produto,
        COALESCE(a.tb014_detalhamento, e.tb015_detalhamento, v.tb016_detalhamento) as descricao_detalhada
    FROM public.tb012_produtos p
    LEFT JOIN public.tb014_prd_alimentos a ON p.tb012_cod_produto = a.tb012_cod_produto
    LEFT JOIN public.tb015_prd_eletros e ON p.tb012_cod_produto = e.tb012_cod_produto
    LEFT JOIN public.tb016_prd_vestuarios v ON p.tb012_cod_produto = v.tb012_cod_produto;

    -- Tabela de Lojas
    CREATE TABLE IF NOT EXISTS stg.lojas AS
    SELECT
        l.tb004_cod_loja,
        'Loja ' || l.tb004_cod_loja::TEXT as nome_loja,
        cid.tb002_nome_cidade as cidade,
        uf.tb001_sigla_uf as uf
    FROM public.tb004_lojas l
    JOIN public.tb003_enderecos e ON l.tb003_cod_endereco = e.tb003_cod_endereco
    JOIN public.tb002_cidades cid ON (e.tb002_cod_cidade = cid.tb002_cod_cidade AND e.tb001_sigla_uf = cid.tb001_sigla_uf)
    JOIN public.tb001_uf uf ON cid.tb001_sigla_uf = uf.tb001_sigla_uf;
    
    -- ATUALIZAR em vez de INSERIR novo
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Staging carregada com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Staging carregada com sucesso';
END;
$$;

-- 3. PROCEDURE PARA LIMPAR DW (CORRIGIDA - ORDEM)
CREATE OR REPLACE PROCEDURE dw.sp_limpar_dw()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('LIMPAR_DW', 'INICIADO', 'Iniciando limpeza do DW', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Limpando data warehouse...';
    
    -- ORDEM CORRETA: Primeiro a tabela fato (por causa das FKs), depois as dimensões
    TRUNCATE TABLE dw.Fato_Vendas RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Tempo RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Cliente RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Funcionario RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Produto RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Loja RESTART IDENTITY CASCADE;
    
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Data warehouse limpo com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Data warehouse limpo com sucesso';
END;
$$;

-- 4. PROCEDURE PARA CARREGAR DIMENSÕES (NOVA - SEPARADA)
CREATE OR REPLACE PROCEDURE dw.sp_carregar_dimensoes()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('CARREGAR_DIMENSOES', 'INICIADO', 'Carregando dimensões', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Carregando dimensões...';
    
    -- 1. MEMBROS "ALL" (PRIMEIRO!)
    INSERT INTO dw.Dim_Tempo (idData, dataCompleta, ano, mes, dia) 
    VALUES (-1, NULL, -1, -1, -1) ON CONFLICT (idData) DO NOTHING;

    INSERT INTO dw.Dim_Cliente (idCliente, cpf, nomeCliente)
    VALUES (-1, -1, 'Todos os Clientes') ON CONFLICT (idCliente) DO NOTHING;

    INSERT INTO dw.Dim_Funcionario (idFuncionario, cpf, nomeFuncionario, cargo)
    VALUES (-1, '-1', 'Todos os Funcionários', 'Total') ON CONFLICT (idFuncionario) DO NOTHING;

    INSERT INTO dw.Dim_Produto (idProduto, codProdutoOrigem, nomeProduto, categoria, descricaoProduto)
    VALUES (-1, -1, 'Todos os Produtos', 'Total', 'Agregado de todos os produtos') ON CONFLICT (idProduto) DO NOTHING;

    INSERT INTO dw.Dim_Loja (idLoja, nomeLoja, cidade, uf)
    VALUES (-1, 'Todas as Lojas', 'Total', 'TT') ON CONFLICT (idLoja) DO NOTHING;

    -- 2. DIMENSÕES NORMAIS
    -- Dim_Tempo (CORRIGIDO: idData INTEGER)
    INSERT INTO dw.Dim_Tempo (idData, dataCompleta, ano, mes, dia)
    SELECT DISTINCT
        CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER) as idData,
        DATE(v.tb010_012_data) as dataCompleta,
        EXTRACT(YEAR FROM v.tb010_012_data) as ano,
        EXTRACT(MONTH FROM v.tb010_012_data) as mes,
        EXTRACT(DAY FROM v.tb010_012_data) as dia
    FROM stg.vendas v ON CONFLICT (idData) DO NOTHING;

    -- Dim_Cliente
    INSERT INTO dw.Dim_Cliente (cpf, nomeCliente)
    SELECT tb010_cpf, tb010_nome FROM stg.clientes ON CONFLICT (cpf) DO NOTHING;

    -- Dim_Funcionario
    INSERT INTO dw.Dim_Funcionario (idFuncionario, cpf, nomeFuncionario, cargo)
    SELECT
        f.tb005_matricula, f.tb005_CPF, f.tb005_nome_completo,
        COALESCE(c.tb006_nome_cargo, 'Cargo Não Definido')
    FROM stg.funcionarios f
    LEFT JOIN stg.cargos_mais_recentes c ON f.tb005_matricula = c.tb005_matricula
    ON CONFLICT (idFuncionario) DO NOTHING;

    -- Dim_Produto
    INSERT INTO dw.Dim_Produto (codProdutoOrigem, nomeProduto, categoria, descricaoProduto)
    SELECT
        p.tb012_cod_produto, p.tb012_descricao, p.categoria,
        COALESCE(pd.descricao_detalhada, 'Sem descrição detalhada')
    FROM stg.produtos p
    LEFT JOIN stg.produtos_detalhes pd ON p.tb012_cod_produto = pd.tb012_cod_produto
    ON CONFLICT (codProdutoOrigem) DO NOTHING;

    -- Dim_Loja
    INSERT INTO dw.Dim_Loja (idLoja, nomeLoja, cidade, uf)
    SELECT tb004_cod_loja, nome_loja, cidade, uf FROM stg.lojas ON CONFLICT (idLoja) DO NOTHING;
    
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Dimensões carregadas com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Dimensões carregadas com sucesso';
END;
$$;

-- 5. PROCEDURE PARA CARREGAR FATO OLAP (NOVA - AGREGAÇÕES)
CREATE OR REPLACE PROCEDURE dw.sp_carregar_fato_olap()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('CARREGAR_FATO_OLAP', 'INICIADO', 'Carregando agregações OLAP', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Carregando agregações OLAP...';

    -- 1. TOTAL GERAL (para totais)
INSERT INTO dw.Fato_Vendas (idData, idCliente, idFuncionario, idLoja, idProduto, quantidade, valor, lucro)
SELECT -1, -1, -1, -1, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO UPDATE SET 
    quantidade = EXCLUDED.quantidade, valor = EXCLUDED.valor, lucro = EXCLUDED.lucro;

-- 2. POR TEMPO (para queries 2, 5, 6 - análise temporal)
INSERT INTO dw.Fato_Vendas (idData, idCliente, idFuncionario, idLoja, idProduto, quantidade, valor, lucro)
SELECT CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER), -1, -1, -1, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
GROUP BY CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER)
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO UPDATE SET 
    quantidade = EXCLUDED.quantidade, valor = EXCLUDED.valor, lucro = EXCLUDED.lucro;

-- 3. POR CLIENTE (para queries 5, 6 - análise de clientes)
INSERT INTO dw.Fato_Vendas (idData, idCliente, idFuncionario, idLoja, idProduto, quantidade, valor, lucro)
SELECT -1, c.idCliente, -1, -1, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
JOIN dw.Dim_Cliente c ON v.tb010_cpf = c.cpf
GROUP BY c.idCliente
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO UPDATE SET 
    quantidade = EXCLUDED.quantidade, valor = EXCLUDED.valor, lucro = EXCLUDED.lucro;

-- 4. POR FUNCIONÁRIO (para queries 2, 3, 4 - análise de funcionários)
INSERT INTO dw.Fato_Vendas (idData, idCliente, idFuncionario, idLoja, idProduto, quantidade, valor, lucro)
SELECT -1, -1, v.tb005_matricula, -1, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
GROUP BY v.tb005_matricula
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO UPDATE SET 
    quantidade = EXCLUDED.quantidade, valor = EXCLUDED.valor, lucro = EXCLUDED.lucro;

-- 5. POR LOJA (para queries 4, 6 - análise por loja)
INSERT INTO dw.Fato_Vendas (idData, idCliente, idFuncionario, idLoja, idProduto, quantidade, valor, lucro)
SELECT -1, -1, -1, l.idLoja, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
JOIN stg.funcionarios f ON v.tb005_matricula = f.tb005_matricula
JOIN dw.Dim_Loja l ON f.tb004_cod_loja = l.idLoja
GROUP BY l.idLoja
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO UPDATE SET 
    quantidade = EXCLUDED.quantidade, valor = EXCLUDED.valor, lucro = EXCLUDED.lucro;

-- 6. POR PRODUTO (para query 1 - análise por categoria)
INSERT INTO dw.Fato_Vendas (idData, idCliente, idFuncionario, idLoja, idProduto, quantidade, valor, lucro)
SELECT -1, -1, -1, -1, p.idProduto,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
JOIN dw.Dim_Produto p ON v.tb012_cod_produto = p.codProdutoOrigem
GROUP BY p.idProduto
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO UPDATE SET 
    quantidade = EXCLUDED.quantidade, valor = EXCLUDED.valor, lucro = EXCLUDED.lucro;

-- 7. TEMPO + FUNCIONÁRIO (para query 2 - hierarquia tempo/funcionário)
INSERT INTO dw.Fato_Vendas (idData, idCliente, idFuncionario, idLoja, idProduto, quantidade, valor, lucro)
SELECT CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER), -1, v.tb005_matricula, -1, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
GROUP BY CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER), v.tb005_matricula
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO UPDATE SET 
    quantidade = EXCLUDED.quantidade, valor = EXCLUDED.valor, lucro = EXCLUDED.lucro;

-- 8. FUNCIONÁRIO + LOJA (para query 4 - funcionário por localidade)
INSERT INTO dw.Fato_Vendas (idData, idCliente, idFuncionario, idLoja, idProduto, quantidade, valor, lucro)
SELECT -1, -1, v.tb005_matricula, l.idLoja, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
JOIN stg.funcionarios f ON v.tb005_matricula = f.tb005_matricula
JOIN dw.Dim_Loja l ON f.tb004_cod_loja = l.idLoja
GROUP BY v.tb005_matricula, l.idLoja
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO UPDATE SET 
    quantidade = EXCLUDED.quantidade, valor = EXCLUDED.valor, lucro = EXCLUDED.lucro;

-- 9. CLIENTE + TEMPO (Data → Cliente → All → All → All) - FALTANDO!
INSERT INTO dw.Fato_Vendas (
    idData, idCliente, idFuncionario, idLoja, idProduto,
    quantidade, valor, lucro
)
SELECT 
    CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER), c.idCliente, -1, -1, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
JOIN dw.Dim_Cliente c ON v.tb010_cpf = c.cpf
GROUP BY CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER), c.idCliente
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) 
DO UPDATE SET 
    quantidade = EXCLUDED.quantidade,
    valor = EXCLUDED.valor,
    lucro = EXCLUDED.lucro;

-- 10. CLIENTE + LOJA (All → Cliente → All → Loja → All) - FALTANDO!
INSERT INTO dw.Fato_Vendas (
    idData, idCliente, idFuncionario, idLoja, idProduto,
    quantidade, valor, lucro
)
SELECT 
    -1, c.idCliente, -1, l.idLoja, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
JOIN dw.Dim_Cliente c ON v.tb010_cpf = c.cpf
JOIN stg.funcionarios f ON v.tb005_matricula = f.tb005_matricula
JOIN dw.Dim_Loja l ON f.tb004_cod_loja = l.idLoja
GROUP BY c.idCliente, l.idLoja
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) 
DO UPDATE SET 
    quantidade = EXCLUDED.quantidade,
    valor = EXCLUDED.valor,
    lucro = EXCLUDED.lucro;

-- 11. CLIENTE + LOJA + TEMPO (Data → Cliente → All → Loja → All) - FALTANDO!
INSERT INTO dw.Fato_Vendas (
    idData, idCliente, idFuncionario, idLoja, idProduto,
    quantidade, valor, lucro
)
SELECT 
    CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER), c.idCliente, -1, l.idLoja, -1,
    SUM(v.tb010_012_quantidade),
    SUM(v.tb010_012_valor_unitario * v.tb010_012_quantidade),
    SUM(calcular_lucro(v.tb012_cod_produto, v.tb010_012_valor_unitario, v.tb010_012_quantidade))
FROM stg.vendas v
JOIN dw.Dim_Cliente c ON v.tb010_cpf = c.cpf
JOIN stg.funcionarios f ON v.tb005_matricula = f.tb005_matricula
JOIN dw.Dim_Loja l ON f.tb004_cod_loja = l.idLoja
GROUP BY CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER), c.idCliente, l.idLoja
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) 
DO UPDATE SET 
    quantidade = EXCLUDED.quantidade,
    valor = EXCLUDED.valor,
    lucro = EXCLUDED.lucro;

    
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Agregações OLAP carregadas com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Agregações OLAP carregadas com sucesso';
END;
$$;

-- 6. PROCEDURE PARA ATUALIZAR ESTATISTICAS (MANTIDA)
CREATE OR REPLACE PROCEDURE dw.sp_atualizar_estatisticas()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('ATUALIZAR_ESTATISTICAS', 'INICIADO', 'Atualizando estatisticas', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Atualizando estatísticas...';
    
    ANALYZE dw.Dim_Tempo;
    ANALYZE dw.Dim_Cliente;
    ANALYZE dw.Dim_Funcionario;
    ANALYZE dw.Dim_Produto;
    ANALYZE dw.Dim_Loja;
    ANALYZE dw.Fato_Vendas;
    
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Estatísticas atualizadas com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;

    RAISE NOTICE 'Estatísticas atualizadas com sucesso';
END;
$$;
-- FUNÇÃO VERIFICAR STATUS 
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
        MAX(log.status)::VARCHAR(20),  -- cast explicito
        (SELECT COUNT(*) FROM dw.Fato_Vendas),
        (SELECT COUNT(*) FROM dw.Dim_Cliente),
        (SELECT COUNT(*) FROM dw.Dim_Produto)
    FROM dw.etl_log log
    WHERE log.data_execucao = (SELECT MAX(data_execucao) FROM dw.etl_log);
END;
$$ LANGUAGE plpgsql;