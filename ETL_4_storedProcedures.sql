-- 1. PROCEDURE PARA LIMPAR STAGING (CORRIGIDA)
CREATE OR REPLACE PROCEDURE dw.sp_limpar_staging()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    -- Log de início (CAPTURAR O ID)
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('LIMPAR_STAGING', 'INICIADO', 'Iniciando limpeza da staging', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Limpando staging area...';
    
    -- Operação principal
    DROP TABLE IF EXISTS
        stg.vendas,
        stg.compras,
        stg.clientes,
        stg.funcionarios,
        stg.cargos_mais_recentes,
        stg.produtos,
        stg.produtos_detalhes,
        stg.lojas;
    
    -- ATUALIZAR em vez de INSERIR novo
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

-- 3. PROCEDURE PARA LIMPAR DW (CORRIGIDA)
CREATE OR REPLACE PROCEDURE dw.sp_limpar_dw()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    -- Log de início (CAPTURAR O ID)
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('LIMPAR_DW', 'INICIADO', 'Iniciando limpeza do DW', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Limpando data warehouse...';
    
    -- ORDEM CORRETA: Primeiro a tabela fato, depois as dimensões
    TRUNCATE TABLE dw.Fato_Vendas RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Tempo RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Cliente RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Funcionario RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Produto RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dw.Dim_Loja RESTART IDENTITY CASCADE;
    
    -- ATUALIZAR em vez de INSERIR novo
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Data warehouse limpo com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Data warehouse limpo com sucesso';
END;
$$;

-- 4. PROCEDURE PARA CARREGAR DW (CORRIGIDA)
CREATE OR REPLACE PROCEDURE dw.sp_carregar_dw()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    -- Log de início (CAPTURAR O ID)
    INSERT INTO dw.etl_log (tipo_operacao, status, mensagem, data_execucao)
    VALUES ('CARREGAR_DW', 'INICIADO', 'Carregando dados para DW', inicio_time)
    RETURNING id_log INTO log_id;
    
    RAISE NOTICE 'Carregando dados para data warehouse...';
    
    -- Popula Dim_Tempo
    INSERT INTO dw.Dim_Tempo (idData, dataCompleta, ano, mes, dia)
    SELECT DISTINCT
        DATE(tb010_012_data) as idData,
        DATE(tb010_012_data) as dataCompleta,
        EXTRACT(YEAR FROM tb010_012_data) as ano,
        EXTRACT(MONTH FROM tb010_012_data) as mes,
        EXTRACT(DAY FROM tb010_012_data) as dia
    FROM stg.vendas
    ON CONFLICT (idData) DO NOTHING;

    -- Popula Dim_Cliente
    INSERT INTO dw.Dim_Cliente (cpf, nomeCliente)
    SELECT
        tb010_cpf,
        tb010_nome
    FROM stg.clientes
    ON CONFLICT (cpf) DO NOTHING;

    -- Popula Dim_Funcionario
    INSERT INTO dw.Dim_Funcionario (idFuncionario, cpf, nomeFuncionario, cargo)
    SELECT
        f.tb005_matricula,
        f.tb005_CPF,
        f.tb005_nome_completo,
        COALESCE(c.tb006_nome_cargo, 'Cargo Não Definido')
    FROM stg.funcionarios f
    LEFT JOIN stg.cargos_mais_recentes c ON f.tb005_matricula = c.tb005_matricula
    ON CONFLICT (idFuncionario) DO NOTHING;

    -- Popula Dim_Produto
    INSERT INTO dw.Dim_Produto (codProdutoOrigem, nomeProduto, categoria, descricaoProduto)
    SELECT
        p.tb012_cod_produto,
        p.tb012_descricao,
        p.categoria,
        COALESCE(pd.descricao_detalhada, 'Sem descrição detalhada')
    FROM stg.produtos p
    LEFT JOIN stg.produtos_detalhes pd ON p.tb012_cod_produto = pd.tb012_cod_produto
    ON CONFLICT (codProdutoOrigem) DO NOTHING;

    -- Popula Dim_Loja
    INSERT INTO dw.Dim_Loja (idLoja, nomeLoja, cidade, uf)
    SELECT
        tb004_cod_loja,
        nome_loja,
        cidade,
        uf
    FROM stg.lojas
    ON CONFLICT (idLoja) DO NOTHING;

    -- Popula Fato_Vendas
    CREATE TEMP TABLE custo_medio_produto AS
    SELECT
        tb012_cod_produto,
        AVG(tb012_017_valor_unitario) as custo_medio
    FROM stg.compras
    GROUP BY tb012_cod_produto;

    INSERT INTO dw.Fato_Vendas (
        idData, idCliente, idFuncionario, idLoja, idProduto,
        quantidade, valorTotal, custoTotal, lucroTotal
    )
    SELECT
        DATE(v.tb010_012_data) as idData,
        c.idCliente,
        f.idFuncionario,
        l.idLoja,
        p.idProduto,
        v.tb010_012_quantidade as quantidade,
        (v.tb010_012_quantidade * v.tb010_012_valor_unitario)::NUMERIC(12,2) as valorTotal,
        (v.tb010_012_quantidade * COALESCE(cmp.custo_medio, 0))::NUMERIC(12,2) as custoTotal,
        ( (v.tb010_012_quantidade * v.tb010_012_valor_unitario) - 
          (v.tb010_012_quantidade * COALESCE(cmp.custo_medio, 0)) )::NUMERIC(12,2) as lucroTotal
    FROM stg.vendas v
    JOIN dw.Dim_Cliente c ON v.tb010_cpf = c.cpf
    JOIN dw.Dim_Funcionario f ON v.tb005_matricula = f.idFuncionario
    JOIN stg.funcionarios sf ON v.tb005_matricula = sf.tb005_matricula
    JOIN dw.Dim_Loja l ON sf.tb004_cod_loja = l.idLoja
    JOIN dw.Dim_Produto p ON v.tb012_cod_produto = p.codProdutoOrigem
    LEFT JOIN custo_medio_produto cmp ON v.tb012_cod_produto = cmp.tb012_cod_produto
    ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO NOTHING;

    DROP TABLE IF EXISTS custo_medio_produto;
    
    -- ATUALIZAR em vez de INSERIR novo
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Data warehouse carregado com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;
    
    RAISE NOTICE 'Data warehouse carregado com sucesso';
END;
$$;

-- 5. PROCEDURE PARA ATUALIZAR ESTATISTICAS (CORRIGIDA)
CREATE OR REPLACE PROCEDURE dw.sp_atualizar_estatisticas()
LANGUAGE plpgsql
AS $$
DECLARE
    inicio_time TIMESTAMP := NOW();
    log_id INTEGER;
BEGIN
    -- Log de início (CAPTURAR O ID)
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
    
    -- ATUALIZAR em vez de INSERIR novo
    UPDATE dw.etl_log 
    SET status = 'SUCESSO',
        mensagem = 'Estatísticas atualizadas com sucesso',
        duracao = NOW() - inicio_time
    WHERE id_log = log_id;

    RAISE NOTICE 'Estatísticas atualizadas com sucesso';
END;
$$;