-- 1. CRIAR O SCHEMA DA STAGING AREA
DROP SCHEMA IF EXISTS stg CASCADE;
CREATE SCHEMA stg;

-- 2. CRIAR TABELAS ESPELHO NA STAGING (apenas colunas necessárias)
-- Tabela principal de Vendas
CREATE TABLE stg.vendas AS
SELECT
    tb010_012_cod_venda,
    tb010_cpf,
    tb012_cod_produto,
    tb005_matricula,
    tb010_012_data,
    tb010_012_quantidade,
    tb010_012_valor_unitario
FROM public.tb010_012_vendas;

-- Tabela de Compras (para cálculo do custo médio)
CREATE TABLE stg.compras AS
SELECT
    tb012_cod_produto,
    tb012_017_valor_unitario
FROM public.tb012_017_compras;

-- Tabela de Clientes
CREATE TABLE stg.clientes AS
SELECT
    tb010_cpf,
    tb010_nome
FROM public.tb010_clientes;

-- Tabela de Funcionários + Loja
CREATE TABLE stg.funcionarios AS
SELECT
    tb005_matricula,
    tb005_CPF,
    tb005_nome_completo,
    tb004_cod_loja
FROM public.tb005_funcionarios;

-- Tabela de Cargos (Precisa da junção para pegar o cargo mais recente)
CREATE TABLE stg.cargos_mais_recentes AS
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
WHERE cr.row_num = 1; -- Pega apenas o cargo mais recente de cada funcionário

-- Tabela de Produtos e Categorias
CREATE TABLE stg.produtos AS
SELECT
    p.tb012_cod_produto,
    p.tb012_descricao,
    cat.tb013_descricao as categoria
FROM public.tb012_produtos p
JOIN public.tb013_categorias cat ON p.tb013_cod_categoria = cat.tb013_cod_categoria;

-- Tabela de Descrição Detalhada dos Produtos (usando COALESCE)
CREATE TABLE stg.produtos_detalhes AS
SELECT
    p.tb012_cod_produto,
    COALESCE(a.tb014_detalhamento, e.tb015_detalhamento, v.tb016_detalhamento) as descricao_detalhada
FROM public.tb012_produtos p
LEFT JOIN public.tb014_prd_alimentos a ON p.tb012_cod_produto = a.tb012_cod_produto
LEFT JOIN public.tb015_prd_eletros e ON p.tb012_cod_produto = e.tb012_cod_produto
LEFT JOIN public.tb016_prd_vestuarios v ON p.tb012_cod_produto = v.tb012_cod_produto;

-- Tabela de Lojas + Endereço
CREATE TABLE stg.lojas AS
SELECT
    l.tb004_cod_loja,
    'Loja ' || l.tb004_cod_loja::TEXT as nome_loja, -- Criando um nome para a loja
    cid.tb002_nome_cidade as cidade,
    uf.tb001_sigla_uf as uf
FROM public.tb004_lojas l
JOIN public.tb003_enderecos e ON l.tb003_cod_endereco = e.tb003_cod_endereco
JOIN public.tb002_cidades cid ON (e.tb002_cod_cidade = cid.tb002_cod_cidade AND e.tb001_sigla_uf = cid.tb001_sigla_uf)
JOIN public.tb001_uf uf ON cid.tb001_sigla_uf = uf.tb001_sigla_uf;  
-- x x x x 