------ 1. STAGING AREA 
------
------
------
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
------ 2. CRIA SCHEMA DW
------
------
------
-- CRIAR O SCHEMA DO DATA WAREHOUSE
DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;

-- CRIAR TABELAS DIMENSIONAIS
CREATE TABLE dw.Dim_Tempo (
    idData DATE PRIMARY KEY,
    dataCompleta DATE,
    ano INT,
    mes INT,
    dia INT
);

CREATE TABLE dw.Dim_Cliente (
    idCliente SERIAL PRIMARY KEY, -- Usando SERIAL para chave substituta
    cpf BIGINT, -- CPF da fonte é NUMERIC(15), usando BIGINT
    nomeCliente VARCHAR(255) -- Aumentando para VARCHAR(255)
);

CREATE TABLE dw.Dim_Funcionario (
    idFuncionario INT PRIMARY KEY, -- Será a matrícula
    cpf VARCHAR(17), -- CPF da fonte é VARCHAR(17)
    nomeFuncionario VARCHAR(255), -- Aumentando para VARCHAR(255)
    cargo VARCHAR(255) -- Aumentando para VARCHAR(255)
);

CREATE TABLE dw.Dim_Produto (
    idProduto SERIAL PRIMARY KEY, -- Chave substituta
    codProdutoOrigem INT, -- Código original da fonte
    nomeProduto VARCHAR(255), -- Aumentando para VARCHAR(255)
    categoria VARCHAR(255), -- Aumentando para VARCHAR(255)
    descricaoProduto VARCHAR(255) -- Aumentando para VARCHAR(255)
);

CREATE TABLE dw.Dim_Loja (
    idLoja INT PRIMARY KEY, -- Será o código da loja
    nomeLoja VARCHAR(100),
    cidade VARCHAR(100),
    uf CHAR(2)
);

-- 3. CRIAR TABELA FATO
CREATE TABLE dw.Fato_Vendas (
    idData DATE REFERENCES dw.Dim_Tempo(idData),
    idCliente INT REFERENCES dw.Dim_Cliente(idCliente),
    idFuncionario INT REFERENCES dw.Dim_Funcionario(idFuncionario),
    idLoja INT REFERENCES dw.Dim_Loja(idLoja),
    idProduto INT REFERENCES dw.Dim_Produto(idProduto),
    quantidade INT NOT NULL,
    valorTotal NUMERIC(12,2) NOT NULL,
    custoTotal NUMERIC(12,2) NOT NULL,
    lucroTotal NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (idData, idCliente, idFuncionario, idLoja, idProduto) -- Chave primária composta
);
------ 3. POPULA DW
------
------
------
-- POPULA DIMENSOES ---------------------------

-- insert Dim_Tempo 
INSERT INTO dw.Dim_Tempo (idData, dataCompleta, ano, mes, dia)
SELECT DISTINCT
    DATE(tb010_012_data) as idData,
    DATE(tb010_012_data) as dataCompleta,
    EXTRACT(YEAR FROM tb010_012_data) as ano,
    EXTRACT(MONTH FROM tb010_012_data) as mes,
    EXTRACT(DAY FROM tb010_012_data) as dia
FROM stg.vendas
ON CONFLICT (idData) DO NOTHING;

-- insert Dim_Cliente
INSERT INTO dw.Dim_Cliente (cpf, nomeCliente)
SELECT
    tb010_cpf,
    tb010_nome
FROM stg.clientes
ON CONFLICT (cpf) DO NOTHING; -- Assume que CPF é único

-- insert Dim_Funcionario (cargo mais recente do funcionario)
INSERT INTO dw.Dim_Funcionario (idFuncionario, cpf, nomeFuncionario, cargo)
SELECT
    f.tb005_matricula,
    f.tb005_CPF,
    f.tb005_nome_completo,
    COALESCE(c.tb006_nome_cargo, 'Cargo Não Definido') -- Usando COALESCE para evitar NULL
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
ON CONFLICT (codProdutoOrigem) DO NOTHING; -- Assume que codProdutoOrigem é único

-- Popula Dim_Loja
INSERT INTO dw.Dim_Loja (idLoja, nomeLoja, cidade, uf)
SELECT
    tb004_cod_loja,
    nome_loja,
    cidade,
    uf
FROM stg.lojas
ON CONFLICT (idLoja) DO NOTHING;

-- POPULA TABELA FATO

-- calculando custo medio por produto
CREATE TEMP TABLE custo_medio_produto AS
SELECT
    tb012_cod_produto,
    AVG(tb012_017_valor_unitario) as custo_medio
FROM stg.compras
GROUP BY tb012_cod_produto;

-- insert Fato_vendas
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
JOIN stg.funcionarios sf ON v.tb005_matricula = sf.tb005_matricula -- Para pegar a loja do funcionário
JOIN dw.Dim_Loja l ON sf.tb004_cod_loja = l.idLoja
JOIN dw.Dim_Produto p ON v.tb012_cod_produto = p.codProdutoOrigem
LEFT JOIN custo_medio_produto cmp ON v.tb012_cod_produto = cmp.tb012_cod_produto
ON CONFLICT (idData, idCliente, idFuncionario, idLoja, idProduto) DO NOTHING;



/** FALTA:
STORED PROCEDURES
TRIGGERS
SCRIPTS
AGENDAMENTOS
**/