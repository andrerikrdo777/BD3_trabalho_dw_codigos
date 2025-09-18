-- 1. CRIAR O SCHEMA DO DATA WAREHOUSE
DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;

-- 2. CRIAR TABELAS DIMENSIONAIS
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

ALTER TABLE dw.Dim_Cliente ADD CONSTRAINT unique_cpf UNIQUE (cpf);
ALTER TABLE dw.Dim_Funcionario ADD CONSTRAINT unique_idFuncionario UNIQUE (idFuncionario);
ALTER TABLE dw.Dim_Produto ADD CONSTRAINT unique_codProdutoOrigem UNIQUE (codProdutoOrigem);
ALTER TABLE dw.Dim_Loja ADD CONSTRAINT unique_idLoja UNIQUE (idLoja);

