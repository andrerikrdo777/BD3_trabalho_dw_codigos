-- BD_VAREJO_Posgres_DDL
-- PostgreSQL

-- DROP DATABASE IF EXISTS ADS;
-- CREATE DATABASE ADS;

-- DDL 

DROP TABLE IF EXISTS tb010_clientes_antigos CASCADE;
DROP TABLE IF EXISTS tb016_prd_vestuarios CASCADE;
DROP TABLE IF EXISTS tb011_logins CASCADE;
DROP TABLE IF EXISTS tb015_prd_eletros CASCADE;
DROP TABLE IF EXISTS tb014_prd_alimentos CASCADE;
DROP TABLE IF EXISTS tb005_006_funcionarios_cargos CASCADE;
DROP TABLE IF EXISTS tb006_cargos CASCADE;
DROP TABLE IF EXISTS tb010_012_vendas CASCADE;
DROP TABLE IF EXISTS tb010_clientes CASCADE;
DROP TABLE IF EXISTS tb005_funcionarios CASCADE;
DROP TABLE IF EXISTS tb004_lojas CASCADE;
DROP TABLE IF EXISTS tb999_log CASCADE;
DROP TABLE IF EXISTS tb012_017_compras CASCADE;
DROP TABLE IF EXISTS tb017_fornecedores CASCADE;
DROP TABLE IF EXISTS tb003_enderecos CASCADE;
DROP TABLE IF EXISTS tb002_cidades CASCADE;
DROP TABLE IF EXISTS tb001_uf CASCADE;
DROP TABLE IF EXISTS tb012_produtos CASCADE;
DROP TABLE IF EXISTS tb013_categorias CASCADE;

CREATE TABLE tb001_uf
( 
	tb001_sigla_uf       VARCHAR(2)  NOT NULL,
	tb001_nome_estado    VARCHAR(255)  NOT NULL
);

ALTER TABLE tb001_uf
	ADD CONSTRAINT XPKtb001_uf PRIMARY KEY (tb001_sigla_uf);
	
CREATE TABLE tb002_cidades
( 
	tb002_cod_cidade     SERIAL,
	tb001_sigla_uf       VARCHAR(2)  NOT NULL,
	tb002_nome_cidade    VARCHAR(255)  NOT NULL
);

ALTER TABLE tb002_cidades
	ADD CONSTRAINT XPKtb002_cidades PRIMARY KEY (tb002_cod_cidade, tb001_sigla_uf);
	
CREATE TABLE tb003_enderecos
( 
	tb003_cod_endereco   SERIAL,
	tb001_sigla_uf       VARCHAR(2)  NOT NULL,
	tb002_cod_cidade     INTEGER  NOT NULL,
	tb003_nome_rua       VARCHAR(255)  NOT NULL,
	tb003_numero_rua     VARCHAR(10)  NOT NULL,
	tb003_complemento    VARCHAR(255)  NULL,
	tb003_ponto_referencia VARCHAR(255)  NULL,
	tb003_bairro         VARCHAR(255)  NOT NULL,
	tb003_CEP            VARCHAR(15)  NOT NULL
);

ALTER TABLE tb003_enderecos
	ADD CONSTRAINT XPKtb003_enderecos PRIMARY KEY (tb003_cod_endereco);
	
CREATE TABLE tb004_lojas
( 
	tb004_cod_loja       SERIAL,
	tb003_cod_endereco   INTEGER  NULL,
	tb004_matriz         INTEGER  NULL,
	tb004_cnpj_loja      VARCHAR(20)  NOT NULL,
	tb004_inscricao_estadual VARCHAR(20)  NULL
);

ALTER TABLE tb004_lojas
	ADD CONSTRAINT XPKtb004_lojas PRIMARY KEY (tb004_cod_loja);
	
CREATE TABLE tb005_006_funcionarios_cargos
( 
	tb005_matricula      INTEGER  NOT NULL,
	tb006_cod_cargo      INTEGER  NOT NULL,
	tb005_006_valor_cargo NUMERIC(10,2)  NOT NULL,
	tb005_006_perc_comissao_cargo NUMERIC(5,2)  NOT NULL,
	tb005_006_data_promocao TIMESTAMP  NOT NULL
);

ALTER TABLE tb005_006_funcionarios_cargos
	ADD CONSTRAINT XPKtb005_006_funcionarios_cargos PRIMARY KEY (tb005_matricula, tb006_cod_cargo);
	
CREATE TABLE tb005_funcionarios
( 
	tb005_matricula      SERIAL,
	tb004_cod_loja       INTEGER  NOT NULL,
	tb003_cod_endereco   INTEGER  NOT NULL,
	tb005_nome_completo  VARCHAR(255)  NOT NULL,
	tb005_data_nascimento TIMESTAMP  NOT NULL,
	tb005_CPF            VARCHAR(17)  NOT NULL,
	tb005_RG             VARCHAR(15)  NOT NULL,
	tb005_status         VARCHAR(20)  NOT NULL,
	tb005_data_contratacao TIMESTAMP  NOT NULL,
	tb005_data_demissao  TIMESTAMP  NULL
);

ALTER TABLE tb005_funcionarios
	ADD CONSTRAINT XPKtb005_funcionarios PRIMARY KEY (tb005_matricula);
	
CREATE TABLE tb006_cargos
( 
	tb006_cod_cargo      SERIAL,
	tb006_nome_cargo     VARCHAR(255)  NOT NULL
);

ALTER TABLE tb006_cargos
	ADD CONSTRAINT XPKtb006_cargos PRIMARY KEY (tb006_cod_cargo);
	
CREATE TABLE tb010_012_vendas
( 
	tb010_012_cod_venda  SERIAL,
	tb010_cpf            NUMERIC(15)  NOT NULL,
	tb012_cod_produto    INTEGER  NOT NULL,
	tb005_matricula      INTEGER  NOT NULL,
	tb010_012_data       TIMESTAMP  NOT NULL,
	tb010_012_quantidade INTEGER  NOT NULL,
	tb010_012_valor_unitario NUMERIC(12,4)  NOT NULL
);

ALTER TABLE tb010_012_vendas
	ADD CONSTRAINT XPKtb010_012_vendas PRIMARY KEY (tb010_012_cod_venda, tb005_matricula, tb010_cpf, tb012_cod_produto);
	
CREATE TABLE tb010_clientes
( 
	tb010_cpf            NUMERIC(15)  NOT NULL,
	tb010_nome           VARCHAR(255)  NOT NULL,
	tb010_fone_residencial VARCHAR(255)  NOT NULL,
	tb010_fone_celular   VARCHAR(255)  NULL
);

ALTER TABLE tb010_clientes
	ADD CONSTRAINT XPKtb010_clientes PRIMARY KEY (tb010_cpf);
	
CREATE TABLE tb010_clientes_antigos
( 
	tb010_cpf            NUMERIC(15,0)  NOT NULL,
	tb010_nome           VARCHAR(255)  NULL
);

ALTER TABLE tb010_clientes_antigos
	ADD CONSTRAINT XPKtb010_clientes_antigos PRIMARY KEY (tb010_cpf);
	
CREATE TABLE tb011_logins
( 
	tb011_logins         VARCHAR(255)  NOT NULL,
	tb010_cpf            NUMERIC(15)  NOT NULL,
	tb011_senha          VARCHAR(255)  NOT NULL,
	tb011_data_cadastro  TIMESTAMP  NULL
);

ALTER TABLE tb011_logins
	ADD CONSTRAINT XPKtb011_logins PRIMARY KEY (tb011_logins);
	
CREATE TABLE tb012_017_compras
( 
	tb012_017_cod_compra SERIAL,
	tb012_cod_produto    INTEGER  NOT NULL,
	tb017_cod_fornecedor INTEGER  NOT NULL,
	tb012_017_data       TIMESTAMP  NULL,
	tb012_017_quantidade INTEGER  NULL,
	tb012_017_valor_unitario NUMERIC(12,2)  NULL
);

ALTER TABLE tb012_017_compras
	ADD CONSTRAINT XPKtb017_compras PRIMARY KEY (tb012_017_cod_compra, tb012_cod_produto, tb017_cod_fornecedor);
	
CREATE TABLE tb012_produtos
( 
	tb012_cod_produto    INTEGER  NOT NULL,
	tb013_cod_categoria  INTEGER  NOT NULL,
	tb012_descricao      VARCHAR(255)  NOT NULL
);

ALTER TABLE tb012_produtos
	ADD CONSTRAINT XPKtb012_produtos PRIMARY KEY (tb012_cod_produto);
	
CREATE TABLE tb013_categorias
( 
	tb013_cod_categoria  SERIAL,
	tb013_descricao      VARCHAR(255)  NOT NULL
);

ALTER TABLE tb013_categorias
	ADD CONSTRAINT XPKtb013_categorias PRIMARY KEY (tb013_cod_categoria);
	
CREATE TABLE tb014_prd_alimentos
( 
	tb014_cod_prd_alimentos SERIAL,
	tb012_cod_produto    INTEGER  NOT NULL,
	tb014_detalhamento   VARCHAR(255)  NOT NULL,
	tb014_unidade_medida VARCHAR(255)  NOT NULL,
	tb014_num_lote       VARCHAR(255)  NULL,
	tb014_data_vencimento TIMESTAMP  NULL,
	tb014_valor_sugerido NUMERIC(10,2)  NULL
);

ALTER TABLE tb014_prd_alimentos
	ADD CONSTRAINT XPKtb014_prd_alimentos PRIMARY KEY (tb014_cod_prd_alimentos, tb012_cod_produto);
	
CREATE TABLE tb015_prd_eletros
( 
	tb015_cod_prd_eletro SERIAL,
	tb012_cod_produto    INTEGER  NOT NULL,
	tb015_detalhamento   VARCHAR(255)  NOT NULL,
	tb015_tensao         VARCHAR(255)  NULL,
	tb015_nivel_consumo_procel CHAR(1)  NULL,
	tb015_valor_sugerido NUMERIC(10,2)  NULL
);

ALTER TABLE tb015_prd_eletros
	ADD CONSTRAINT XPKtb015_prd_tvs PRIMARY KEY (tb015_cod_prd_eletro, tb012_cod_produto);
	
CREATE TABLE tb016_prd_vestuarios
( 
	tb016_cod_prd_vestuario SERIAL,
	tb012_cod_produto    INTEGER  NOT NULL,
	tb016_detalhamento   VARCHAR(255)  NOT NULL,
	tb016_sexo           CHAR(1)  NOT NULL,
	tb016_tamanho        VARCHAR(255)  NULL,
	tb016_numeracao      NUMERIC(3)  NULL,
	tb016_valor_sugerido NUMERIC(10,2)  NULL
);

ALTER TABLE tb016_prd_vestuarios
	ADD CONSTRAINT XPKtb016_refrigeradores PRIMARY KEY (tb016_cod_prd_vestuario, tb012_cod_produto);
	
CREATE TABLE tb017_fornecedores
( 
	tb017_cod_fornecedor SERIAL,
	tb017_razao_social   VARCHAR(255)  NULL,
	tb017_nome_fantasia  VARCHAR(255)  NULL,
	tb017_fone           VARCHAR(15)  NULL,
	tb003_cod_endereco   INTEGER  NULL
);

ALTER TABLE tb017_fornecedores
	ADD CONSTRAINT XPKtb017_fornecedor PRIMARY KEY (tb017_cod_fornecedor);
	
CREATE TABLE tb999_log
( 
	tb999_cod_log        SERIAL,
	tb099_objeto         VARCHAR(100)  NOT NULL,
	tb999_dml            VARCHAR(25)  NOT NULL,
	tb999_data           TIMESTAMP  NOT NULL
);

-- CONSTRAINTS 

ALTER TABLE tb999_log
	ADD CONSTRAINT XPKtb999_log PRIMARY KEY (tb999_cod_log);
	
ALTER TABLE tb002_cidades
	ADD CONSTRAINT CONST_UF_CIDADE FOREIGN KEY (tb001_sigla_uf) REFERENCES tb001_uf(tb001_sigla_uf)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb003_enderecos
	ADD CONSTRAINT CONST_CIDADE_END FOREIGN KEY (tb002_cod_cidade, tb001_sigla_uf) REFERENCES tb002_cidades(tb002_cod_cidade, tb001_sigla_uf)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb004_lojas
	ADD CONSTRAINT CONST_END_LOJAS FOREIGN KEY (tb003_cod_endereco) REFERENCES tb003_enderecos(tb003_cod_endereco)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb005_006_funcionarios_cargos
	ADD CONSTRAINT CONST_FUNC_FUNCCARGO FOREIGN KEY (tb005_matricula) REFERENCES tb005_funcionarios(tb005_matricula)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb005_006_funcionarios_cargos
	ADD CONSTRAINT CONST_CARGO_FUNCCARGO FOREIGN KEY (tb006_cod_cargo) REFERENCES tb006_cargos(tb006_cod_cargo)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb005_funcionarios
	ADD CONSTRAINT CONST_END_FUNC FOREIGN KEY (tb003_cod_endereco) REFERENCES tb003_enderecos(tb003_cod_endereco)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb005_funcionarios
	ADD CONSTRAINT CONST_LOJAS_FUNC FOREIGN KEY (tb004_cod_loja) REFERENCES tb004_lojas(tb004_cod_loja)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb010_012_vendas
	ADD CONSTRAINT CONST_FUNC_VENDAS FOREIGN KEY (tb005_matricula) REFERENCES tb005_funcionarios(tb005_matricula)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb010_012_vendas
	ADD CONSTRAINT CONST_CLI_VENDAS FOREIGN KEY (tb010_cpf) REFERENCES tb010_clientes(tb010_cpf)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb010_012_vendas
	ADD CONSTRAINT CONST_PRD_VENDAS FOREIGN KEY (tb012_cod_produto) REFERENCES tb012_produtos(tb012_cod_produto)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb011_logins
	ADD CONSTRAINT CONST_CLI_LOGIN FOREIGN KEY (tb010_cpf) REFERENCES tb010_clientes(tb010_cpf)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb012_017_compras
	ADD CONSTRAINT CONST_PRD_COMPRAS FOREIGN KEY (tb012_cod_produto) REFERENCES tb012_produtos(tb012_cod_produto)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb012_017_compras
	ADD CONSTRAINT CONST_FORN_COMPRAS FOREIGN KEY (tb017_cod_fornecedor) REFERENCES tb017_fornecedores(tb017_cod_fornecedor)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb012_produtos
	ADD CONSTRAINT CONST_CAT_PRD FOREIGN KEY (tb013_cod_categoria) REFERENCES tb013_categorias(tb013_cod_categoria)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb014_prd_alimentos
	ADD CONSTRAINT CONST_PRD_ALIM FOREIGN KEY (tb012_cod_produto) REFERENCES tb012_produtos(tb012_cod_produto)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb015_prd_eletros
	ADD CONSTRAINT CONST_PRD_ELET FOREIGN KEY (tb012_cod_produto) REFERENCES tb012_produtos(tb012_cod_produto)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb016_prd_vestuarios
	ADD CONSTRAINT CONST_PRD_VEST FOREIGN KEY (tb012_cod_produto) REFERENCES tb012_produtos(tb012_cod_produto)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;
		
ALTER TABLE tb017_fornecedores
	ADD CONSTRAINT CONST_END_FORN FOREIGN KEY (tb003_cod_endereco) REFERENCES tb003_enderecos(tb003_cod_endereco)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION;