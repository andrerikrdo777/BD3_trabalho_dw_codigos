---
---
---
INSERT INTO dw.Dim_Tempo (idData, dataCompleta, ano, mes, dia) 
VALUES (-1, NULL, -1, -1, -1)
ON CONFLICT (idData) DO NOTHING;

INSERT INTO dw.Dim_Cliente (idCliente, cpf, nomeCliente)
VALUES (-1, -1, 'Todos os Clientes')
ON CONFLICT (idCliente) DO NOTHING;

INSERT INTO dw.Dim_Funcionario (idFuncionario, cpf, nomeFuncionario, cargo)
VALUES (-1, '-1', 'Todos os Funcionários', 'Total')
ON CONFLICT (idFuncionario) DO NOTHING;

INSERT INTO dw.Dim_Produto (idProduto, codProdutoOrigem, nomeProduto, categoria, descricaoProduto)
VALUES (-1, -1, 'Todos os Produtos', 'Total', 'Agregado de todos os produtos')
ON CONFLICT (idProduto) DO NOTHING;

INSERT INTO dw.Dim_Loja (idLoja, nomeLoja, cidade, uf)
VALUES (-1, 'Todas as Lojas', 'Total', 'TT')
ON CONFLICT (idLoja) DO NOTHING;
---
---
-- POPULA DIMENSOES ---------------------------

INSERT INTO dw.Dim_Tempo (idData, dataCompleta, ano, mes, dia)
SELECT DISTINCT
    CAST(TO_CHAR(v.tb010_012_data, 'YYYYMMDD') AS INTEGER) as idData,
    DATE(v.tb010_012_data) as dataCompleta,
    EXTRACT(YEAR FROM v.tb010_012_data) as ano,
    EXTRACT(MONTH FROM v.tb010_012_data) as mes,
    EXTRACT(DAY FROM v.tb010_012_data) as dia
FROM stg.vendas v
ON CONFLICT (idData) DO NOTHING;

INSERT INTO dw.Dim_Cliente (cpf, nomeCliente)
SELECT
    c.tb010_cpf,
    c.tb010_nome
FROM stg.clientes c
ON CONFLICT (cpf) DO NOTHING;

INSERT INTO dw.Dim_Funcionario (idFuncionario, cpf, nomeFuncionario, cargo)
SELECT
    f.tb005_matricula,
    f.tb005_CPF,
    f.tb005_nome_completo,
    COALESCE(c.tb006_nome_cargo, 'Cargo Não Definido')
FROM stg.funcionarios f
LEFT JOIN stg.cargos_mais_recentes c ON f.tb005_matricula = c.tb005_matricula
ON CONFLICT (idFuncionario) DO NOTHING;

INSERT INTO dw.Dim_Produto (codProdutoOrigem, nomeProduto, categoria, descricaoProduto)
SELECT
    p.tb012_cod_produto,
    p.tb012_descricao,
    p.categoria,
    COALESCE(pd.descricao_detalhada, 'Sem descrição detalhada')
FROM stg.produtos p
LEFT JOIN stg.produtos_detalhes pd ON p.tb012_cod_produto = pd.tb012_cod_produto
ON CONFLICT (codProdutoOrigem) DO NOTHING;

INSERT INTO dw.Dim_Loja (idLoja, nomeLoja, cidade, uf)
SELECT
    l.tb004_cod_loja,
    l.nome_loja,
    l.cidade,
    l.uf
FROM stg.lojas l
ON CONFLICT (idLoja) DO NOTHING;
---
--- Funcao calcular_lucro
CREATE OR REPLACE FUNCTION calcular_lucro(cod_produto INT, valor_unitario NUMERIC, quantidade INT)
RETURNS NUMERIC AS $$
DECLARE
    custo_medio NUMERIC;
BEGIN
    SELECT AVG(tb012_017_valor_unitario) INTO custo_medio
    FROM stg.compras 
    WHERE tb012_cod_produto = cod_produto;
    
    RETURN (valor_unitario - COALESCE(custo_medio, 0)) * quantidade;
END;
$$ LANGUAGE plpgsql;
--- 



-- Total Geral
-- POPULA FATO_VENDAS COM TODAS AS AGREGAÇÕES OLAP POSSÍVEIS

delete from dw.fato_vendas 
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
--
--
--
select iddata from dw.fato_vendas where iddata!= -1 order by iddata asc
select iddata from dw.dim_tempo where iddata!= -1 order by iddata asc