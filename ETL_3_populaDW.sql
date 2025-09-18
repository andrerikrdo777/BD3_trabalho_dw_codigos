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