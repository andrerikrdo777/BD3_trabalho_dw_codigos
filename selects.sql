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