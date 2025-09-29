

-- 1. Valor das vendas por funcionário e tempo 
SELECT 
    dt.ano, 
    dt.mes, 
    df.nomeFuncionario,
    fv.valor AS "ValorTotalVendas"
FROM dw.Fato_Vendas AS fv 
JOIN dw.Dim_Funcionario AS df ON fv.idFuncionario = df.idFuncionario 
JOIN dw.Dim_Tempo AS dt ON fv.idData = dt.idData
WHERE fv.idCliente = -1 
  AND fv.idLoja = -1 
  AND fv.idProduto = -1
  AND fv.idData != -1
  AND fv.idFuncionario != -1
ORDER BY dt.ano, dt.mes, df.nomeFuncionario;

-- 2. Volume das vendas por funcionário (OLAP)
SELECT 
    df.nomeFuncionario, 
    fv.quantidade AS "TotalProdutosVendidos", 
    fv.valor AS "ValorTotalVendas"
FROM dw.Fato_Vendas AS fv 
JOIN dw.Dim_Funcionario AS df ON fv.idFuncionario = df.idFuncionario
WHERE fv.idData = -1 
  AND fv.idCliente = -1 
  AND fv.idLoja = -1 
  AND fv.idProduto = -1
  AND fv.idFuncionario != -1
ORDER BY "ValorTotalVendas" DESC;

-- 3. Atendimentos por funcionário e localidade (OLAP)
SELECT 
    df.nomeFuncionario, 
    dl.nomeLoja, 
    dl.cidade, 
    dl.uf, 
    fv.quantidade AS "NumeroDeAtendimentos"
FROM dw.Fato_Vendas AS fv 
JOIN dw.Dim_Funcionario AS df ON fv.idFuncionario = df.idFuncionario 
JOIN dw.Dim_Loja AS dl ON fv.idLoja = dl.idLoja
WHERE fv.idData = -1 
  AND fv.idCliente = -1 
  AND fv.idProduto = -1
  AND fv.idFuncionario != -1
  AND fv.idLoja != -1
ORDER BY df.nomeFuncionario, "NumeroDeAtendimentos" DESC;


-- 4. Valor das últimas vendas por cliente 
WITH UltimaVendaCliente AS (
    SELECT 
        fv.idCliente,
        MAX(fv.idData) as ultima_data
    FROM dw.Fato_Vendas fv
    WHERE fv.idFuncionario = -1 
      AND fv.idLoja = -1 
      AND fv.idProduto = -1
      AND fv.idCliente != -1
      AND fv.idData != -1
    GROUP BY fv.idCliente
)
SELECT 
    dc.nomeCliente,
    fv.valor AS "valorTotal",
    dt.dataCompleta
FROM dw.Fato_Vendas fv
JOIN dw.Dim_Cliente dc ON fv.idCliente = dc.idCliente
JOIN dw.Dim_Tempo dt ON fv.idData = dt.idData
JOIN UltimaVendaCliente uvc ON fv.idCliente = uvc.idCliente AND fv.idData = uvc.ultima_data
WHERE fv.idFuncionario = -1 
  AND fv.idLoja = -1 
  AND fv.idProduto = -1
ORDER BY dc.nomeCliente;

-- 5. Clientes que mais compraram na loja virtual com valor acumulado por período

SELECT 
    dc.nomeCliente, 
    fv.valor AS "ValorAcumulado",
    'Loja Virtual' as tipo_loja,
    dt.dataCompleta
FROM dw.Fato_Vendas fv 
JOIN dw.Dim_Cliente dc ON fv.idCliente = dc.idCliente 
JOIN dw.Dim_Tempo dt ON fv.idData = dt.idData
WHERE fv.idFuncionario = -1 
  AND fv.idProduto = -1
  AND fv.idCliente != -1
  AND fv.idData != -1
  AND fv.idLoja = -1  -- Apenas loja virtual
  AND dt.dataCompleta BETWEEN '2023-01-01' AND '2025-03-31'
ORDER BY "ValorAcumulado" desc;

-- 6. Quantidade de vendas por categoria (OLAP)
	SELECT 
	    p.categoria,
	    SUM(fv.quantidade) AS "QuantidadeTotalVendida"
	FROM dw.Fato_Vendas AS fv 
	JOIN dw.Dim_Produto AS p ON fv.idProduto = p.idProduto
	WHERE fv.idData = -1 
	  AND fv.idCliente = -1 
	  AND fv.idFuncionario = -1 
	  AND fv.idLoja = -1
	  AND fv.idProduto != -1
	GROUP BY p.categoria
	ORDER BY "QuantidadeTotalVendida" DESC;

SELECT 
    'Total por Categoria' as categoria,
    fv.quantidade AS "QuantidadeTotalVendida"
FROM dw.Fato_Vendas AS fv 
WHERE fv.idData = -1 
  AND fv.idCliente = -1 
  AND fv.idFuncionario = -1 
  AND fv.idLoja = -1
  AND fv.idProduto = -1;



