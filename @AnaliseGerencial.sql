-- 1. Quantidade de vendas agrupada por tipo e categoria
select p.categoria,SUM(fv.quantidade) AS "QuantidadeTotalVendida"
from dw.Fato_Vendas AS fv join dw.Dim_Produto AS p ON fv.idProduto = p.idProduto
GROUP BY p.categoria
ORDER by "QuantidadeTotalVendida" DESC;

-- 2. Valor das vendas por funcionário, permitindo uma visão hierárquica por tempo.
select dt.ano, dt.mes,df.nomeFuncionario,SUM(fv.valorTotal) AS "ValorTotalVendas"
from dw.Fato_Vendas AS fv join dw.Dim_Funcionario AS df ON fv.idFuncionario = df.idFuncionario join dw.Dim_Tempo AS dt ON fv.idData = dt.idData
GROUP by dt.ano, dt.mes, df.nomeFuncionario
ORDER by dt.ano, dt.mes,df.nomeFuncionario;

-- 3. Volume das vendas por funcionário
select df.nomeFuncionario, SUM(fv.quantidade) AS "TotalProdutosVendidos", SUM(fv.valorTotal) AS "ValorTotalVendas"
from dw.Fato_Vendas AS fv join dw.Dim_Funcionario AS df ON fv.idFuncionario = df.idFuncionario
GROUP by df.nomeFuncionario
ORDER by "ValorTotalVendas" DESC;

-- 4. Quantidade de atendimentos realizados por funcionário e localidade.
select df.nomeFuncionario, dl.nomeLoja, dl.cidade, dl.uf, COUNT(*) AS "NumeroDeAtendimentos" -- Conta cada linha de venda como um atendimento
from dw.Fato_Vendas AS fv join dw.Dim_Funcionario AS df ON fv.idFuncionario = df.idFuncionario JOIN dw.Dim_Loja AS dl ON fv.idLoja = dl.idLoja
GROUP by df.nomeFuncionario, dl.nomeLoja, dl.cidade, dl.uf
ORDER by df.nomeFuncionario, "NumeroDeAtendimentos" DESC;

-- 5. Valor das últimas vendas realizadas por cliente.
WITH VendasNumeradas AS (select dc.nomeCliente, fv.valorTotal, dt.dataCompleta, ROW_NUMBER() OVER(PARTITION BY dc.idCliente ORDER BY dt.dataCompleta DESC) as rn
from dw.Fato_Vendas AS fv join dw.Dim_Cliente AS dc ON fv.idCliente = dc.idCliente join dw.Dim_Tempo AS dt ON fv.idData = dt.idData
)
select nomeCliente, valorTotal, dataCompleta
from VendasNumeradas where rn = 1
ORDER by nomeCliente;

-- 6. Clientes que mais compraram na loja virtual com valor acumulado por período
select dc.nomeCliente, SUM(fv.valorTotal) AS "ValorAcumulado"
from dw.Fato_Vendas AS fv join dw.Dim_Cliente AS dc ON fv.idCliente = dc.idCliente join dw.Dim_Loja AS dl ON fv.idLoja = dl.idLoja join dw.Dim_Tempo AS dt ON fv.idData = dt.idData
where dl.nomeLoja =
'Loja 3' -- DEFINIR LOJA
AND dt.dataCompleta BETWEEN 
'2025-01-01' -- DEFINIR PERIODO
AND 
'2025-03-31' -- DEFINIR PERIODO
GROUP by dc.nomeCliente
ORDER by "ValorAcumulado" desc LIMIT 10; 