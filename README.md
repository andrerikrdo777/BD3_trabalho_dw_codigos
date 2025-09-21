# Data Warehouse para Loja de Varejo

Este projeto consiste na implementação de um Data Warehouse (DW) para uma loja de varejo, migrando de um sistema operacional transacional (OLTP) para um modelo dimensional otimizado para análise de dados.

## 🎯 O que foi desenvolvido

### 1. Modelagem do Data Warehouse
- **Modelo Estrela** com tabelas fato e dimensões  
  - **Fato**: Fato_Vendas (medidas: quantidade, valorTotal, custoTotal, lucroTotal)
  - **Dimensões**:
    - Dim_Tempo (hierarquia temporal completa)
    - Dim_Cliente (dados dos clientes)
    - Dim_Funcionario (dados dos funcionários com cargo mais recente)
    - Dim_Produto (dados dos produtos com categorias)
    - Dim_Loja (dados das lojas com localização)

### 2. Processo ETL Completo
- Staging Area para isolamento e preparação dos dados
- Cálculos automáticos de custo médio e lucro por produto
- Transformações de dados com regras de negócio
- Procedures armazenadas para automação do processo

### 3. Automação e Monitoramento
- Stored procedure principal (`sp_etl_completo()`) para execução do fluxo completo
- Sistema de log e rastreamento de execuções (`etl_log`)
- Controle de concorrência para evitar execuções simultâneas
- Preparado para agendamento automático

## 🚀 Como executar o projeto

### 📋 Pré-requisitos
- PostgreSQL instalado
- Backup do banco transacional original (`BD_VAREJO_Postgres_Completo.sql`)
- Acesso de superusuário (`postgres`)

### 🔄 Ordem de execução dos arquivos
Execute os scripts na seguinte ordem:

1. **Restaurar o banco transacional:**
   ```bash
   psql -U postgres -d postgres -f BD_VAREJO_Postgres_Completo.sql
   ```
2. **Criar a Staging Area:**
   ```sql
   \i ETL_1_stagingarea.sql
   ```
3. **Criar a estrutura do DW:**
   ```sql
   \i ETL_2_schemadw.sql
   ```
4. **Popular o DW com dados iniciais:**
   ```sql
   \i ETL_3_populaDW.sql
   ```

5. *(Opcional)* **Executar arquivo que cria tabela de logs:**
   ```sql
   \i ETL_6_triggers.sql
   ```
6. **Criar as stored procedures:**
   ```sql
   \i ETL_4_storedProcedures.sql
   ```
7. **Criar a procedure principal:**
   ```sql
   \i ETL_5_storedProcedurePrincipal.sql
   ```


## ⚡ Como usar

- Executar o ETL completo manualmente:
  ```sql
  CALL dw.sp_etl_completo();
  ```

- Verificar o status da última execução:
  ```sql
  SELECT * FROM dw.fn_verificar_status_etl();
  ```

- **Agendamento automático (Linux cron):**
  ```bash
  # Adicionar no crontab -e
  0 2 * * * psql -U postgres -d ADS -c "CALL dw.sp_etl_completo();"
  ```

## 🏗️ Estrutura do Banco

**Schemas:**
- `public`: Tabelas do sistema transacional original
- `stg`: Staging area com dados extraídos e preparados
- `dw`: Data Warehouse com modelo dimensional

**Principais Tabelas:**
- `stg.vendas`, `stg.compras`, `stg.clientes`, `stg.funcionarios`
- `dw.Fato_Vendas`, `dw.Dim_Cliente`, `dw.Dim_Produto`, `dw.Dim_Tempo`
