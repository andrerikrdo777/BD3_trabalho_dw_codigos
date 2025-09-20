------ SCRIPTS DE AGENDAMENTO ------

-- 9. SCRIPT PARA EXECUTAR VIA CRON (Linux) ou AGENDADOR DE TAREFAS (Windows)
/*
-- Para executar via linha de comando:
psql -U seu_usuario -d seu_banco -c "CALL dw.sp_etl_completo();"


-- Para Windows Task Scheduler, criar um arquivo .bat:
@echo off
psql -U seu_usuario -d seu_banco -c "CALL dw.sp_etl_completo();"
*/