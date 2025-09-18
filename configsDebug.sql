SELECT datname FROM pg_database;

SELECT current_user;

SELECT datname, datistemplate 
FROM pg_database 
WHERE datistemplate = false 
ORDER BY datname;

SET search_path TO ads;
	
SELECT datname, datistemplate 
FROM pg_database 
WHERE datname = 'ads';

SELECT table_name 
FROM information_schema.tables 
WHERE table_catalog = 'ads' 
AND table_schema = 'public';