SELECT 'CREATE DATABASE lakekeeper'
WHERE NOT EXISTS (
    SELECT
        1
    FROM
        pg_database
    WHERE
        datname = 'lakekeeper'
)
\gexec

/* docker exec -it postgres psql \
  -U "$POSTGRES_USER" \
  -d airflow \
  -c "CREATE DATABASE lakekeeper;" */