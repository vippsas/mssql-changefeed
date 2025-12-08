#!/bin/sh

# Start the SQL Server test container
docker compose -p mssql-changefeed -f docker-compose.test.yml up -d --force-recreate

echo "Waiting for SQL Server to be ready..."
sleep 5

echo "SQL Server test container is running on localhost:1433"
echo "Connection string: sqlserver://localhost?database=master&user id=sa&password=VippsPw1"
