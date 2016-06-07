#!/bin/bash
PG_PATH="/Applications/Postgres.app/Contents/Versions/9.5/bin"
PG_BIN="psql"
SQL_CMD="queries.sql"
OUTPUT_FILE="test.csv"
DB_NAME="d9tcbshsv9grg5"
DB_HOSTNAME="localhost"
DB_PORT="5432"

${PG_PATH}/${PG_BIN} -h ${DB_HOSTNAME} -p${DB_PORT} -d${DB_NAME} -A -F"|" -f ${SQL_CMD} -o ${OUTPUT_FILE}
