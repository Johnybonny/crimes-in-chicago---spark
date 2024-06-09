#!/bin/bash

# Ustawienie zmiennych
DB_NAME="streamoutput"
DB_USER="postgres"
DB_PASSWORD="mysecretpassword"
DB_HOST="localhost"
DB_PORT="8432"

export PGPASSWORD=$DB_PASSWORD

# Funckcja do wypisania wyników w pętli
fetch_data() {
    while true; do
        clear
        psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT * FROM crime_stats;"
        sleep 5
    done
}

fetch_data
