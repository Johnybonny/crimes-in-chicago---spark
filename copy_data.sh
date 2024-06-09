#!/bin/bash

# Sprawdzenie, czy został podany parametr
if [ $# -ne 1 ]; then
    echo "Użycie: $0 <lokalizacja danych>"
    exit 1
fi

# Zmienna zawierająca lokalizację zasobnika
export DATA_LOCATION=$1

# Tworzenie lokalnego katalogu ./data
mkdir -p ./data

# Kopiowanie plików z zasobnika do lokalnego katalogu ./data
hadoop fs -copyToLocal ${DATA_LOCATION}/* ./data/

# Sprawdzenie, czy kopiowanie zakończyło się sukcesem
if [ $? -eq 0 ]; then
    echo "Pliki zostały pomyślnie skopiowane z ${DATA_LOCATION} do ./data/"
else
    echo "Wystąpił błąd podczas kopiowania plików strumieniowych."
    exit 1
fi
