#!/bin/bash

# --- Krok 0: Konfiguracja i sprzątanie ---
echo "--- Przygotowanie środowiska ---"
INPUT_FILE="gettysburg.txt"
MAPPER_SCRIPT="mapper.py"
REDUCER_SCRIPT="reducer.py"
NUM_MAPPERS=4

# Tworzymy tymczasowe katalogi na podzielone pliki i wyniki mapperów
SPLIT_DIR="splits"
MAP_OUTPUT_DIR="map_outputs"
mkdir -p $SPLIT_DIR $MAP_OUTPUT_DIR
rm -f ${SPLIT_DIR}/* ${MAP_OUTPUT_DIR}/*
echo "Środowisko gotowe."

# --- Krok 1: SPLIT ---
echo -e "\n--- Faza 1: Dzielenie pliku wejściowego na $NUM_MAPPERS części ---"
# Dzielimy plik wejściowy na N części według liczby linii
split -n l/$NUM_MAPPERS $INPUT_FILE $SPLIT_DIR/split_
ls -l $SPLIT_DIR
echo "Plik podzielony."

# --- Krok 2: MAP (równolegle) ---
echo -e "\n--- Faza 2: Uruchamianie $NUM_MAPPERS procesów mapujących w tle ---"
for split_file in ${SPLIT_DIR}/*; do
    echo "Uruchamiam mapper dla pliku: $split_file"
    # Uruchamiamy mapper dla każdego pliku w tle (&)
    # Wynik zapisujemy do osobnego pliku wyjściowego
    cat $split_file | python $MAPPER_SCRIPT > "${MAP_OUTPUT_DIR}/map_out_$(basename $split_file)" &
done

# Czekamy, aż WSZYSTKIE procesy działające w tle zakończą pracę
wait
echo "Wszystkie mappery zakończyły pracę."
ls -l $MAP_OUTPUT_DIR

# --- Krok 3, 4, 5: GATHER, SORT, REDUCE ---
echo -e "\n--- Faza 3: Łączenie, sortowanie i redukcja wyników ---"
# Łączymy wszystkie pliki wyjściowe z mapperów, sortujemy je i przekazujemy do reducera
cat ${MAP_OUTPUT_DIR}/* | sort | python $REDUCER_SCRIPT

# --- Krok 6: Sprzątanie po sobie ---
echo -e "\n--- Zakończono. Sprzątanie plików tymczasowych. ---"
rm -rf $SPLIT_DIR $MAP_OUTPUT_DIR