#!/usr/bin/env python3
import sys
import re

def clean_word(word):
    # Usuwa wszystkie znaki, które nie są literami ani cyframi
    return re.sub(r'\W+', '', word).lower()

# Przetwarzamy dane wejściowe linia po linii
for line in sys.stdin:
    # Usuwamy białe znaki z początku i końca linii
    line = line.strip()
    
    # Dzielimy linię na słowa
    words = line.split()
    
    # Emitujemy pary (słowo, 1) dla każdego słowa
    for word in words:
        cleaned = clean_word(word)
        if cleaned:  # Upewniamy się, że słowo nie jest puste po czyszczeniu
            # Drukujemy wynik na standardowe wyjście
            # Używamy tabulatora jako separatora
            print(f'{cleaned}\t1')