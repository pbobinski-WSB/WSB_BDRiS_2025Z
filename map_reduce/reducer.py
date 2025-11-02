#!/usr/bin/env python3
import sys

current_word = None
current_count = 0
word = None

# Przetwarzamy dane wejściowe (posortowane przez potok) linia po linii
for line in sys.stdin:
    line = line.strip()
    
    # Dzielimy linię na słowo i licznk (który jest zawsze '1')
    word, count_str = line.split('\t', 1)
    
    try:
        count = int(count_str)
    except ValueError:
        # Ignorujemy linie, które nie pasują do formatu
        continue

    # Ten warunek działa, ponieważ wejście jest posortowane.
    # Wszystkie wystąpienia tego samego słowa są obok siebie.
    if current_word == word:
        current_count += count
    else:
        # Napotkaliśmy nowe słowo.
        # Drukujemy wynik dla poprzedniego słowa (jeśli jakieś było).
        if current_word:
            print(f'{current_word}\t{current_count}')
        
        # Resetujemy liczniki dla nowego słowa.
        current_word = word
        current_count = count

# Pamiętaj, aby wydrukować ostatnie słowo po zakończeniu pętli!
if current_word == word:
    print(f'{current_word}\t{current_count}')