# --- Krok 0: Konfiguracja i sprzątanie ---
Write-Host "--- Przygotowanie środowiska ---" -ForegroundColor Yellow

# Pobieramy ścieżkę do katalogu, w którym znajduje się ten skrypt
$scriptRoot = $PSScriptRoot

# Budujemy ścieżki bezwzględne
$inputFile = Join-Path $scriptRoot "gettysburg.txt"
$mapperScript = Join-Path $scriptRoot "mapper.py"
$reducerScript = Join-Path $scriptRoot "reducer.py"
$numMappers = 4

$splitDir = Join-Path $scriptRoot "splits"
$mapOutputDir = Join-Path $scriptRoot "map_outputs"
if (Test-Path $splitDir) { Remove-Item $splitDir -Recurse -Force }
if (Test-Path $mapOutputDir) { Remove-Item $mapOutputDir -Recurse -Force }
New-Item -ItemType Directory -Path $splitDir | Out-Null
New-Item -ItemType Directory -Path $mapOutputDir | Out-Null
Write-Host "Środowisko gotowe."

# --- Krok 1: SPLIT ---
Write-Host ""
Write-Host "--- Faza 1: Dzielenie pliku wejściowego na $numMappers części ---" -ForegroundColor Yellow
$lines = Get-Content $inputFile
$totalLines = $lines.Count
$linesPerSplit = [math]::Ceiling($totalLines / $numMappers)
$offset = 0

for ($i = 0; $i -lt $numMappers; $i++) {
    $splitFilePath = Join-Path $splitDir "split_$i.txt"
    $lines | Select-Object -Skip $offset -First $linesPerSplit | Set-Content $splitFilePath
    $offset += $linesPerSplit
}
Get-ChildItem $splitDir | ForEach-Object { Write-Host "Stworzono plik $($_.Name) o rozmiarze $($_.Length) bajtów" }
Write-Host "Plik podzielony."

# --- Krok 2: MAP (równolegle) ---
Write-Host ""
Write-Host "--- Faza 2: Uruchamianie $numMappers procesów mapujących w tle ---" -ForegroundColor Yellow
$jobs = @()

Get-ChildItem -Path $splitDir | ForEach-Object {
    $splitFile = $_
    $outputFile = Join-Path $mapOutputDir "map_out_$($splitFile.Name)"
    Write-Host "Uruchamiam mapper dla pliku: $($splitFile.Name)"
    
    # Przekazujemy pełne, bezwzględne ścieżki do zadania w tle
    $job = Start-Job -ScriptBlock {
        param($sFile, $mScript, $oFile)
        Get-Content $sFile | python $mScript | Out-File $oFile
    } -ArgumentList $splitFile.FullName, $mapperScript, $outputFile
    
    $jobs += $job
}

Write-Host "Oczekiwanie na zakończenie pracy wszystkich mapperów..."
Wait-Job -Job $jobs

# --- NOWOŚĆ: Sprawdzanie błędów w zadaniach ---
$failedJobs = $jobs | Where-Object { $_.State -eq 'Failed' }
if ($failedJobs) {
    Write-Host "`nWYSTĄPIŁY BŁĘDY W ZADANIACH MAPUJĄCYCH!" -ForegroundColor Red
    $failedJobs | ForEach-Object {
        Write-Host "--- Błędy dla zadania $($_.Id) ---" -ForegroundColor Yellow
        # Odbieramy i wyświetlamy błędy (stderr) z zadania
        Receive-Job -Job $_
    }
    # Przerywamy dalsze wykonywanie skryptu
    return
} else {
    Write-Host "Wszystkie mappery zakończyły pracę poprawnie."
    Get-ChildItem $mapOutputDir | ForEach-Object { Write-Host "Stworzono plik wynikowy $($_.Name) o rozmiarze $($_.Length) bajtów" }
}

# --- Krok 3, 4, 5: GATHER, SORT, REDUCE ---
Write-Host ""
Write-Host "--- Faza 3: Łączenie, sortowanie i redukcja wyników ---" -ForegroundColor Yellow
# Teraz ścieżka do plików wyjściowych na pewno będzie poprawna
Get-Content -Path (Join-Path $mapOutputDir "*") | Sort-Object | python $reducerScript

# --- Krok 6: Sprzątanie po sobie ---
Write-Host ""
Write-Host "--- Zakończono. Sprzątanie plików tymczasowych. ---" -ForegroundColor Yellow
Remove-Item $splitDir -Recurse -Force
Remove-Item $mapOutputDir -Recurse -Force