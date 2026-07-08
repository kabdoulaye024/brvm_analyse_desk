# Fichiers d'entrée optionnels

Par défaut, le screener lit la base SQLite du desk (`../data/brvm.db`).
Tout fichier déposé ici **remplace** la source correspondante (`.xlsx` ou `.csv`) :

| Fichier | Colonnes minimales | Remplace |
|---|---|---|
| `daily_prices` | date, ticker, close, volume (+ value_traded, company_name, sector) | table `daily_quotes` |
| `brvm30` | date, close | table `indices` (BRVM 30) — **rafraîchi automatiquement** (voir ci-dessous) |
| `fundamentals` | ticker, year, + ratios/agrégats (voir spec §2.2) | `fundamentals_dataset` + `fundamentals` |
| `corporate_events` | ticker, event_type, event_date (+ dividend_amount, yield_pct) | table `corporate_events` |
| `halal_classification` | ticker, halal_status (Compatible / Non-compatible / À vérifier) | heuristique secteur + dette/actif |

Unités attendues : agrégats comptables en **millions FCFA**, données par action
(EPS, dividende, cours) en **FCFA**.

## Benchmark BRVM 30 — export R automatique

Si `brvm30.csv` est absent ou plus vieux que `r_export_max_age_days` (7 jours),
`data_loader.py` lance automatiquement `../export_brvm30.R`, qui télécharge
l'historique quotidien via le **package R BRVM** (K.F. Sessie,
`remotes::install_github("Koffi-Fredysessie/BRVM")`) et écrit `brvm30.csv` ici
(repli : `brvm_composite.csv`). Prérequis : `Rscript` dans le PATH + package BRVM.

Cascade de benchmark : `brvm30.csv` → table `indices` (DB) → `brvm_composite.csv`
→ indice synthétique équipondéré (dernier recours, signalé dans le rapport).
Désactivable via `PARAMS["r_export_enabled"] = False`.

## Lancement

Chaîne complète (rafraîchissement des cotations → export R de l'indice →
screening → ouverture du rapport) : **`./screener.sh`** à la racine du projet.
Options : `off|flexible|strict`, `--no-refresh`, `--no-open`, `--force-index`,
`--composite` (enchaîne aussi `run_screener.py`).

Screener seul : `python -m brvm_screener.main [off|flexible|strict] [--refresh] [--open] [--desktop] [--force-index]`.
Rafraîchissement seul : `python -m brvm_screener.refresh_data`.
Sortie : `outputs/brvm_hybrid_screener_output.xlsx` (7 onglets).

## Exécution quotidienne automatique (macOS)

Le LaunchAgent `com.brvm.screener.plist` (copie versionnée dans ce package)
lance `./screener.sh --no-open --desktop` chaque jour de bourse à **16h45 GMT**
(clôture BRVM + 1h15) : données rafraîchies, rapport copié sur le Bureau
(`~/Desktop/BRVM_Screener.xlsx`) et notification macOS avec les signaux du jour.
Journal : `logs/screener_daily.log`.

(Ré)installation :
```bash
cp brvm_screener/com.brvm.screener.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.brvm.screener.plist
```
Désactivation : `launchctl bootout gui/$(id -u)/com.brvm.screener`.
Lancement manuel immédiat : `launchctl kickstart gui/$(id -u)/com.brvm.screener`.
