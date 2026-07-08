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

Lancement : `python -m brvm_screener.main [off|flexible|strict]` depuis la racine du projet.
Sortie : `outputs/brvm_hybrid_screener_output.xlsx` (7 onglets).
