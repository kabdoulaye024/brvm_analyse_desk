# Fichiers d'entrée optionnels

Par défaut, le screener lit la base SQLite du desk (`../data/brvm.db`).
Tout fichier déposé ici **remplace** la source correspondante (`.xlsx` ou `.csv`) :

| Fichier | Colonnes minimales | Remplace |
|---|---|---|
| `daily_prices` | date, ticker, close, volume (+ value_traded, company_name, sector) | table `daily_quotes` |
| `brvm30` | date, close | table `indices` (BRVM 30) |
| `fundamentals` | ticker, year, + ratios/agrégats (voir spec §2.2) | `fundamentals_dataset` + `fundamentals` |
| `corporate_events` | ticker, event_type, event_date (+ dividend_amount, yield_pct) | table `corporate_events` |
| `halal_classification` | ticker, halal_status (Compatible / Non-compatible / À vérifier) | heuristique secteur + dette/actif |

Unités attendues : agrégats comptables en **millions FCFA**, données par action
(EPS, dividende, cours) en **FCFA**.

Lancement : `python -m brvm_screener.main [off|flexible|strict]` depuis la racine du projet.
Sortie : `outputs/brvm_hybrid_screener_output.xlsx` (7 onglets).
