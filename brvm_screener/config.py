"""
Configuration du screener hybride BRVM.

Stratégie : Graham (sélection fondamentale) + Weinstein (timing) +
Catalyst (accélérateur BRVM) + Liquidité (contrôle d'exécution) + Halal flexible.

Tout paramètre modifiable par un analyste se trouve ici.
"""
import os

# ── Chemins ──────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")          # fichiers d'entrée optionnels
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "brvm_hybrid_screener_output.xlsx")

# Base SQLite du desk Python (source de repli si aucun fichier dans data/)
PROJECT_ROOT = os.path.dirname(BASE_DIR)
SQLITE_DB = os.path.join(PROJECT_ROOT, "data", "brvm.db")

# ── Paramètres globaux ───────────────────────────────────────────────────────
PARAMS = {
    "benchmark": "BRVM30",
    "halal_mode": "flexible",  # off, flexible, strict

    # Pondérations du score final (doivent sommer à 1.0)
    "technical_weight": 0.30,
    "graham_weight": 0.40,
    "catalyst_weight": 0.20,
    "liquidity_weight": 0.10,

    # Export R automatique de l'indice (package BRVM de K.F. Sessie).
    # Si l'historique BRVM 30 local est absent/trop vieux et que Rscript est
    # disponible, data_loader lance export_brvm30.R avant tout repli synthétique.
    "r_export_enabled": True,
    "r_export_max_age_days": 7,        # fraîcheur exigée du fichier brvm30.csv
    "r_export_timeout_s": 300,
    "r_export_from": "2019-01-01",

    # Technique
    "ma_weeks": 30,
    "ma_slope_lookback_weeks": 4,
    "relative_strength_lookback_weeks": 26,
    "stale_weeks_threshold": 4,        # dernier échange trop ancien au-delà
    "breakout_lookback_weeks": 26,     # nouveau plus haut 26 semaines

    # Classification liquidité A/B/C
    "liquidity_A_min_trading_frequency": 0.60,
    "liquidity_A_min_weekly_value": 5_000_000,
    "liquidity_B_min_trading_frequency": 0.30,
    "liquidity_B_min_weekly_value": 1_000_000,
    "liquidity_lookback_weeks": 26,    # fenêtre de mesure de la liquidité

    # Plafond du Technical Score selon la liquidité
    "technical_cap_A": 100,
    "technical_cap_B": 75,
    "technical_cap_C": 50,

    # Position sizing
    "max_position_A": 0.20,
    "max_position_B": 0.15,
    "max_position_C": 0.08,

    # Seuils de décision
    "strong_buy_threshold": 80,
    "progressive_buy_threshold": 70,
    "priority_watchlist_threshold": 60,
    "simple_watchlist_threshold": 50,

    # Garde-fous décision (règles complémentaires §16)
    "strong_buy_min_graham": 60,       # achat fort exige Graham solide
    "strong_buy_min_technical": 55,    # ... et un technique raisonnablement confirmé
    "progressive_min_graham": 60,      # achat progressif via Graham+Catalyst
    "progressive_min_catalyst": 60,
}

# ── Univers / classification ────────────────────────────────────────────────
# Titres non compatibles halal par activité (tabac, brasserie, loterie).
HALAL_EXCLUDED_ACTIVITY = {"STBC", "SLBC", "LNBB"}
# Les financières conventionnelles (banques) sont marquées Non-compatible.
FINANCIAL_SECTOR = "Services Financiers"
# Seuil d'endettement (dette totale / total actif) pour le filtre halal heuristique.
HALAL_MAX_DEBT_TO_ASSETS = 0.33

# Unités : les agrégats comptables du workbook sont en millions FCFA,
# les données par action (EPS, dividende, cours) en FCFA.
MILLIONS = 1_000_000
