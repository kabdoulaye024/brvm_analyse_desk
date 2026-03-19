"""
BRVM reference data — tickers, sectors, constants.
Ported from the existing screener.
"""

TICKERS_BRVM = {
    "SNTS":  ("SONATEL SENEGAL",                         "Télécommunications",  "SN"),
    "ORAC":  ("ORANGE COTE D'IVOIRE",                    "Télécommunications",  "CI"),
    "ONTBF": ("ONATEL BURKINA FASO",                     "Télécommunications",  "BF"),
    "BOAB":  ("BANK OF AFRICA BENIN",                    "Services Financiers", "BJ"),
    "BOABF": ("BANK OF AFRICA BURKINA FASO",             "Services Financiers", "BF"),
    "BOAC":  ("BANK OF AFRICA COTE D'IVOIRE",            "Services Financiers", "CI"),
    "BOAM":  ("BANK OF AFRICA MALI",                     "Services Financiers", "ML"),
    "BOAN":  ("BANK OF AFRICA NIGER",                    "Services Financiers", "NE"),
    "BOAS":  ("BANK OF AFRICA SENEGAL",                  "Services Financiers", "SN"),
    "BICB":  ("BICICI BENIN",                            "Services Financiers", "BJ"),
    "BICC":  ("BICI COTE D'IVOIRE",                      "Services Financiers", "CI"),
    "CBIBF": ("CORIS BANK INTERNATIONAL BURKINA FASO",   "Services Financiers", "BF"),
    "ECOC":  ("ECOBANK COTE D'IVOIRE",                   "Services Financiers", "CI"),
    "ETIT":  ("ECOBANK TRANSNATIONAL (ETI) TOGO",        "Services Financiers", "TG"),
    "NSBC":  ("NSIA BANQUE COTE D'IVOIRE",               "Services Financiers", "CI"),
    "ORGT":  ("ORAGROUP TOGO",                           "Services Financiers", "TG"),
    "SAFC":  ("ALIOS FINANCE COTE D'IVOIRE",             "Services Financiers", "CI"),
    "SGBC":  ("SGB COTE D'IVOIRE",                       "Services Financiers", "CI"),
    "SIBC":  ("SOCIETE IVOIRIENNE DE BANQUE",            "Services Financiers", "CI"),
    "CIEC":  ("CIE COTE D'IVOIRE",                       "Services Publics",    "CI"),
    "SDCC":  ("SODE COTE D'IVOIRE",                      "Services Publics",    "CI"),
    "TTLC":  ("TOTAL ENERGIES COTE D'IVOIRE",            "Énergie",             "CI"),
    "TTLS":  ("TOTAL ENERGIES SENEGAL",                  "Énergie",             "SN"),
    "SHEC":  ("VIVO ENERGY COTE D'IVOIRE",               "Énergie",             "CI"),
    "SMBC":  ("SMB COTE D'IVOIRE",                       "Énergie",             "CI"),
    "FTSC":  ("FILTISAC COTE D'IVOIRE",                  "Industriels",         "CI"),
    "CABC":  ("SICABLE COTE D'IVOIRE",                   "Industriels",         "CI"),
    "STAC":  ("SETAO COTE D'IVOIRE",                     "Industriels",         "CI"),
    "SDSC":  ("AFRICA GLOBAL LOGISTICS CI",              "Industriels",         "CI"),
    "SEMC":  ("EVIOSYS PACKAGING SIEM CI",               "Industriels",         "CI"),
    "SIVC":  ("ERIUM CI",                                "Industriels",         "CI"),
    "NTLC":  ("NESTLE COTE D'IVOIRE",                    "Consommation de base","CI"),
    "PALC":  ("PALM COTE D'IVOIRE",                      "Consommation de base","CI"),
    "SPHC":  ("SAPH COTE D'IVOIRE",                      "Consommation de base","CI"),
    "SICC":  ("SICOR COTE D'IVOIRE",                     "Consommation de base","CI"),
    "STBC":  ("SITAB COTE D'IVOIRE",                     "Consommation de base","CI"),
    "SOGC":  ("SOGB COTE D'IVOIRE",                      "Consommation de base","CI"),
    "SLBC":  ("SOLIBRA COTE D'IVOIRE",                   "Consommation de base","CI"),
    "SCRC":  ("SUCRIVOIRE COTE D'IVOIRE",                "Consommation de base","CI"),
    "UNLC":  ("UNILEVER COTE D'IVOIRE",                  "Consommation de base","CI"),
    "BNBC":  ("BERNABE COTE D'IVOIRE",                   "Consommation discrétionnaire", "CI"),
    "CFAC":  ("CFAO MOTORS COTE D'IVOIRE",               "Consommation discrétionnaire", "CI"),
    "LNBB":  ("LOTERIE NATIONALE DU BENIN",              "Consommation discrétionnaire", "BJ"),
    "NEIC":  ("NEI-CEDA COTE D'IVOIRE",                  "Consommation discrétionnaire", "CI"),
    "ABJC":  ("SERVAIR ABIDJAN COTE D'IVOIRE",           "Consommation discrétionnaire", "CI"),
    "PRSC":  ("TRACTAFRIC MOTORS COTE D'IVOIRE",         "Consommation discrétionnaire", "CI"),
    "UNXC":  ("UNIWAX COTE D'IVOIRE",                    "Consommation discrétionnaire", "CI"),
}

SECTORS = [
    "Télécommunications",
    "Services Financiers",
    "Consommation de base",
    "Consommation discrétionnaire",
    "Industriels",
    "Énergie",
    "Services Publics",
]

INDICES = [
    "BRVM Composite",
    "BRVM 30",
    "BRVM Prestige",
    "BRVM Principal",
]

# Sectoral indices follow the pattern "BRVM - {sector}"
SECTORAL_INDICES = [f"BRVM - {s}" for s in SECTORS]

# Trading hours: 9h00 - 15h30 GMT, Mon-Fri
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30

# Max daily variation: ±7.5%
MAX_DAILY_VARIATION = 7.5

# Default brokerage fee (round-trip)
DEFAULT_FEE_PCT = 1.8

# Sectoral PER benchmarks
PER_SECTORIELS = {
    "Télécommunications": 10.11,
    "Consommation discrétionnaire": 72.48,
    "Services Financiers": 11.08,
    "Consommation de base": 14.80,
    "Industriels": 22.23,
    "Énergie": 17.63,
    "Services Publics": 17.65,
}
