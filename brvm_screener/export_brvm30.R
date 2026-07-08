#!/usr/bin/env Rscript
# ─────────────────────────────────────────────────────────────────────────────
# Export de l'historique de l'indice BRVM 30 vers brvm_screener/data/brvm30.csv
# (repli : BRVM Composite -> brvm_composite.csv).
#
# Appelé AUTOMATIQUEMENT par data_loader.py quand l'historique BRVM 30 local
# est absent ou trop ancien. Utilisable aussi à la main :
#   Rscript brvm_screener/export_brvm30.R [dossier_sortie] [date_debut]
#
# Source : package BRVM (Koffi Frédéric Sessie) — même API que weinstein_brvm.R
#   BRVM_get(ticker, Period, from, to)
# ─────────────────────────────────────────────────────────────────────────────

args <- commandArgs(trailingOnly = TRUE)

# Dossier de sortie : argument 1, sinon <dossier du script>/data
script_path <- sub("--file=", "", grep("--file=", commandArgs(), value = TRUE)[1])
default_out <- file.path(dirname(normalizePath(script_path)), "data")
out_dir  <- if (length(args) >= 1 && nzchar(args[1])) args[1] else default_out
from_date <- if (length(args) >= 2 && nzchar(args[2])) args[2] else "2019-01-01"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

if (!requireNamespace("BRVM", quietly = TRUE)) {
  message("Package BRVM absent. Installation : remotes::install_github('Koffi-Fredysessie/BRVM')")
  quit(status = 2)
}
suppressMessages(library(BRVM))

# Récupère un symbole et normalise en deux colonnes : date, close.
fetch_index <- function(sym) {
  df <- tryCatch(
    BRVM_get(ticker = sym, Period = "daily", from = from_date, to = Sys.Date()),
    error = function(e) { message(sym, " : echec — ", e$message); NULL })
  if (is.null(df) || !is.data.frame(df) || nrow(df) == 0) return(NULL)

  noms <- tolower(names(df))
  col_date  <- names(df)[which(noms %in% c("date", "dates", "time", "index"))[1]]
  col_close <- names(df)[which(noms %in% c("close", "cours", "price", "value", "valeur", "last"))[1]]
  if (is.na(col_date) || is.na(col_close)) {
    message(sym, " : colonnes date/close introuvables (", paste(names(df), collapse = ","), ")")
    return(NULL)
  }
  out <- data.frame(date = as.Date(df[[col_date]]),
                    close = as.numeric(df[[col_close]]))
  out <- out[!is.na(out$date) & !is.na(out$close), ]
  out <- out[!duplicated(out$date), ]
  out[order(out$date), ]
}

write_out <- function(df, filename) {
  path <- file.path(out_dir, filename)
  write.csv(df, path, row.names = FALSE)
  cat(sprintf("OK %s : %d lignes, %s -> %s\n",
              filename, nrow(df), min(df$date), max(df$date)))
  path
}

# BRVM 30 en priorité (benchmark du screener), Composite en repli.
brvm30 <- fetch_index("BRVM30")
if (!is.null(brvm30) && nrow(brvm30) >= 30) {
  write_out(brvm30, "brvm30.csv")
  quit(status = 0)
}
message("BRVM 30 indisponible — tentative BRVM Composite.")
brvmc <- fetch_index("BRVMC")
if (!is.null(brvmc) && nrow(brvmc) >= 30) {
  write_out(brvmc, "brvm_composite.csv")
  quit(status = 0)
}
message("Aucun indice recuperable — le screener utilisera le benchmark synthetique.")
quit(status = 1)
