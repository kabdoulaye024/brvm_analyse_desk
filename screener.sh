#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Chaîne complète du screener hybride BRVM en une commande :
#   1. rafraîchit cotations + indices (scrapers du desk, best-effort)
#   2. met à jour l'historique BRVM 30 (export R automatique, package BRVM)
#   3. lance le screener hybride Graham + Weinstein + Catalyst + Liquidité
#   4. ouvre le rapport Excel
#
# Usage :
#   ./screener.sh                       # tout, mode halal flexible
#   ./screener.sh strict                # mode halal : off | flexible | strict
#   ./screener.sh --no-refresh          # sans mise à jour des données (rapide)
#   ./screener.sh --force-index         # force le re-téléchargement du BRVM 30
#   ./screener.sh --no-open             # sans ouvrir Excel
#   ./screener.sh --desktop             # copie le rapport sur le Bureau + notification
#   ./screener.sh --composite           # enchaîne aussi le screener composite (run_screener.py)
#
# Exécution quotidienne automatique (16h45 GMT, lun-ven) : LaunchAgent
# ~/Library/LaunchAgents/com.brvm.screener.plist -> journal dans logs/screener_daily.log
# ─────────────────────────────────────────────────────────────────────────────
set -u
cd "$(dirname "$0")"

MODE=""
REFRESH=1
OPEN=1
DESKTOP=0
FORCE_INDEX=0
COMPOSITE=0

for arg in "$@"; do
  case "$arg" in
    off|flexible|strict) MODE="$arg" ;;
    --no-refresh)  REFRESH=0 ;;
    --no-open)     OPEN=0 ;;
    --desktop)     DESKTOP=1 ;;
    --force-index) FORCE_INDEX=1 ;;
    --composite)   COMPOSITE=1 ;;
    *) echo "Argument inconnu : $arg (voir l'en-tête de screener.sh)"; exit 1 ;;
  esac
done

ARGS=()
[ -n "$MODE" ] && ARGS+=("$MODE")
[ "$REFRESH" -eq 1 ] && ARGS+=(--refresh)
[ "$OPEN" -eq 1 ] && ARGS+=(--open)
[ "$DESKTOP" -eq 1 ] && ARGS+=(--desktop)
[ "$FORCE_INDEX" -eq 1 ] && ARGS+=(--force-index)

echo "════════════════════════════════════════════════════════════════════"
echo " SCREENER HYBRIDE BRVM — chaîne complète  ($(date '+%Y-%m-%d %H:%M'))"
echo "════════════════════════════════════════════════════════════════════"

python3 -m brvm_screener.main ${ARGS[@]+"${ARGS[@]}"}
STATUS=$?

if [ "$COMPOSITE" -eq 1 ] && [ "$STATUS" -eq 0 ]; then
  echo
  echo "── Screener composite (pipeline backend) ────────────────────────────"
  python3 run_screener.py balanced
fi

exit $STATUS
