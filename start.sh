#!/bin/bash
# BRVM Trading Desk — démarrage complet
# Lance le serveur FastAPI + Cloudflare Tunnel et affiche l'URL publique

set -e
cd "$(dirname "$0")"

export PATH="$HOME/bin:$PATH"

# Vérifier que l'env Python est activé
if ! python3 -c "import fastapi" 2>/dev/null; then
    echo "⚠️  Activation de l'environnement Python..."
    source .venv/bin/activate 2>/dev/null || true
fi

# Arrêter les anciens processus si déjà lancés
pkill -f "uvicorn app:app" 2>/dev/null || true
pkill -f "cloudflared tunnel" 2>/dev/null || true
sleep 1

echo "🚀 Démarrage du serveur BRVM..."
python3 -m uvicorn app:app --host 0.0.0.0 --port 8000 > /tmp/brvm_server.log 2>&1 &
SERVER_PID=$!
echo "   Serveur PID: $SERVER_PID"

# Attendre que le serveur soit prêt
sleep 3

echo "🌐 Démarrage du tunnel Cloudflare..."
cloudflared tunnel --url http://localhost:8000 --no-autoupdate > /tmp/brvm_tunnel.log 2>&1 &
TUNNEL_PID=$!

# Attendre l'URL publique
echo "   Attente de l'URL publique..."
URL=""
for i in $(seq 1 20); do
    URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' /tmp/brvm_tunnel.log 2>/dev/null | head -1)
    if [ -n "$URL" ]; then
        break
    fi
    sleep 2
done

echo ""
echo "════════════════════════════════════════════════════════"
if [ -n "$URL" ]; then
    echo "  ✅ BRVM Trading Desk est en ligne !"
    echo ""
    echo "  🔗 URL publique : $URL"
    echo "  💻 Local        : http://localhost:8000"
else
    echo "  ✅ Serveur démarré — tunnel en cours..."
    echo "  💻 Local : http://localhost:8000"
    echo "  📄 Logs tunnel : tail -f /tmp/brvm_tunnel.log"
fi
echo "════════════════════════════════════════════════════════"
echo ""
echo "Pour arrêter : ./stop.sh  ou  Ctrl+C"

# Garder le script actif et afficher les logs
wait $SERVER_PID
