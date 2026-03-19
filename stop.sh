#!/bin/bash
# Arrêter BRVM Trading Desk
pkill -f "uvicorn app:app" 2>/dev/null && echo "✅ Serveur arrêté" || echo "   Serveur déjà arrêté"
pkill -f "cloudflared tunnel" 2>/dev/null && echo "✅ Tunnel arrêté" || echo "   Tunnel déjà arrêté"
