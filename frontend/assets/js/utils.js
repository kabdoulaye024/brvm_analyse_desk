/* ═══════════════════════════════════════════════════
   Utilities — Formatters, helpers
   ═══════════════════════════════════════════════════ */

const Utils = {
    /**
     * Format number as FCFA currency with space separator
     * 1000000 → "1 000 000 FCFA"
     */
    formatFCFA(value, showCurrency = true) {
        if (value == null || isNaN(value)) return '—';
        const formatted = Math.round(value).toLocaleString('fr-FR');
        return showCurrency ? `${formatted} FCFA` : formatted;
    },

    /**
     * Format number with space thousands separator
     */
    formatNumber(value, decimals = 0) {
        if (value == null || isNaN(value)) return '—';
        return Number(value).toLocaleString('fr-FR', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals,
        });
    },

    /**
     * Format percentage with sign and color class
     */
    formatPct(value, decimals = 2) {
        if (value == null || isNaN(value)) return '—';
        const sign = value >= 0 ? '+' : '';
        return `${sign}${Number(value).toFixed(decimals)}%`;
    },

    /**
     * Get CSS class for positive/negative values
     */
    changeClass(value) {
        if (value == null || isNaN(value)) return 'neutral';
        if (value > 0) return 'positive';
        if (value < 0) return 'negative';
        return 'neutral';
    },

    /**
     * Get text color class for positive/negative
     */
    textColorClass(value) {
        if (value == null || isNaN(value)) return 'text-muted';
        if (value > 0) return 'text-green';
        if (value < 0) return 'text-red';
        return 'text-muted';
    },

    /**
     * Color for heatmap cell based on change %
     */
    heatmapColor(changePct) {
        if (changePct == null) return '#1e2633';
        const intensity = Math.min(Math.abs(changePct) / 7.5, 1);
        if (changePct > 0) {
            const r = Math.round(13 + (63 - 13) * (1 - intensity));
            const g = Math.round(31 + (185 - 31) * intensity);
            const b = Math.round(18 + (80 - 18) * (1 - intensity));
            return `rgb(${r},${g},${b})`;
        } else if (changePct < 0) {
            const r = Math.round(248 * intensity + 31 * (1 - intensity));
            const g = Math.round(81 * intensity + 13 * (1 - intensity));
            const b = Math.round(73 * intensity + 13 * (1 - intensity));
            return `rgb(${r},${g},${b})`;
        }
        return '#1e2633';
    },

    /**
     * Range bar color based on position
     */
    rangeColor(pct) {
        if (pct < 20) return 'var(--red)';
        if (pct < 40) return 'var(--yellow)';
        if (pct < 70) return 'var(--blue)';
        return 'var(--green)';
    },

    /**
     * Debounce function
     */
    debounce(fn, ms = 300) {
        let timer;
        return (...args) => {
            clearTimeout(timer);
            timer = setTimeout(() => fn(...args), ms);
        };
    },

    /**
     * Show toast notification
     */
    toast(message, type = 'info', duration = 3000) {
        const container = document.getElementById('toasts');
        const el = document.createElement('div');
        el.className = `toast toast-${type}`;
        el.textContent = message;
        container.appendChild(el);
        setTimeout(() => {
            el.style.opacity = '0';
            el.style.transform = 'translateX(100%)';
            setTimeout(() => el.remove(), 200);
        }, duration);
    },

    /**
     * Format volume compactly
     */
    formatVolume(v) {
        if (v == null || v === 0) return '—';
        if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
        if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
        return v.toString();
    },

    /**
     * Format date to French locale
     */
    formatDate(dateStr) {
        if (!dateStr) return '—';
        const d = new Date(dateStr);
        return d.toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: 'numeric' });
    },

    /**
     * Calculate days between date and now
     */
    daysSince(dateStr) {
        if (!dateStr) return 0;
        const d = new Date(dateStr);
        const now = new Date();
        return Math.floor((now - d) / (1000 * 60 * 60 * 24));
    },

    /**
     * Sector emoji
     */
    sectorEmoji(sector) {
        const map = {
            'Télécommunications': '📡',
            'Services Financiers': '🏦',
            'Consommation de base': '🛒',
            'Consommation discrétionnaire': '🛍️',
            'Industriels': '⚙️',
            'Énergie': '⚡',
            'Services Publics': '🏛️',
        };
        return map[sector] || '📊';
    },

    /**
     * Escape HTML entities to prevent XSS
     */
    escapeHtml(str) {
        if (!str) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    },
};
