/* ═══════════════════════════════════════════════════
   API Client — Fetch wrapper for backend
   ═══════════════════════════════════════════════════ */

const API = {
    base: '/api',

    async get(path, params = {}) {
        const url = new URL(this.base + path, window.location.origin);
        Object.entries(params).forEach(([k, v]) => {
            if (v != null && v !== '') url.searchParams.set(k, v);
        });
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`API ${resp.status}: ${resp.statusText}`);
        return resp.json();
    },

    async post(path, body) {
        const resp = await fetch(this.base + path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!resp.ok) throw new Error(`API ${resp.status}: ${resp.statusText}`);
        return resp.json();
    },

    async del(path) {
        const resp = await fetch(this.base + path, { method: 'DELETE' });
        if (!resp.ok) throw new Error(`API ${resp.status}: ${resp.statusText}`);
        return resp.json();
    },

    delete(path) { return this.del(path); },

    // Market data
    getStocks: () => API.get('/stocks'),
    getQuotes: (refresh = false) => API.get('/quotes', { refresh }),
    refreshQuotes: () => API.get('/quotes/refresh'),
    getIndices: (refresh = false) => API.get('/indices', { refresh }),
    getHistory: (ticker, days = 250) => API.get(`/history/${ticker}`, { days }),
    getTopMovers: () => API.get('/top-movers'),
    getSectors: () => API.get('/sectors'),

    // Portfolio
    getPositions: () => API.get('/portfolio/positions'),
    getTrades: () => API.get('/portfolio/trades'),
    getMetrics: () => API.get('/portfolio/metrics'),
    addTransaction: (data) => API.post('/portfolio/transaction', data),
    addCapitalFlow: (data) => API.post('/portfolio/capital-flow', data),
    getCapitalFlows: () => API.get('/portfolio/capital-flows'),

    // Watchlist
    getWatchlist: () => API.get('/watchlist'),
    addToWatchlist: (data) => API.post('/watchlist', data),
    removeFromWatchlist: (ticker) => API.del(`/watchlist/${ticker}`),

    // Alerts
    getAlerts: () => API.get('/alerts'),
    createAlert: (data) => API.post('/alerts', data),
    deleteAlert: (id) => API.del(`/alerts/${id}`),

    // Screener
    runScreener: (params) => API.get('/screener', params),
    getScores: (ticker) => API.get(`/scores/${ticker}`),

    // Fundamentals
    getFundamentals: (ticker, refresh = false) => API.get(`/fundamentals/${ticker}`, { refresh }),
    saveFundamentals: (ticker, data) => API.post(`/fundamentals/${ticker}`, data),
    deleteFundamentals: (ticker) => API.del(`/fundamentals/${ticker}`),

    // Portfolio charts
    getEquityCurve: () => API.get('/portfolio/equity-curve'),

    // Intelligence
    getNews: (refresh = false) => API.get('/news', { refresh }),
    getCalendar: (refresh = false) => API.get('/calendar', { refresh }),
    getWeeklySummary: () => API.get('/weekly-summary'),

    // System
    getStatus: () => API.get('/status'),
};
