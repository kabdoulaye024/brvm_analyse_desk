/* ═══════════════════════════════════════════════════
   App — Main router and initialization
   ═══════════════════════════════════════════════════ */

const App = {
    currentPage: 'market',

    async init() {
        // Load stocks reference for autocomplete
        try {
            const stocks = await API.getStocks();
            window._stocksList = {};
            for (const s of stocks) {
                window._stocksList[s.ticker] = s;
            }
        } catch (err) {
            console.warn('Could not load stocks reference:', err);
            window._stocksList = {};
        }

        // Setup navigation
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', () => {
                const page = item.dataset.page;
                this.navigate(page);
            });
        });

        // Check API status
        try {
            const status = await API.getStatus();
            document.getElementById('connectionStatus').className = 'status-dot online';
            document.getElementById('statusText').textContent =
                status.unique_stocks > 0
                    ? `${status.unique_stocks} titres · ${Utils.formatDate(status.latest_date)}`
                    : 'Connecte — aucune donnee';
        } catch (err) {
            document.getElementById('connectionStatus').className = 'status-dot offline';
            document.getElementById('statusText').textContent = 'Hors ligne';
        }

        // Navigate to default page
        this.navigate('market');
    },

    navigate(page) {
        // Stop timers when leaving pages
        if (this.currentPage === 'market' && page !== 'market') {
            MarketPage.stopAutoRefresh();
        }
        if (this.currentPage === 'intelligence' && page !== 'intelligence') {
            IntelligencePage.stopTimers();
        }

        this.currentPage = page;

        // Update sidebar
        document.querySelectorAll('.nav-item').forEach(item => {
            item.classList.toggle('active', item.dataset.page === page);
        });

        // Render page
        switch (page) {
            case 'market':
                MarketPage.render();
                break;
            case 'portfolio':
                PortfolioPage.render();
                break;
            case 'analysis':
                AnalysisPage.render();
                break;
            case 'intelligence':
                IntelligencePage.render();
                break;
        }
    },

    /**
     * Quick chart access from any page
     */
    showChart(ticker) {
        AnalysisPage.chartTicker = ticker;
        AnalysisPage.activeTab = 'chart';
        this.navigate('analysis');
    },
};

// ── Boot ──────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    App.init();
});
