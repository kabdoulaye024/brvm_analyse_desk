/* ═══════════════════════════════════════════════════
   Module 1 — Market Overview
   Dashboard: quotes table, indices, top movers, heatmap
   ═══════════════════════════════════════════════════ */

const MarketPage = {
    quotes: [],
    indices: [],
    topMovers: {},
    sectors: [],
    sortKey: 'ticker',
    sortDir: 'asc',
    filterSector: null,
    filterSearch: '',
    filterType: null, // 'top_gain', 'top_loss', 'top_vol'
    _refreshTimer: null,
    _lastRefreshTime: null,

    async render() {
        const content = document.getElementById('content');
        content.innerHTML = `
            <div class="page-header">
                <div>
                    <h1 class="page-title">Marche BRVM</h1>
                    <div class="page-subtitle" id="marketDate">Chargement...</div>
                </div>
                <button class="btn btn-primary" id="btnRefresh" onclick="MarketPage.refresh()">
                    Actualiser
                </button>
            </div>

            <!-- Indices bar -->
            <div class="indices-bar" id="indicesBar">
                ${Components.loading('Chargement des indices...')}
            </div>

            <!-- Top movers -->
            <div class="section-title">Top movers du jour</div>
            <div class="movers-grid" id="moversGrid">
                ${Components.loading()}
            </div>

            <!-- Heatmap -->
            <div class="section-title mt-16">Carte sectorielle</div>
            <div class="card">
                <div class="heatmap-container" id="heatmap">
                    ${Components.loading()}
                </div>
            </div>

            <!-- Quotes table -->
            <div class="section-title mt-16">Tous les titres</div>
            <div class="filters-bar">
                <input type="text" class="search-input" placeholder="Rechercher..."
                       id="searchInput" oninput="MarketPage.onSearch(this.value)">
                <div class="filter-chip ${!this.filterSector ? 'active' : ''}"
                     onclick="MarketPage.filterBySector(null)">Tous</div>
                ${['Télécommunications', 'Services Financiers', 'Consommation de base',
                   'Industriels', 'Énergie', 'Services Publics'].map(s =>
                    `<div class="filter-chip ${this.filterSector === s ? 'active' : ''}"
                          onclick="MarketPage.filterBySector('${s}')">${Utils.sectorEmoji(s)} ${s.split(' ')[0]}</div>`
                ).join('')}
            </div>
            <div id="quotesTable">${Components.loading('Chargement des cours...')}</div>
        `;

        await this.loadData();
    },

    async loadData() {
        try {
            const [quotes, indices, movers, sectors] = await Promise.all([
                API.getQuotes(),
                API.getIndices(),
                API.getTopMovers(),
                API.getSectors(),
            ]);

            this.quotes = quotes || [];
            this.indices = indices || [];
            this.topMovers = movers || {};
            this.sectors = sectors || [];

            this.renderIndices();
            this.renderTopMovers();
            this.renderHeatmap();
            this.renderTable();

            this._lastRefreshTime = new Date();
            this._updateDateLabel();
            this._startAutoRefresh();

            document.getElementById('connectionStatus').className = 'status-dot online';
            document.getElementById('statusText').textContent = `${this.quotes.filter(q => !q.inactive).length} titres actifs`;
        } catch (err) {
            console.error('Market load error:', err);
            document.getElementById('connectionStatus').className = 'status-dot offline';
            document.getElementById('statusText').textContent = 'Erreur';
            Utils.toast('Erreur de chargement des donnees', 'error');
        }
    },

    _updateDateLabel() {
        const dateEl = document.getElementById('marketDate');
        if (!dateEl) return;
        const latest = this.quotes.length > 0 ? this.quotes[0].date : null;
        const now = this._lastRefreshTime;
        const timeStr = now ? now.toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' }) : '';
        const active = this.quotes.filter(q => !q.inactive).length;
        const src = this.quotes[0]?.source || '';
        if (latest) {
            dateEl.innerHTML = `Séance du ${Utils.formatDate(latest)} · <span style="color:var(--text-muted);font-size:0.85em">${active} titres · MAJ ${timeStr} · <span style="color:#3fb950">${src}</span></span>`;
        } else {
            dateEl.textContent = 'Aucune donnee — cliquez Actualiser';
        }
    },

    _startAutoRefresh() {
        // Clear previous timer
        if (this._refreshTimer) clearInterval(this._refreshTimer);
        // Check if we're in trading hours (BRVM: Mon-Fri 9h-15h30 GMT)
        const checkAndRefresh = async () => {
            const now = new Date();
            const utcH = now.getUTCHours();
            const utcM = now.getUTCMinutes();
            const isWeekday = now.getUTCDay() >= 1 && now.getUTCDay() <= 5;
            const isTradingHour = utcH >= 9 && (utcH < 15 || (utcH === 15 && utcM <= 30));
            if (isWeekday && isTradingHour) {
                try {
                    await API.getQuotes(true);
                    await API.getIndices(true);
                    await this.loadData();
                } catch(e) { /* silent */ }
            } else {
                // Outside hours — just update the clock label
                this._updateDateLabel();
            }
        };
        // Auto-refresh every 3 minutes
        this._refreshTimer = setInterval(checkAndRefresh, 3 * 60 * 1000);
    },

    stopAutoRefresh() {
        if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
    },

    async refresh() {
        const btn = document.getElementById('btnRefresh');
        btn.textContent = 'Actualisation...';
        btn.disabled = true;
        try {
            await API.getQuotes(true);
            await API.getIndices(true);
            await this.loadData();
            Utils.toast(`Donnees actualisees (${this.quotes.filter(q=>!q.inactive).length} titres)`, 'success');
        } catch (err) {
            Utils.toast('Erreur d\'actualisation', 'error');
        }
        btn.textContent = 'Actualiser';
        btn.disabled = false;
    },

    renderIndices() {
        const el = document.getElementById('indicesBar');
        if (!this.indices.length) {
            el.innerHTML = `<div class="index-card">
                <div class="index-name">Aucun indice disponible</div>
                <div class="index-value text-muted">—</div>
            </div>`;
            return;
        }
        el.innerHTML = this.indices.map(idx => {
            const cls = Utils.textColorClass(idx.change_pct);
            return `<div class="index-card">
                <div class="index-name">${idx.name}</div>
                <div class="index-value">${Utils.formatNumber(idx.value, 2)}</div>
                <div class="index-change ${cls}">${Utils.formatPct(idx.change_pct)}</div>
            </div>`;
        }).join('');
    },

    renderTopMovers() {
        const el = document.getElementById('moversGrid');
        const { gainers = [], losers = [], most_traded = [] } = this.topMovers;

        const renderColumn = (title, items, valueKey) => {
            if (!items.length) return `<div class="card"><div class="card-title">${title}</div>
                <div class="text-muted" style="padding:16px;text-align:center;font-size:0.85em">Aucune donnee</div></div>`;
            return `<div class="card">
                <div class="card-title">${title}</div>
                ${items.map(item => {
                    const cls = Utils.textColorClass(item.change_pct);
                    return `<div class="mover-item">
                        <div>
                            <div class="mover-ticker">${item.ticker}</div>
                            <div class="mover-volume">${Utils.formatVolume(item.volume)} titres</div>
                        </div>
                        <div class="change-badge ${Utils.changeClass(item.change_pct)}">
                            ${Utils.formatPct(item.change_pct)}
                        </div>
                    </div>`;
                }).join('')}
            </div>`;
        };

        el.innerHTML = renderColumn('Top hausses', gainers) +
                       renderColumn('Top baisses', losers) +
                       renderColumn('Plus echanges', most_traded);
    },

    renderHeatmap() {
        const el = document.getElementById('heatmap');
        if (!this.sectors.length) {
            el.innerHTML = '<div class="text-muted" style="padding:20px;text-align:center">Aucune donnee sectorielle</div>';
            return;
        }

        el.innerHTML = this.sectors.map(sector => {
            const stocks = sector.stocks || [];
            if (!stocks.length) return '';

            return `<div class="heatmap-sector">
                <div style="width:100%;font-size:0.7em;color:var(--text-secondary);font-family:var(--font-mono);
                            padding:2px 4px;letter-spacing:0.5px">${Utils.sectorEmoji(sector.name)} ${sector.name}</div>
                ${stocks.map(s => {
                    const bg = Utils.heatmapColor(s.change_pct);
                    const textColor = Math.abs(s.change_pct || 0) > 2 ? '#fff' : 'var(--text-primary)';
                    return `<div class="heatmap-cell" style="background:${bg};color:${textColor}"
                                 onclick="App.showChart('${s.ticker}')" title="${s.name}">
                        <span class="ticker">${s.ticker}</span>
                        <span class="change">${Utils.formatPct(s.change_pct, 1)}</span>
                    </div>`;
                }).join('')}
            </div>`;
        }).join('');
    },

    renderTable() {
        const el = document.getElementById('quotesTable');
        let data = [...this.quotes];

        // Filters
        if (this.filterSector) {
            data = data.filter(q => q.sector === this.filterSector);
        }
        if (this.filterSearch) {
            const s = this.filterSearch.toLowerCase();
            data = data.filter(q =>
                q.ticker.toLowerCase().includes(s) ||
                (q.name || '').toLowerCase().includes(s)
            );
        }

        // Sort
        data.sort((a, b) => {
            let va = a[this.sortKey], vb = b[this.sortKey];
            if (typeof va === 'string') va = va.toLowerCase();
            if (typeof vb === 'string') vb = vb.toLowerCase();
            if (va == null) va = this.sortDir === 'asc' ? Infinity : -Infinity;
            if (vb == null) vb = this.sortDir === 'asc' ? Infinity : -Infinity;
            if (va < vb) return this.sortDir === 'asc' ? -1 : 1;
            if (va > vb) return this.sortDir === 'asc' ? 1 : -1;
            return 0;
        });

        const columns = [
            { key: 'ticker', label: 'Ticker', render: (v, r) =>
                `<span class="font-mono text-bold">${v}</span>${r.inactive ? ' <span class="pill" style="font-size:0.65em;opacity:0.6" title="Cours indisponible - titre peu liquide">inactif</span>' : ''}` },
            { key: 'name', label: 'Societe', render: (v) =>
                `<span style="font-size:0.85em;max-width:200px;overflow:hidden;text-overflow:ellipsis;display:inline-block">${v || '—'}</span>` },
            { key: 'sector', label: 'Secteur', render: (v) =>
                `<span class="pill">${Utils.sectorEmoji(v)} ${(v || '').split(' ')[0]}</span>` },
            { key: 'price', label: 'Cours', align: 'right', mono: true,
              render: (v, r) => r.inactive ? '<span class="text-muted">—</span>' : Utils.formatFCFA(v, false) },
            { key: 'change_pct', label: 'Var %', align: 'right',
              render: (v) => Components.changeBadge(v) },
            { key: 'volume', label: 'Volume', align: 'right', mono: true,
              render: (v) => Utils.formatVolume(v) },
            { key: 'value', label: 'Valeur (FCFA)', align: 'right', mono: true,
              render: (v) => {
                if (!v || v === 0) return '<span class="text-muted">—</span>';
                if (v >= 1e9) return `<span style="font-size:0.85em">${(v/1e9).toFixed(1)} Mds</span>`;
                if (v >= 1e6) return `<span style="font-size:0.85em">${(v/1e6).toFixed(0)} M</span>`;
                return `<span style="font-size:0.85em">${v.toLocaleString('fr-FR')}</span>`;
              }},
            { key: 'high_3m', label: 'H3M', align: 'right', mono: true,
              render: (v) => Utils.formatNumber(v) },
            { key: 'low_3m', label: 'L3M', align: 'right', mono: true,
              render: (v) => Utils.formatNumber(v) },
            { key: 'range_3m_pct', label: 'Range 3M', align: 'right',
              render: (v) => Components.rangeBar(v) },
            { key: 'per', label: 'PER', align: 'right', mono: true,
              render: (v) => v ? v.toFixed(1) : '—' },
        ];

        el.innerHTML = Components.table('quotesTable', columns, data, {
            onRowClick: true,
            sortBy: this.sortKey,
            sortDir: this.sortDir,
        });

        // Bind sort and click
        el.querySelectorAll('th[data-sort-key]').forEach(th => {
            th.addEventListener('click', () => {
                const key = th.dataset.sortKey;
                if (this.sortKey === key) {
                    this.sortDir = this.sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    this.sortKey = key;
                    this.sortDir = 'asc';
                }
                this.renderTable();
            });
        });

        el.querySelectorAll('tr[data-ticker]').forEach(tr => {
            tr.addEventListener('click', () => {
                App.showChart(tr.dataset.ticker);
            });
        });
    },

    filterBySector(sector) {
        this.filterSector = sector;
        this.render();
    },

    onSearch(value) {
        this.filterSearch = value;
        this.renderTable();
    },
};
