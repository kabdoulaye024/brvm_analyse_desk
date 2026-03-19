/* ═══════════════════════════════════════════════════
   Module 3 — Analysis
   Screener, Charts, Company profiles, Alerts
   ═══════════════════════════════════════════════════ */

const AnalysisPage = {
    activeTab: 'screener',
    screenerResults: [],
    alerts: [],
    chartInstance: null,
    chartTicker: null,
    chartDays: 365,
    chartType: 'heikin',   // 'candle' | 'heikin' | 'line'
    ficheTicker: null,

    async render() {
        const content = document.getElementById('content');
        content.innerHTML = `
            <div class="page-header">
                <div>
                    <h1 class="page-title">Analyse</h1>
                    <div class="page-subtitle">Screener, graphiques, fondamentaux et alertes</div>
                </div>
            </div>

            <div class="tabs">
                <button class="tab ${this.activeTab === 'screener' ? 'active' : ''}"
                        onclick="AnalysisPage.switchTab('screener')">Screener</button>
                <button class="tab ${this.activeTab === 'chart' ? 'active' : ''}"
                        onclick="AnalysisPage.switchTab('chart')">Graphique</button>
                <button class="tab ${this.activeTab === 'fiche' ? 'active' : ''}"
                        onclick="AnalysisPage.switchTab('fiche')">Fiche Societe</button>
                <button class="tab ${this.activeTab === 'alerts' ? 'active' : ''}"
                        onclick="AnalysisPage.switchTab('alerts')">Alertes</button>
            </div>

            <div id="analysisContent"></div>
        `;

        this.renderContent();
    },

    switchTab(tab) {
        this.activeTab = tab;
        document.querySelectorAll('.tab').forEach((t, i) => {
            t.classList.toggle('active', ['screener', 'chart', 'fiche', 'alerts'][i] === tab);
        });
        this.renderContent();
    },

    async renderContent() {
        const el = document.getElementById('analysisContent');
        switch (this.activeTab) {
            case 'screener': return this.renderScreener(el);
            case 'chart':    return this.renderChart(el);
            case 'fiche':    return await this.renderFiche(el);
            case 'alerts':   return await this.renderAlerts(el);
        }
    },

    /* ── SCREENER ──────────────────────────────────── */
    renderScreener(el) {
        el.innerHTML = `
            <div class="card">
                <div class="card-header">
                    <span class="card-title">Criteres de filtrage</span>
                    <button class="btn btn-primary btn-sm" onclick="AnalysisPage.runScreener()">
                        Filtrer
                    </button>
                </div>

                <!-- Section Technique -->
                <div style="font-size:0.75em;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;font-weight:600">
                    📈 Technique / Marché
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label class="form-label">Secteur</label>
                        <select class="form-select" id="scrSector">
                            <option value="">Tous</option>
                            <option value="Télécommunications">Telecoms</option>
                            <option value="Services Financiers">Finance</option>
                            <option value="Consommation de base">Conso. base</option>
                            <option value="Consommation discrétionnaire">Conso. discret.</option>
                            <option value="Industriels">Industriels</option>
                            <option value="Énergie">Energie</option>
                            <option value="Services Publics">Services Publics</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Var min %</label>
                        <input type="number" class="form-input" id="scrMinChange" step="0.1" value="-2" placeholder="-2">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Var max %</label>
                        <input type="number" class="form-input" id="scrMaxChange" step="0.1" value="5" placeholder="5">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Volume min</label>
                        <input type="number" class="form-input" id="scrMinVol" value="1000" placeholder="1000">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Upside min %</label>
                        <input type="number" class="form-input" id="fUpside" value="30" placeholder="30">
                    </div>
                </div>

                <!-- Section Fondamentaux -->
                <div style="font-size:0.75em;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;margin:14px 0 8px;font-weight:600">
                    🏦 Fondamentaux
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label class="form-label">PER max</label>
                        <input type="number" class="form-input" id="scrMaxPer" step="1" value="12" placeholder="12">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Rendement div. min %</label>
                        <input type="number" class="form-input" id="scrMinDivYield" step="0.5" value="3" placeholder="3">
                    </div>
                    <div class="form-group">
                        <label class="form-label">ROE min %</label>
                        <input type="number" class="form-input" id="scrMinRoe" step="1" value="20" placeholder="20">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Debt / Equity max</label>
                        <input type="number" class="form-input" id="scrMaxDebtEq" step="0.1" placeholder="ex: 1.5">
                    </div>
                    <div class="form-group" style="display:flex;align-items:flex-end;padding-bottom:4px">
                        <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:0.85em">
                            <input type="checkbox" id="scrHasDividend" style="width:16px;height:16px">
                            Versement dividende
                        </label>
                    </div>
                </div>

                <!-- Quick presets -->
                <div style="font-size:0.75em;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;margin:14px 0 8px;font-weight:600">
                    ⚡ Presets rapides
                </div>
                <div class="flex gap-8" style="flex-wrap:wrap">
                    <button class="btn btn-secondary btn-sm" onclick="AnalysisPage.presetBreakout()">
                        Breakout volume
                    </button>
                    <button class="btn btn-secondary btn-sm" onclick="AnalysisPage.presetOversold()">
                        Rebond survente
                    </button>
                    <button class="btn btn-secondary btn-sm" onclick="AnalysisPage.presetMomentum()">
                        Momentum
                    </button>
                    <button class="btn btn-secondary btn-sm" onclick="AnalysisPage.presetValue()">
                        🏦 Value (PER bas)
                    </button>
                    <button class="btn btn-secondary btn-sm" onclick="AnalysisPage.presetIncome()">
                        💰 Revenu (dividende)
                    </button>
                    <button class="btn btn-secondary btn-sm" onclick="AnalysisPage.presetQuality()">
                        ⭐ Qualite (ROE fort)
                    </button>
                    <button class="btn btn-ghost btn-sm" onclick="AnalysisPage.clearScreener()">
                        Effacer
                    </button>
                </div>
            </div>

            <div id="screenerResults">
                <div class="empty-state">
                    <div class="icon">🔍</div>
                    <div class="title">Definissez vos criteres</div>
                    <div class="desc">Filtres techniques et fondamentaux combinables</div>
                </div>
            </div>
        `;
    },

    _setScreenerFields(vals) {
        const set = (id, v) => { const el = document.getElementById(id); if (el) { if (el.type === 'checkbox') el.checked = !!v; else el.value = v !== null && v !== undefined ? v : ''; } };
        set('scrSector',      vals.sector      ?? '');
        set('scrMinChange',   vals.minChange   ?? '');
        set('scrMaxChange',   vals.maxChange   ?? '');
        set('scrMinVol',      vals.minVol      ?? '');
        set('fUpside',        vals.minUpside   ?? '');
        set('scrMaxPer',      vals.maxPer      ?? '');
        set('scrMinDivYield', vals.minDivYield ?? '');
        set('scrMinRoe',      vals.minRoe      ?? '');
        set('scrHasDividend', vals.hasDividend ?? false);
        set('scrMaxDebtEq',     vals.maxDebtEq     ?? '');
    },

    presetBreakout()  { this._setScreenerFields({ minVol: 500, minChange: 2 }); this.runScreener(); },
    presetOversold()  { this._setScreenerFields({}); this.runScreener(); },
    presetMomentum()  { this._setScreenerFields({ minVol: 100, minChange: 0, minUpside: 10 }); this.runScreener(); },
    presetValue()     { this._setScreenerFields({ maxPer: 12 }); this.runScreener(); },
    presetIncome()    { this._setScreenerFields({ minDivYield: 3, hasDividend: true }); this.runScreener(); },
    presetQuality()   { this._setScreenerFields({ minRoe: 12 }); this.runScreener(); },
    clearScreener()   { this._setScreenerFields({}); document.getElementById('screenerResults').innerHTML = `<div class="empty-state"><div class="icon">🔍</div><div class="title">Definissez vos criteres</div></div>`; },

    async runScreener() {
        const params = {};
        const g = (id) => document.getElementById(id)?.value;
        const gb = (id) => document.getElementById(id)?.checked;

        const sector      = g('scrSector');
        const minChange   = g('scrMinChange');
        const maxChange   = g('scrMaxChange');
        const minVol      = g('scrMinVol');
        const minUpside   = g('fUpside');
        const maxPer      = g('scrMaxPer');
        const minDivYield = g('scrMinDivYield');
        const minRoe      = g('scrMinRoe');
        const hasDividend = gb('scrHasDividend');

        if (sector)      params.sector = sector;
        if (minChange)   params.min_change = parseFloat(minChange);
        if (maxChange)   params.max_change = parseFloat(maxChange);
        if (minVol)      params.min_volume = parseInt(minVol);
        if (minUpside)   params.min_upside = parseFloat(minUpside);
        if (maxPer)      params.max_per = parseFloat(maxPer);
        if (minDivYield) params.min_div_yield = parseFloat(minDivYield);
        if (minRoe)      params.min_roe = parseFloat(minRoe);
        if (hasDividend) params.has_dividend = true;

        const maxDebtEq = g('scrMaxDebtEq');
        if (maxDebtEq) params.max_debt_equity = parseFloat(maxDebtEq);

        const resultsEl = document.getElementById('screenerResults');
        resultsEl.innerHTML = Components.loading('Screening...');

        try {
            this.screenerResults = await API.runScreener(params);
            this.renderScreenerResults(resultsEl);
        } catch (err) {
            resultsEl.innerHTML = Components.emptyState('⚠️', 'Erreur', err.message);
        }
    },

    renderScreenerResults(el) {
        if (!this.screenerResults.length) {
            el.innerHTML = Components.emptyState('🔍', 'Aucun resultat',
                'Aucun titre ne correspond a vos criteres');
            return;
        }

        // Compute local scores for each result using available data
        const withScores = this.screenerResults.map(q => ({
            ...q,
            _score: this._computeLocalScore(q),
        }));

        const hasFundamentals = withScores.some(q => q.per || q.div_yield || q.roe);

        const columns = [
            { key: 'ticker', label: 'Ticker', render: (v, r) => {
                const regBadge = r.is_regulated ? ' <span style="font-size:0.6em;color:#d29922" title="Secteur réglementé">⚠️</span>' : '';
                const excBadge = r.flag_exceptional ? ' <span style="font-size:0.6em;color:#f85149" title="Résultat potentiellement exceptionnel">🚩</span>' : '';
                return `<span class="font-mono text-bold" style="cursor:pointer" onclick="AnalysisPage.openFiche('${v}')">${v}</span>${regBadge}${excBadge}`;
            }},
            { key: 'name', label: 'Societe', render: (v) =>
                `<span style="font-size:0.82em">${(v || '').substring(0, 20)}</span>` },
            { key: 'price', label: 'Cours', align: 'right', mono: true,
              render: (v) => Utils.formatFCFA(v, false) },
            { key: 'change_pct', label: 'Var %', align: 'right',
              render: (v) => Components.changeBadge(v) },
            { key: 'volume', label: 'Volume', align: 'right', mono: true,
              render: (v) => Utils.formatVolume(v) },
            { key: 'upside_pct', label: 'Upside/Down', align: 'right',
              render: (v, r) => {
                if (v == null) return '<span class="text-muted">—</span>';
                const cls = v > 0 ? 'change-up' : v < 0 ? 'change-down' : 'change-neutral';
                const arrow = v > 0 ? '▲' : v < 0 ? '▼' : '→';
                const fv = r.fair_value ? ` <span class="text-muted" style="font-size:0.75em">(${Utils.formatNumber(r.fair_value)})</span>` : '';
                return `<span class="change-badge ${cls}" style="font-size:0.8em">${arrow} ${v > 0 ? '+' : ''}${v}%</span>${fv}`;
              }},
            { key: 'per', label: 'PER', align: 'right', mono: true,
              render: (v) => v ? `<span style="color:${v < 10 ? '#3fb950' : v > 25 ? '#f85149' : 'inherit'}">${v.toFixed(1)}</span>` : '—' },
            { key: 'div_yield', label: 'Rdt Div', align: 'right', mono: true,
              render: (v) => v ? `<span style="color:${v > 5 ? '#3fb950' : v > 2 ? '#d29922' : 'inherit'}">${v.toFixed(1)}%</span>` : '—' },
            { key: 'roe', label: 'ROE', align: 'right', mono: true,
              render: (v) => v ? `<span style="color:${v > 15 ? '#3fb950' : v > 8 ? '#d29922' : '#f85149'}">${v.toFixed(1)}%</span>` : '—' },
            { key: '_score', label: 'Score', align: 'center',
              render: (v) => this._renderScoreBadge(v) },
        ];

        el.innerHTML = `
            <div style="margin:12px 0;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
                <span class="pill pill-green">${this.screenerResults.length} resultats</span>
                ${hasFundamentals
                    ? '<span class="pill pill-blue">Fondamentaux disponibles</span>'
                    : '<span class="text-muted" style="font-size:0.8em">⚠️ Fondamentaux non disponibles — saisir via Fiche Societe</span>'}
                <span class="text-muted" style="font-size:0.78em">Score = Momentum 25% + Tech 25% + Valeur 20% + Qualite 30%</span>
            </div>
            ${Components.table('screenerTable', columns, withScores)}
        `;
    },

    _computeLocalScore(q) {
        // Simplified scoring using available screener data (no history needed)
        let momentum = 50;
        let technical = 50;
        let value = 50;

        const chg = q.change_pct || 0;
        const range = q.range_52w_pct || 50;
        const vol = q.volume || 0;

        // Momentum: daily change + position in 52w range
        if (chg > 5) momentum = 85;
        else if (chg > 2) momentum = 70;
        else if (chg > 0) momentum = 58;
        else if (chg > -2) momentum = 42;
        else if (chg > -5) momentum = 30;
        else momentum = 15;

        // Range adjustment
        if (range > 80) momentum = Math.min(100, momentum + 10);
        else if (range > 60) momentum = Math.min(100, momentum + 5);
        else if (range < 20) momentum = Math.max(0, momentum - 10);

        // Technical: 52w range position
        if (range > 75) technical = 80;
        else if (range > 55) technical = 65;
        else if (range > 35) technical = 55;
        else if (range > 15) technical = 40;
        else technical = 25;

        // High volume = confirmation signal
        if (vol > 1000 && chg > 0) technical = Math.min(100, technical + 10);

        // Value: inverted range (lower price = potentially cheaper)
        if (range < 20) value = 80;
        else if (range < 35) value = 65;
        else if (range < 50) value = 55;
        else if (range < 70) value = 45;
        else value = 30;

        // PER-based adjustment
        if (q.per && q.per > 0 && q.per < 10) value = Math.min(100, value + 15);
        else if (q.per && q.per > 30) value = Math.max(0, value - 15);

        // Quality: ROE, margin, dividend stability
        let quality = 50;
        if (q.roe) {
            if (q.roe > 25)      quality = 85;
            else if (q.roe > 15) quality = 70;
            else if (q.roe > 8)  quality = 55;
            else                 quality = 35;
        }
        if (q.div_yield && q.div_yield > 4) quality = Math.min(100, quality + 10);
        if (q.pbr && q.pbr < 1.5)           quality = Math.min(100, quality + 8);
        if (q.per && q.per > 0 && q.per < 8) quality = Math.min(100, quality + 8);
        if (q.net_margin && q.net_margin > 20) quality = Math.min(100, quality + 10);

        const global = Math.round(momentum * 0.25 + technical * 0.25 + value * 0.20 + quality * 0.30);
        return { global, momentum, technical, value, quality };
    },

    _renderScoreBadge(score) {
        if (!score) return '—';
        const g = score.global;
        const color = g >= 70 ? '#3fb950' : g >= 50 ? '#d29922' : '#f85149';
        const label = g >= 70 ? 'Fort' : g >= 50 ? 'Moyen' : 'Faible';
        const qColor = score.quality >= 70 ? '#3fb950' : score.quality >= 50 ? '#d29922' : '#8b949e';
        return `
            <div style="display:flex;flex-direction:column;align-items:center;gap:2px">
                <div style="font-size:1em;font-weight:bold;color:${color};font-family:monospace">${g}</div>
                <div style="font-size:0.65em;color:${color};text-transform:uppercase;letter-spacing:0.5px">${label}</div>
                <div style="font-size:0.6em;color:${qColor}">Q:${score.quality ?? '—'}</div>
            </div>`;
    },

    /* ── CHART ─────────────────────────────────────── */
    renderChart(el) {
        const periods = [
            { label: '1M', days: 30 },
            { label: '3M', days: 90 },
            { label: '6M', days: 180 },
            { label: '1A', days: 365 },
            { label: '2A', days: 730 },
            { label: '5A', days: 1825 },
        ];
        const colorPicker = (id, def) =>
            `<input type="color" id="${id}" value="${def}"
                    title="Changer la couleur"
                    oninput="AnalysisPage.reloadChartIfLoaded()"
                    style="width:18px;height:18px;border:1px solid var(--border);border-radius:3px;
                           padding:0;cursor:pointer;background:none;vertical-align:middle">`;

        el.innerHTML = `
            <div class="card">
                <!-- Row 1: ticker + chart type -->
                <div class="chart-toolbar" style="flex-wrap:wrap;gap:6px">
                    <input type="text" class="form-input" id="chartTicker"
                           placeholder="Ticker (ex: SNTS)" style="width:130px;text-transform:uppercase"
                           list="tickerListChart" value="${this.chartTicker || ''}">
                    <datalist id="tickerListChart">
                        ${Object.keys(window._stocksList || {}).map(tk =>
                            `<option value="${tk}">`).join('')}
                    </datalist>
                    <button class="btn btn-primary btn-sm" onclick="AnalysisPage.loadChart()">Charger</button>
                    <div style="display:flex;gap:2px;margin-left:4px">
                        ${[['candle','🕯 Bougies'],['heikin','𝐇 Heikin-Ashi'],['line','╌ Ligne']].map(([t,l]) => `
                            <button id="chartType-${t}"
                                class="btn btn-sm ${this.chartType===t?'btn-primary':'btn-secondary'}"
                                style="padding:3px 7px;font-size:0.75em"
                                onclick="AnalysisPage.setChartType('${t}')">${l}</button>
                        `).join('')}
                    </div>
                    <div style="flex:1"></div>
                    <!-- Indicators with color pickers -->
                    <label style="font-size:0.8em;color:var(--text-secondary);display:flex;align-items:center;gap:3px">
                        <input type="checkbox" id="toggleSMA20" checked onchange="AnalysisPage.reloadChartIfLoaded()">
                        EMA20 ${colorPicker('colorSMA20','#d29922')}
                    </label>
                    <label style="font-size:0.8em;color:var(--text-secondary);display:flex;align-items:center;gap:3px">
                        <input type="checkbox" id="toggleSMA50" onchange="AnalysisPage.reloadChartIfLoaded()">
                        SMA50 ${colorPicker('colorSMA50','#79c0ff')}
                    </label>
                    <label style="font-size:0.8em;color:var(--text-secondary);display:flex;align-items:center;gap:3px">
                        <input type="checkbox" id="toggleBB" onchange="AnalysisPage.reloadChartIfLoaded()">
                        BB ${colorPicker('colorBB','#ffa028')}
                    </label>
                    <label style="font-size:0.8em;color:var(--text-secondary);display:flex;align-items:center;gap:3px">
                        <input type="checkbox" id="toggleRSI" checked onchange="AnalysisPage.reloadChartIfLoaded()">
                        RSI ${colorPicker('colorRSI','#a371f7')}
                    </label>
                </div>

                <!-- Period selector -->
                <div style="display:flex;align-items:center;gap:4px;padding:8px 0 4px;border-bottom:1px solid var(--border)">
                    <span style="font-size:0.75em;color:var(--text-muted);margin-right:4px">PÉRIODE</span>
                    ${periods.map(p => `
                        <button id="period-${p.days}"
                            class="btn btn-sm ${this.chartDays === p.days ? 'btn-primary' : 'btn-secondary'}"
                            style="min-width:36px;padding:3px 8px;font-size:0.78em"
                            onclick="AnalysisPage.selectPeriod(${p.days})">${p.label}</button>
                    `).join('')}
                    <span style="margin-left:auto;font-size:0.72em;color:var(--text-muted)" id="chartDataInfo"></span>
                </div>

                <div id="chartArea" class="chart-area" style="height:380px"></div>
                <div id="rsiArea" style="height:110px;display:none;border-top:1px solid var(--border)"></div>
            </div>

            <!-- Indicators panel -->
            <div id="chartIndicators" class="mt-16"></div>
        `;

        if (this.chartTicker) {
            this.loadChart();
        }
    },

    selectPeriod(days) {
        this.chartDays = days;
        // Update button states
        [30, 90, 180, 365, 730, 1825].forEach(d => {
            const btn = document.getElementById(`period-${d}`);
            if (btn) {
                btn.className = `btn btn-sm ${d === days ? 'btn-primary' : 'btn-secondary'}`;
                btn.style.cssText = 'min-width:36px;padding:3px 8px;font-size:0.78em';
            }
        });
        if (this.chartTicker) this.loadChart();
    },

    reloadChartIfLoaded() {
        if (this.chartTicker) this.loadChart();
    },

    setChartType(type) {
        this.chartType = type;
        ['candle', 'heikin', 'line'].forEach(t => {
            const btn = document.getElementById(`chartType-${t}`);
            if (btn) btn.className = `btn btn-sm ${t === type ? 'btn-primary' : 'btn-secondary'}`;
            if (btn) btn.style.cssText = 'padding:3px 7px;font-size:0.75em';
        });
        if (this.chartTicker) this.loadChart();
    },

    openChart(ticker) {
        this.chartTicker = ticker;
        this.switchTab('chart');
    },

    openFiche(ticker) {
        this.ficheTicker = ticker;
        this.switchTab('fiche');
    },

    async loadChart() {
        const ticker = (document.getElementById('chartTicker')?.value || '').toUpperCase().trim();
        if (!ticker) {
            Utils.toast('Saisissez un ticker', 'error');
            return;
        }
        this.chartTicker = ticker;

        const chartArea = document.getElementById('chartArea');
        chartArea.innerHTML = Components.loading('Chargement du graphique...');

        // Update data info label
        const infoEl = document.getElementById('chartDataInfo');
        if (infoEl) infoEl.textContent = 'Chargement...';

        try {
            const data = await API.getHistory(ticker, this.chartDays);
            const series = data?.indicators?.series;

            if (!series || !series.dates || series.dates.length < 5) {
                chartArea.innerHTML = Components.emptyState('📊', 'Pas de donnees',
                    `Aucun historique disponible pour ${ticker} sur cette periode. Essayez une periode plus courte ou cliquez Charger.`);
                if (infoEl) infoEl.textContent = '0 points';
                return;
            }

            // Show data span info
            if (infoEl && series.dates.length > 0) {
                const first = series.dates[0];
                const last = series.dates[series.dates.length - 1];
                const nPts = series.dates.length;
                const requested = this.chartDays;
                // Estimate expected trading days (≈252/year)
                const expectedPts = Math.round(requested * 252 / 365);
                const coveragePct = Math.round(nPts / expectedPts * 100);
                if (coveragePct < 80) {
                    infoEl.innerHTML = `<span style="color:#d29922">⚠️ ${nPts} séances dispo (${first} → ${last}) · Historique s'enrichit quotidiennement</span>`;
                } else {
                    infoEl.textContent = `${nPts} séances · ${first} → ${last}`;
                }
            }

            // Clear previous chart
            chartArea.innerHTML = '';
            if (this._chartCleanup) { this._chartCleanup(); this._chartCleanup = null; }

            const chartOptions = {
                layout: {
                    background: { color: '#0d1117' },
                    textColor: '#8b949e',
                    fontFamily: "'Space Mono', monospace",
                },
                grid: {
                    vertLines: { color: '#1e2633' },
                    horzLines: { color: '#1e2633' },
                },
                crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
                rightPriceScale: { borderColor: '#1e2633' },
                timeScale: { borderColor: '#1e2633', timeVisible: false },
            };

            // Read user-chosen colors (fallback to defaults)
            const col = {
                sma20: document.getElementById('colorSMA20')?.value || '#d29922',
                sma50: document.getElementById('colorSMA50')?.value || '#79c0ff',
                bb:    document.getElementById('colorBB')?.value    || '#ffa028',
                rsi:   document.getElementById('colorRSI')?.value   || '#a371f7',
            };

            // ── Main price chart ──────────────────────────────────────────
            const chart = LightweightCharts.createChart(chartArea, {
                ...chartOptions,
                width: chartArea.clientWidth,
                height: chartArea.clientHeight,
            });

            // Build base OHLC array (close-only fallback)
            const hasOHLC = series.open?.some(v => v != null);
            const baseCandles = series.dates.map((d, i) => {
                const c = series.close[i]; if (!c) return null;
                const o = hasOHLC ? (series.open[i]  || c) : c;
                const h = hasOHLC ? (series.high[i]  || Math.max(o, c)) : c;
                const l = hasOHLC ? (series.low[i]   || Math.min(o, c)) : c;
                return { time: d, open: o, high: h, low: l, close: c };
            }).filter(Boolean);

            // ── Heikin-Ashi transform ─────────────────────────────────────
            const toHA = (candles) => {
                const ha = [];
                for (let i = 0; i < candles.length; i++) {
                    const { time, open: o, high: h, low: l, close: c } = candles[i];
                    const haC = (o + h + l + c) / 4;
                    const haO = i === 0 ? (o + c) / 2 : (ha[i - 1].open + ha[i - 1].close) / 2;
                    const haH = Math.max(h, haO, haC);
                    const haL = Math.min(l, haO, haC);
                    ha.push({ time, open: haO, high: haH, low: haL, close: haC });
                }
                return ha;
            };

            const chartType = this.chartType;

            if (chartType === 'line') {
                const lineSeries = chart.addLineSeries({ color: '#79c0ff', lineWidth: 2 });
                lineSeries.setData(baseCandles.map(d => ({ time: d.time, value: d.close })));
            } else {
                // Candle or Heikin-Ashi — both use addCandlestickSeries
                const displayData = chartType === 'heikin' ? toHA(baseCandles) : baseCandles;
                // HA uses softer colors; regular candles use bright green/red
                const upC   = chartType === 'heikin' ? '#26a69a' : '#3fb950';
                const downC = chartType === 'heikin' ? '#ef5350' : '#f85149';
                const candleSeries = chart.addCandlestickSeries({
                    upColor: upC, downColor: downC,
                    borderUpColor: upC, borderDownColor: downC,
                    wickUpColor: upC, wickDownColor: downC,
                    // HA: no borders between body and wick for cleaner look
                    borderVisible: chartType !== 'heikin',
                });
                candleSeries.setData(displayData);
            }

            // ── EMA 20 ────────────────────────────────────────────────────
            if (document.getElementById('toggleSMA20')?.checked && series.ema20?.length > 0) {
                const s20 = chart.addLineSeries({ color: col.sma20, lineWidth: 1, title: 'EMA20' });
                s20.setData(series.dates.map((d, i) => ({ time: d, value: series.ema20[i] }))
                    .filter(d => d.value != null && !isNaN(d.value)));
            }

            // ── SMA 50 ────────────────────────────────────────────────────
            if (document.getElementById('toggleSMA50')?.checked && series.sma50?.length > 0) {
                const s50 = chart.addLineSeries({ color: col.sma50, lineWidth: 1, lineStyle: 2, title: 'SMA50' });
                s50.setData(series.dates.map((d, i) => ({ time: d, value: series.sma50[i] }))
                    .filter(d => d.value != null && !isNaN(d.value)));
            }

            // ── Bollinger Bands ───────────────────────────────────────────
            if (document.getElementById('toggleBB')?.checked) {
                const bbAlpha  = col.bb + 'cc';   // add 80% alpha via hex suffix
                const bbMiddle = col.bb + '66';
                if (series.bb_upper?.length > 0) {
                    const bbU = chart.addLineSeries({ color: col.bb, lineWidth: 1, lineStyle: 1, title: 'BB+' });
                    bbU.setData(series.dates.map((d, i) => ({ time: d, value: series.bb_upper[i] }))
                        .filter(d => d.value != null && !isNaN(d.value)));
                }
                if (series.bb_lower?.length > 0) {
                    const bbL = chart.addLineSeries({ color: col.bb, lineWidth: 1, lineStyle: 1, title: 'BB-' });
                    bbL.setData(series.dates.map((d, i) => ({ time: d, value: series.bb_lower[i] }))
                        .filter(d => d.value != null && !isNaN(d.value)));
                }
                if (series.bb_middle?.length > 0) {
                    const bbM = chart.addLineSeries({ color: bbMiddle, lineWidth: 1, lineStyle: 3 });
                    bbM.setData(series.dates.map((d, i) => ({ time: d, value: series.bb_middle[i] }))
                        .filter(d => d.value != null && !isNaN(d.value)));
                }
            }

            // Volume (overlay at bottom 20%)
            if (series.volume?.length > 0) {
                const volSeries = chart.addHistogramSeries({
                    priceFormat: { type: 'volume' },
                    priceScaleId: 'volume',
                });
                chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
                volSeries.setData(series.dates.map((d, i) => ({
                    time: d,
                    value: series.volume[i] || 0,
                    color: (series.close[i] || 0) >= (series.close[i - 1] || 0)
                        ? 'rgba(63, 185, 80, 0.3)' : 'rgba(248, 81, 73, 0.3)',
                })));
            }

            chart.timeScale().fitContent();

            // ── RSI sub-chart ─────────────────────────────────────────────
            const rsiArea = document.getElementById('rsiArea');
            const showRSI = document.getElementById('toggleRSI')?.checked && series.rsi?.some(v => v != null);
            rsiArea.style.display = showRSI ? 'block' : 'none';

            if (showRSI && rsiArea) {
                rsiArea.innerHTML = '';
                const rsiChart = LightweightCharts.createChart(rsiArea, {
                    ...chartOptions,
                    width: rsiArea.clientWidth,
                    height: 110,
                    timeScale: { borderColor: '#1e2633', timeVisible: true },
                    rightPriceScale: {
                        borderColor: '#1e2633',
                        scaleMargins: { top: 0.1, bottom: 0.1 },
                    },
                });

                // RSI line (user color)
                const rsiLine = rsiChart.addLineSeries({ color: col.rsi, lineWidth: 1, title: 'RSI' });
                rsiLine.setData(series.dates.map((d, i) => ({ time: d, value: series.rsi[i] }))
                    .filter(d => d.value != null && !isNaN(d.value)));

                // Overbought / Oversold bands at 70 / 30
                const ob = rsiChart.addLineSeries({ color: 'rgba(248,81,73,0.45)', lineWidth: 1, lineStyle: 2 });
                const os = rsiChart.addLineSeries({ color: 'rgba(63,185,80,0.45)',  lineWidth: 1, lineStyle: 2 });
                const rsiDates = series.dates.filter((_, i) => series.rsi[i] != null);
                if (rsiDates.length) {
                    ob.setData([{ time: rsiDates[0], value: 70 }, { time: rsiDates[rsiDates.length - 1], value: 70 }]);
                    os.setData([{ time: rsiDates[0], value: 30 }, { time: rsiDates[rsiDates.length - 1], value: 30 }]);
                }

                // Sync time scales
                chart.timeScale().subscribeVisibleLogicalRangeChange(range => {
                    if (range) rsiChart.timeScale().setVisibleLogicalRange(range);
                });
                rsiChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
                    if (range) chart.timeScale().setVisibleLogicalRange(range);
                });
                rsiChart.timeScale().fitContent();

                // Resize handler for RSI chart
                const rsiResizeH = () => rsiChart.applyOptions({ width: rsiArea.clientWidth });
                window.addEventListener('resize', rsiResizeH);
                const prevCleanup = this._chartCleanup;
                this._chartCleanup = () => {
                    if (prevCleanup) prevCleanup();
                    window.removeEventListener('resize', rsiResizeH);
                };
            }

            // Resize handler for main chart
            const resizeHandler = () => chart.applyOptions({ width: chartArea.clientWidth });
            window.addEventListener('resize', resizeHandler);
            const prevCleanup2 = this._chartCleanup;
            this._chartCleanup = () => {
                if (prevCleanup2) prevCleanup2();
                window.removeEventListener('resize', resizeHandler);
            };

            // Render indicators panel
            this.renderIndicatorsPanel(data.indicators);

        } catch (err) {
            chartArea.innerHTML = Components.emptyState('⚠️', 'Erreur', err.message);
        }
    },

    renderIndicatorsPanel(ind) {
        const el = document.getElementById('chartIndicators');
        if (!ind || !ind.current_price) {
            el.innerHTML = '';
            return;
        }

        const rsiColor = ind.rsi > 75 ? 'text-red' : (ind.rsi < 25 ? 'text-green' : 'text-blue');
        const rsiLabel = ind.rsi > 75 ? '⚠️ Surachat' : (ind.rsi < 25 ? '⚡ Survente' : '✓ Neutre');

        // Compute local scores
        const q = {
            change_pct: ind.var_1w || 0,
            range_52w_pct: ind.range_52w_pct || 50,
            volume: ind.vol_avg_20d || 0,
        };
        const score = this._computeLocalScore(q);

        // RSI gauge bar
        const rsiGaugeColor = ind.rsi > 70 ? '#f85149' : ind.rsi < 30 ? '#3fb950' : '#58a6ff';

        // BB position
        const px = ind.current_price;
        const bbUpper = ind.bb_upper || px;
        const bbLower = ind.bb_lower || px;
        let bbPos = 50;
        if (bbUpper !== bbLower) bbPos = Math.round((px - bbLower) / (bbUpper - bbLower) * 100);
        const bbLabel = bbPos > 80 ? 'Bande haute' : bbPos < 20 ? 'Bande basse' : 'Zone centrale';

        // EMA alignment
        const ema20 = ind.ema20 || px;
        const sma50 = ind.sma50 || 0;
        let alignLabel = '—';
        let alignColor = '#8b949e';
        if (px > ema20 && ema20 > sma50 && sma50 > 0) { alignLabel = '↑↑ Haussier fort'; alignColor = '#3fb950'; }
        else if (px > ema20) { alignLabel = '↑ Au-dessus EMA'; alignColor = '#3fb950'; }
        else if (px < ema20) { alignLabel = '↓ En-dessous EMA'; alignColor = '#f85149'; }

        el.innerHTML = `
            <!-- Score synthétique -->
            <div class="card" style="margin-bottom:12px">
                <div class="flex-between" style="margin-bottom:12px">
                    <span class="card-title">Score de synthese — ${this.chartTicker}</span>
                    <span style="font-size:0.8em;color:var(--text-muted)">Liquidite: ${ind.liquidity_pct || 0}% · RSI(${ind.rsi_period || 14})</span>
                </div>
                <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px">
                    ${['Momentum', 'Technique', 'Valeur'].map((name, i) => {
                        const val = [score.momentum, score.technical, score.value][i];
                        const color = val >= 70 ? '#3fb950' : val >= 50 ? '#d29922' : '#f85149';
                        return `
                            <div style="text-align:center">
                                <div style="font-size:1.6em;font-weight:bold;color:${color};font-family:monospace">${val}</div>
                                <div style="font-size:0.75em;color:var(--text-muted);margin:2px 0">${name}</div>
                                <div style="background:var(--bg-tertiary);height:4px;border-radius:2px;overflow:hidden;margin-top:4px">
                                    <div style="width:${val}%;height:100%;background:${color};border-radius:2px;transition:width 0.8s ease"></div>
                                </div>
                            </div>`;
                    }).join('')}
                </div>
                <div style="text-align:center;margin-top:12px;padding-top:10px;border-top:1px solid var(--border)">
                    <span style="font-size:1.1em;font-weight:bold;color:${score.global >= 70 ? '#3fb950' : score.global >= 50 ? '#d29922' : '#f85149'};font-family:monospace">
                        Score Global: ${score.global}/100
                    </span>
                    <span style="margin-left:12px;font-size:0.85em;color:var(--text-muted)">
                        ${score.global >= 70 ? '🟢 Signal fort' : score.global >= 50 ? '🟡 Signal neutre' : '🔴 Signal faible'}
                    </span>
                </div>
            </div>

            <!-- Indicateurs détaillés -->
            <div class="metrics-row">
                <div class="card">
                    <div class="text-muted" style="font-size:0.75em;margin-bottom:6px">RSI(${ind.rsi_period || 14})</div>
                    <div class="font-mono text-bold" style="font-size:1.3em;color:${rsiGaugeColor}">${ind.rsi ? ind.rsi.toFixed(1) : '—'}</div>
                    <div style="background:var(--bg-tertiary);height:4px;border-radius:2px;overflow:hidden;margin:6px 0;position:relative">
                        <div style="width:100%;height:100%;background:linear-gradient(to right,#3fb950,#d29922,#f85149)"></div>
                        <div style="position:absolute;top:-3px;left:${ind.rsi || 50}%;width:2px;height:10px;background:white;border-radius:1px;transform:translateX(-50%)"></div>
                    </div>
                    <div style="font-size:0.75em;color:${rsiGaugeColor}">${rsiLabel}</div>
                </div>

                <div class="card">
                    <div class="text-muted" style="font-size:0.75em;margin-bottom:6px">Alignement</div>
                    <div style="font-size:0.85em;font-weight:bold;color:${alignColor}">${alignLabel}</div>
                    <div class="text-muted" style="font-size:0.75em;margin-top:6px">EMA20: ${Utils.formatNumber(ind.ema20)}</div>
                    ${sma50 ? `<div class="text-muted" style="font-size:0.75em">SMA50: ${Utils.formatNumber(sma50)}</div>` : ''}
                </div>

                <div class="card">
                    <div class="text-muted" style="font-size:0.75em;margin-bottom:6px">Bollinger (${bbLabel})</div>
                    <div style="background:var(--bg-tertiary);height:4px;border-radius:2px;overflow:hidden;margin:6px 0;position:relative">
                        <div style="width:100%;height:100%;background:linear-gradient(to right,#3fb950 0%,#58a6ff 50%,#f85149 100%)"></div>
                        <div style="position:absolute;top:-3px;left:${Math.min(98,Math.max(2,bbPos))}%;width:2px;height:10px;background:white;border-radius:1px;transform:translateX(-50%)"></div>
                    </div>
                    <div class="font-mono" style="font-size:0.75em">${Utils.formatNumber(bbLower)} — ${Utils.formatNumber(bbUpper)}</div>
                </div>

                ${Components.metricCard('Var 1s',
                    Utils.formatPct(ind.var_1w),
                    { textClass: Utils.textColorClass(ind.var_1w) })}
                ${Components.metricCard('Var 1m',
                    Utils.formatPct(ind.var_1m),
                    { textClass: Utils.textColorClass(ind.var_1m),
                      accent: (ind.var_1m || 0) >= 0 ? 'green' : 'red' })}
                ${Components.metricCard('Var 3m',
                    Utils.formatPct(ind.var_3m),
                    { textClass: Utils.textColorClass(ind.var_3m) })}
                ${Components.metricCard('Range 52s',
                    ind.range_52w_pct ? ind.range_52w_pct.toFixed(0) + '%' : '—',
                    { sub: `${Utils.formatNumber(ind.low_52w)} - ${Utils.formatNumber(ind.high_52w)}` })}
                ${Components.metricCard('Vol moy 20j',
                    Utils.formatVolume(ind.vol_avg_20d),
                    { sub: `${ind.nb_points || 0} points` })}
            </div>
        `;
    },

    /* ── FICHE SOCIETE ──────────────────────────────── */
    async renderFiche(el) {
        // Ticker picker header
        el.innerHTML = `
            <div class="card" style="margin-bottom:12px">
                <div class="flex-center gap-8">
                    <input type="text" class="form-input" id="ficheTicker"
                           style="width:160px;text-transform:uppercase"
                           placeholder="Ticker (ex: SNTS)"
                           list="ficheTickerList"
                           value="${this.ficheTicker || ''}">
                    <datalist id="ficheTickerList">
                        ${Object.keys(window._stocksList || {}).map(tk =>
                            `<option value="${tk}">`).join('')}
                    </datalist>
                    <button class="btn btn-primary btn-sm"
                            onclick="AnalysisPage.loadFiche()">Charger</button>
                    <button class="btn btn-secondary btn-sm"
                            onclick="AnalysisPage.loadFiche(true)">Actualiser scraping</button>
                    <button class="btn btn-secondary btn-sm"
                            onclick="AnalysisPage.showFicheForm()">Saisie manuelle</button>
                    ${this.ficheTicker ? `
                        <button class="btn btn-ghost btn-sm"
                                onclick="App.showChart('${this.ficheTicker}')">Voir graphique →</button>
                    ` : ''}
                </div>
            </div>
            <div id="ficheContent">
                ${this.ficheTicker
                    ? Components.loading('Chargement de la fiche...')
                    : Components.emptyState('🏦', 'Selectionnez une societe', 'Saisissez un ticker pour afficher la fiche fondamentale')}
            </div>
        `;

        if (this.ficheTicker) {
            await this.loadFiche();
        }
    },

    async loadFiche(refresh = false) {
        const ticker = (document.getElementById('ficheTicker')?.value || this.ficheTicker || '').toUpperCase().trim();
        if (!ticker) { Utils.toast('Saisissez un ticker', 'error'); return; }
        this.ficheTicker = ticker;

        const el = document.getElementById('ficheContent');
        el.innerHTML = Components.loading('Chargement des fondamentaux...');

        try {
            const fund = await API.getFundamentals(ticker, refresh);
            this._renderFicheData(el, fund, ticker);
        } catch (err) {
            el.innerHTML = Components.emptyState('⚠️', 'Erreur', err.message);
        }
    },

    _renderFicheData(el, fund, ticker) {
        const ref = (window._stocksList || {})[ticker] || {};
        const name = fund.name || ref.name || ticker;
        const sector = fund.sector || ref.sector || '';
        const noData = fund.no_data;

        const fmt = (v, dec = 0) => (v != null && !isNaN(v))
            ? Number(v).toLocaleString('fr-FR', { minimumFractionDigits: dec, maximumFractionDigits: dec })
            : '—';
        const fmtM = (v) => (v != null && !isNaN(v))
            ? `${(v/1e9 >= 1 ? (v/1e9).toFixed(1) + ' Mds' : (v/1e6).toFixed(0) + ' M')} FCFA`
            : '—';
        const pct = (v) => v != null ? `${v > 0 ? '+' : ''}${v.toFixed(1)}%` : '—';

        // Key ratios card helper
        const ratioRow = (label, value, hint = '', color = '') => `
            <div class="flex-between" style="padding:7px 0;border-bottom:1px solid var(--border)">
                <span class="text-muted" style="font-size:0.85em">${label}</span>
                <div style="text-align:right">
                    <span class="font-mono" style="${color ? `color:${color}` : ''}">${value}</span>
                    ${hint ? `<div style="font-size:0.72em;color:var(--text-muted)">${hint}</div>` : ''}
                </div>
            </div>`;

        const per = fund.per;
        const pbr = fund.pbr;
        const roe = fund.roe;
        const divYield = fund.div_yield;
        const debtEq = fund.debt_equity;
        const payout = fund.payout_ratio;
        const bench = fund.sector_per_benchmark;
        const perVsSector = fund.per_vs_sector;

        if (noData) {
            el.innerHTML = `
                <div class="card">
                    <div style="text-align:center;padding:20px">
                        <div style="font-size:1.5em;margin-bottom:8px">🏦</div>
                        <div class="font-mono text-bold">${name}</div>
                        <div class="text-muted" style="margin:8px 0">${sector}</div>
                        <div class="text-muted" style="font-size:0.85em">Aucune donnee fondamentale disponible pour ${ticker}.</div>
                        <div style="margin-top:16px">
                            <button class="btn btn-primary" onclick="AnalysisPage.showFicheForm()">
                                Saisir manuellement
                            </button>
                            <button class="btn btn-secondary" style="margin-left:8px"
                                    onclick="AnalysisPage.loadFiche(true)">
                                Tenter le scraping
                            </button>
                        </div>
                    </div>
                </div>`;
            return;
        }

        const isBank = fund.is_bank;

        el.innerHTML = `
            <!-- Header -->
            <div class="card" style="margin-bottom:12px">
                <div class="flex-between">
                    <div>
                        <div class="font-mono text-bold" style="font-size:1.4em">${ticker}</div>
                        <div style="font-size:0.9em;margin-top:2px">${name}</div>
                        <div class="text-muted" style="font-size:0.8em;margin-top:4px">
                            <span class="pill">${Utils.sectorEmoji(sector)} ${sector}</span>
                            ${fund.period ? `<span class="pill" style="margin-left:4px">Exercice ${fund.period}</span>` : ''}
                            ${fund.updated_at ? `<span style="margin-left:8px">MAJ: ${Utils.formatDate(fund.updated_at)}</span>` : ''}
                        </div>
                    </div>
                    <div style="text-align:right">
                        ${fund.current_price
                            ? `<div class="font-mono" style="font-size:1.6em;font-weight:bold">${Utils.formatFCFA(fund.current_price, false)}</div>
                               <div class="text-muted" style="font-size:0.8em">Cours actuel</div>`
                            : ''}
                    </div>
                </div>
            </div>

            <div class="grid-2" style="margin-bottom:12px">

                <!-- Valorisation -->
                <div class="card">
                    <div class="card-title" style="margin-bottom:12px">📊 Valorisation</div>
                    ${ratioRow('PER (Price/Earnings)',
                        per ? `${per.toFixed(1)}x` : '—',
                        bench ? `Secteur: ${bench.toFixed(1)}x${perVsSector ? ` (${perVsSector > 0 ? '-' : '+'}${Math.abs(perVsSector).toFixed(0)}% vs bench)` : ''}` : '',
                        per && bench ? (per < bench * 0.8 ? '#3fb950' : per > bench * 1.2 ? '#f85149' : '') : ''
                    )}
                    ${ratioRow('PBR (Price/Book)',
                        pbr ? `${pbr.toFixed(2)}x` : '—',
                        pbr ? (pbr < 1 ? 'En dessous du book' : pbr < 2 ? 'Valorisation raisonnable' : 'Prime elevee') : '',
                        pbr ? (pbr < 1 ? '#3fb950' : pbr > 3 ? '#f85149' : '') : ''
                    )}
                    ${ratioRow('Valeur comptable/action', fund.bvps ? `${fmt(fund.bvps)} FCFA` : '—')}
                    ${ratioRow('Capitalisation boursiere',
                        fund.market_cap ? fmtM(fund.market_cap * 1e6) : '—')}
                    ${ratioRow('BPA (Benefice/action)',
                        fund.eps_prev ? `${fmt(fund.eps_prev)} FCFA` : '—',
                        fund.eps_n2 ? `N-1: ${fmt(fund.eps_n2)} FCFA` : ''
                    )}
                    ${fund.eps_growth != null ? ratioRow('Croissance BPA',
                        pct(fund.eps_growth), '',
                        fund.eps_growth > 0 ? '#3fb950' : '#f85149') : ''}
                </div>

                <!-- Dividende -->
                <div class="card">
                    <div class="card-title" style="margin-bottom:12px">💰 Dividende & Rentabilite</div>
                    ${ratioRow('Dividende/action',
                        fund.dividend ? `${fmt(fund.dividend)} FCFA` : '—')}
                    ${ratioRow('Rendement dividende',
                        divYield ? `${divYield.toFixed(2)}%` : '—',
                        '',
                        divYield ? (divYield > 5 ? '#3fb950' : divYield > 2 ? '#d29922' : '') : ''
                    )}
                    ${ratioRow('Taux distribution (Payout)',
                        payout ? `${payout.toFixed(0)}%` : '—',
                        payout ? (payout > 80 ? '⚠️ Taux eleve' : payout > 50 ? 'Genereux' : 'Conservateur') : ''
                    )}
                    <div style="border-top:1px solid var(--border);margin:8px 0;padding-top:8px"></div>
                    ${ratioRow('ROE (Rentabilite capitaux)',
                        roe != null ? `${roe.toFixed(1)}%` : '—',
                        '',
                        roe != null ? (roe > 15 ? '#3fb950' : roe > 8 ? '#d29922' : '#f85149') : ''
                    )}
                    ${ratioRow('Resultat net',
                        fund.net_income ? fmtM(fund.net_income * 1e6) : '—')}
                    ${ratioRow('Capitaux propres',
                        fund.equity ? fmtM(fund.equity * 1e6) : '—')}
                    ${ratioRow('Nombre de titres',
                        fund.shares_outstanding ? `${(fund.shares_outstanding / 1e6).toFixed(2)} M titres` : '—')}
                </div>
            </div>

            <div class="grid-2" style="margin-bottom:12px">
                <!-- Solidite financiere -->
                <div class="card">
                    <div class="card-title" style="margin-bottom:12px">🏗️ Bilan & Solidite</div>
                    ${ratioRow('Total actif (bilan)',
                        fund.total_assets ? fmtM(fund.total_assets * 1e6) : '—')}
                    ${ratioRow('Dettes totales',
                        fund.total_debt ? fmtM(fund.total_debt * 1e6) : '—')}
                    ${ratioRow('Levier (Dettes/Capitaux)',
                        debtEq != null ? `${debtEq.toFixed(2)}x` : '—',
                        '',
                        debtEq != null ? (debtEq < 0.5 ? '#3fb950' : debtEq > 1.5 ? '#f85149' : '#d29922') : ''
                    )}
                    ${fund.debt_assets != null ? ratioRow('Dettes/Total actif',
                        `${fund.debt_assets.toFixed(1)}%`) : ''}
                </div>

                <!-- Données banque ou sectorielles -->
                ${isBank ? `
                <div class="card">
                    <div class="card-title" style="margin-bottom:12px">🏦 Indicateurs Bancaires</div>
                    ${ratioRow('PNB (Produit Net Bancaire)',
                        fund.pnb ? fmtM(fund.pnb * 1e6) : '—')}
                    ${ratioRow('Resultat brut exploitation',
                        fund.bank_result ? fmtM(fund.bank_result * 1e6) : '—')}
                    ${ratioRow('Encours credits clientele',
                        fund.credit_outstanding ? fmtM(fund.credit_outstanding * 1e6) : '—')}
                    ${ratioRow('Depots clientele',
                        fund.client_deposits ? fmtM(fund.client_deposits * 1e6) : '—')}
                    ${(fund.credit_outstanding && fund.client_deposits)
                        ? ratioRow('Ratio credits/depots',
                            `${(fund.credit_outstanding / fund.client_deposits * 100).toFixed(0)}%`) : ''}
                </div>
                ` : `
                <div class="card">
                    <div class="card-title" style="margin-bottom:12px">🎯 Synthese</div>
                    ${[
                        { label: 'Valorisation', val: per && bench ? (per < bench ? '✅ Decote vs secteur' : per < bench * 1.1 ? '⚖️ A la valeur' : '⚠️ Prime') : '—' },
                        { label: 'Rendement', val: divYield ? (divYield > 5 ? '✅ Genereux' : divYield > 2 ? '⚖️ Correct' : '📉 Faible') : '—' },
                        { label: 'Rentabilite', val: roe != null ? (roe > 15 ? '✅ Excellente' : roe > 8 ? '⚖️ Correcte' : '⚠️ Faible') : '—' },
                        { label: 'Solidite', val: debtEq != null ? (debtEq < 0.5 ? '✅ Peu endettee' : debtEq < 1 ? '⚖️ Moderee' : '⚠️ Endettee') : '—' },
                    ].map(r => ratioRow(r.label, r.val)).join('')}
                </div>`}
            </div>

            <div style="margin-top:4px;text-align:right">
                <button class="btn btn-secondary btn-sm"
                        onclick="AnalysisPage.showFicheForm()">Modifier les donnees</button>
                <button class="btn btn-danger btn-sm" style="margin-left:8px"
                        onclick="AnalysisPage.deleteFicheData('${ticker}')">Effacer</button>
            </div>
        `;
    },

    async showFicheForm() {
        const ticker = (document.getElementById('ficheTicker')?.value || this.ficheTicker || '').toUpperCase();
        let existing = {};
        if (ticker) {
            try {
                const fund = await API.getFundamentals(ticker);
                if (!fund.no_data) existing = fund;
            } catch(e) {}
        }

        const isBank = existing.is_bank || (window._stocksList?.[ticker]?.sector === 'Services Financiers');

        const body = `
            <div style="max-height:70vh;overflow-y:auto;padding-right:4px">
                <div class="form-row">
                    <div class="form-group">
                        <label class="form-label">Ticker</label>
                        <input type="text" class="form-input" id="ffTicker" value="${ticker}"
                               style="text-transform:uppercase">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Exercice / Periode</label>
                        <input type="text" class="form-input" id="ffPeriod"
                               value="${existing.period || ''}" placeholder="ex: 2023, S1-2024">
                    </div>
                    <div class="form-group" style="display:flex;align-items:flex-end">
                        <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
                            <input type="checkbox" id="ffIsBank" ${isBank ? 'checked' : ''}>
                            Etablissement bancaire
                        </label>
                    </div>
                </div>

                <div style="font-size:0.75em;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;margin:12px 0 6px;font-weight:600">
                    Par action (FCFA)
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label class="form-label">BPA (EPS) N</label>
                        <input type="number" class="form-input" id="ffEps"
                               value="${existing.eps_prev || ''}" placeholder="ex: 2500">
                    </div>
                    <div class="form-group">
                        <label class="form-label">BPA N-1</label>
                        <input type="number" class="form-input" id="ffEpsPrev"
                               value="${existing.eps_n2 || ''}" placeholder="ex: 2200">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Dividende/action</label>
                        <input type="number" class="form-input" id="ffDividend"
                               value="${existing.dividend || ''}" placeholder="ex: 1200">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Nombre de titres</label>
                        <input type="number" class="form-input" id="ffShares"
                               value="${existing.shares_outstanding || ''}" placeholder="ex: 125000000">
                    </div>
                </div>

                <div style="font-size:0.75em;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;margin:12px 0 6px;font-weight:600">
                    Bilan (en millions FCFA)
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label class="form-label">Capitaux propres</label>
                        <input type="number" class="form-input" id="ffEquity"
                               value="${existing.equity || ''}" placeholder="ex: 150000">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Resultat net</label>
                        <input type="number" class="form-input" id="ffNetIncome"
                               value="${existing.net_income || ''}" placeholder="ex: 45000">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Total actif</label>
                        <input type="number" class="form-input" id="ffTotalAssets"
                               value="${existing.total_assets || ''}" placeholder="ex: 800000">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Dettes totales</label>
                        <input type="number" class="form-input" id="ffTotalDebt"
                               value="${existing.total_debt || ''}" placeholder="ex: 300000">
                    </div>
                </div>

                <div style="font-size:0.75em;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;margin:12px 0 6px;font-weight:600">
                    Valorisation (optionnel — calcule automatiquement si vide)
                </div>
                <div class="form-row">
                    <div class="form-group">
                        <label class="form-label">PER constate</label>
                        <input type="number" class="form-input" id="ffPer"
                               value="${existing.per || ''}" placeholder="Auto">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Market Cap (M FCFA)</label>
                        <input type="number" class="form-input" id="ffMarketCap"
                               value="${existing.market_cap || ''}" placeholder="Auto">
                    </div>
                </div>

                <div id="bankFields" style="display:${isBank ? 'block' : 'none'}">
                    <div style="font-size:0.75em;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;margin:12px 0 6px;font-weight:600">
                        🏦 Indicateurs bancaires (M FCFA)
                    </div>
                    <div class="form-row">
                        <div class="form-group">
                            <label class="form-label">PNB</label>
                            <input type="number" class="form-input" id="ffPnb"
                                   value="${existing.pnb || ''}" placeholder="ex: 95000">
                        </div>
                        <div class="form-group">
                            <label class="form-label">Resultat brut exploitation</label>
                            <input type="number" class="form-input" id="ffBankResult"
                                   value="${existing.bank_result || ''}" placeholder="ex: 55000">
                        </div>
                        <div class="form-group">
                            <label class="form-label">Encours credits</label>
                            <input type="number" class="form-input" id="ffCredits"
                                   value="${existing.credit_outstanding || ''}" placeholder="ex: 600000">
                        </div>
                        <div class="form-group">
                            <label class="form-label">Depots clients</label>
                            <input type="number" class="form-input" id="ffDeposits"
                                   value="${existing.client_deposits || ''}" placeholder="ex: 700000">
                        </div>
                    </div>
                </div>
            </div>
        `;

        const footer = `
            <button class="btn btn-secondary" onclick="Components.closeModal()">Annuler</button>
            <button class="btn btn-primary" onclick="AnalysisPage.submitFicheForm()">Enregistrer</button>
        `;
        Components.showModal(`Saisie fondamentaux — ${ticker || 'Nouveau'}`, body, footer);

        // Toggle bank fields
        setTimeout(() => {
            document.getElementById('ffIsBank')?.addEventListener('change', (e) => {
                document.getElementById('bankFields').style.display = e.target.checked ? 'block' : 'none';
            });
        }, 50);
    },

    async submitFicheForm() {
        const g = (id) => document.getElementById(id);
        const num = (id) => { const v = g(id)?.value; return v ? parseFloat(v) : null; };

        const ticker = (g('ffTicker')?.value || '').toUpperCase().trim();
        if (!ticker) { Utils.toast('Saisissez un ticker', 'error'); return; }

        const data = {
            period:             g('ffPeriod')?.value || null,
            is_bank:            g('ffIsBank')?.checked || false,
            eps:                num('ffEps'),
            eps_n2:             num('ffEpsPrev'),
            dividend:           num('ffDividend'),
            shares_outstanding: num('ffShares'),
            equity:             num('ffEquity'),
            net_income:         num('ffNetIncome'),
            total_assets:       num('ffTotalAssets'),
            total_debt:         num('ffTotalDebt'),
            per:                num('ffPer'),
            market_cap:         num('ffMarketCap'),
            pnb:                num('ffPnb'),
            bank_result:        num('ffBankResult'),
            credit_outstanding: num('ffCredits'),
            client_deposits:    num('ffDeposits'),
        };

        try {
            await API.saveFundamentals(ticker, data);
            Components.closeModal();
            this.ficheTicker = ticker;
            Utils.toast(`Fondamentaux de ${ticker} enregistres`, 'success');
            await this.loadFiche();
        } catch (err) {
            Utils.toast('Erreur d\'enregistrement', 'error');
        }
    },

    async deleteFicheData(ticker) {
        try {
            await API.deleteFundamentals(ticker);
            Utils.toast(`Donnees de ${ticker} supprimees`, 'success');
            await this.loadFiche();
        } catch (err) {
            Utils.toast('Erreur', 'error');
        }
    },

    /* ── ALERTS ────────────────────────────────────── */
    async renderAlerts(el) {
        el.innerHTML = Components.loading();
        try {
            this.alerts = await API.getAlerts();
        } catch (err) {
            this.alerts = [];
        }

        let html = `
            <div class="flex-between mb-16">
                <span class="pill ${this.alerts.length > 0 ? 'pill-yellow' : ''}">${this.alerts.length} alerte(s) active(s)</span>
                <button class="btn btn-primary btn-sm" onclick="AnalysisPage.showAlertForm()">
                    Nouvelle alerte
                </button>
            </div>
        `;

        if (!this.alerts.length) {
            html += Components.emptyState('🔔', 'Aucune alerte', 'Configurez des alertes de prix ou de volume');
        } else {
            for (const alert of this.alerts) {
                const icon = alert.type === 'price' ? '💰' : (alert.type === 'volume' ? '📊' : '⚠️');
                html += `
                    <div class="card flex-between">
                        <div>
                            <span style="font-size:1.2em">${icon}</span>
                            <span class="font-mono text-bold">${alert.ticker || ''}</span>
                            <span class="text-muted" style="margin-left:8px">${alert.condition}</span>
                            ${alert.target_value ? `<span class="font-mono"> → ${Utils.formatFCFA(alert.target_value, false)}</span>` : ''}
                            ${alert.message ? `<br><span class="text-muted" style="font-size:0.85em">${alert.message}</span>` : ''}
                        </div>
                        <button class="btn btn-danger btn-sm" onclick="AnalysisPage.deleteAlert(${alert.id})">
                            Supprimer
                        </button>
                    </div>
                `;
            }
        }

        el.innerHTML = html;
    },

    showAlertForm() {
        const body = `
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Type</label>
                    <select class="form-select" id="alertType">
                        <option value="price">Prix</option>
                        <option value="volume">Volume</option>
                        <option value="portfolio">Portefeuille</option>
                        <option value="calendar">Calendrier</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Ticker</label>
                    <input type="text" class="form-input" id="alertTicker"
                           placeholder="SNTS" style="text-transform:uppercase">
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Condition</label>
                    <select class="form-select" id="alertCondition">
                        <option value="above">Prix au-dessus de</option>
                        <option value="below">Prix en-dessous de</option>
                        <option value="vol_spike">Volume > 2x moyenne</option>
                        <option value="pnl_below">P&L < seuil</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Valeur cible</label>
                    <input type="number" class="form-input" id="alertValue" placeholder="30000">
                </div>
            </div>
            <div class="form-group">
                <label class="form-label">Message / Note</label>
                <input type="text" class="form-input" id="alertMessage" placeholder="Rappel...">
            </div>
        `;

        const footer = `
            <button class="btn btn-secondary" onclick="Components.closeModal()">Annuler</button>
            <button class="btn btn-primary" onclick="AnalysisPage.submitAlert()">Creer</button>
        `;

        Components.showModal('Nouvelle alerte', body, footer);
    },

    async submitAlert() {
        const data = {
            type: document.getElementById('alertType').value,
            ticker: document.getElementById('alertTicker').value.toUpperCase() || null,
            condition: document.getElementById('alertCondition').value,
            target_value: document.getElementById('alertValue').value ? parseFloat(document.getElementById('alertValue').value) : null,
            message: document.getElementById('alertMessage').value || null,
        };

        try {
            await API.createAlert(data);
            Components.closeModal();
            Utils.toast('Alerte creee', 'success');
            this.renderContent();
        } catch (err) {
            Utils.toast('Erreur', 'error');
        }
    },

    async deleteAlert(id) {
        try {
            await API.deleteAlert(id);
            Utils.toast('Alerte supprimee', 'success');
            this.renderContent();
        } catch (err) {
            Utils.toast('Erreur', 'error');
        }
    },
};
