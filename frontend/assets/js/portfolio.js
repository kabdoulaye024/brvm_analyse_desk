/* ═══════════════════════════════════════════════════
   Module 2 — Portfolio Management
   Positions, trades, metrics, transaction form
   ═══════════════════════════════════════════════════ */

const PortfolioPage = {
    metrics: null,
    positions: [],
    trades: [],
    flows: [],
    activeTab: 'positions',

    async render() {
        const content = document.getElementById('content');
        content.innerHTML = `
            <div class="page-header">
                <div>
                    <h1 class="page-title">Portefeuille</h1>
                    <div class="page-subtitle">Suivi de positions et performance</div>
                </div>
                <div class="flex gap-8">
                    <button class="btn btn-secondary" onclick="PortfolioPage.showCapitalFlowForm()">
                        Apport capital
                    </button>
                    <button class="btn btn-primary" onclick="PortfolioPage.showTransactionForm()">
                        Nouvelle transaction
                    </button>
                </div>
            </div>

            <!-- KPI metrics -->
            <div class="metrics-row" id="portfolioMetrics">
                ${Components.loading()}
            </div>

            <!-- Tabs -->
            <div class="tabs">
                <button class="tab ${this.activeTab === 'positions' ? 'active' : ''}"
                        onclick="PortfolioPage.switchTab('positions')">Positions ouvertes</button>
                <button class="tab ${this.activeTab === 'trades' ? 'active' : ''}"
                        onclick="PortfolioPage.switchTab('trades')">Journal de trading</button>
                <button class="tab ${this.activeTab === 'performance' ? 'active' : ''}"
                        onclick="PortfolioPage.switchTab('performance')">Performance</button>
                <button class="tab ${this.activeTab === 'flows' ? 'active' : ''}"
                        onclick="PortfolioPage.switchTab('flows')">Apports</button>
            </div>

            <div id="portfolioContent">${Components.loading()}</div>
        `;

        await this.loadData();
    },

    async loadData() {
        try {
            const [metrics, trades, flows] = await Promise.all([
                API.getMetrics(),
                API.getTrades(),
                API.getCapitalFlows(),
            ]);

            this.metrics = metrics;
            this.positions = metrics?.positions || [];
            this.trades = trades || [];
            this.flows = flows || [];

            this.renderMetrics();
            this.renderContent();
        } catch (err) {
            console.error('Portfolio load error:', err);
            Utils.toast('Erreur de chargement du portefeuille', 'error');
        }
    },

    renderMetrics() {
        const el = document.getElementById('portfolioMetrics');
        const m = this.metrics;
        if (!m) {
            el.innerHTML = Components.emptyState('💼', 'Aucune donnee', 'Enregistrez votre premiere transaction');
            return;
        }

        const totalPnlClass = m.total_pnl >= 0 ? 'text-green' : 'text-red';
        const returnClass = m.return_pct >= 0 ? 'text-green' : 'text-red';

        el.innerHTML = `
            ${Components.metricCard('Capital total', Utils.formatFCFA(m.portfolio_value, false),
                { accent: 'blue', sub: `Depart: ${Utils.formatFCFA(m.total_capital, false)}` })}
            ${Components.metricCard('P&L total', Utils.formatFCFA(m.total_pnl, false),
                { accent: m.total_pnl >= 0 ? 'green' : 'red', textClass: totalPnlClass,
                  sub: `Realise: ${Utils.formatFCFA(m.realized_pnl, false)} | Non realise: ${Utils.formatFCFA(m.unrealized_pnl, false)}` })}
            ${Components.metricCard('Rendement', Utils.formatPct(m.return_pct),
                { accent: m.return_pct >= 0 ? 'green' : 'red', textClass: returnClass })}
            ${Components.metricCard('Positions', m.num_positions.toString(),
                { accent: 'yellow', sub: `${m.num_trades} transactions` })}
            ${Components.metricCard('Cash disponible', Utils.formatFCFA(m.cash, false),
                { sub: `Investi: ${Utils.formatFCFA(m.invested, false)}` })}
        `;
    },

    switchTab(tab) {
        this.activeTab = tab;
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelector(`.tab:nth-child(${['positions','trades','performance','flows'].indexOf(tab) + 1})`).classList.add('active');
        this.renderContent();
    },

    async renderContent() {
        const el = document.getElementById('portfolioContent');
        switch (this.activeTab) {
            case 'positions': return this.renderPositions(el);
            case 'trades': return this.renderTrades(el);
            case 'performance': return await this.renderPerformance(el);
            case 'flows': return this.renderFlows(el);
        }
    },

    renderPositions(el) {
        if (!this.positions.length) {
            el.innerHTML = Components.emptyState('💼', 'Aucune position ouverte',
                'Enregistrez un achat pour commencer');
            return;
        }

        const columns = [
            { key: 'ticker', label: 'Ticker', render: (v, r) => {
                let badges = '';
                if (r.time_stop_warning) badges += ' <span class="pill pill-yellow">4s+</span>';
                if (r.stop_loss_warning) badges += ' <span class="pill pill-red">SL</span>';
                return `<span class="font-mono text-bold" style="cursor:pointer" onclick="AnalysisPage && AnalysisPage.openChart('${v}')">${v}</span>${badges}`;
            }},
            { key: 'name', label: 'Société', render: (v) =>
                `<span style="font-size:0.85em">${(v || '').substring(0, 22)}</span>` },
            { key: 'pru', label: 'CMP', align: 'right', mono: true,
              render: (v) => Utils.formatNumber(v) },
            { key: 'quantity', label: 'Qté', align: 'right', mono: true },
            { key: 'current_price', label: 'Cours', align: 'right', mono: true,
              render: (v) => Utils.formatNumber(v) },
            { key: 'cost_basis', label: 'Montant acq.', align: 'right', mono: true,
              render: (v) => Utils.formatFCFA(v, false) },
            { key: 'pnl', label: '+/- Value', align: 'right', mono: true,
              render: (v) => `<span class="${Utils.textColorClass(v)}">${Utils.formatFCFA(v, false)}</span>` },
            { key: 'pnl_pct', label: 'Perf %', align: 'right',
              render: (v) => Components.changeBadge(v) },
            { key: 'weight_pct', label: 'Pond. %', align: 'right', mono: true,
              render: (v) => `${v}%` },
            { key: 'market_value', label: 'Valorisation', align: 'right', mono: true,
              render: (v) => Utils.formatFCFA(v, false) },
        ];

        el.innerHTML = `
            <div class="card" style="margin-bottom:16px">
                <div class="card-title" style="margin-bottom:12px">Répartition du portefeuille
                    <span class="text-muted" style="font-size:0.75em;font-weight:400;margin-left:8px">par secteur et par titre</span>
                </div>
                <div style="display:flex;align-items:center;gap:24px">
                    <canvas id="positionsPieChart" style="flex-shrink:0"></canvas>
                    <div id="positionsPieLegend" style="flex:1;min-width:0;display:flex;flex-direction:column;gap:5px;max-height:360px;overflow-y:auto"></div>
                </div>
            </div>
            ${Components.table('positionsTable', columns, this.positions)}
        `;

        // Draw after DOM is ready
        requestAnimationFrame(() => this._drawPositionsPie());
    },

    _drawPositionsPie() {
        const canvas = document.getElementById('positionsPieChart');
        const legendEl = document.getElementById('positionsPieLegend');
        if (!canvas || !legendEl) return;
        const pos = this.positions;
        const total = pos.reduce((s, p) => s + p.market_value, 0);
        if (total <= 0) return;

        const PALETTE = [
            { base: '#e8633a', light: '#f2956e' },
            { base: '#2980b9', light: '#5dade2' },
            { base: '#27ae60', light: '#58d68d' },
            { base: '#8e44ad', light: '#bb8fce' },
            { base: '#d4ac0d', light: '#f4d03f' },
            { base: '#16a085', light: '#48c9b0' },
            { base: '#c0392b', light: '#e05c4e' },
            { base: '#e74c3c', light: '#f1948a' },
        ];

        // Group by sector
        const sectorMap = {};
        for (const p of pos) {
            const sec = p.sector || 'Autre';
            if (!sectorMap[sec]) sectorMap[sec] = [];
            sectorMap[sec].push(p);
        }
        const sectors = Object.entries(sectorMap)
            .sort((a, b) => b[1].reduce((s,p)=>s+p.market_value,0) - a[1].reduce((s,p)=>s+p.market_value,0));

        const sectorColor = {};
        sectors.forEach(([sec], i) => { sectorColor[sec] = PALETTE[i % PALETTE.length]; });

        // Canvas — square, fits in left column
        const SIZE = Math.min(320, canvas.parentElement.clientWidth * 0.45);
        canvas.width  = SIZE;
        canvas.height = SIZE;
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, SIZE, SIZE);

        const cx = SIZE / 2, cy = SIZE / 2;
        const R2 = cx * 0.90;
        const R1 = R2 * 0.68;
        const R0 = R1 * 0.50;
        const GAP = 0.014;
        let angle = -Math.PI / 2;

        // Inner ring: sectors
        for (const [sec, stocks] of sectors) {
            const secVal = stocks.reduce((s, p) => s + p.market_value, 0);
            const sweep  = (secVal / total) * 2 * Math.PI - GAP;
            const col    = sectorColor[sec];
            ctx.beginPath();
            ctx.moveTo(cx + R0 * Math.cos(angle), cy + R0 * Math.sin(angle));
            ctx.arc(cx, cy, R1, angle, angle + sweep);
            ctx.arc(cx, cy, R0, angle + sweep, angle, true);
            ctx.closePath();
            ctx.fillStyle = col.base;
            ctx.fill();
            ctx.strokeStyle = '#0d1117';
            ctx.lineWidth = 2;
            ctx.stroke();
            angle += (secVal / total) * 2 * Math.PI;
        }

        // Outer ring: individual stocks (no labels)
        angle = -Math.PI / 2;
        for (const [sec, stocks] of sectors) {
            const col = sectorColor[sec];
            for (const p of stocks.sort((a, b) => b.market_value - a.market_value)) {
                const sweep = (p.market_value / total) * 2 * Math.PI - GAP * 0.5;
                ctx.beginPath();
                ctx.moveTo(cx + R1 * Math.cos(angle), cy + R1 * Math.sin(angle));
                ctx.arc(cx, cy, R2, angle, angle + sweep);
                ctx.arc(cx, cy, R1, angle + sweep, angle, true);
                ctx.closePath();
                ctx.fillStyle = col.light;
                ctx.fill();
                ctx.strokeStyle = '#0d1117';
                ctx.lineWidth = 1.5;
                ctx.stroke();
                angle += (p.market_value / total) * 2 * Math.PI;
            }
        }

        // Centre label
        const totalPnl    = pos.reduce((s, p) => s + p.pnl, 0);
        const totalCost   = pos.reduce((s, p) => s + p.cost_basis, 0);
        const retPct      = totalCost > 0 ? totalPnl / totalCost * 100 : 0;
        ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillStyle = '#8b949e';
        ctx.font = `${Math.max(8, SIZE * 0.032)}px Space Mono, monospace`;
        ctx.fillText('VALORISATION', cx, cy - SIZE * 0.044);
        ctx.fillStyle = '#e6edf3';
        ctx.font = `bold ${Math.max(10, SIZE * 0.042)}px Space Mono, monospace`;
        ctx.fillText(Utils.formatFCFA(total, false), cx, cy + SIZE * 0.005);
        ctx.fillStyle = retPct >= 0 ? '#3fb950' : '#f85149';
        ctx.font = `bold ${Math.max(9, SIZE * 0.038)}px Space Mono, monospace`;
        ctx.fillText(`${retPct >= 0 ? '+' : ''}${retPct.toFixed(2)}%`, cx, cy + SIZE * 0.058);

        // ── Legend HTML (right panel) ────────────────────────────────
        let html = '';
        for (const [sec, stocks] of sectors) {
            const col    = sectorColor[sec];
            const secVal = stocks.reduce((s, p) => s + p.market_value, 0);
            const secPct = (secVal / total * 100).toFixed(1);
            // Sector header row
            html += `
                <div style="display:flex;align-items:center;gap:8px;padding:3px 0;border-bottom:1px solid var(--border)">
                    <span style="display:inline-block;width:12px;height:12px;border-radius:2px;background:${col.base};flex-shrink:0"></span>
                    <span style="font-size:0.75em;font-weight:700;color:var(--text-secondary);text-transform:uppercase;letter-spacing:0.5px;flex:1">${sec}</span>
                    <span style="font-size:0.75em;font-family:monospace;color:var(--text-muted)">${secPct}%</span>
                </div>`;
            // Stock rows
            for (const p of stocks.sort((a, b) => b.market_value - a.market_value)) {
                const pct      = (p.market_value / total * 100).toFixed(1);
                const pnlColor = p.pnl_pct >= 0 ? '#3fb950' : '#f85149';
                const sign     = p.pnl_pct >= 0 ? '+' : '';
                html += `
                    <div style="display:flex;align-items:center;gap:8px;padding:2px 0 2px 20px;cursor:pointer"
                         onclick="AnalysisPage && AnalysisPage.openChart('${p.ticker}')">
                        <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${col.light};flex-shrink:0"></span>
                        <span style="font-size:0.82em;font-weight:600;font-family:monospace;flex:0 0 52px">${p.ticker}</span>
                        <span style="font-size:0.78em;color:var(--text-muted);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${(p.name || '').substring(0,20)}</span>
                        <span style="font-size:0.78em;font-family:monospace;color:var(--text-secondary);flex:0 0 36px;text-align:right">${pct}%</span>
                        <span style="font-size:0.78em;font-family:monospace;color:${pnlColor};flex:0 0 52px;text-align:right">${sign}${p.pnl_pct.toFixed(2)}%</span>
                    </div>`;
            }
        }
        legendEl.innerHTML = html;
    },

    renderTrades(el) {
        if (!this.trades.length) {
            el.innerHTML = Components.emptyState('📒', 'Aucun trade enregistre',
                'Les transactions apparaitront ici');
            return;
        }

        const columns = [
            { key: 'type', label: 'Type', render: (v) =>
                `<span class="pill ${v === 'BUY' ? 'pill-green' : 'pill-red'}">${v === 'BUY' ? 'ACHAT' : 'VENTE'}</span>` },
            { key: 'asset_type', label: 'Instrument', render: (v) => {
                const icons = { ACTION: '📈', FCP: '🏦', OBLIGATION: '📋' };
                const labels = { ACTION: 'Action', FCP: 'FCP', OBLIGATION: 'Obligation' };
                const t = v || 'ACTION';
                return `<span style="font-size:0.8em">${icons[t]||'📈'} ${labels[t]||t}</span>`;
            }},
            { key: 'ticker', label: 'Ticker', render: (v, r) => {
                const assetBadge = r.asset_type && r.asset_type !== 'ACTION'
                    ? ` <span class="pill" style="font-size:0.7em;opacity:0.8">${r.asset_type}</span>`
                    : '';
                return `<span class="font-mono text-bold">${v}</span>${assetBadge}`;
            }},
            { key: 'date', label: 'Date', render: (v) => Utils.formatDate(v) },
            { key: 'price', label: 'Prix', align: 'right', mono: true,
              render: (v) => Utils.formatNumber(v) },
            { key: 'quantity', label: 'Qte', align: 'right', mono: true },
            { key: 'fees', label: 'Frais', align: 'right', mono: true,
              render: (v) => Utils.formatNumber(v) },
            { key: 'catalyst', label: 'Catalyseur', render: (v) => v || '—' },
            { key: 'notes', label: 'Notes', render: (v) =>
                v ? `<span style="font-size:0.85em;max-width:150px;overflow:hidden;text-overflow:ellipsis;display:inline-block">${v}</span>` : '—' },
            { key: 'id', label: '', align: 'right',
              render: (v) => `<button class="btn btn-sm"
                style="padding:2px 8px;font-size:0.75em;color:#f85149;border-color:#f85149;opacity:0.7"
                onclick="event.stopPropagation();PortfolioPage.confirmDeleteTrade(${v})"
                title="Supprimer cette transaction">✕</button>` },
        ];

        el.innerHTML = Components.table('tradesTable', columns, this.trades);
    },

    async renderPerformance(el) {
        const m = this.metrics;
        if (!m) {
            el.innerHTML = Components.emptyState('📊', 'Pas de donnees', '');
            return;
        }

        const cashPct = m.portfolio_value > 0 ? (m.cash / m.portfolio_value * 100) : 100;
        const investedPct = 100 - cashPct;

        // Build position P&L bars data
        const positions = this.positions || [];
        const pnlBars = positions
            .map(p => ({ ticker: p.ticker, pnl: p.pnl, pnl_pct: p.pnl_pct }))
            .sort((a, b) => b.pnl - a.pnl);

        el.innerHTML = `
            <!-- Top row: KPI cards -->
            <div class="grid-4" style="margin-bottom:16px">
                <div class="card" style="text-align:center">
                    <div class="text-muted" style="font-size:0.8em;margin-bottom:4px">P&amp;L Realise</div>
                    <div class="font-mono text-bold ${Utils.textColorClass(m.realized_pnl)}" style="font-size:1.2em">
                        ${Utils.formatFCFA(m.realized_pnl, false)}
                    </div>
                </div>
                <div class="card" style="text-align:center">
                    <div class="text-muted" style="font-size:0.8em;margin-bottom:4px">P&amp;L Non realise</div>
                    <div class="font-mono text-bold ${Utils.textColorClass(m.unrealized_pnl)}" style="font-size:1.2em">
                        ${Utils.formatFCFA(m.unrealized_pnl, false)}
                    </div>
                </div>
                <div class="card" style="text-align:center">
                    <div class="text-muted" style="font-size:0.8em;margin-bottom:4px">Rendement global</div>
                    <div class="font-mono text-bold ${Utils.textColorClass(m.return_pct)}" style="font-size:1.2em">
                        ${Utils.formatPct(m.return_pct)}
                    </div>
                </div>
                <div class="card" style="text-align:center">
                    <div class="text-muted" style="font-size:0.8em;margin-bottom:4px">Trades</div>
                    <div class="font-mono text-bold" style="font-size:1.2em">${m.num_trades}</div>
                </div>
            </div>

            <!-- Equity curve -->
            <div class="card" style="margin-bottom:16px">
                <div class="card-title">Courbe de valorisation du portefeuille</div>
                <div id="equityCurveChart" style="height:280px;margin-top:8px">
                    ${Components.loading('Calcul de la courbe...')}
                </div>
            </div>

            <div style="margin-bottom:16px">
                <!-- Allocation graphique étendue -->
                <div class="card">
                    <div class="card-title">Répartition du portefeuille</div>
                    <div style="display:flex;gap:32px;margin-top:16px;flex-wrap:wrap;align-items:flex-start">
                        <!-- Donut principal: classes d'actifs -->
                        <div style="display:flex;flex-direction:column;align-items:center;gap:12px">
                            <canvas id="allocationDonut" width="160" height="160"></canvas>
                            <div style="font-size:0.72em;color:var(--text-muted);text-align:center">Classes d'actifs</div>
                        </div>
                        <!-- Légende + détail secteurs -->
                        <div style="flex:1;min-width:200px">
                            <div id="allocationLegend"></div>
                        </div>
                        <!-- Barres secteurs (Actions uniquement) -->
                        <div style="flex:2;min-width:240px">
                            <div style="font-size:0.78em;font-weight:600;color:var(--text-secondary);margin-bottom:10px;text-transform:uppercase;letter-spacing:0.5px">Répartition sectorielle</div>
                            <div id="sectorBars"></div>
                        </div>
                    </div>
                    <!-- Totaux -->
                    <div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--border);display:flex;gap:24px;flex-wrap:wrap">
                        <div class="flex-between" style="flex:1;min-width:160px">
                            <span class="text-muted" style="font-size:0.85em">Total portefeuille</span>
                            <span class="font-mono text-bold">${Utils.formatFCFA(m.portfolio_value, false)}</span>
                        </div>
                        <div class="flex-between" style="flex:1;min-width:160px">
                            <span class="text-muted" style="font-size:0.85em">Capital de départ</span>
                            <span class="font-mono">${Utils.formatFCFA(m.total_capital, false)}</span>
                        </div>
                        <div class="flex-between" style="flex:1;min-width:160px">
                            <span class="text-muted" style="font-size:0.85em">Rendement</span>
                            <span class="font-mono ${Utils.textColorClass(m.return_pct)}">${Utils.formatPct(m.return_pct)}</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- P&L by position (bar chart) -->
            <div class="card" style="margin-bottom:16px">
                <div class="card-title">P&amp;L par position</div>
                <div id="pnlBarsChart" style="margin-top:12px">
                    ${pnlBars.length === 0
                        ? '<div class="text-muted" style="text-align:center;padding:20px">Aucune position ouverte</div>'
                        : (() => { const _maxAbs = Math.max(...pnlBars.map(x => Math.abs(x.pnl)), 1); return pnlBars.map(p => {
                            const maxAbs = _maxAbs;
                            const barWidth = Math.abs(p.pnl) / maxAbs * 100;
                            const isPos = p.pnl >= 0;
                            return `
                                <div style="margin-bottom:8px">
                                    <div class="flex-between" style="margin-bottom:3px">
                                        <span class="font-mono" style="font-size:0.85em">${p.ticker}</span>
                                        <span class="font-mono ${Utils.textColorClass(p.pnl)}" style="font-size:0.85em">
                                            ${Utils.formatFCFA(p.pnl, false)} (${p.pnl_pct >= 0 ? '+' : ''}${p.pnl_pct.toFixed(1)}%)
                                        </span>
                                    </div>
                                    <div style="background:var(--bg-tertiary);height:6px;border-radius:3px;overflow:hidden">
                                        <div style="width:${barWidth}%;height:100%;background:${isPos ? 'var(--green)' : 'var(--red)'};border-radius:3px;transition:width 0.5s ease"></div>
                                    </div>
                                </div>
                            `;
                        }).join(''); })()}
                </div>
            </div>
        `;

        // Draw allocation full
        this._drawAllocationFull(m);

        // Load and render equity curve
        this._renderEquityCurve();
    },

    _drawAllocationFull(m) {
        const canvas = document.getElementById('allocationDonut');
        const legendEl = document.getElementById('allocationLegend');
        const sectorsEl = document.getElementById('sectorBars');
        if (!canvas) return;

        const total = m.portfolio_value || 1;

        // Asset class colors
        const COLORS = {
            ACTION:     '#238636',
            FCP:        '#1f6feb',
            OBLIGATION: '#9e6a03',
            CASH:       '#30363d',
        };
        const LABELS = {
            ACTION:     '📈 Actions BRVM',
            FCP:        '🏦 FCP / OPCVM',
            OBLIGATION: '📋 Obligations',
            CASH:       '💵 Cash',
        };
        // Sector palette (rotate through these for sector breakdown)
        const SECTOR_COLORS = [
            '#2ea043','#388bfd','#a371f7','#d29922',
            '#3fb950','#58a6ff','#bc8cff','#e3b341',
        ];

        // Build segments from allocation
        const allocation = m.allocation || [];
        const segments = allocation.map(a => ({
            type: a.type,
            value: a.value,
            pct: a.value / total,
            color: COLORS[a.type] || '#555',
            sectors: a.sectors || [],
        }));

        // Draw donut
        const ctx = canvas.getContext('2d');
        const cx = 80, cy = 80, r = 68, lw = 22;
        ctx.clearRect(0, 0, 160, 160);
        let angle = -Math.PI / 2;
        const gap = 0.03;

        segments.forEach(seg => {
            if (seg.pct <= 0) return;
            const sweep = seg.pct * 2 * Math.PI - gap;
            ctx.beginPath();
            ctx.arc(cx, cy, r, angle, angle + sweep);
            ctx.strokeStyle = seg.color;
            ctx.lineWidth = lw;
            ctx.lineCap = 'butt';
            ctx.stroke();
            angle += seg.pct * 2 * Math.PI;
        });

        // Center: return %
        const ret = m.return_pct || 0;
        ctx.fillStyle = ret >= 0 ? '#3fb950' : '#f85149';
        ctx.font = 'bold 14px Space Mono, monospace';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText((ret >= 0 ? '+' : '') + ret.toFixed(1) + '%', cx, cy - 8);
        ctx.fillStyle = '#8b949e';
        ctx.font = '10px Space Mono, monospace';
        ctx.fillText('perf.', cx, cy + 10);

        // Legend
        if (legendEl) {
            legendEl.innerHTML = segments.filter(s => s.pct > 0).map(seg => `
                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;gap:8px">
                    <span style="display:flex;align-items:center;gap:8px">
                        <span style="width:12px;height:12px;border-radius:3px;background:${seg.color};flex-shrink:0;display:inline-block"></span>
                        <span style="font-size:0.85em">${LABELS[seg.type] || seg.type}</span>
                    </span>
                    <span style="font-family:monospace;font-size:0.85em;white-space:nowrap">
                        ${Utils.formatFCFA(seg.value, false)}
                        <span style="color:var(--text-muted);margin-left:4px">${(seg.pct*100).toFixed(1)}%</span>
                    </span>
                </div>
            `).join('');
        }

        // Sector bars (for ACTION class only)
        if (sectorsEl) {
            const actionSeg = segments.find(s => s.type === 'ACTION');
            if (actionSeg && actionSeg.sectors.length > 0) {
                const maxSec = Math.max(...actionSeg.sectors.map(s => s.value), 1);
                sectorsEl.innerHTML = actionSeg.sectors.map((sec, i) => {
                    const barW = Math.round(sec.value / maxSec * 100);
                    const pct  = (sec.value / total * 100).toFixed(1);
                    const col  = SECTOR_COLORS[i % SECTOR_COLORS.length];
                    return `
                        <div style="margin-bottom:10px">
                            <div style="display:flex;justify-content:space-between;margin-bottom:3px;font-size:0.82em">
                                <span style="color:var(--text-secondary)">${sec.sector}</span>
                                <span style="font-family:monospace;color:var(--text-muted)">${pct}%</span>
                            </div>
                            <div style="background:var(--bg-tertiary);height:7px;border-radius:4px;overflow:hidden">
                                <div style="width:${barW}%;height:100%;background:${col};border-radius:4px;transition:width 0.6s ease"></div>
                            </div>
                        </div>`;
                }).join('');
            } else if (!actionSeg || actionSeg.sectors.length === 0) {
                sectorsEl.innerHTML = '<div class="text-muted" style="font-size:0.82em">Aucune action en portefeuille</div>';
            }
        }
    },

    async _renderEquityCurve() {
        const chartEl = document.getElementById('equityCurveChart');
        if (!chartEl) return;

        try {
            const data = await API.getEquityCurve();

            if (!data || !data.dates || data.dates.length < 2) {
                chartEl.innerHTML = `
                    <div style="display:flex;align-items:center;justify-content:center;height:100%;flex-direction:column;gap:8px">
                        <div style="font-size:1.5em">📈</div>
                        <div class="text-muted">Pas encore de donnees pour la courbe</div>
                        <div class="text-muted" style="font-size:0.8em">Enregistrez des transactions et des cours historiques pour voir la courbe</div>
                    </div>`;
                return;
            }

            chartEl.innerHTML = '';

            if (typeof LightweightCharts === 'undefined') {
                chartEl.innerHTML = '<div class="text-muted" style="text-align:center;padding:40px">TradingView Charts non charge</div>';
                return;
            }

            const chart = LightweightCharts.createChart(chartEl, {
                width: chartEl.clientWidth || 600,
                height: 270,
                layout: {
                    background: { color: '#0d1117' },
                    textColor: '#8b949e',
                    fontFamily: "'Space Mono', monospace",
                },
                grid: {
                    vertLines: { color: '#1e2633' },
                    horzLines: { color: '#1e2633' },
                },
                rightPriceScale: { borderColor: '#1e2633' },
                timeScale: { borderColor: '#1e2633', timeVisible: false },
                crosshair: { mode: 1 },
            });

            // Portfolio value area series
            const areaSeries = chart.addAreaSeries({
                lineColor: '#58a6ff',
                topColor: 'rgba(88, 166, 255, 0.25)',
                bottomColor: 'rgba(88, 166, 255, 0.02)',
                lineWidth: 2,
                title: 'Valeur portefeuille',
            });

            const areaData = data.dates.map((d, i) => ({
                time: d,
                value: data.values[i],
            })).filter(p => p.value > 0);
            areaSeries.setData(areaData);

            // Capital invested line
            const investedSeries = chart.addLineSeries({
                color: 'rgba(255, 255, 255, 0.25)',
                lineWidth: 1,
                lineStyle: 2,  // dashed
                title: 'Capital investi',
            });
            const investedData = data.dates.map((d, i) => ({
                time: d,
                value: data.invested[i],
            })).filter(p => p.value > 0);
            investedSeries.setData(investedData);

            chart.timeScale().fitContent();

            // Resize handler
            const resizeObs = new ResizeObserver(() => {
                chart.applyOptions({ width: chartEl.clientWidth });
            });
            resizeObs.observe(chartEl);

        } catch (err) {
            chartEl.innerHTML = `<div class="text-muted" style="text-align:center;padding:40px">Erreur: ${err.message}</div>`;
        }
    },

    renderFlows(el) {
        if (!this.flows.length) {
            el.innerHTML = Components.emptyState('💰', 'Aucun apport enregistre',
                'Capital initial: 100 000 FCFA');
            return;
        }

        const columns = [
            { key: 'date', label: 'Date', render: (v) => Utils.formatDate(v) },
            { key: 'amount', label: 'Montant', align: 'right', mono: true,
              render: (v) => `<span class="text-green">${Utils.formatFCFA(v, false)}</span>` },
            { key: 'notes', label: 'Notes', render: (v) => v || '—' },
        ];

        const total = this.flows.reduce((s, f) => s + f.amount, 0);

        el.innerHTML = `
            <div class="card" style="margin-bottom:16px">
                <div class="flex-between">
                    <span class="card-title">Total des apports</span>
                    <span class="font-mono text-bold text-green">${Utils.formatFCFA(total)}</span>
                </div>
            </div>
            ${Components.table('flowsTable', columns, this.flows)}
        `;
    },

    confirmDeleteTrade(id) {
        if (!confirm(`Supprimer la transaction #${id} ? Cette action est irréversible.`)) return;
        this.deleteTrade(id);
    },

    async deleteTrade(id) {
        try {
            await API.delete(`/portfolio/transaction/${id}`);
            Utils.toast('Transaction supprimée', 'success');
            await this.loadData();
            this.renderContent();
        } catch (err) {
            Utils.toast('Erreur lors de la suppression', 'error');
        }
    },

    onAssetTypeChange() {
        const type = document.getElementById('txnAssetType')?.value;
        const label = document.getElementById('txnTickerLabel');
        const input = document.getElementById('txnTicker');
        const datalist = document.getElementById('tickerList');
        if (type === 'ACTION') {
            if (label) label.textContent = 'Ticker';
            if (input) { input.placeholder = 'ex: SNTS'; input.style.textTransform = 'uppercase'; }
            if (datalist) datalist.style.display = '';
        } else if (type === 'FCP') {
            if (label) label.textContent = 'Code FCP';
            if (input) { input.placeholder = 'ex: SICAV-CI'; input.style.textTransform = 'uppercase'; }
        } else if (type === 'OBLIGATION') {
            if (label) label.textContent = 'Code / Libellé';
            if (input) { input.placeholder = 'ex: TPCI 6.5% 2028'; input.style.textTransform = 'none'; }
        }
    },

    showTransactionForm() {
        const body = `
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Type</label>
                    <select class="form-select" id="txnType">
                        <option value="BUY">ACHAT</option>
                        <option value="SELL">VENTE</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Instrument</label>
                    <select class="form-select" id="txnAssetType" onchange="PortfolioPage.onAssetTypeChange()">
                        <option value="ACTION">Action BRVM</option>
                        <option value="FCP">FCP / OPCVM</option>
                        <option value="OBLIGATION">Obligation</option>
                    </select>
                </div>
                <div class="form-group" id="txnTickerGroup">
                    <label class="form-label" id="txnTickerLabel">Ticker</label>
                    <input type="text" class="form-input" id="txnTicker" placeholder="ex: SNTS"
                           style="text-transform:uppercase" list="tickerList">
                    <datalist id="tickerList">
                        ${Object.keys(window._stocksList || {}).map(tk =>
                            `<option value="${tk}">`).join('')}
                    </datalist>
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Date</label>
                    <input type="date" class="form-input" id="txnDate"
                           value="${new Date().toISOString().split('T')[0]}">
                </div>
                <div class="form-group">
                    <label class="form-label">Prix unitaire (FCFA)</label>
                    <input type="number" class="form-input" id="txnPrice" min="1">
                </div>
                <div class="form-group">
                    <label class="form-label">Quantite</label>
                    <input type="number" class="form-input" id="txnQty" min="1">
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Frais (auto ~1.8%)</label>
                    <input type="number" class="form-input" id="txnFees" placeholder="Auto">
                </div>
                <div class="form-group">
                    <label class="form-label">Catalyseur</label>
                    <input type="text" class="form-input" id="txnCatalyst"
                           placeholder="Resultats T3, Momentum...">
                </div>
            </div>
            <div class="form-group">
                <label class="form-label">Notes</label>
                <textarea class="form-textarea" id="txnNotes" rows="2"
                          placeholder="Notes personnelles..."></textarea>
            </div>
        `;

        const footer = `
            <button class="btn btn-secondary" onclick="Components.closeModal()">Annuler</button>
            <button class="btn btn-primary" onclick="PortfolioPage.submitTransaction()">Enregistrer</button>
        `;

        Components.showModal('Nouvelle transaction', body, footer);
    },

    async submitTransaction() {
        const data = {
            type: document.getElementById('txnType').value,
            asset_type: document.getElementById('txnAssetType')?.value || 'ACTION',
            ticker: document.getElementById('txnTicker').value.toUpperCase(),
            date: document.getElementById('txnDate').value,
            price: parseFloat(document.getElementById('txnPrice').value),
            quantity: parseInt(document.getElementById('txnQty').value),
            fees: document.getElementById('txnFees').value ? parseFloat(document.getElementById('txnFees').value) : null,
            catalyst: document.getElementById('txnCatalyst').value || null,
            notes: document.getElementById('txnNotes').value || null,
        };

        if (!data.ticker || !data.price || !data.quantity) {
            Utils.toast('Remplissez les champs obligatoires', 'error');
            return;
        }

        try {
            await API.addTransaction(data);
            Components.closeModal();
            Utils.toast(`${data.type === 'BUY' ? 'Achat' : 'Vente'} ${data.ticker} enregistre`, 'success');
            await this.loadData();
        } catch (err) {
            Utils.toast('Erreur d\'enregistrement', 'error');
        }
    },

    showCapitalFlowForm() {
        const body = `
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Date</label>
                    <input type="date" class="form-input" id="flowDate"
                           value="${new Date().toISOString().split('T')[0]}">
                </div>
                <div class="form-group">
                    <label class="form-label">Montant (FCFA)</label>
                    <input type="number" class="form-input" id="flowAmount" min="1"
                           placeholder="ex: 100000">
                </div>
            </div>
            <div class="form-group">
                <label class="form-label">Notes</label>
                <input type="text" class="form-input" id="flowNotes" placeholder="Apport mensuel...">
            </div>
        `;

        const footer = `
            <button class="btn btn-secondary" onclick="Components.closeModal()">Annuler</button>
            <button class="btn btn-primary" onclick="PortfolioPage.submitCapitalFlow()">Enregistrer</button>
        `;

        Components.showModal('Apport de capital', body, footer);
    },

    async submitCapitalFlow() {
        const data = {
            date: document.getElementById('flowDate').value,
            amount: parseFloat(document.getElementById('flowAmount').value),
            notes: document.getElementById('flowNotes').value || null,
        };

        if (!data.amount) {
            Utils.toast('Saisissez un montant', 'error');
            return;
        }

        try {
            await API.addCapitalFlow(data);
            Components.closeModal();
            Utils.toast('Apport enregistre', 'success');
            await this.loadData();
        } catch (err) {
            Utils.toast('Erreur d\'enregistrement', 'error');
        }
    },
};
