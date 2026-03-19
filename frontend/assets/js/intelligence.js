/* ═══════════════════════════════════════════════════
   Module 4 — Market Intelligence
   News feed, Corporate calendar, Watchlist
   ═══════════════════════════════════════════════════ */

const IntelligencePage = {
    activeTab: 'semaine',
    watchlist: [],
    news: [],
    events: [],
    _weeklyTimer: null,

    async render() {
        const content = document.getElementById('content');
        content.innerHTML = `
            <div class="page-header">
                <div>
                    <h1 class="page-title">Intelligence</h1>
                    <div class="page-subtitle">Watchlist, news et evenements</div>
                </div>
            </div>

            <div class="tabs">
                <button class="tab ${this.activeTab === 'alertes' ? 'active' : ''}"
                        onclick="IntelligencePage.switchTab('alertes')">Alertes</button>
                <button class="tab ${this.activeTab === 'semaine' ? 'active' : ''}"
                        onclick="IntelligencePage.switchTab('semaine')">Semaine</button>
                <button class="tab ${this.activeTab === 'news' ? 'active' : ''}"
                        onclick="IntelligencePage.switchTab('news')">Actualites</button>
                <button class="tab ${this.activeTab === 'calendar' ? 'active' : ''}"
                        onclick="IntelligencePage.switchTab('calendar')">Calendrier</button>
            </div>

            <div id="intelligenceContent"></div>
        `;

        this.renderContent();
    },

    switchTab(tab) {
        this.activeTab = tab;
        // Stop weekly auto-refresh if leaving that tab
        if (tab !== 'semaine' && this._weeklyTimer) {
            clearInterval(this._weeklyTimer);
            this._weeklyTimer = null;
        }
        document.querySelectorAll('.tab').forEach((t, i) => {
            t.classList.toggle('active', ['alertes', 'semaine', 'news', 'calendar'][i] === tab);
        });
        this.renderContent();
    },

    stopTimers() {
        if (this._weeklyTimer) { clearInterval(this._weeklyTimer); this._weeklyTimer = null; }
    },

    async renderContent() {
        const el = document.getElementById('intelligenceContent');
        switch (this.activeTab) {
            case 'alertes':   return await this.renderAlertes(el);
            case 'semaine':   return await this.renderWeekly(el);
            case 'news':      return await this.renderNews(el);
            case 'calendar':  return await this.renderCalendar(el);
        }
    },

    /* ── WATCHLIST ──────────────────────────────────── */
    async renderWatchlist(el) {
        el.innerHTML = Components.loading();
        try {
            this.watchlist = await API.getWatchlist();
        } catch (err) {
            this.watchlist = [];
        }

        // Get latest quotes for enrichment
        let quotesMap = {};
        try {
            const quotes = await API.getQuotes();
            for (const q of (quotes || [])) {
                quotesMap[q.ticker] = q;
            }
        } catch (err) {}

        let html = `
            <div class="flex-between mb-16">
                <span class="pill">${this.watchlist.length} titre(s) surveille(s)</span>
                <button class="btn btn-primary btn-sm" onclick="IntelligencePage.showAddWatchlistForm()">
                    Ajouter un titre
                </button>
            </div>
        `;

        if (!this.watchlist.length) {
            html += Components.emptyState('👁️', 'Watchlist vide',
                'Ajoutez des titres a surveiller');
            el.innerHTML = html;
            return;
        }

        // Group by priority
        const groups = { 'Hot': [], 'Warm': [], 'Cold': [] };
        for (const item of this.watchlist) {
            const group = groups[item.priority] || groups['Warm'];
            group.push(item);
        }

        for (const [priority, items] of Object.entries(groups)) {
            if (!items.length) continue;
            const pClass = priority === 'Hot' ? 'priority-hot' :
                           priority === 'Warm' ? 'priority-warm' : 'priority-cold';
            const pIcon = priority === 'Hot' ? '🔥' : priority === 'Warm' ? '👀' : '❄️';

            html += `<div class="section-title ${pClass}">${pIcon} ${priority}</div>`;

            for (const item of items) {
                const quote = quotesMap[item.ticker] || {};
                const changeCls = Utils.changeClass(quote.change_pct);

                html += `
                    <div class="card" style="margin-bottom:8px">
                        <div class="flex-between">
                            <div style="flex:1">
                                <div class="flex-center gap-8">
                                    <span class="font-mono text-bold" style="font-size:1.1em;cursor:pointer"
                                          onclick="App.showChart('${item.ticker}')">${item.ticker}</span>
                                    <span class="text-muted">${item.name || ''}</span>
                                    <span class="pill">${Utils.sectorEmoji(item.sector)} ${(item.sector || '').split(' ')[0]}</span>
                                </div>
                                <div class="flex-center gap-16" style="margin-top:6px">
                                    ${quote.price ? `
                                        <span class="font-mono">${Utils.formatFCFA(quote.price, false)}</span>
                                        <span class="change-badge ${changeCls}">${Utils.formatPct(quote.change_pct)}</span>
                                        <span class="text-muted font-mono" style="font-size:0.8em">Vol: ${Utils.formatVolume(quote.volume)}</span>
                                    ` : '<span class="text-muted">Pas de cours</span>'}
                                </div>
                                ${item.notes ? `<div class="text-muted" style="margin-top:6px;font-size:0.85em;font-style:italic">${item.notes}</div>` : ''}
                            </div>
                            <div class="flex gap-8">
                                <button class="btn btn-secondary btn-sm"
                                        onclick="IntelligencePage.editWatchlistItem('${item.ticker}', '${item.priority}', '${(item.notes || '').replace(/'/g, "\\'")}')">
                                    Editer
                                </button>
                                <button class="btn btn-danger btn-sm"
                                        onclick="IntelligencePage.removeFromWatchlist('${item.ticker}')">
                                    ×
                                </button>
                            </div>
                        </div>
                    </div>
                `;
            }
        }

        el.innerHTML = html;
    },

    async renderAlertes(el) {
        // Redirect to Analysis alerts tab
        el.innerHTML = `
            <div class="card" style="margin-bottom:12px">
                <div class="card-title">🔔 Alertes de prix</div>
                <p class="text-muted" style="font-size:0.9em;margin-bottom:16px">
                    Gérez vos alertes de prix directement depuis la section Analyse → onglet Alertes.
                </p>
                <button class="btn btn-primary" onclick="App.navigate('analysis'); setTimeout(() => AnalysisPage.switchTab('alertes'), 200)">
                    Ouvrir les Alertes →
                </button>
            </div>
            <div id="alertesPreview">${Components.loading('Chargement des alertes...')}</div>
        `;
        // Load alerts preview
        try {
            const alerts = await API.getAlerts();
            const preview = document.getElementById('alertesPreview');
            if (!preview) return;
            if (!alerts || !alerts.length) {
                preview.innerHTML = Components.emptyState('🔔', 'Aucune alerte', 'Ajoutez des alertes depuis la section Analyse');
                return;
            }
            const statusIcon = (s) => s === 'TRIGGERED' ? '🔥' : s === 'ACTIVE' ? '✅' : '⏸️';
            preview.innerHTML = `
                <div class="card" style="padding:0;overflow:hidden">
                    <table class="data-table" style="margin:0">
                        <thead><tr>
                            <th>Ticker</th><th>Condition</th><th>Cible</th><th>Statut</th><th>Créée le</th>
                        </tr></thead>
                        <tbody>
                            ${alerts.map(a => `
                                <tr>
                                    <td><span class="font-mono text-bold">${a.ticker}</span></td>
                                    <td style="font-size:0.85em">${a.condition || ''}</td>
                                    <td class="font-mono" style="text-align:right">${a.target_value ? Utils.formatNumber(a.target_value) : '—'}</td>
                                    <td>${statusIcon(a.status)} <span class="pill" style="font-size:0.75em">${a.status}</span></td>
                                    <td style="font-size:0.8em;color:var(--text-muted)">${Utils.formatDate(a.created_at)}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>`;
        } catch(e) {
            document.getElementById('alertesPreview').innerHTML = Components.emptyState('⚠️','Erreur','Impossible de charger les alertes');
        }
    },

    showAddWatchlistForm() {
        const body = `
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Ticker</label>
                    <input type="text" class="form-input" id="wlTicker"
                           placeholder="SNTS" style="text-transform:uppercase"
                           list="tickerListWL">
                    <datalist id="tickerListWL">
                        ${Object.keys(window._stocksList || {}).map(tk =>
                            `<option value="${tk}">`).join('')}
                    </datalist>
                </div>
                <div class="form-group">
                    <label class="form-label">Priorite</label>
                    <select class="form-select" id="wlPriority">
                        <option value="Hot">🔥 Hot</option>
                        <option value="Warm" selected>👀 Warm</option>
                        <option value="Cold">❄️ Cold</option>
                    </select>
                </div>
            </div>
            <div class="form-group">
                <label class="form-label">Notes</label>
                <textarea class="form-textarea" id="wlNotes" rows="3"
                          placeholder="Attendre resultats T4 avant entree..."></textarea>
            </div>
        `;
        const footer = `
            <button class="btn btn-secondary" onclick="Components.closeModal()">Annuler</button>
            <button class="btn btn-primary" onclick="IntelligencePage.submitWatchlist()">Ajouter</button>
        `;
        Components.showModal('Ajouter a la watchlist', body, footer);
    },

    editWatchlistItem(ticker, priority, notes) {
        this.showAddWatchlistForm();
        setTimeout(() => {
            const tkEl = document.getElementById('wlTicker');
            const prEl = document.getElementById('wlPriority');
            const ntEl = document.getElementById('wlNotes');
            if (tkEl) tkEl.value = ticker;
            if (prEl) prEl.value = priority;
            if (ntEl) ntEl.value = notes;
        }, 100);
    },

    async submitWatchlist() {
        const data = {
            ticker: (document.getElementById('wlTicker')?.value || '').toUpperCase(),
            priority: document.getElementById('wlPriority')?.value || 'Warm',
            notes: document.getElementById('wlNotes')?.value || null,
        };
        if (!data.ticker) {
            Utils.toast('Saisissez un ticker', 'error');
            return;
        }
        try {
            await API.addToWatchlist(data);
            Components.closeModal();
            Utils.toast(`${data.ticker} ajoute a la watchlist`, 'success');
            this.renderContent();
        } catch (err) {
            Utils.toast('Erreur', 'error');
        }
    },

    async removeFromWatchlist(ticker) {
        try {
            await API.removeFromWatchlist(ticker);
            Utils.toast(`${ticker} retire de la watchlist`, 'success');
            this.renderContent();
        } catch (err) {
            Utils.toast('Erreur', 'error');
        }
    },

    /* ── SEMAINE ────────────────────────────────────── */
    async renderWeekly(el) {
        el.innerHTML = Components.loading('Chargement du resume de la semaine...');
        let data;
        try {
            data = await API.getWeeklySummary();
        } catch (err) {
            el.innerHTML = Components.emptyState('⚠️', 'Erreur', 'Impossible de charger le resume hebdomadaire');
            return;
        }

        const stats = data.stats || {};
        const now = new Date().toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' });

        const pct = (n, total) => total > 0 ? Math.round(n / total * 100) : 0;
        const posW = pct(stats.positive, stats.total);
        const negW = pct(stats.negative, stats.total);

        const moverCard = (item, showVar = true) => {
            const ref = window._stocksList?.[item.ticker];
            const varW = item.var_week;
            const cls  = varW > 0 ? 'change-up' : varW < 0 ? 'change-down' : 'change-neutral';
            return `
                <div class="mover-item" style="cursor:pointer" onclick="App.showChart('${item.ticker}')">
                    <div>
                        <div class="mover-ticker">${item.ticker}</div>
                        <div class="mover-volume" style="font-size:0.75em">${Utils.formatVolume(item.volume)} titres</div>
                    </div>
                    <div class="change-badge ${cls}">
                        ${showVar && varW != null ? Utils.formatPct(varW) : Utils.formatPct(item.change_pct)}
                    </div>
                </div>`;
        };

        const sectorBar = (s) => {
            const cls = s.avg_var > 0 ? '#3fb950' : s.avg_var < 0 ? '#f85149' : '#8b949e';
            const w = Math.min(Math.abs(s.avg_var) * 10, 100);
            return `
                <div style="margin-bottom:8px">
                    <div class="flex-between" style="font-size:0.82em;margin-bottom:3px">
                        <span>${Utils.sectorEmoji(s.sector)} ${s.sector.split(' ')[0]}</span>
                        <span class="font-mono" style="color:${cls}">${s.avg_var > 0 ? '+' : ''}${s.avg_var}%</span>
                    </div>
                    <div style="background:var(--bg-tertiary);height:5px;border-radius:3px;overflow:hidden">
                        <div style="width:${w}%;height:100%;background:${cls};border-radius:3px;margin-left:${s.avg_var < 0 ? 'auto' : '0'}"></div>
                    </div>
                </div>`;
        };

        el.innerHTML = `
            <!-- Header -->
            <div class="flex-between mb-16">
                <div>
                    <span class="pill">Séance du ${Utils.formatDate(data.date)}</span>
                    <span class="text-muted" style="font-size:0.78em;margin-left:8px">MAJ ${now} · auto-rafraîchi toutes les 5 min</span>
                </div>
                <button class="btn btn-primary btn-sm" onclick="IntelligencePage.renderWeekly(document.getElementById('intelligenceContent'))">↺ Actualiser</button>
            </div>

            <!-- Market breadth -->
            <div class="card" style="margin-bottom:12px">
                <div class="card-title">Breadth de la semaine — ${stats.total} titres</div>
                <div style="display:flex;gap:4px;height:12px;border-radius:6px;overflow:hidden;margin:8px 0">
                    <div style="width:${posW}%;background:#3fb950;transition:width 0.8s"></div>
                    <div style="width:${pct(stats.neutral, stats.total)}%;background:#8b949e"></div>
                    <div style="width:${negW}%;background:#f85149"></div>
                </div>
                <div style="display:flex;gap:24px;font-size:0.82em">
                    <span style="color:#3fb950">▲ ${stats.positive} en hausse (${posW}%)</span>
                    <span style="color:#8b949e">→ ${stats.neutral} stables</span>
                    <span style="color:#f85149">▼ ${stats.negative} en baisse (${negW}%)</span>
                </div>
            </div>

            <!-- Movers grid -->
            <div class="movers-grid" style="margin-bottom:12px">
                <div class="card">
                    <div class="card-title">🚀 Top hausses semaine</div>
                    ${(data.gainers || []).slice(0, 8).map(i => moverCard(i)).join('')}
                </div>
                <div class="card">
                    <div class="card-title">📉 Top baisses semaine</div>
                    ${(data.losers || []).slice(0, 8).map(i => moverCard(i)).join('')}
                </div>
                <div class="card">
                    <div class="card-title">🔥 Plus actifs (volume)</div>
                    ${(data.most_active || []).slice(0, 8).map(i => moverCard(i, false)).join('')}
                </div>
            </div>

            <!-- Sector performance -->
            <div class="card" style="margin-bottom:12px">
                <div class="card-title">Performance sectorielle (variation jour)</div>
                <div style="padding:8px 0">
                    ${(data.sectors || []).map(s => sectorBar(s)).join('')}
                </div>
            </div>

            <!-- Full table toggle -->
            <div class="card">
                <div class="card-title flex-between">
                    <span>Tous les titres — variation jour</span>
                    <span class="text-muted" style="font-size:0.75em">${data.stocks?.length || 0} titres</span>
                </div>
                <div style="max-height:320px;overflow-y:auto">
                    <table class="data-table" style="margin:0">
                        <thead><tr>
                            <th>Ticker</th>
                            <th>Societe</th>
                            <th style="text-align:right">Cours</th>
                            <th style="text-align:right">Var jour</th>
                            <th style="text-align:right">Var semaine</th>
                            <th style="text-align:right">Volume</th>
                        </tr></thead>
                        <tbody>
                            ${(data.stocks || []).sort((a, b) => (b.var_week ?? -99) - (a.var_week ?? -99)).map(s => `
                                <tr data-ticker="${s.ticker}" style="cursor:pointer" onclick="App.showChart('${s.ticker}')">
                                    <td><span class="font-mono text-bold">${s.ticker}</span></td>
                                    <td style="font-size:0.82em">${s.name || ''}</td>
                                    <td style="text-align:right;font-family:monospace">${Utils.formatFCFA(s.price, false)}</td>
                                    <td style="text-align:right">${Components.changeBadge(s.change_pct)}</td>
                                    <td style="text-align:right">${s.var_week != null ? Components.changeBadge(s.var_week) : '<span class="text-muted">—</span>'}</td>
                                    <td style="text-align:right;font-family:monospace;font-size:0.82em">${Utils.formatVolume(s.volume)}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        // Auto-refresh every 5 minutes (stop previous timer first)
        if (this._weeklyTimer) clearInterval(this._weeklyTimer);
        this._weeklyTimer = setInterval(() => {
            if (this.activeTab === 'semaine') {
                this.renderWeekly(document.getElementById('intelligenceContent'));
            }
        }, 5 * 60 * 1000);
    },

    /* ── NEWS ───────────────────────────────────────── */
    async renderNews(el) {
        el.innerHTML = Components.loading('Chargement des actualites...');

        try {
            const news = await API.getNews();
            this.news = news || [];
        } catch (err) {
            this.news = [];
        }

        const header = `
            <div class="flex-between mb-16">
                <span class="pill">${this.news.length} article(s)</span>
                <div class="flex gap-8">
                    <a href="https://www.richbourse.com/common/actualite/index" target="_blank"
                       class="btn btn-secondary btn-sm">Richbourse →</a>
                    <a href="https://www.sikafinance.com/marches" target="_blank"
                       class="btn btn-secondary btn-sm">Sikafinance →</a>
                    <button class="btn btn-primary btn-sm"
                            onclick="IntelligencePage.refreshNews()">Actualiser</button>
                </div>
            </div>`;

        if (!this.news.length) {
            el.innerHTML = header + Components.emptyState('📰', 'Aucune actualite',
                'Cliquez sur Actualiser pour charger les dernieres nouvelles');
            return;
        }

        const articles = this.news.map(a => `
            <div class="card" style="margin-bottom:8px">
                <div style="display:flex;gap:12px;align-items:flex-start">
                    <div style="font-size:1.4em;flex-shrink:0">📰</div>
                    <div style="flex:1;min-width:0">
                        <div style="font-weight:600;margin-bottom:4px;line-height:1.4">
                            ${a.url
                                ? `<a href="${a.url}" target="_blank" style="color:var(--text-primary);text-decoration:none"
                                      onmouseover="this.style.color='var(--accent)'"
                                      onmouseout="this.style.color='var(--text-primary)'">${Utils.escapeHtml(a.title)}</a>`
                                : Utils.escapeHtml(a.title)}
                        </div>
                        ${a.summary ? `<div class="text-muted" style="font-size:0.82em;margin-bottom:4px">${Utils.escapeHtml(a.summary).substring(0, 180)}${a.summary.length > 180 ? '...' : ''}</div>` : ''}
                        <div style="font-size:0.75em;color:var(--text-muted)">
                            <span class="pill" style="font-size:0.9em">${a.source || 'richbourse'}</span>
                            ${a.published_at ? `<span style="margin-left:8px">${a.published_at}</span>` : ''}
                            ${a.fetched_at ? `<span style="margin-left:8px">Collecte: ${Utils.formatDate(a.fetched_at)}</span>` : ''}
                        </div>
                    </div>
                </div>
            </div>
        `).join('');

        el.innerHTML = header + articles;
    },

    async refreshNews() {
        const el = document.getElementById('intelligenceContent');
        el.innerHTML = Components.loading('Mise a jour des actualites...');
        try {
            this.news = await API.getNews(true);
        } catch (err) {
            Utils.toast('Erreur lors du chargement des actualites', 'error');
            this.news = [];
        }
        await this.renderNews(el);
    },

    /* ── CALENDAR ───────────────────────────────────── */
    async renderCalendar(el) {
        el.innerHTML = Components.loading('Chargement du calendrier...');

        try {
            const events = await API.getCalendar();
            this.events = events || [];
        } catch (err) {
            this.events = [];
        }

        const header = `
            <div class="flex-between mb-16">
                <span class="pill">${this.events.length} evenement(s)</span>
                <div class="flex gap-8">
                    <a href="https://www.richbourse.com/common/dividende/index" target="_blank"
                       class="btn btn-secondary btn-sm">Dividendes Richbourse →</a>
                    <button class="btn btn-primary btn-sm"
                            onclick="IntelligencePage.refreshCalendar()">Actualiser</button>
                </div>
            </div>`;

        if (!this.events.length) {
            el.innerHTML = header + Components.emptyState('📅', 'Aucun evenement',
                'Cliquez sur Actualiser pour charger le calendrier des dividendes');
            return;
        }

        // Group by event type
        const byType = {};
        for (const ev of this.events) {
            const t = ev.event_type || 'AUTRE';
            if (!byType[t]) byType[t] = [];
            byType[t].push(ev);
        }

        const typeIcon = { DIVIDEND: '💰', AGO: '🗣️', RESULTATS: '📊', AUTRE: '📋' };

        let html = header;
        for (const [type, evs] of Object.entries(byType)) {
            html += `<div class="section-title">${typeIcon[type] || '📋'} ${type}</div>`;

            // Table for dividends
            if (type === 'DIVIDEND') {
                html += `
                    <div class="card" style="padding:0;overflow:hidden">
                        <table class="data-table" style="margin:0">
                            <thead>
                                <tr>
                                    <th>Ticker</th>
                                    <th>Societe</th>
                                    <th style="text-align:right">Description</th>
                                    <th style="text-align:right">Date evenement</th>
                                    <th>Source</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${evs.map(ev => `
                                    <tr>
                                        <td><span class="font-mono text-bold" style="cursor:pointer"
                                            onclick="App.showChart('${ev.ticker}')">${ev.ticker || '—'}</span></td>
                                        <td style="font-size:0.85em">${Utils.escapeHtml(ev.name || ev.company || ev.ticker || '')}</td>
                                        <td style="text-align:right;font-size:0.85em">${Utils.escapeHtml(ev.description || '')}</td>
                                        <td style="text-align:right;font-family:monospace;font-size:0.85em">${ev.event_date || '—'}</td>
                                        <td><span class="pill">${ev.source || ''}</span></td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>`;
            } else {
                html += evs.map(ev => `
                    <div class="card" style="margin-bottom:8px">
                        <div class="flex-between">
                            <div>
                                <span class="font-mono text-bold">${ev.ticker || '—'}</span>
                                <span class="text-muted" style="margin-left:8px;font-size:0.85em">${Utils.escapeHtml(ev.description || '')}</span>
                            </div>
                            <span class="font-mono" style="font-size:0.85em">${ev.event_date || '—'}</span>
                        </div>
                    </div>
                `).join('');
            }
        }

        el.innerHTML = html;
    },

    async refreshCalendar() {
        const el = document.getElementById('intelligenceContent');
        el.innerHTML = Components.loading('Mise a jour du calendrier...');
        try {
            this.events = await API.getCalendar(true);
        } catch (err) {
            Utils.toast('Erreur lors du chargement du calendrier', 'error');
            this.events = [];
        }
        await this.renderCalendar(el);
    },
};
