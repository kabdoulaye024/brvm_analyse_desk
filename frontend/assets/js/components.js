/* ═══════════════════════════════════════════════════
   Reusable UI Components
   ═══════════════════════════════════════════════════ */

const Components = {
    /**
     * Sortable, filterable table
     */
    table(id, columns, data, options = {}) {
        const { onRowClick, sortBy, sortDir = 'asc' } = options;

        let html = `<div class="table-container"><table id="${id}">`;
        html += '<thead><tr>';
        for (const col of columns) {
            const isSorted = sortBy === col.key;
            const arrow = isSorted ? (sortDir === 'asc' ? '↑' : '↓') : '↕';
            html += `<th class="${col.align === 'right' ? 'text-right' : ''} ${isSorted ? 'sorted' : ''}"
                         data-sort-key="${col.key}">
                        ${col.label} <span class="sort-icon">${arrow}</span>
                     </th>`;
        }
        html += '</tr></thead><tbody>';

        for (const row of data) {
            const clickable = onRowClick ? 'class="clickable"' : '';
            const clickAttr = onRowClick ? `data-ticker="${row.ticker || ''}"` : '';
            html += `<tr ${clickable} ${clickAttr}>`;
            for (const col of columns) {
                const val = row[col.key];
                const align = col.align === 'right' ? 'text-right' : '';
                const mono = col.mono ? 'font-mono' : '';
                let content = col.render ? col.render(val, row) : (val ?? '—');
                html += `<td class="${align} ${mono}">${content}</td>`;
            }
            html += '</tr>';
        }

        html += '</tbody></table></div>';
        return html;
    },

    /**
     * Change badge component
     */
    changeBadge(value) {
        if (value == null || isNaN(value)) return '<span class="change-badge neutral">—</span>';
        const cls = Utils.changeClass(value);
        return `<span class="change-badge ${cls}">${Utils.formatPct(value)}</span>`;
    },

    /**
     * 52-week range bar
     */
    rangeBar(pct) {
        if (pct == null) return '—';
        const color = Utils.rangeColor(pct);
        return `<div class="range-bar">
                    <div class="range-bar-fill" style="width:${pct}%;background:${color}"></div>
                </div>
                <span class="font-mono text-muted" style="font-size:0.75em;margin-left:4px">${pct.toFixed(0)}%</span>`;
    },

    /**
     * Metric card
     */
    metricCard(label, value, options = {}) {
        const { accent, sub, textClass } = options;
        const accentClass = accent ? `accent-${accent}` : '';
        const valClass = textClass || '';
        return `<div class="metric-card ${accentClass}">
                    <div class="metric-label">${label}</div>
                    <div class="metric-value ${valClass}">${value}</div>
                    ${sub ? `<div class="metric-sub">${sub}</div>` : ''}
                </div>`;
    },

    /**
     * Loading spinner
     */
    loading(message = 'Chargement...') {
        return `<div class="loading">
                    <div class="spinner"></div>
                    <div style="margin-top:12px;font-family:var(--font-mono);font-size:0.85em">${message}</div>
                </div>`;
    },

    /**
     * Empty state
     */
    emptyState(icon, title, desc) {
        return `<div class="empty-state">
                    <div class="icon">${icon}</div>
                    <div class="title">${title}</div>
                    <div class="desc">${desc}</div>
                </div>`;
    },

    /**
     * Modal dialog
     */
    showModal(title, bodyHtml, footerHtml = '') {
        const existing = document.querySelector('.modal-overlay');
        if (existing) existing.remove();

        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';
        overlay.innerHTML = `
            <div class="modal">
                <div class="modal-header">
                    <span class="modal-title">${title}</span>
                    <button class="modal-close" onclick="Components.closeModal()">&times;</button>
                </div>
                <div class="modal-body">${bodyHtml}</div>
                ${footerHtml ? `<div class="modal-footer">${footerHtml}</div>` : ''}
            </div>`;

        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) Components.closeModal();
        });

        document.body.appendChild(overlay);
    },

    closeModal() {
        const overlay = document.querySelector('.modal-overlay');
        if (overlay) overlay.remove();
    },

    /**
     * Pill/tag
     */
    pill(text, variant = '') {
        return `<span class="pill ${variant ? 'pill-' + variant : ''}">${text}</span>`;
    },
};
