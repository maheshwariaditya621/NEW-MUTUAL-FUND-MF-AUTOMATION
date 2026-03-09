import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { useWatchlist } from '../contexts/WatchlistContext';
import { getBulkPrices } from '../api/stocks';
import {
    getPeriods,
    getWatchlistDashboard,
    getStockActivity,
    getSchemeActivity,
    getActivityFeed,
    exportWatchlist,
} from '../api/watchlist';
import './WatchlistDashboardPage.css';

// ─────────────────────────── Helpers ─────────────────────────────────────────

const fmt = (n) => {
    if (n == null) return '—';
    const v = Math.abs(Number(n));
    if (v >= 1e7) return `${(n / 1e7).toFixed(2)}Cr`;
    if (v >= 1e5) return `${(n / 1e5).toFixed(2)}L`;
    if (v >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
    return String(Math.round(n));
};

const pct = (v) => {
    if (v == null) return '—';
    return `${v > 0 ? '+' : ''}${Number(v).toFixed(1)}%`;
};

const TrendBadge = ({ trend }) => {
    const map = { up: ['↑ Rising', 'up'], down: ['↓ Falling', 'down'], stable: ['→ Stable', 'stable'] };
    const [label, cls] = map[trend] || ['—', 'stable'];
    return <span className={`wl-trend-badge ${cls}`}>{label}</span>;
};

const SortTh = ({ col, sortBy, dir, onSort, children }) => (
    <th onClick={() => onSort(col)}>
        {children}
        <span className={`wl-sort-icon ${sortBy === col ? 'active' : ''}`}>
            {sortBy === col ? (dir === 'desc' ? '▼' : '▲') : '⇅'}
        </span>
    </th>
);

// ─────────────────────────── Stock Detail Drawer ─────────────────────────────

const StockDrawer = ({ isin, name, periodId, modules, onClose }) => {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        setLoading(true);
        getStockActivity(isin, periodId)
            .then(setData)
            .catch(console.error)
            .finally(() => setLoading(false));
    }, [isin, periodId]);

    // API shape:
    // data.buying[]      → { scheme_name, amc_name, delta, curr_shares, prev_shares, pct_change, percent_of_nav }
    // data.selling[]     → same
    // data.top_holders[] → { scheme_name, amc_name, shares_held, pct_of_nav, scheme_aum_cr }
    // data.net_activity  → { total_buying (str), total_selling (str), net_raw (int), net_change (str), direction }
    // data.period        → "Jan 2026"

    const netRaw = data?.net_activity?.net_raw ?? 0;
    const totBuy = data?.buying?.reduce((s, r) => s + (r.delta || 0), 0) ?? 0;
    const totSell = data?.selling?.reduce((s, r) => s + (r.delta || 0), 0) ?? 0;

    return (
        <>
            <div className="wl-drawer-overlay" onClick={onClose} />
            <div className="wl-drawer">
                <div className="wl-drawer-header">
                    <div>
                        <h3>{name}</h3>
                        <p>MF Activity — {data?.period || '…'}</p>
                    </div>
                    <button className="wl-drawer-close" onClick={onClose}>✕</button>
                </div>
                <div className="wl-drawer-body">
                    {loading && (
                        <>
                            <div className="wl-skeleton-row" />
                            <div className="wl-skeleton-row" />
                            <div className="wl-skeleton-row" style={{ width: '60%' }} />
                        </>
                    )}
                    {!loading && data && (
                        <>
                            {/* Net banner */}
                            <div className="wl-net-banner">
                                <div className="wl-net-stat buying">
                                    <div className="wl-net-stat-value green">+{fmt(totBuy)}</div>
                                    <div className="wl-net-stat-label">MF Buying</div>
                                </div>
                                <div className="wl-net-stat selling">
                                    <div className="wl-net-stat-value red">-{fmt(totSell)}</div>
                                    <div className="wl-net-stat-label">MF Selling</div>
                                </div>
                                <div className="wl-net-stat net">
                                    <div className={`wl-net-stat-value ${netRaw >= 0 ? 'green' : 'red'}`}>
                                        {netRaw >= 0 ? '+' : ''}{fmt(netRaw)}
                                    </div>
                                    <div className="wl-net-stat-label">Net Activity</div>
                                </div>
                            </div>

                            {/* Schemes Buying */}
                            {data.buying?.length > 0 && (
                                <div className="wl-drawer-section">
                                    <div className="wl-drawer-section-title">🟢 Schemes Buying</div>
                                    <table className="wl-detail-table">
                                        <thead><tr><th>Scheme</th><th>AMC</th><th>Shares Added</th><th>% Change</th></tr></thead>
                                        <tbody>
                                            {data.buying.map((r, i) => (
                                                <tr key={i}>
                                                    <td style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.scheme_name}</td>
                                                    <td style={{ color: 'var(--text-secondary)', fontSize: '0.75rem' }}>{r.amc_name}</td>
                                                    <td className="wl-val-positive">+{fmt(r.delta)}</td>
                                                    <td style={{ fontSize: '0.78rem', color: 'var(--text-secondary)' }}>
                                                        {r.prev_shares === 0 ? <span style={{ background: 'rgba(16,185,129,0.12)', padding: '1px 5px', borderRadius: 4, color: '#10b981', fontSize: '0.72rem' }}>New</span>
                                                            : r.pct_change != null ? `+${Math.abs(r.pct_change).toFixed(1)}%` : '—'}
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}

                            {/* Schemes Selling */}
                            {data.selling?.length > 0 && (
                                <div className="wl-drawer-section">
                                    <div className="wl-drawer-section-title">🔴 Schemes Selling</div>
                                    <table className="wl-detail-table">
                                        <thead><tr><th>Scheme</th><th>AMC</th><th>Shares Sold</th><th>% Change</th></tr></thead>
                                        <tbody>
                                            {data.selling.map((r, i) => (
                                                <tr key={i}>
                                                    <td style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.scheme_name}</td>
                                                    <td style={{ color: 'var(--text-secondary)', fontSize: '0.75rem' }}>{r.amc_name}</td>
                                                    <td className="wl-val-negative">-{fmt(r.delta)}</td>
                                                    <td style={{ fontSize: '0.78rem', color: 'var(--text-secondary)' }}>
                                                        {r.curr_shares === 0
                                                            ? <span style={{ background: 'rgba(239,68,68,0.12)', padding: '1px 5px', borderRadius: 4, color: '#ef4444', fontSize: '0.72rem' }}>Fully Exited</span>
                                                            : r.pct_change != null ? `-${Math.abs(r.pct_change).toFixed(1)}%` : '—'}
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}

                            {/* Top Holders */}
                            {data.top_holders?.length > 0 && (
                                <div className="wl-drawer-section">
                                    <div className="wl-drawer-section-title">📊 Top MF Holders (Current)</div>
                                    <table className="wl-detail-table">
                                        <thead><tr><th>Scheme</th><th>AMC</th><th>Shares Held</th><th>% of NAV</th></tr></thead>
                                        <tbody>
                                            {data.top_holders.map((r, i) => (
                                                <tr key={i}>
                                                    <td style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.scheme_name}</td>
                                                    <td style={{ color: 'var(--text-secondary)', fontSize: '0.75rem' }}>{r.amc_name}</td>
                                                    <td>{fmt(r.shares_held)}</td>
                                                    <td>{r.pct_of_nav != null ? `${Number(r.pct_of_nav).toFixed(2)}%` : '—'}</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}

                            {!data.buying?.length && !data.selling?.length && (
                                <div className="wl-empty" style={{ padding: '30px 0' }}>
                                    <p>No MF activity changes detected for this period.</p>
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </>
    );
};

// ─────────────────────────── Scheme Detail Drawer ────────────────────────────

const SchemeDrawer = ({ schemeId, name, periodId, onClose }) => {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        setLoading(true);
        getSchemeActivity(schemeId, periodId)
            .then(setData)
            .catch(console.error)
            .finally(() => setLoading(false));
    }, [schemeId, periodId]);

    return (
        <>
            <div className="wl-drawer-overlay" onClick={onClose} />
            <div className="wl-drawer">
                <div className="wl-drawer-header">
                    <div>
                        <h3>{name}</h3>
                        <p>Portfolio changes — {data?.month || '…'}</p>
                    </div>
                    <button className="wl-drawer-close" onClick={onClose}>✕</button>
                </div>
                <div className="wl-drawer-body">
                    {loading && (
                        <>
                            <div className="wl-skeleton-row" />
                            <div className="wl-skeleton-row" />
                            <div className="wl-skeleton-row" style={{ width: '60%' }} />
                        </>
                    )}
                    {!loading && data && (
                        <>
                            {data.new_adds?.length > 0 && (
                                <div className="wl-drawer-section">
                                    <div className="wl-drawer-section-title">🟢 New Positions</div>
                                    <table className="wl-detail-table">
                                        <thead><tr><th>Stock</th><th>Shares Added</th><th>% of NAV</th></tr></thead>
                                        <tbody>
                                            {data.new_adds.map((r, i) => (
                                                <tr key={i}>
                                                    <td>{r.stock_name}</td>
                                                    <td className="wl-val-positive">+{fmt(r.curr_shares)}</td>
                                                    <td>{r.pct_of_nav != null ? `${Number(r.pct_of_nav).toFixed(2)}%` : '—'}</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                            {data.reductions?.length > 0 && (
                                <div className="wl-drawer-section">
                                    <div className="wl-drawer-section-title">🔴 Reduced / Exited</div>
                                    <table className="wl-detail-table">
                                        <thead><tr><th>Stock</th><th>Shares Reduced</th><th>% Change</th></tr></thead>
                                        <tbody>
                                            {data.reductions.map((r, i) => (
                                                <tr key={i}>
                                                    <td>{r.stock_name}</td>
                                                    <td className="wl-val-negative">-{fmt(r.delta)}</td>
                                                    <td className="wl-val-negative">
                                                        {r.curr_shares === 0
                                                            ? <span style={{ fontSize: '0.72rem', background: 'rgba(239,68,68,0.15)', padding: '1px 6px', borderRadius: 4 }}>Fully Exited</span>
                                                            : pct(r.pct_change ? -Math.abs(r.pct_change) : null)
                                                        }
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                            {data.top_holdings?.length > 0 && (
                                <div className="wl-drawer-section">
                                    <div className="wl-drawer-section-title">📊 Top Holdings</div>
                                    <table className="wl-detail-table">
                                        <thead><tr><th>Stock</th><th>Sector</th><th>Shares</th><th>% NAV</th></tr></thead>
                                        <tbody>
                                            {data.top_holdings.slice(0, 10).map((r, i) => (
                                                <tr key={i}>
                                                    <td>{r.stock_name}</td>
                                                    <td style={{ color: 'var(--text-secondary)', fontSize: '0.75rem' }}>{r.sector || '—'}</td>
                                                    <td>{fmt(r.curr_shares)}</td>
                                                    <td>{r.pct_of_nav != null ? `${Number(r.pct_of_nav).toFixed(2)}%` : '—'}</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </>
    );
};

// ─────────────────────────── Main Page ────────────────────────────────────────

const MODULE_LABELS = {
    mf_buying: { label: 'MF Buying Activity', emoji: '🟢' },
    mf_selling: { label: 'MF Selling Activity', emoji: '🔴' },
    net_activity: { label: 'Net Institutional Activity', emoji: '📊' },
    top_holders: { label: 'Top MF Holders', emoji: '🏦' },
    trend_indicator: { label: 'Smart Trend Indicator', emoji: '🔭' },
    popularity_score: { label: 'MF Popularity Score', emoji: '⭐' },
};

export default function WatchlistDashboardPage() {
    const { preferences, updatePreferences, version, removeFromWatchlist } = useWatchlist();

    const [periods, setPeriods] = useState([]);
    const [selectedPid, setSelectedPid] = useState(null);
    const [dashboard, setDashboard] = useState(null);
    const [feed, setFeed] = useState([]);
    const [livePrices, setLivePrices] = useState({});
    const [loading, setLoading] = useState(true);
    const [showCustomize, setShowCustomize] = useState(false);
    const [activeDrawer, setActiveDrawer] = useState(null);
    const [sortBy, setSortBy] = useState('buying');
    const [sortDir, setSortDir] = useState('desc');
    const [exporting, setExporting] = useState(false);

    // Track state across refreshes to avoid skeletons
    const hasDataRef = React.useRef(false);
    const prevPidRef = React.useRef(null);

    // Edit Mode states
    const [isEditMode, setIsEditMode] = useState(false);
    const [selectedItems, setSelectedItems] = useState(new Set());

    // Load available periods once
    useEffect(() => {
        getPeriods()
            .then(d => setPeriods(d.periods || []))
            .catch(console.error);
    }, []);

    const load = useCallback((pid) => {
        const isPidChange = prevPidRef.current !== pid;
        if (!hasDataRef.current || isPidChange) {
            setLoading(true);
        }
        prevPidRef.current = pid;

        Promise.all([getWatchlistDashboard(pid), getActivityFeed(pid)])
            .then(([dash, feedData]) => {
                setDashboard(dash);
                setFeed(feedData?.feed || []);
                hasDataRef.current = true;
                const isins = (dash?.stock_summaries || []).map(s => s.isin).filter(Boolean);
                if (isins.length > 0) {
                    getBulkPrices(isins)
                        .then(priceMap => {
                            if (priceMap) setLivePrices(prev => ({ ...prev, ...priceMap }));
                        })
                        .catch(() => { });
                }
            })
            .catch(console.error)
            .finally(() => setLoading(false));
    }, []);

    useEffect(() => { load(selectedPid); }, [load, selectedPid, version]);

    // Price Polling (every 10 seconds)
    useEffect(() => {
        if (!dashboard?.stock_summaries?.length) return;

        const poll = () => {
            const isins = dashboard.stock_summaries.map(s => s.isin).filter(Boolean);
            if (isins.length > 0) {
                getBulkPrices(isins)
                    .then(priceMap => {
                        if (priceMap) setLivePrices(prev => ({ ...prev, ...priceMap }));
                    })
                    .catch(() => { });
            }
        };

        const interval = setInterval(poll, 30000);
        return () => clearInterval(interval);
    }, [dashboard?.stock_summaries]);

    const handlePeriodChange = (e) => {
        setSelectedPid(e.target.value ? Number(e.target.value) : null);
    };

    const handleSort = (col) => {
        if (sortBy === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc');
        else { setSortBy(col); setSortDir('desc'); }
    };

    const handleExport = async (format) => {
        setExporting(true);
        try { await exportWatchlist(format); }
        catch (e) { alert('Export failed: ' + e.message); }
        finally { setExporting(false); }
    };

    const handleToggleSelect = (type, id) => {
        const key = `${type}:${id}`;
        setSelectedItems(prev => {
            const next = new Set(prev);
            if (next.has(key)) next.delete(key);
            else next.add(key);
            return next;
        });
    };

    const handleDeleteSingle = async (e, type, id) => {
        e.stopPropagation();
        if (!window.confirm(`Delete this ${type} from watchlist?`)) return;
        try {
            await removeFromWatchlist(type, id);
        } catch (err) {
            alert('Failed to delete item');
        }
    };

    const handleDeleteSelected = async () => {
        if (selectedItems.size === 0) return;
        if (!window.confirm(`Delete ${selectedItems.size} items from watchlist?`)) return;

        setLoading(true);
        try {
            for (const itemKey of selectedItems) {
                const [type, id] = itemKey.split(':');
                await removeFromWatchlist(type, id);
            }
            setSelectedItems(new Set());
            setIsEditMode(false);
        } catch (err) {
            alert('Bulk delete partially failed');
        } finally {
            setLoading(false);
        }
    };

    const sortedStocks = useMemo(() => {
        const rows = [...(dashboard?.stock_summaries || [])];
        rows.sort((a, b) => {
            const va = a[sortBy] ?? 0;
            const vb = b[sortBy] ?? 0;
            return sortDir === 'desc' ? vb - va : va - vb;
        });
        return rows;
    }, [dashboard, sortBy, sortDir]);

    const isEmpty = !loading && dashboard?.total_assets === 0;

    const selectedPeriodLabel = selectedPid
        ? (periods.find(p => p.period_id === selectedPid)?.label || '')
        : (dashboard?.period || 'Latest');

    /* hero insight strip data */
    const topBuyer = dashboard?.strongest_buying?.[0];
    const topSeller = dashboard?.strongest_selling?.[0];
    const topScheme = dashboard?.most_active_schemes?.[0];

    return (
        <div className="wl-page">
            <div className="container">

                {/* ── Compact Toolbar ── */}
                <div className="wl-toolbar">
                    <div className="wl-toolbar-left">
                        <h2 className="wl-toolbar-title">🔖 Smart Watchlist</h2>

                        {/* Small stat pills */}
                        <div className="wl-stat-pills">
                            <span className="wl-stat-pill stocks">
                                📈 {dashboard?.total_stocks ?? '—'} Stocks
                            </span>
                            <span className="wl-stat-pill schemes">
                                🏦 {dashboard?.total_schemes ?? '—'} Schemes
                            </span>
                        </div>
                    </div>

                    <div className="wl-toolbar-right">
                        {/* Period picker */}
                        <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', fontWeight: 600 }}>Period:</span>
                        <select
                            value={selectedPid || ''}
                            onChange={handlePeriodChange}
                            className="shp-picker-select"
                            id="wl-period-select"
                        >
                            <option value="">Latest</option>
                            {periods.map(p => (
                                <option key={p.period_id} value={p.period_id}>{p.label}</option>
                            ))}
                        </select>

                        <button className="wl-export-btn" onClick={() => handleExport('csv')} disabled={exporting} id="wl-export-csv">📥 CSV</button>
                        <button className="wl-export-btn" onClick={() => handleExport('excel')} disabled={exporting} id="wl-export-excel">📊 Excel</button>
                        <button
                            className={`wl-edit-mode-btn ${isEditMode ? 'active' : ''}`}
                            onClick={() => { setIsEditMode(!isEditMode); setSelectedItems(new Set()); }}
                        >
                            {isEditMode ? '✅ Done' : '✏️ Edit'}
                        </button>
                        <button className="wl-customize-btn" onClick={() => setShowCustomize(s => !s)} id="wl-customize-btn">⚙️ Customise</button>
                    </div>
                </div>

                {/* Edit Mode Actions Bar */}
                {isEditMode && selectedItems.size > 0 && (
                    <div className="wl-edit-actions">
                        <span>{selectedItems.size} items selected</span>
                        <button className="wl-bulk-delete-btn" onClick={handleDeleteSelected}>
                            🗑️ Delete Selected
                        </button>
                    </div>
                )}

                {/* ── Customize drawer ── */}
                {showCustomize && (
                    <div className="wl-customize-drawer">
                        <h4>Customise Watchlist Modules</h4>
                        <div className="wl-module-grid">
                            {Object.entries(MODULE_LABELS).map(([key, { label, emoji }]) => (
                                <label key={key} className={`wl-module-toggle ${preferences[key] ? 'active' : ''}`} htmlFor={`toggle-${key}`}>
                                    <span className="wl-module-label">{emoji} {label}</span>
                                    <div className="wl-toggle-switch">
                                        <input id={`toggle-${key}`} type="checkbox" checked={!!preferences[key]} onChange={() => updatePreferences({ [key]: !preferences[key] })} />
                                        <span className="wl-toggle-slider" />
                                    </div>
                                </label>
                            ))}
                        </div>
                    </div>
                )}

                {/* Period warning */}
                {selectedPid && (
                    <div className="wl-period-warning">
                        📅 Showing data for <strong style={{ margin: '0 4px' }}>{selectedPeriodLabel}</strong>. Some AMCs may not have uploaded data for this period.
                        <button onClick={() => setSelectedPid(null)}>↩ Latest</button>
                    </div>
                )}

                {/* ── Empty State ── */}
                {isEmpty && (
                    <div className="wl-empty">
                        <div className="wl-empty-icon">📋</div>
                        <h3>Your watchlist is empty</h3>
                        <p>Search for a stock or scheme in the header search bar and click the <strong>🔖</strong> button to add it.</p>
                        <div style={{ display: 'flex', gap: 10, justifyContent: 'center', flexWrap: 'wrap' }}>
                            <Link to="/stocks" className="wl-export-btn">Browse Stocks</Link>
                            <Link to="/schemes" className="wl-export-btn">Browse Schemes</Link>
                        </div>
                    </div>
                )}

                {!isEmpty && (
                    <>
                        {/* ── Insight Strip ── */}
                        {(topBuyer || topSeller || topScheme) && (
                            <div className="wl-insight-strip">
                                <div className="wl-insight-card buy">
                                    <div className="wl-insight-icon">🟢</div>
                                    <div className="wl-insight-body">
                                        <div className="wl-insight-label-sm">Strongest MF Buying</div>
                                        <div className="wl-insight-name">{topBuyer?.name || '—'}</div>
                                        <div className="wl-insight-val green">
                                            {topBuyer ? `+${fmt(topBuyer.buying)}` : '—'}
                                        </div>
                                    </div>
                                </div>
                                <div className="wl-insight-card sell">
                                    <div className="wl-insight-icon">🔴</div>
                                    <div className="wl-insight-body">
                                        <div className="wl-insight-label-sm">Strongest MF Selling</div>
                                        <div className="wl-insight-name">{topSeller?.name || '—'}</div>
                                        <div className="wl-insight-val red">
                                            {topSeller ? `-${fmt(topSeller.selling)}` : '—'}
                                        </div>
                                    </div>
                                </div>
                                <div className="wl-insight-card active">
                                    <div className="wl-insight-icon">⚡</div>
                                    <div className="wl-insight-body">
                                        <div className="wl-insight-label-sm">Most Active Scheme</div>
                                        <div className="wl-insight-name">{topScheme?.name || '—'}</div>
                                        <div className="wl-insight-val amber">
                                            {topScheme ? `${topScheme.delta > 0 ? '+' : ''}${topScheme.delta} stocks` : '—'}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* ── Main Body: Table + Feed ── */}
                        <div className="wl-body">

                            {/* LEFT: Tables */}
                            <div>
                                {/* Loading skeleton */}
                                {loading && (
                                    <div className="wl-card" style={{ padding: 20 }}>
                                        <div className="wl-skeleton-row" />
                                        <div className="wl-skeleton-row" />
                                        <div className="wl-skeleton-row" style={{ width: '70%' }} />
                                        <div className="wl-skeleton-row" style={{ width: '50%' }} />
                                    </div>
                                )}

                                {/* Stock Activity Table */}
                                {!loading && sortedStocks.length > 0 && (
                                    <div className="wl-card">
                                        <div className="wl-card-header">
                                            <h3>📊 Stock Activity Summary</h3>
                                            <span className="wl-card-badge">{sortedStocks.length} stocks — {selectedPeriodLabel}</span>
                                        </div>
                                        <div className="wl-table-wrapper">
                                            <table className="wl-table">
                                                <thead>
                                                    <tr>
                                                        {isEditMode && <th style={{ width: 40 }}></th>}
                                                        <th>Stock</th>
                                                        <th>LTP</th>
                                                        <SortTh col="buying" sortBy={sortBy} dir={sortDir} onSort={handleSort}>MF Buying ▾</SortTh>
                                                        <SortTh col="selling" sortBy={sortBy} dir={sortDir} onSort={handleSort}>MF Selling ▾</SortTh>
                                                        <SortTh col="net_change" sortBy={sortBy} dir={sortDir} onSort={handleSort}>Net</SortTh>
                                                        {preferences.trend_indicator && <th>Trend</th>}
                                                        <th>Funds</th>
                                                        {isEditMode && <th style={{ width: 40 }}></th>}
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {sortedStocks.map((s) => {
                                                        const ltp = livePrices[s.isin];
                                                        const netCls = s.net_change > 0 ? 'pos' : s.net_change < 0 ? 'neg' : 'zer';
                                                        const isSelected = selectedItems.has(`stock:${s.isin}`);
                                                        return (
                                                            <tr
                                                                key={s.company_id}
                                                                onClick={() => isEditMode ? handleToggleSelect('stock', s.isin) : setActiveDrawer({ type: 'stock', isin: s.isin, name: s.name })}
                                                                className={isSelected ? 'wl-row-selected' : ''}
                                                                title={isEditMode ? 'Click to select' : `View MF activity for ${s.name}`}
                                                            >
                                                                {isEditMode && (
                                                                    <td>
                                                                        <input
                                                                            type="checkbox"
                                                                            checked={isSelected}
                                                                            onChange={() => handleToggleSelect('stock', s.isin)}
                                                                            onClick={(e) => e.stopPropagation()}
                                                                        />
                                                                    </td>
                                                                )}
                                                                <td>
                                                                    <div style={{ fontWeight: 700 }}>{s.name}</div>
                                                                    {s.sector && <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{s.sector}</div>}
                                                                </td>
                                                                <td style={{ fontWeight: 600, color: 'var(--text-secondary)', fontSize: '0.82rem' }}>
                                                                    {ltp ? `₹${Number(ltp).toLocaleString('en-IN')}` : '—'}
                                                                </td>
                                                                <td className="wl-val-positive">
                                                                    {s.buying > 0 ? (
                                                                        <>
                                                                            +{fmt(s.buying)}
                                                                            {s.buying_schemes > 0 && <div style={{ fontSize: '0.67rem', opacity: 0.6 }}>{s.buying_schemes} funds</div>}
                                                                        </>
                                                                    ) : '—'}
                                                                </td>
                                                                <td className="wl-val-negative">
                                                                    {s.selling > 0 ? (
                                                                        <>
                                                                            -{fmt(s.selling)}
                                                                            {s.selling_schemes > 0 && <div style={{ fontSize: '0.67rem', opacity: 0.6 }}>{s.selling_schemes} funds</div>}
                                                                        </>
                                                                    ) : '—'}
                                                                </td>
                                                                <td>
                                                                    <span className={`wl-net-badge ${netCls}`}>
                                                                        {s.net_change > 0 ? `+${fmt(s.net_change)}` : fmt(s.net_change)}
                                                                    </span>
                                                                </td>
                                                                {preferences.trend_indicator && <td><TrendBadge trend={s.trend} /></td>}
                                                                <td style={{ color: 'var(--text-secondary)', fontWeight: 600 }}>{s.num_funds}</td>
                                                                {isEditMode && (
                                                                    <td>
                                                                        <button className="wl-row-delete-btn" onClick={(e) => handleDeleteSingle(e, 'stock', s.isin)}>
                                                                            🗑️
                                                                        </button>
                                                                    </td>
                                                                )}
                                                            </tr>
                                                        );
                                                    })}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                )}

                                {/* Scheme Summary Table */}
                                {!loading && dashboard?.scheme_summaries?.length > 0 && (
                                    <div className="wl-card">
                                        <div className="wl-card-header">
                                            <h3>🏦 Scheme Portfolio Changes</h3>
                                            <span className="wl-card-badge">{dashboard.scheme_summaries.length} schemes</span>
                                        </div>
                                        <div className="wl-table-wrapper">
                                            <table className="wl-table">
                                                <thead>
                                                    <tr>
                                                        {isEditMode && <th style={{ width: 40 }}></th>}
                                                        <th>Scheme</th>
                                                        <th>AMC</th>
                                                        <th>Current</th>
                                                        <th>Previous</th>
                                                        <th>Δ Stocks</th>
                                                        {isEditMode && <th style={{ width: 40 }}></th>}
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {dashboard.scheme_summaries.map((s) => {
                                                        const isSelected = selectedItems.has(`scheme:${s.scheme_id}`);
                                                        return (
                                                            <tr
                                                                key={s.scheme_id}
                                                                onClick={() => isEditMode ? handleToggleSelect('scheme', s.scheme_id) : setActiveDrawer({ type: 'scheme', schemeId: s.scheme_id, name: s.name })}
                                                                className={isSelected ? 'wl-row-selected' : ''}
                                                                title={isEditMode ? 'Click to select' : `View portfolio for ${s.name}`}
                                                            >
                                                                {isEditMode && (
                                                                    <td>
                                                                        <input
                                                                            type="checkbox"
                                                                            checked={isSelected}
                                                                            onChange={() => handleToggleSelect('scheme', s.scheme_id)}
                                                                            onClick={(e) => e.stopPropagation()}
                                                                        />
                                                                    </td>
                                                                )}
                                                                <td style={{ fontWeight: 700 }}>{s.name}</td>
                                                                <td style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>{s.amc_name}</td>
                                                                <td>{s.curr_holdings}</td>
                                                                <td>{s.prev_holdings}</td>
                                                                <td>
                                                                    <span className={`wl-net-badge ${s.delta > 0 ? 'pos' : s.delta < 0 ? 'neg' : 'zer'}`}>
                                                                        {s.delta > 0 ? `+${s.delta}` : s.delta}
                                                                    </span>
                                                                </td>
                                                                {isEditMode && (
                                                                    <td>
                                                                        <button className="wl-row-delete-btn" onClick={(e) => handleDeleteSingle(e, 'scheme', s.scheme_id)}>
                                                                            🗑️
                                                                        </button>
                                                                    </td>
                                                                )}
                                                            </tr>
                                                        );
                                                    })}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                )}

                                {/* No stocks yet */}
                                {!loading && sortedStocks.length === 0 && !dashboard?.scheme_summaries?.length && (
                                    <div className="wl-card">
                                        <div className="wl-empty" style={{ padding: '40px 20px' }}>
                                            <p style={{ margin: 0 }}>No data available for <strong>{selectedPeriodLabel}</strong>. Try selecting a different period.</p>
                                        </div>
                                    </div>
                                )}
                            </div>

                            {/* RIGHT: Activity Feed — full height, sticky */}
                            <div className="wl-feed-card">
                                <div className="wl-feed-header">
                                    <h3>⚡ Activity Feed</h3>
                                    <span className="wl-card-badge">{feed.length} events</span>
                                </div>

                                {feed.length === 0 && !loading ? (
                                    <div className="wl-feed-empty">
                                        No significant activity detected for <strong>{selectedPeriodLabel}</strong>.<br />
                                        <span style={{ fontSize: '0.75rem', opacity: 0.7 }}>This is normal if period data is incomplete.</span>
                                    </div>
                                ) : (
                                    <div className="wl-feed-scroll">
                                        {loading ? (
                                            <div style={{ padding: 16 }}>
                                                <div className="wl-skeleton-row" />
                                                <div className="wl-skeleton-row" />
                                                <div className="wl-skeleton-row" style={{ width: '80%' }} />
                                            </div>
                                        ) : feed.map((item, i) => {
                                            const isBuy = item.delta > 0;
                                            return (
                                                <div className="wl-feed-item" key={i}>
                                                    <span className={`wl-feed-dot ${isBuy ? 'buy' : item.delta < 0 ? 'sell' : 'neutral'}`} />
                                                    <div>
                                                        <div className="wl-feed-msg">{item.message}</div>
                                                        <div className="wl-feed-period">{item.month}</div>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                )}
                            </div>
                        </div>
                    </>
                )}

                {/* ── Detail Drawers ── */}
                {activeDrawer?.type === 'stock' && (
                    <StockDrawer
                        isin={activeDrawer.isin}
                        name={activeDrawer.name}
                        periodId={selectedPid}
                        modules={preferences}
                        onClose={() => setActiveDrawer(null)}
                    />
                )}
                {activeDrawer?.type === 'scheme' && (
                    <SchemeDrawer
                        schemeId={activeDrawer.schemeId}
                        name={activeDrawer.name}
                        periodId={selectedPid}
                        onClose={() => setActiveDrawer(null)}
                    />
                )}
            </div>
        </div>
    );
}
