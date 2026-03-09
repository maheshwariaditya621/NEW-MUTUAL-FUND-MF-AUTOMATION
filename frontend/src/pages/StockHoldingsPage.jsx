import React, { useState, useMemo, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import MissingData from '../components/common/MissingData';
import PageEmptyState from '../components/common/PageEmptyState';
import ExportButton from '../components/common/ExportButton';
import { getStockHoldings, getStockPrice } from '../api/stocks';
import { handleApiError } from '../api/client';
import { formatNumber } from '../utils/helpers';
import './StockHoldingsPage.css';

// ── helpers ──────────────────────────────────────────────────────────────────
const fmt = (n) => (n === null || n === undefined) ? null : (n === 0 ? '-' : Number(n).toLocaleString('en-IN'));
const fmtCr = (n) => (n === null || n === undefined) ? null : (n === 0 ? '-' : Number(n).toLocaleString('en-IN', { maximumFractionDigits: 2 }));
const fmtPct = (n, digits = 2) => (n === null || n === undefined) ? null : (n === 0 ? '-' : `${Number(n).toFixed(digits)}%`);

const MONTH_NAMES = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];

function labelToDate(label) {
    if (!label) return new Date();
    const [mon, yr] = label.split('-');
    const m = MONTH_NAMES.indexOf(mon.toUpperCase());
    const y = parseInt(yr, 10) + 2000;
    return new Date(y, m);
}

function dateToLabel(date) {
    return `${MONTH_NAMES[date.getMonth()]}-${String(date.getFullYear()).slice(2)}`;
}

function computeDisplayMonths(endLabel, n = 3) {
    const end = labelToDate(endLabel);
    const result = [];
    for (let i = 0; i < n; i++) {
        const d = new Date(end.getFullYear(), end.getMonth() - i);
        result.push(dateToLabel(d));
    }
    return result;
}

function ChangeCell({ change, pct }) {
    if (change == null) return <MissingData inline />;
    if (change === 0) return <span className="shp-neutral">-</span>;
    const isPos = change > 0;
    const isNeg = change < 0;
    const cls = isPos ? 'shp-pos' : isNeg ? 'shp-neg' : 'shp-neutral';
    const sign = isPos ? '+' : '';

    return (
        <div className="shp-change-cell">
            <span className={cls}>{sign}{fmt(change)}</span>
            {pct != null && (
                <span className={`shp-change-pct ${cls}`}>
                    ({sign}{pct.toFixed(2)}%)
                </span>
            )}
        </div>
    );
}

// ── Main Component ────────────────────────────────────────────────────────────
export default function StockHoldingsPage() {
    const [searchParams, setSearchParams] = useSearchParams();
    const identifierParam = searchParams.get('isin');
    const filterParam = searchParams.get('filter'); // 'entrants' | 'exits'

    const [summary, setSummary] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [livePrice, setLivePrice] = useState(null);
    const [viewMode, setViewMode] = useState('scheme'); // 'scheme' | 'amc'
    const [filterText, setFilterText] = useState('');
    const [sortConfig, setSortConfig] = useState({ key: 'shares_0', direction: 'desc' });
    const [selectedMonth, setSelectedMonth] = useState(null);
    const [aumViewMode, setAumViewMode] = useState('total'); // 'total' or 'equity'
    const [allAvailableMonths, setAllAvailableMonths] = useState([]);
    const [showAmcStatus, setShowAmcStatus] = useState(false);

    useEffect(() => {
        if (!summary?.holdings?.[0]?.history) return;
        const seen = new Set(summary.holdings[0].history.map(h => h.month));
        setAllAvailableMonths(prev => {
            const merged = Array.from(new Set([...prev, ...seen]));
            merged.sort((a, b) => labelToDate(b) - labelToDate(a));
            return merged;
        });
    }, [summary]);

    const displayMonths = useMemo(() => {
        if (selectedMonth) return computeDisplayMonths(selectedMonth, 3);
        const latest = summary?.holdings?.[0]?.history?.[0]?.month;
        if (latest) return computeDisplayMonths(latest, 3);
        return [];
    }, [selectedMonth, summary]);

    useEffect(() => {
        if (identifierParam) {
            handleFetchHoldings(identifierParam, 4, null);
        }
    }, [identifierParam]);

    // Fast polling for live price
    useEffect(() => {
        if (!summary?.isin) return;

        const interval = setInterval(async () => {
            try {
                const data = await getStockPrice(summary.isin);
                if (data.price !== undefined && data.price !== null) {
                    setLivePrice(data.price);
                }
            } catch (err) {
                console.error("Failed to poll live price:", err);
            }
        }, 30000); // 30 seconds

        return () => clearInterval(interval);
    }, [summary?.isin]);

    const handleFetchHoldings = async (identifier, months, endMonth) => {
        setLoading(true);
        setError(null);
        setSummary(null);
        try {
            const data = await getStockHoldings(identifier, months, endMonth);
            setSummary(data);

            // If live_price is missing from aggregated response, fetch it immediately
            if (data.live_price) {
                setLivePrice(data.live_price);
            } else {
                getStockPrice(data.isin).then(res => {
                    if (res.price) setLivePrice(res.price);
                }).catch(e => console.error("Price fetch error:", e));
            }
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    const handleMonthChange = (monthLabel) => {
        const val = monthLabel || null;
        setSelectedMonth(val);
        if (identifierParam) handleFetchHoldings(identifierParam, 4, val);
    };

    const handleSort = (key) => {
        setSortConfig(prev => {
            if (prev.key !== key) return { key, direction: 'desc' };
            if (prev.direction === 'desc') return { key, direction: 'asc' };
            return { key: null, direction: 'desc' }; // 3-state cycle: desc -> asc -> none
        });
    };

    const sortArrow = (key) =>
        sortConfig.key === key ? (sortConfig.direction === 'asc' ? ' ↑' : ' ↓') : null;

    const amcAggregated = useMemo(() => {
        if (!summary?.holdings) return [];
        const map = {};
        for (const scheme of summary.holdings) {
            const amc = scheme.amc_name;
            if (!map[amc]) {
                map[amc] = {
                    amc_name: amc,
                    scheme_count: 0,
                    equity_aum_cr: 0,
                    total_aum_cr: 0,
                    history: scheme.history.map(h => ({ ...h, num_shares: null, month_change: 0 })),
                };
            }
            const entry = map[amc];
            entry.scheme_count += 1;
            entry.equity_aum_cr += (parseFloat(scheme.equity_aum_cr) || 0);
            entry.total_aum_cr += (parseFloat(scheme.total_aum_cr) || 0);
            scheme.history.forEach((h, idx) => {
                if (entry.history[idx]) {
                    if (h.num_shares !== null) {
                        if (entry.history[idx].num_shares === null) entry.history[idx].num_shares = 0;
                        entry.history[idx].num_shares += h.num_shares;
                    }
                    entry.history[idx].month_change = (entry.history[idx].month_change || 0) + (h.month_change || 0);
                    if (summary.shares_outstanding) {
                        entry.history[idx].ownership_percent = (entry.history[idx].num_shares / summary.shares_outstanding) * 100;
                    }
                }
            });
        }
        return Object.values(map);
    }, [summary]);

    const schemesToRender = useMemo(() => {
        if (!summary?.holdings) return [];
        let items = [...summary.holdings];

        if (filterParam === 'entrants') {
            items = items.filter(h => (h.history[0]?.num_shares > 0 && h.history[1]?.num_shares === 0));
        } else if (filterParam === 'exits') {
            items = items.filter(h => (h.history[1]?.num_shares > 0 && h.history[0]?.num_shares === 0));
        }

        if (filterText) {
            const low = filterText.toLowerCase();
            items = items.filter(h => h.scheme_name.toLowerCase().includes(low) || h.amc_name.toLowerCase().includes(low));
        }

        if (sortConfig.key) {
            items.sort((a, b) => {
                let aVal, bVal;
                if (sortConfig.key === 'scheme_name') { aVal = a.scheme_name; bVal = b.scheme_name; }
                else if (sortConfig.key === 'amc_name') { aVal = a.amc_name; bVal = b.amc_name; }
                else if (sortConfig.key === 'pnav') { aVal = parseFloat(a.history[0]?.percent_to_aum) || 0; bVal = parseFloat(b.history[0]?.percent_to_aum) || 0; }
                else if (sortConfig.key.startsWith('shares_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.num_shares || 0; bVal = b.history[idx]?.num_shares || 0;
                }
                else if (sortConfig.key.startsWith('change_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.month_change || 0; bVal = b.history[idx]?.month_change || 0;
                }
                else if (sortConfig.key.startsWith('own_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.ownership_percent || 0; bVal = b.history[idx]?.ownership_percent || 0;
                }

                if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
                if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }
        return items;
    }, [summary, filterText, sortConfig, filterParam]);

    const amcToRender = useMemo(() => {
        let items = [...amcAggregated];
        if (filterParam === 'entrants') {
            items = items.filter(h => (h.history[0]?.num_shares > 0 && h.history[1]?.num_shares === 0));
        } else if (filterParam === 'exits') {
            items = items.filter(h => (h.history[1]?.num_shares > 0 && h.history[0]?.num_shares === 0));
        }
        if (filterText) {
            const low = filterText.toLowerCase();
            items = items.filter(h => h.amc_name.toLowerCase().includes(low));
        }

        if (sortConfig.key) {
            items.sort((a, b) => {
                let aVal, bVal;
                if (sortConfig.key === 'amc_name') { aVal = a.amc_name; bVal = b.amc_name; }
                else if (sortConfig.key === 'equity_aum_cr') { aVal = a.equity_aum_cr; bVal = b.equity_aum_cr; }
                else if (sortConfig.key === 'total_aum_cr') { aVal = a.total_aum_cr; bVal = b.total_aum_cr; }
                else if (sortConfig.key.startsWith('shares_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.num_shares || 0; bVal = b.history[idx]?.num_shares || 0;
                }
                else if (sortConfig.key.startsWith('change_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.month_change || 0; bVal = b.history[idx]?.month_change || 0;
                }
                else if (sortConfig.key.startsWith('own_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.ownership_percent || 0; bVal = b.history[idx]?.ownership_percent || 0;
                }
                if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
                if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }
        return items;
    }, [amcAggregated, filterText, sortConfig, filterParam]);

    return (
        <div className="shp-page">
            <div className="shp-container">
                {loading && <Loading message="Loading stock holdings..." />}
                {error && (
                    <ErrorMessage
                        message={error}
                        onRetry={() => identifierParam && handleFetchHoldings(identifierParam, 4, selectedMonth)}
                    />
                )}

                {!identifierParam && !loading && !error && (
                    <PageEmptyState
                        title="Institutional Ownership Insights"
                        description="Unlock the secrets of institutional stock ownership. Track which mutual funds are backing your favorite stocks, monitor exactly how much they are buying or selling each month, and analyze ownership trends to make smarter investment decisions."
                        placeholder="Search by Stock Name or ISIN (e.g. Reliance, INE002A01018)"
                        type="stock"
                        suggestions={["Reliance", "HDFC Bank", "ICICI Bank", "Infosys", "ITC"]}
                        onSearch={(val) => {
                            searchParams.set('isin', val);
                            setSearchParams(searchParams);
                        }}
                    />
                )}

                {summary && !loading && !error && (
                    <>
                        <div className="shp-identity-bar">
                            <div className="shp-identity-row">
                                <div className="shp-identity-left">
                                    <div className="shp-id-top" style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                                        <span className="shp-company-name">{summary.company_name}</span>
                                        <div className="rv-live-price-wrapper" style={{ display: 'flex', alignItems: 'center', minWidth: '120px' }}>
                                            {livePrice ? (
                                                <div className="rv-live-price-chip">
                                                    <span>LTP</span>
                                                    ₹ {formatNumber(livePrice)}
                                                    <div
                                                        className="shp-mcap-info"
                                                        style={{ display: 'inline-flex', alignItems: 'center', position: 'relative' }}
                                                    >
                                                        <span className="shp-mcap-info-icon">i</span>
                                                        <span className="shp-mcap-tooltip" style={{ minWidth: '180px', left: '50%', transform: 'translateX(-50%)' }}>
                                                            Live prices refresh every 30 seconds.
                                                        </span>
                                                    </div>
                                                </div>
                                            ) : (
                                                <div style={{ color: 'var(--text-secondary)', fontSize: '12px', display: 'flex', alignItems: 'center', gap: '6px', opacity: 0.6 }}>
                                                    <div className="shp-spinner-tiny"></div>
                                                    Fetching LTP...
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                    <div className="shp-id-bottom" style={{ display: 'flex', gap: '8px', marginTop: '8px' }}>
                                        <span className="shp-isin-chip">{summary.isin}</span>
                                        {summary.sector && <span className="shp-sector-chip">{summary.sector}</span>}
                                        {summary.market_cap && (
                                            <span className="shp-mcap-chip">
                                                ₹ {fmtCr(summary.market_cap) || <MissingData inline />} Cr
                                                {summary.mcap_type && (
                                                    <span className={`shp-mcap-badge ${summary.mcap_type.toLowerCase().replace(' ', '-')}`}>
                                                        {summary.mcap_type}
                                                    </span>
                                                )}
                                                <span className="shp-mcap-info">
                                                    <span className="shp-mcap-info-icon">i</span>
                                                    <span className="shp-mcap-tooltip">
                                                        Market cap as on {summary.mcap_updated_at ? new Date(summary.mcap_updated_at).toLocaleDateString('en-IN') : 'N/A'}
                                                    </span>
                                                </span>
                                            </span>
                                        )}
                                        {summary.shares_outstanding && (
                                            <span className="shp-mcap-chip">
                                                Shares: {fmt(summary.shares_outstanding)}
                                                <span className="shp-mcap-info">
                                                    <span className="shp-mcap-info-icon">i</span>
                                                    <span className="shp-mcap-tooltip">
                                                        As on {summary.shares_last_updated_at ? new Date(summary.shares_last_updated_at).toLocaleDateString('en-IN') : 'N/A'}
                                                    </span>
                                                </span>
                                            </span>
                                        )}
                                    </div>
                                </div>
                                <div className="shp-stat-chips">
                                    <div className="shp-stat-chip">
                                        <span className="shp-stat-label">Ownership %</span>
                                        <span className="shp-stat-value shp-accent" style={{ color: '#10b981' }}>
                                            {summary.ownership_percent !== null ? `${summary.ownership_percent.toFixed(2)}%` : <MissingData inline />}
                                        </span>
                                    </div>
                                    <div className="shp-stat-chip">
                                        <span className="shp-stat-label">Funds</span>
                                        <span className="shp-stat-value">{summary.total_funds}</span>
                                    </div>
                                    <div className="shp-stat-chip">
                                        <span className="shp-stat-label">Total MF Shares</span>
                                        <span className="shp-stat-value shp-accent">{fmt(summary.total_shares)}</span>
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* ── Partial Data Warning Banner ── */}
                        {summary.data_warning && (
                            <div style={{
                                background: 'rgba(234, 179, 8, 0.08)',
                                border: '1px solid rgba(234, 179, 8, 0.35)',
                                borderRadius: '8px', padding: '10px 14px',
                                marginBottom: '16px', fontSize: '12px',
                                color: 'var(--text-primary)', lineHeight: '1.5'
                            }}>
                                <div style={{ display: 'flex', alignItems: 'flex-start', gap: '10px' }}>
                                    <span style={{ fontSize: '16px', flexShrink: 0 }}>⚠️</span>
                                    <div style={{ flex: 1 }}>
                                        <strong style={{ color: '#eab308' }}>Partial Data — {summary.data_warning.latest_label}</strong>
                                        <span style={{ color: 'var(--text-secondary)', marginLeft: '6px' }}>
                                            <strong style={{ color: 'var(--text-primary)' }}>
                                                {summary.data_warning.amcs_uploaded} of {summary.data_warning.amcs_expected} AMCs
                                            </strong> have submitted {summary.data_warning.latest_label} data
                                            {summary.data_warning.amcs_pending > 0 && (
                                                <span style={{ color: '#f87171' }}> ({summary.data_warning.amcs_pending} still pending)</span>
                                            )}.
                                            {' '}Summary figures (Ownership%, Funds, Total Shares) reflect
                                            {' '}<strong>{summary.data_warning.complete_label}</strong> — the last complete month.
                                            Banner disappears automatically once all AMCs upload.
                                        </span>
                                    </div>
                                    <button
                                        onClick={() => setShowAmcStatus(s => !s)}
                                        style={{
                                            flexShrink: 0, fontSize: '11px', fontWeight: '600',
                                            padding: '3px 10px', borderRadius: '4px', cursor: 'pointer',
                                            background: 'rgba(234,179,8,0.15)',
                                            border: '1px solid rgba(234,179,8,0.4)',
                                            color: '#eab308', whiteSpace: 'nowrap'
                                        }}
                                    >
                                        {showAmcStatus ? 'Hide' : 'View AMC Status'}
                                    </button>
                                </div>

                                {/* ── Expandable AMC Status Panel ── */}
                                {showAmcStatus && (
                                    <div style={{
                                        display: 'grid', gridTemplateColumns: '1fr 1fr',
                                        gap: '12px', marginTop: '12px', paddingTop: '12px',
                                        borderTop: '1px solid rgba(234,179,8,0.2)'
                                    }}>
                                        {/* Pending AMCs */}
                                        <div>
                                            <div style={{ fontSize: '11px', fontWeight: '700', color: '#f87171', marginBottom: '6px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                                                ⏳ Pending ({summary.data_warning.amcs_pending})
                                            </div>
                                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                                {(summary.data_warning.pending_amc_names || []).map(amc => (
                                                    <span key={amc} style={{
                                                        fontSize: '10px', padding: '2px 7px',
                                                        borderRadius: '3px', fontWeight: '500',
                                                        background: 'rgba(248,113,113,0.1)',
                                                        border: '1px solid rgba(248,113,113,0.3)',
                                                        color: '#f87171'
                                                    }}>{amc}</span>
                                                ))}
                                                {(!summary.data_warning.pending_amc_names?.length) && (
                                                    <span style={{ color: 'var(--text-secondary)', fontSize: '11px' }}>None — all uploaded!</span>
                                                )}
                                            </div>
                                        </div>
                                        {/* Uploaded AMCs */}
                                        <div>
                                            <div style={{ fontSize: '11px', fontWeight: '700', color: '#10b981', marginBottom: '6px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                                                ✅ Uploaded ({summary.data_warning.amcs_uploaded})
                                            </div>
                                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                                {(summary.data_warning.uploaded_amc_names || []).map(amc => (
                                                    <span key={amc} style={{
                                                        fontSize: '10px', padding: '2px 7px',
                                                        borderRadius: '3px', fontWeight: '500',
                                                        background: 'rgba(16,185,129,0.08)',
                                                        border: '1px solid rgba(16,185,129,0.25)',
                                                        color: '#10b981'
                                                    }}>{amc}</span>
                                                ))}
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        )}

                        <div className="shp-summary-table-wrap" style={{ marginBottom: '24px' }}>
                            <table className="shp-table shp-summary-table">
                                <thead>
                                    <tr><th className="shp-th shp-th-month-group" colSpan={displayMonths.length} style={{ textAlign: 'center' }}>Industry Holding</th></tr>
                                    <tr>{displayMonths.map((m, i) => {
                                        const isPartialMonth = summary.data_warning && m === summary.data_warning.latest_label;
                                        return (
                                            <th key={m} className={`shp-th shp-th-sub shp-month-${i}`} style={{ textAlign: 'center' }}>
                                                {m}{isPartialMonth && <span title="Partial data — not all AMCs have uploaded yet" style={{ marginLeft: '4px', color: '#eab308', fontSize: '10px' }}>⚠️ Partial</span>}
                                            </th>
                                        );
                                    })}</tr>
                                </thead>
                                <tbody>
                                    <tr className="shp-row">
                                        {displayMonths.map((m, i) => {
                                            const trendData = summary.monthly_trend.find(t => t.month === m);
                                            if (!trendData) return <td key={m} className={`shp-td shp-num shp-month-${i}`}><MissingData /></td>;
                                            const isPos = trendData.month_change > 0;
                                            return (
                                                <td key={m} className={`shp-td shp-num shp-month-${i}`} style={{ textAlign: 'center' }}>
                                                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '3px' }}>
                                                        <div style={{ fontWeight: 600, display: 'flex', alignItems: 'center', gap: '4px' }}>
                                                            {fmt(trendData.total_shares)}
                                                            {trendData.month_change !== 0 && <span className={isPos ? 'shp-pos' : 'shp-neg'} style={{ fontSize: '10px' }}>{isPos ? '▲' : '▼'}</span>}
                                                        </div>
                                                        {trendData.ownership_percent !== null && (
                                                            <div className="shp-stake-badge" style={{ fontSize: '10px', color: '#10b981' }}>
                                                                {trendData.ownership_percent.toFixed(2)}%
                                                            </div>
                                                        )}
                                                    </div>
                                                </td>
                                            );
                                        })}
                                    </tr>
                                </tbody>
                            </table>
                        </div>

                        <div className="shp-table-wrap">
                            <div className="shp-controls-row shp-controls-header">
                                <div className="shp-view-toggle">
                                    <button className={`shp-toggle-btn ${viewMode === 'scheme' ? 'active' : ''}`} onClick={() => setViewMode('scheme')}>Scheme-wise</button>
                                    <button className={`shp-toggle-btn ${viewMode === 'amc' ? 'active' : ''}`} onClick={() => setViewMode('amc')}>AMC-wise</button>
                                </div>
                                <div className="shp-filter-toggle" style={{ display: 'flex', gap: '4px' }}>
                                    <button className={`shp-toggle-btn ${!filterParam ? 'active' : ''}`} onClick={() => { searchParams.delete('filter'); setSearchParams(searchParams); }}>All</button>
                                    <button className={`shp-toggle-btn ${filterParam === 'entrants' ? 'active' : ''}`} onClick={() => { searchParams.set('filter', 'entrants'); setSearchParams(searchParams); }}>Entrants</button>
                                    <button className={`shp-toggle-btn ${filterParam === 'exits' ? 'active' : ''}`} onClick={() => { searchParams.set('filter', 'exits'); setSearchParams(searchParams); }}>Exits</button>
                                </div>
                                <div className="shp-aum-toggle" style={{ display: 'flex', background: 'var(--bg-tertiary)', padding: '2px', borderRadius: '6px', border: '1px solid var(--border-color)', fontSize: '11px', fontWeight: '500', marginLeft: 'auto' }}>
                                    <button
                                        onClick={() => setAumViewMode('total')}
                                        style={{ padding: '4px 8px', borderRadius: '4px', border: 'none', background: aumViewMode === 'total' ? 'var(--accent-primary)' : 'transparent', color: aumViewMode === 'total' ? '#fff' : 'var(--text-secondary)', cursor: 'pointer', transition: 'all 0.2s' }}
                                    >
                                        Total AUM
                                    </button>
                                    <button
                                        onClick={() => setAumViewMode('equity')}
                                        style={{ padding: '4px 8px', borderRadius: '4px', border: 'none', background: aumViewMode === 'equity' ? 'var(--accent-primary)' : 'transparent', color: aumViewMode === 'equity' ? '#fff' : 'var(--text-secondary)', cursor: 'pointer', transition: 'all 0.2s' }}
                                    >
                                        Equity AUM
                                    </button>
                                </div>
                                <div className="shp-controls-right" style={{ marginLeft: '12px' }}>
                                    <select className="shp-picker-select" value={selectedMonth || ''} onChange={(e) => handleMonthChange(e.target.value)}>
                                        <option value="">Latest</option>
                                        {allAvailableMonths.map(m => <option key={m} value={m}>{m}</option>)}
                                    </select>
                                    <input type="text" className="shp-search-input" placeholder="Filter..." value={filterText} onChange={(e) => setFilterText(e.target.value)} />
                                    <ExportButton
                                        getData={() => {
                                            if (viewMode === 'scheme') {
                                                // Scheme-wise: mirrors UI table exactly
                                                return schemesToRender.map(item => {
                                                    const row = {
                                                        scheme_name: item.scheme_name,
                                                        amc_name: item.amc_name,
                                                        plan_type: item.plan_type ?? '',
                                                        option_type: item.option_type ?? '',
                                                        // AUM column respects the Total/Equity toggle
                                                        aum_cr: aumViewMode === 'equity'
                                                            ? (item.equity_aum_cr ?? null)
                                                            : (item.total_aum_cr ?? null),
                                                    };
                                                    displayMonths.forEach((m, idx) => {
                                                        const h = (item.history || []).find(x => x.month === m);
                                                        // % AUM only for the first (latest) month, matching the UI
                                                        if (idx === 0) row[`pct_aum_${m}`] = h?.percent_to_aum ?? null;
                                                        row[`own_pct_${m}`] = h?.ownership_percent ?? null;
                                                        row[`qty_${m}`] = h?.num_shares ?? null;
                                                        row[`change_${m}`] = h?.month_change ?? null;
                                                        // Change % only for latest month
                                                        if (idx === 0) {
                                                            const prev = h?.num_shares != null && h?.month_change != null
                                                                ? (h.num_shares - h.month_change) : null;
                                                            row[`change_pct_${m}`] = prev && prev !== 0
                                                                ? (h.month_change / prev) * 100 : null;
                                                        }
                                                    });
                                                    return row;
                                                });
                                            } else {
                                                // AMC-wise: mirrors UI table exactly
                                                return amcToRender.map(item => {
                                                    const row = {
                                                        amc_name: item.amc_name,
                                                        scheme_count: item.scheme_count,
                                                        aum_cr: aumViewMode === 'equity'
                                                            ? (item.equity_aum_cr ?? null)
                                                            : (item.total_aum_cr ?? null),
                                                    };
                                                    displayMonths.forEach((m, idx) => {
                                                        const h = (item.history || []).find(x => x.month === m);
                                                        row[`own_pct_${m}`] = h?.ownership_percent ?? null;
                                                        row[`qty_${m}`] = h?.num_shares ?? null;
                                                        row[`change_${m}`] = h?.month_change ?? null;
                                                    });
                                                    return row;
                                                });
                                            }
                                        }}
                                        columns={viewMode === 'scheme'
                                            ? [
                                                { key: 'scheme_name', label: 'Scheme Name', exportFormat: 'string' },
                                                { key: 'amc_name', label: 'AMC', exportFormat: 'string' },
                                                { key: 'plan_type', label: 'Plan', exportFormat: 'string' },
                                                { key: 'option_type', label: 'Option', exportFormat: 'string' },
                                                { key: 'aum_cr', label: aumViewMode === 'equity' ? 'Equity AUM (Cr)' : 'Total AUM (Cr)', exportFormat: 'numeric' },
                                                // Latest month has % AUM + Own% + Qty + Change + Change%
                                                ...(displayMonths[0] ? [
                                                    { key: `pct_aum_${displayMonths[0]}`, label: `% AUM (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                    { key: `own_pct_${displayMonths[0]}`, label: `Ownership % (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                    { key: `qty_${displayMonths[0]}`, label: `Qty / Shares (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                    { key: `change_${displayMonths[0]}`, label: `Change in Qty (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                    { key: `change_pct_${displayMonths[0]}`, label: `Change % (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                ] : []),
                                                // Older months: Own%, Qty, Change only (matches UI)
                                                ...displayMonths.slice(1).flatMap(m => [
                                                    { key: `own_pct_${m}`, label: `Ownership % (${m})`, exportFormat: 'numeric' },
                                                    { key: `qty_${m}`, label: `Qty / Shares (${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Change in Qty (${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]
                                            : [
                                                { key: 'amc_name', label: 'AMC Name', exportFormat: 'string' },
                                                { key: 'scheme_count', label: 'Schemes', exportFormat: 'numeric' },
                                                { key: 'aum_cr', label: aumViewMode === 'equity' ? 'Equity AUM (Cr)' : 'Total AUM (Cr)', exportFormat: 'numeric' },
                                                ...displayMonths.flatMap(m => [
                                                    { key: `own_pct_${m}`, label: `Ownership % (${m})`, exportFormat: 'numeric' },
                                                    { key: `qty_${m}`, label: `Qty / Shares (${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Change in Qty (${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]
                                        }
                                        pdfColumns={viewMode === 'scheme'
                                            ? [
                                                { key: 'scheme_name', label: 'Scheme Name', exportFormat: 'string' },
                                                { key: 'amc_name', label: 'AMC', exportFormat: 'string' },
                                                { key: 'plan_type', label: 'Plan', exportFormat: 'string' },
                                                { key: 'aum_cr', label: aumViewMode === 'equity' ? 'Eq.AUM(Cr)' : 'Tot.AUM(Cr)', exportFormat: 'numeric' },
                                                ...displayMonths.flatMap((m, idx) => [
                                                    ...(idx === 0 ? [{ key: `pct_aum_${m}`, label: `%AUM(${m})`, exportFormat: 'numeric' }] : []),
                                                    { key: `own_pct_${m}`, label: `Own%(${m})`, exportFormat: 'numeric' },
                                                    { key: `qty_${m}`, label: `Qty(${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Chg(${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]
                                            : [
                                                { key: 'amc_name', label: 'AMC', exportFormat: 'string' },
                                                { key: 'scheme_count', label: 'Schemes', exportFormat: 'numeric' },
                                                { key: 'aum_cr', label: aumViewMode === 'equity' ? 'Eq.AUM(Cr)' : 'Tot.AUM(Cr)', exportFormat: 'numeric' },
                                                ...displayMonths.flatMap(m => [
                                                    { key: `own_pct_${m}`, label: `Own%(${m})`, exportFormat: 'numeric' },
                                                    { key: `qty_${m}`, label: `Qty(${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Chg(${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]
                                        }
                                        fileNameConfig={{
                                            page: 'stock-holdings',
                                            filters: {
                                                stock: summary?.company_name,
                                                period: displayMonths[0],
                                                view: viewMode,
                                            },
                                        }}
                                        metadata={{
                                            title: `Stock Holdings \u2014 ${summary?.company_name || ''}`,
                                            filters: {
                                                'Stock': summary?.company_name,
                                                'ISIN': summary?.isin,
                                                'Sector': summary?.sector,
                                                'LTP (\u20b9)': livePrice ? `\u20b9${livePrice}` : undefined,
                                                'Market Cap': summary?.market_cap ? `\u20b9${Number(summary.market_cap).toLocaleString('en-IN', { maximumFractionDigits: 2 })} Cr (${summary.mcap_type || ''})` : undefined,
                                                'Total Shares (Company)': summary?.shares_outstanding ? Number(summary.shares_outstanding).toLocaleString('en-IN') : undefined,
                                                'MF Ownership %': summary?.ownership_percent ? `${summary.ownership_percent.toFixed(2)}%` : undefined,
                                                'Total MF Funds': summary?.total_funds,
                                                'Total MF Shares': summary?.total_shares ? Number(summary.total_shares).toLocaleString('en-IN') : undefined,
                                                'Displayed Periods': displayMonths.join(', '),
                                                'View': viewMode === 'scheme' ? 'Scheme-wise' : 'AMC-wise',
                                                'AUM Mode': aumViewMode === 'total' ? 'Total AUM' : 'Equity AUM',
                                                'Fund Filter': filterParam === 'entrants' ? 'New Entrants Only' : filterParam === 'exits' ? 'Exits Only' : undefined,
                                                'Search': filterText || undefined,
                                            },
                                        }}
                                    />
                                </div>
                            </div>

                            <div className="shp-table-inner">
                                <table className="shp-table">
                                    <thead>
                                        <tr>
                                            <th className="shp-th" rowSpan={2} onClick={() => handleSort(viewMode === 'scheme' ? 'scheme_name' : 'amc_name')}>Name{sortArrow(viewMode === 'scheme' ? 'scheme_name' : 'amc_name')}</th>
                                            {displayMonths.map((m, i) => <th key={m} className={`shp-th shp-month-${i}`} colSpan={i === 0 ? 4 : 3}>{m}</th>)}
                                        </tr>
                                        <tr>
                                            {displayMonths.map((m, i) => (
                                                <React.Fragment key={m}>
                                                    {i === 0 && (
                                                        <th className="shp-th shp-th-sub shp-month-0 sortable" onClick={() => handleSort('pnav')}>
                                                            % AUM {sortArrow('pnav')}
                                                        </th>
                                                    )}
                                                    <th className={`shp-th shp-th-sub shp-month-${i} sortable`} onClick={() => handleSort(`own_${i}`)}>
                                                        Own % {sortArrow(`own_${i}`)}
                                                        <div className="shp-mcap-info" style={{ display: 'inline-flex', marginLeft: '4px' }}>
                                                            <span className="shp-mcap-info-icon" style={{ width: '12px', height: '12px', fontSize: '9px' }}>i</span>
                                                            <span className="shp-mcap-tooltip" style={{ textTransform: 'none', fontWeight: '400' }}>
                                                                (Shares Held / Total Outstanding Shares) * 100
                                                            </span>
                                                        </div>
                                                    </th>
                                                    <th className={`shp-th shp-th-sub shp-month-${i} sortable`} onClick={() => handleSort(`shares_${i}`)}>
                                                        Qty {sortArrow(`shares_${i}`)}
                                                    </th>
                                                    <th className={`shp-th shp-th-sub shp-month-${i} sortable`} onClick={() => handleSort(`change_${i}`)}>
                                                        Change {sortArrow(`change_${i}`)}
                                                    </th>
                                                </React.Fragment>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {(viewMode === 'scheme' ? schemesToRender : amcToRender).map((item, idx) => (
                                            <tr key={idx} className="shp-row">
                                                <td className="shp-td shp-td-name">
                                                    <div className="shp-fund-name">{viewMode === 'scheme' ? item.scheme_name : item.amc_name}</div>
                                                    <div className="shp-fund-sub">
                                                        {viewMode === 'scheme'
                                                            ? item.amc_name
                                                            : `${item.scheme_count} schemes | ${aumViewMode === 'total' ? 'Total' : 'EQ'} AUM: ₹${fmtCr(aumViewMode === 'total' ? item.total_aum_cr : item.equity_aum_cr)} Cr`}
                                                    </div>
                                                </td>
                                                {displayMonths.map((m, i) => {
                                                    const history = (item.history || []).find(h => h.month === m);
                                                    return (
                                                        <React.Fragment key={m}>
                                                            {i === 0 && <td className="shp-td shp-num shp-month-0">{fmtPct(history?.percent_to_aum) || <MissingData />}</td>}
                                                            <td className={`shp-td shp-num shp-month-${i}`} style={{ color: '#10b981', fontWeight: '600' }}>
                                                                {fmtPct(history?.ownership_percent) || <MissingData />}
                                                            </td>
                                                            <td className={`shp-td shp-num shp-month-${i}`}>{fmt(history?.num_shares) || <MissingData />}</td>
                                                            <td className={`shp-td shp-num shp-month-${i}`}><ChangeCell change={history?.month_change} pct={history?.percent_change} /></td>
                                                        </React.Fragment>
                                                    );
                                                })}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </div >
    );
}
