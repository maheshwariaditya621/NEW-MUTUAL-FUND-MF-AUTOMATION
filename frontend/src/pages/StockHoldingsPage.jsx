import React, { useState, useMemo, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import { searchStocks, getStockHoldings } from '../api/stocks';
import { handleApiError } from '../api/client';
import { formatNumber } from '../utils/helpers';
import './StockHoldingsPage.css';

// ── helpers ──────────────────────────────────────────────────────────────────
const fmt = (n) => (n != null && n !== 0) ? Number(n).toLocaleString('en-IN') : '-';
const fmtCr = (n) => (n != null ? Number(n).toLocaleString('en-IN', { maximumFractionDigits: 2 }) : '-');
const fmtPct = (n, digits = 2) => (n != null ? `${Number(n).toFixed(digits)}%` : '-');

// ── Month label helpers ───────────────────────────────────────────────────────
// Label format used throughout: "JAN-26", "DEC-25", etc.
const MONTH_NAMES = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];

function labelToDate(label) {
    // "JAN-26" → Date(2026, 0)
    const [mon, yr] = label.split('-');
    const m = MONTH_NAMES.indexOf(mon.toUpperCase());
    const y = parseInt(yr, 10) + 2000;
    return new Date(y, m);
}

function dateToLabel(date) {
    // Date(2026, 0) → "JAN-26"
    return `${MONTH_NAMES[date.getMonth()]}-${String(date.getFullYear()).slice(2)}`;
}

// Returns array of N month labels ending at endLabel (newest first)
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
    if (change == null || change === 0) return <span className="shp-neutral">-</span>;
    const isPos = change > 0;
    const isNeg = change < 0;
    const cls = isPos ? 'shp-pos' : isNeg ? 'shp-neg' : 'shp-neutral';
    const sign = isPos ? '+' : '';

    return (
        <div className="shp-change-cell">
            <span className={cls}>{sign}{fmt(change)}</span>
            {pct != null && (
                <span className={`shp-change-pct ${cls}`}>
                    {sign}{pct.toFixed(2)}%
                </span>
            )}
        </div>
    );
}

// ── Main Component ────────────────────────────────────────────────────────────
export default function StockHoldingsPage() {
    const [searchParams] = useSearchParams();
    const isinParam = searchParams.get('isin');

    const [holdings, setHoldings] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [viewMode, setViewMode] = useState('scheme'); // 'scheme' | 'amc'
    const [filterText, setFilterText] = useState('');
    const [sortConfig, setSortConfig] = useState({ key: 'shares_0', direction: 'desc' });
    const [selectedMonth, setSelectedMonth] = useState(null); // label of selected end month

    // ── Available months for the dropdown ─────────────────────────────────────
    // We collect all unique month labels ever seen across API responses
    // and derive a sorted dropdown list from them.
    const [allAvailableMonths, setAllAvailableMonths] = useState([]); // newest-first sorted list

    // After new data arrives, expand the known-months list
    useEffect(() => {
        if (!holdings?.holdings?.[0]?.history) return;
        const seen = new Set(holdings.holdings[0].history.map(h => h.month));
        setAllAvailableMonths(prev => {
            const merged = Array.from(new Set([...prev, ...seen]));
            merged.sort((a, b) => labelToDate(b) - labelToDate(a)); // newest first
            return merged;
        });
    }, [holdings]);

    // ── displayMonths: always exactly 3 months ending at the selected period ──
    // Computed deterministically — not derived from API response.
    const displayMonths = useMemo(() => {
        // If user has selected a period AND we have data, use that period.
        if (selectedMonth) return computeDisplayMonths(selectedMonth, 3);
        // Default: use the latest month from what the API returned.
        const latest = holdings?.holdings?.[0]?.history?.[0]?.month;
        if (latest) return computeDisplayMonths(latest, 3);
        return [];
    }, [selectedMonth, holdings]);

    useEffect(() => {
        if (isinParam) fetchHoldings(isinParam, null);
    }, [isinParam]);

    const fetchHoldings = async (isin, endMonth) => {
        setLoading(true);
        setError(null);
        setHoldings(null);
        try {
            const data = await getStockHoldings(isin, 3, endMonth);
            setHoldings(data);
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    const handleMonthChange = (monthLabel) => {
        // monthLabel: "JAN-26" | "" (latest)
        const val = monthLabel || null;
        setSelectedMonth(val);
        if (isinParam) fetchHoldings(isinParam, val);
    };

    const handleSort = (key) => {
        setSortConfig(prev => ({
            key,
            direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
        }));
    };

    const sortArrow = (key) =>
        sortConfig.key === key ? (sortConfig.direction === 'asc' ? ' ↑' : ' ↓') : '';

    // ── AMC Aggregation ───────────────────────────────────────────────────────
    const amcAggregated = useMemo(() => {
        if (!holdings?.holdings) return [];
        const map = {};
        for (const scheme of holdings.holdings) {
            const amc = scheme.amc_name;
            if (!map[amc]) {
                map[amc] = {
                    amc_name: amc,
                    scheme_count: 0,
                    aum_cr: 0,
                    history: scheme.history.map(h => ({ ...h, num_shares: null, month_change: 0 })),
                };
            }
            const entry = map[amc];
            entry.scheme_count += 1;
            entry.aum_cr += (parseFloat(scheme.aum_cr) || 0);
            scheme.history.forEach((h, idx) => {
                if (entry.history[idx]) {
                    if (h.num_shares !== null) {
                        if (entry.history[idx].num_shares === null) {
                            entry.history[idx].num_shares = 0;
                        }
                        entry.history[idx].num_shares += h.num_shares;
                    }
                    entry.history[idx].month_change = (entry.history[idx].month_change || 0) + (h.month_change || 0);
                    // Stake % for AMC
                    if (holdings.shares_outstanding) {
                        entry.history[idx].stake_pct = (entry.history[idx].num_shares / holdings.shares_outstanding) * 100;
                    }
                }
            });
        }
        return Object.values(map);
    }, [holdings]);

    // ── Filtered + Sorted scheme list ─────────────────────────────────────────
    const schemesToRender = useMemo(() => {
        if (!holdings?.holdings) return [];
        let items = [...holdings.holdings];
        if (filterText) {
            const low = filterText.toLowerCase();
            items = items.filter(h =>
                h.scheme_name.toLowerCase().includes(low) ||
                h.amc_name.toLowerCase().includes(low)
            );
        }
        items.sort((a, b) => {
            let aVal, bVal;
            if (sortConfig.key === 'scheme_name') { aVal = a.scheme_name; bVal = b.scheme_name; }
            else if (sortConfig.key === 'aum_cr') { aVal = parseFloat(a.aum_cr) || 0; bVal = parseFloat(b.aum_cr) || 0; }
            else if (sortConfig.key === 'pnav') { aVal = parseFloat(a.history[0]?.percent_to_aum) || 0; bVal = parseFloat(b.history[0]?.percent_to_aum) || 0; }
            else if (sortConfig.key.startsWith('shares_')) {
                const idx = parseInt(sortConfig.key.split('_')[1]);
                aVal = a.history[idx]?.num_shares || 0; bVal = b.history[idx]?.num_shares || 0;
            }
            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });
        return items;
    }, [holdings, filterText, sortConfig]);

    // ── Filtered + Sorted AMC list ────────────────────────────────────────────
    const amcToRender = useMemo(() => {
        let items = [...amcAggregated];
        if (filterText) {
            const low = filterText.toLowerCase();
            items = items.filter(h => h.amc_name.toLowerCase().includes(low));
        }
        items.sort((a, b) => {
            let aVal, bVal;
            if (sortConfig.key === 'amc_name') { aVal = a.amc_name; bVal = b.amc_name; }
            else if (sortConfig.key === 'aum_cr') { aVal = a.aum_cr; bVal = b.aum_cr; }
            else if (sortConfig.key.startsWith('shares_')) {
                const idx = parseInt(sortConfig.key.split('_')[1]);
                aVal = a.history[idx]?.num_shares || 0; bVal = b.history[idx]?.num_shares || 0;
            }
            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });
        return items;
    }, [amcAggregated, filterText, sortConfig]);

    // ── Shared Table Header ───────────────────────────────────────────────────
    // Latest month: Fund AUM | % AUM | Shares Held | Month Change | % Change  (5 sub-cols)
    // Older months: Shares Held | Month Change (pct inline) | % AUM            (3 sub-cols)
    const renderSchemeHeader = () => (
        <thead>
            {/* Row 1: Group headers */}
            <tr>
                <th className="shp-th shp-th-name" rowSpan={2}>
                    <span className="shp-sortable" onClick={() => handleSort('scheme_name')}>
                        Fund Name{sortArrow('scheme_name')}
                    </span>
                </th>
                {/* Latest month: 4 sub-cols */}
                {displayMonths[0] && (
                    <th className="shp-th shp-th-month-group shp-month-0" colSpan={4}>
                        {displayMonths[0]}
                    </th>
                )}
                {/* Older months: 2 sub-cols each */}
                {displayMonths.slice(1).map((m, i) => (
                    <th key={m} className={`shp-th shp-th-month-group shp-month-${i + 1}`} colSpan={2}>
                        {m}
                    </th>
                ))}
            </tr>
            {/* Row 2: Sub-column headers */}
            <tr>
                {/* Latest month sub-cols */}
                {displayMonths[0] && (
                    <>
                        {/* First sub-col gets shp-group-start for bold group border */}
                        <th className="shp-th shp-th-sub shp-month-0 shp-group-start">
                            <span className="shp-sortable" onClick={() => handleSort('aum_cr')}>
                                Fund AUM (Cr){sortArrow('aum_cr')}
                            </span>
                        </th>
                        <th className="shp-th shp-th-sub shp-month-0">
                            <span className="shp-sortable" onClick={() => handleSort('pnav')}>
                                % of AUM{sortArrow('pnav')}
                            </span>
                        </th>
                        <th className="shp-th shp-th-sub shp-month-0">
                            <span className="shp-sortable" onClick={() => handleSort('shares_0')}>
                                Quantity{sortArrow('shares_0')}
                            </span>
                        </th>
                        <th className="shp-th shp-th-sub shp-month-0">Month Change</th>
                    </>
                )}
                {/* Older months sub-cols */}
                {displayMonths.slice(1).map((m, i) => (
                    <React.Fragment key={m}>
                        {/* First sub-col of older month gets shp-group-start */}
                        <th className={`shp-th shp-th-sub shp-month-${i + 1} shp-group-start`}>
                            <span className="shp-sortable" onClick={() => handleSort(`shares_${i + 1}`)}>
                                Quantity{sortArrow(`shares_${i + 1}`)}
                            </span>
                        </th>
                        <th className={`shp-th shp-th-sub shp-month-${i + 1}`}>Month Change</th>
                    </React.Fragment>
                ))}
            </tr>
        </thead>
    );

    const renderSchemeRow = (scheme, idx) => {
        // Build a lookup map: month label → history entry
        const histMap = {};
        (scheme.history || []).forEach(h => { histMap[h.month] = h; });

        return (
            <tr key={idx} className="shp-row">
                {/* Fund Name + AMC below */}
                <td className="shp-td shp-td-name">
                    <div className="shp-fund-name">{scheme.scheme_name}</div>
                    <div className="shp-fund-sub">{scheme.amc_name}</div>
                </td>

                {/* Latest month (displayMonths[0]): 4 cols */}
                {displayMonths[0] && (() => {
                    const h = histMap[displayMonths[0]];
                    return h ? (
                        <>
                            <td className="shp-td shp-td-num shp-month-0 shp-group-start">{fmtCr(scheme.aum_cr)}</td>
                            <td className="shp-td shp-td-num shp-month-0">{fmtPct(h.percent_to_aum)}</td>
                            <td className="shp-td shp-td-num shp-month-0">{h.num_shares === null ? <span className="shp-not-uploaded" title="AMC Data Not Uploaded">N/A</span> : (h.num_shares > 0 ? fmt(h.num_shares) : '-')}</td>
                            <td className="shp-td shp-td-num shp-month-0">
                                <ChangeCell change={h.month_change} pct={h.percent_change} />
                            </td>
                        </>
                    ) : (
                        <>
                            <td className="shp-td shp-month-0 shp-group-start">-</td>
                            <td className="shp-td shp-month-0">-</td>
                            <td className="shp-td shp-month-0">-</td>
                            <td className="shp-td shp-month-0">-</td>
                        </>
                    );
                })()}

                {/* Older months: 2 cols each, look up by label */}
                {displayMonths.slice(1).map((monthLabel, hIdx) => {
                    const h = histMap[monthLabel];
                    return (
                        <React.Fragment key={monthLabel}>
                            <td className={`shp-td shp-td-num shp-month-${hIdx + 1} shp-group-start`}>
                                {h && h.num_shares === null ? <span className="shp-not-uploaded" title="AMC Data Not Uploaded">N/A</span> : (h && h.num_shares > 0 ? fmt(h.num_shares) : '-')}
                            </td>
                            <td className={`shp-td shp-td-num shp-month-${hIdx + 1}`}>
                                {h ? <ChangeCell change={h.month_change} pct={h.percent_change} /> : <span className="shp-neutral">-</span>}
                            </td>
                        </React.Fragment>
                    );
                })}
            </tr>
        );
    };

    // ══════════════════════════════════════════════════════════════════════════
    return (
        <div className="shp-page">
            <div className="shp-container">

                {loading && <Loading message="Loading stock holdings..." />}
                {error && (
                    <ErrorMessage
                        message={error}
                        onRetry={() => isinParam && fetchHoldings(isinParam, selectedMonth)}
                    />
                )}

                {holdings && !loading && !error && (
                    <>
                        {/* ── Compact Identity + Controls Bar ── */}
                        <div className="shp-identity-bar">

                            {/* Row 1: company identity left | stat chips right */}
                            <div className="shp-identity-row">
                                <div className="shp-identity-left">
                                    <span className="shp-company-name">{holdings.company_name}</span>
                                    <span className="shp-isin-chip">{holdings.isin}</span>
                                    {holdings.sector && (
                                        <span className="shp-sector-chip">{holdings.sector}</span>
                                    )}
                                    {holdings.market_cap && (
                                        <span className="shp-mcap-chip">
                                            ₹ {fmtCr(holdings.market_cap)} Cr
                                            {holdings.mcap_type && (
                                                <span className={`shp-mcap-badge ${holdings.mcap_type.toLowerCase().replace(' ', '-')}`}>
                                                    {holdings.mcap_type}
                                                </span>
                                            )}
                                            {/* (i) info button */}
                                            <span className="shp-mcap-info">
                                                <span className="shp-mcap-info-icon">i</span>
                                                <span className="shp-mcap-tooltip">
                                                    Market cap as on{' '}
                                                    {holdings.mcap_updated_at
                                                        ? new Date(holdings.mcap_updated_at).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })
                                                        : 'N/A'}
                                                </span>
                                            </span>
                                        </span>
                                    )}
                                    {holdings.shares_outstanding && (
                                        <span className="shp-mcap-chip">
                                            Outstanding Shares: {fmt(holdings.shares_outstanding)}
                                            {/* (i) info button */}
                                            <span className="shp-mcap-info">
                                                <span className="shp-mcap-info-icon">i</span>
                                                <span className="shp-mcap-tooltip">
                                                    Shares as on{' '}
                                                    {holdings.shares_last_updated_at
                                                        ? new Date(holdings.shares_last_updated_at).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })
                                                        : 'N/A'}
                                                </span>
                                            </span>
                                        </span>
                                    )}
                                </div>
                                <div className="shp-stat-chips">
                                    <div className="shp-stat-chip">
                                        <span className="shp-stat-label">Funds</span>
                                        <span className="shp-stat-value">{holdings.total_funds}</span>
                                    </div>
                                    <div className="shp-stat-chip">
                                        <span className="shp-stat-label">Total MF Shares</span>
                                        <span className="shp-stat-value shp-accent">{fmt(holdings.total_shares)}</span>
                                    </div>
                                </div>
                            </div>

                        </div>

                        {/* ── SUMMARY TABLE ── */}
                        <div className="shp-summary-table-wrap" style={{ marginBottom: '24px', position: 'relative', zIndex: 200 }}>
                            <table className="shp-table shp-summary-table">
                                <thead>
                                    <tr>
                                        <th className="shp-th shp-th-month-group" colSpan={displayMonths.length} style={{ textAlign: 'center' }}>Industry Holding</th>
                                    </tr>
                                    <tr>
                                        {displayMonths.map((m, i) => (
                                            <th key={m} className={`shp-th shp-th-sub shp-month-${i}`} style={{ textAlign: 'center' }}>
                                                {m}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr className="shp-row">
                                        {displayMonths.map((m, i) => {
                                            const trendData = holdings.monthly_trend.find(t => t.month === m);
                                            if (!trendData) return <td key={m} className={`shp-td shp-num shp-month-${i}`}>-</td>;

                                            const hasChange = trendData.month_change != null && trendData.month_change !== 0;
                                            const isPos = trendData.month_change > 0;
                                            return (
                                                <td key={m} className={`shp-td shp-num shp-month-${i}`} style={{ textAlign: 'center' }}>
                                                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '3px', padding: '4px 0' }}>
                                                        <div style={{ fontWeight: 600, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: '4px' }}>
                                                            {fmt(trendData.total_shares)}
                                                            {hasChange && (
                                                                <span className={isPos ? 'shp-pos' : 'shp-neg'} style={{ fontSize: '10px' }}>
                                                                    {isPos ? '▲' : '▼'}
                                                                </span>
                                                            )}
                                                        </div>
                                                        {hasChange && (
                                                            <div className={`shp-change-pct ${isPos ? 'shp-pos' : 'shp-neg'}`}>
                                                                {isPos ? '+' : ''}{fmt(trendData.month_change)} ({isPos ? '+' : ''}{trendData.percent_change.toFixed(2)}%)
                                                            </div>
                                                        )}
                                                        {holdings.shares_outstanding && (
                                                            <div className="shp-stake-badge shp-mcap-info" style={{ marginTop: '4px', fontSize: '10px', fontWeight: 700, background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 6px', borderRadius: '4px', cursor: 'help' }}>
                                                                Ownership: {((trendData.total_shares / holdings.shares_outstanding) * 100).toFixed(2)}%
                                                                <span className="shp-mcap-tooltip" style={{ fontSize: '10px', minWidth: '150px', textAlign: 'center', fontWeight: 'normal', top: '100%', bottom: 'auto', marginTop: '6px', padding: '6px 8px', zIndex: 9999, boxShadow: '0 4px 12px rgba(0,0,0,0.3)', borderRadius: '6px', color: '#f8fafc', whiteSpace: 'nowrap' }}>
                                                                    <div style={{ color: 'rgba(255,255,255,0.6)', fontSize: '8.5px', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.5px', fontWeight: 600 }}>Calculated As</div>
                                                                    <div style={{ fontWeight: 500, color: '#f8fafc', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px' }}>
                                                                        <span>Total shares held by all funds</span>
                                                                        <span style={{ color: 'rgba(255,255,255,0.4)', fontWeight: 300 }}>/</span>
                                                                        <span>Total outstanding shares</span>
                                                                    </div>
                                                                </span>
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

                        {/* ── DATA TABLES CONTAINER ── */}
                        <div className="shp-table-wrap" style={{ marginTop: '24px' }}>
                            {/* ── TABLE CONTROLS ── */}
                            <div className="shp-controls-row shp-controls-header">
                                <div className="shp-view-toggle">
                                    <button
                                        className={`shp-toggle-btn ${viewMode === 'scheme' ? 'active' : ''}`}
                                        onClick={() => { setViewMode('scheme'); setSortConfig({ key: 'shares_0', direction: 'desc' }); }}
                                    >
                                        Scheme-wise
                                    </button>
                                    <button
                                        className={`shp-toggle-btn ${viewMode === 'amc' ? 'active' : ''}`}
                                        onClick={() => { setViewMode('amc'); setSortConfig({ key: 'shares_0', direction: 'desc' }); }}
                                    >
                                        AMC-wise
                                    </button>
                                </div>
                                <div className="shp-controls-right">
                                    <div className="shp-month-picker">
                                        <label className="shp-picker-label">Period</label>
                                        <select
                                            className="shp-picker-select"
                                            value={selectedMonth || ''}
                                            onChange={(e) => handleMonthChange(e.target.value || null)}
                                        >
                                            <option value="">Latest</option>
                                            {allAvailableMonths.map(m => (
                                                <option key={m} value={m}>{m}</option>
                                            ))}
                                        </select>
                                    </div>
                                    <div className="shp-search">
                                        <input
                                            type="text"
                                            className="shp-search-input"
                                            placeholder={viewMode === 'amc' ? 'Filter AMC...' : 'Filter fund or AMC...'}
                                            value={filterText}
                                            onChange={(e) => setFilterText(e.target.value)}
                                        />
                                    </div>
                                </div>
                            </div>

                            {/* ── SCHEME TABLE ── */}
                            {viewMode === 'scheme' && (
                                <div className="shp-table-inner">
                                    <table className="shp-table">
                                        {renderSchemeHeader()}
                                        <tbody>
                                            {schemesToRender.map((scheme, idx) => renderSchemeRow(scheme, idx))}
                                            {schemesToRender.length === 0 && (
                                                <tr><td colSpan={1 + 4 + 2 * Math.max(displayMonths.length - 1, 0)} className="shp-empty">No results found.</td></tr>
                                            )}
                                        </tbody>
                                    </table>
                                </div>
                            )}

                            {/* ── AMC TABLE ── */}
                            {viewMode === 'amc' && (
                                <div className="shp-table-inner">
                                    <table className="shp-table">
                                        <thead>
                                            <tr>
                                                <th className="shp-th shp-th-name" rowSpan={2}>
                                                    <span className="shp-sortable" onClick={() => handleSort('amc_name')}>
                                                        AMC Name{sortArrow('amc_name')}
                                                    </span>
                                                </th>
                                                <th className="shp-th shp-th-num" rowSpan={2}>Schemes</th>
                                                {displayMonths[0] && (
                                                    <th className="shp-th shp-th-month-group shp-month-0" colSpan={3}>{displayMonths[0]}</th>
                                                )}
                                                {displayMonths.slice(1).map((m, i) => (
                                                    <th key={m} className={`shp-th shp-th-month-group shp-month-${i + 1}`} colSpan={3}>{m}</th>
                                                ))}
                                            </tr>
                                            <tr>
                                                {displayMonths[0] && (
                                                    <>
                                                        <th className="shp-th shp-th-sub shp-month-0 shp-group-start">
                                                            <span className="shp-sortable" onClick={() => handleSort('shares_0')}>Quantity{sortArrow('shares_0')}</span>
                                                        </th>
                                                        <th className="shp-th shp-th-sub shp-month-0">
                                                            <div className="shp-mcap-info" style={{ display: 'inline-flex', alignItems: 'center', cursor: 'help' }}>
                                                                Ownership %
                                                                <span className="shp-mcap-tooltip" style={{ fontSize: '10px', minWidth: '150px', textAlign: 'center', fontWeight: 'normal', top: '100%', bottom: 'auto', marginTop: '10px', padding: '6px 8px', zIndex: 9999, boxShadow: '0 4px 12px rgba(0,0,0,0.3)', borderRadius: '6px', color: '#f8fafc', whiteSpace: 'nowrap', textTransform: 'none', letterSpacing: 'normal' }}>
                                                                    <div style={{ color: 'rgba(255,255,255,0.6)', fontSize: '8.5px', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.5px', fontWeight: 600 }}>Calculated As</div>
                                                                    <div style={{ fontWeight: 500, color: '#f8fafc', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px' }}>
                                                                        <span>Total shares held by all funds</span>
                                                                        <span style={{ color: 'rgba(255,255,255,0.4)', fontWeight: 300 }}>/</span>
                                                                        <span>Total outstanding shares</span>
                                                                    </div>
                                                                </span>
                                                            </div>
                                                        </th>
                                                        <th className="shp-th shp-th-sub shp-month-0">Month Change</th>
                                                    </>
                                                )}
                                                {displayMonths.slice(1).map((m, i) => (
                                                    <React.Fragment key={m}>
                                                        <th className={`shp-th shp-th-sub shp-month-${i + 1} shp-group-start`}>
                                                            <span className="shp-sortable" onClick={() => handleSort(`shares_${i + 1}`)}>Quantity{sortArrow(`shares_${i + 1}`)}</span>
                                                        </th>
                                                        <th className={`shp-th shp-th-sub shp-month-${i + 1}`}>
                                                            <div className="shp-mcap-info" style={{ display: 'inline-flex', alignItems: 'center', cursor: 'help' }}>
                                                                Ownership %
                                                                <span className="shp-mcap-tooltip" style={{ fontSize: '10px', minWidth: '150px', textAlign: 'center', fontWeight: 'normal', top: '100%', bottom: 'auto', marginTop: '10px', padding: '6px 8px', zIndex: 9999, boxShadow: '0 4px 12px rgba(0,0,0,0.3)', borderRadius: '6px', color: '#f8fafc', whiteSpace: 'nowrap', textTransform: 'none', letterSpacing: 'normal' }}>
                                                                    <div style={{ color: 'rgba(255,255,255,0.6)', fontSize: '8.5px', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.5px', fontWeight: 600 }}>Calculated As</div>
                                                                    <div style={{ fontWeight: 500, color: '#f8fafc', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px' }}>
                                                                        <span>Total shares held by all funds</span>
                                                                        <span style={{ color: 'rgba(255,255,255,0.4)', fontWeight: 300 }}>/</span>
                                                                        <span>Total outstanding shares</span>
                                                                    </div>
                                                                </span>
                                                            </div>
                                                        </th>
                                                        <th className={`shp-th shp-th-sub shp-month-${i + 1}`}>Month Change</th>
                                                    </React.Fragment>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {amcToRender.map((amc, aIdx) => {
                                                const histMap = {};
                                                (amc.history || []).forEach(h => { histMap[h.month] = h; });

                                                return (
                                                    <tr key={aIdx} className="shp-row">
                                                        <td className="shp-td shp-td-name">
                                                            <div className="shp-fund-name">{amc.amc_name}</div>
                                                            <div className="shp-fund-sub">{amc.scheme_count} scheme{amc.scheme_count !== 1 ? 's' : ''}</div>
                                                        </td>
                                                        <td className="shp-td shp-td-num">
                                                            <span className="shp-scheme-badge">{amc.scheme_count}</span>
                                                        </td>
                                                        {displayMonths.map((monthLabel, hIdx) => {
                                                            const h = histMap[monthLabel];
                                                            return (
                                                                <React.Fragment key={monthLabel}>
                                                                    <td className={`shp-td shp-td-num shp-month-${hIdx} shp-group-start`}>
                                                                        {h && h.num_shares === null ? <span className="shp-not-uploaded" title="AMC Data Not Uploaded">N/A</span> : (h && h.num_shares > 0 ? fmt(h.num_shares) : '-')}
                                                                    </td>
                                                                    <td className={`shp-td shp-td-num shp-month-${hIdx}`} style={{ fontWeight: 600 }}>
                                                                        {holdings.shares_outstanding && h && h.num_shares != null ? fmtPct((h.num_shares / holdings.shares_outstanding) * 100, 4) : '-'}
                                                                    </td>
                                                                    <td className={`shp-td shp-td-num shp-month-${hIdx}`}>
                                                                        {h ? <ChangeCell change={h.month_change} pct={null} /> : <span className="shp-neutral">-</span>}
                                                                    </td>
                                                                </React.Fragment>
                                                            );
                                                        })}
                                                    </tr>
                                                );
                                            })}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </div>
                    </>
                )}

                {!isinParam && !loading && !error && (
                    <div className="shp-empty-state">
                        <div className="shp-empty-icon">📊</div>
                        <p>Search for a stock above to view its mutual fund holdings</p>
                    </div>
                )}
            </div>
        </div >
    );
}
