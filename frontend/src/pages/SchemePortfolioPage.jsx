import React, { useState, useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import { searchSchemes, getSchemePortfolio } from '../api/schemes';
import { handleApiError } from '../api/client';
import { formatCrores, formatPercent, formatNumber } from '../utils/helpers';
import './SchemePortfolioPage.css';

// ── helpers ──────────────────────────────────────────────────────────────────
const fmt = (n) => (n != null && n !== 0) ? Number(n).toLocaleString('en-IN') : '-';
const fmtCr = (n) => (n != null ? Number(n).toLocaleString('en-IN', { maximumFractionDigits: 2 }) : '-');
const fmtPct = (n, digits = 2) => (n != null ? `${Number(n).toFixed(digits)}%` : '-');

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

function computeDisplayMonths(endLabel, n = 4) {
    const end = labelToDate(endLabel);
    const result = [];
    for (let i = 0; i < n; i++) {
        const d = new Date(end.getFullYear(), end.getMonth() - i);
        result.push(dateToLabel(d));
    }
    return result;
}

function TrendIcon({ current, previous }) {
    if (previous == null || previous === 0) return null;
    const isUp = current > previous;
    const isDown = current < previous;
    if (!isUp && !isDown) return null;
    const cls = isUp ? 'shp-pos' : 'shp-neg';
    const char = isUp ? '▲' : '▼';
    return <span className={`rv-trend ${cls}`}>{char}</span>;
}

export default function SchemePortfolioPage() {
    const [searchParams] = useSearchParams();
    const schemeIdParam = searchParams.get('scheme_id');

    const [portfolio, setPortfolio] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [sortConfig, setSortConfig] = useState({ key: 'shares_0', direction: 'desc' });
    const [filterText, setFilterText] = useState('');
    const [selectedMonth, setSelectedMonth] = useState(null);

    const [allAvailableMonths, setAllAvailableMonths] = useState([]);

    useEffect(() => {
        if (!portfolio?.holdings?.[0]?.monthly_data) return;
        const seen = new Set(portfolio.holdings[0].monthly_data.map(m => m.month));
        setAllAvailableMonths(prev => {
            const merged = Array.from(new Set([...prev, ...seen]));
            merged.sort((a, b) => labelToDate(b) - labelToDate(a));
            return merged;
        });
    }, [portfolio]);

    const displayMonths = useMemo(() => {
        if (selectedMonth) return computeDisplayMonths(selectedMonth, 4);
        const aumList = portfolio?.monthly_aum;
        const latest = aumList?.[aumList.length - 1]?.month;
        if (latest) return computeDisplayMonths(latest, 4);
        return [];
    }, [selectedMonth, portfolio]);

    useEffect(() => {
        if (schemeIdParam) {
            handleSelectSchemeById(schemeIdParam, null);
        }
    }, [schemeIdParam]);

    const handleSelectSchemeById = async (schemeId, endMonth) => {
        setLoading(true);
        setError(null);
        setPortfolio(null);

        try {
            const data = await getSchemePortfolio(schemeId, 4, endMonth);
            setPortfolio(data);
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    const handleMonthChange = (monthLabel) => {
        const val = monthLabel || null;
        setSelectedMonth(val);
        if (schemeIdParam) handleSelectSchemeById(schemeIdParam, val);
    };

    const handleSort = (key) => {
        setSortConfig(prev => ({
            key,
            direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
        }));
    };

    const getHoldingsToRender = () => {
        if (!portfolio || !portfolio.holdings) return [];

        let items = [...portfolio.holdings];

        if (filterText) {
            const lowFilter = filterText.toLowerCase();
            items = items.filter(h =>
                h.company_name.toLowerCase().includes(lowFilter) ||
                (h.sector && h.sector.toLowerCase().includes(lowFilter)) ||
                (h.isin && h.isin.toLowerCase().includes(lowFilter))
            );
        }

        items.sort((a, b) => {
            let aVal, bVal;

            if (sortConfig.key === 'company_name') {
                aVal = a.company_name;
                bVal = b.company_name;
            } else if (sortConfig.key.startsWith('shares_')) {
                const monthIdx = parseInt(sortConfig.key.split('_')[1]);
                const monthLabel = displayMonths[monthIdx];
                const aM = a.monthly_data.find(m => m.month === monthLabel);
                const bM = b.monthly_data.find(m => m.month === monthLabel);
                aVal = aM ? aM.num_shares || 0 : 0;
                bVal = bM ? bM.num_shares || 0 : 0;
            }

            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });

        return items;
    };

    const renderSortArrow = (key) => {
        if (sortConfig.key !== key) return null;
        return <span style={{ marginLeft: '4px', opacity: 0.9 }}>{sortConfig.direction === 'asc' ? '↑' : '↓'}</span>;
    };

    return (
        <div className="scheme-portfolio-page">
            <div className="container-wide" style={{ padding: '20px' }}>

                {loading && <Loading message="Loading portfolio data..." />}
                {error && (
                    <ErrorMessage
                        message={error}
                        onRetry={() => schemeIdParam && handleSelectSchemeById(schemeIdParam, selectedMonth)}
                    />
                )}

                {portfolio && !loading && !error && (
                    <div className="portfolio-section">

                        <div className="shp-identity-bar" style={{ marginBottom: '16px', background: 'var(--bg-card)', padding: '16px', borderRadius: '8px', border: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <div className="shp-identity-left" style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
                                <div className="shp-company-name" style={{ fontSize: '18px', fontWeight: 'bold' }}>{portfolio.scheme_name}</div>
                                <span className="shp-sector-chip" style={{ background: 'var(--bg-tertiary)', padding: '2px 8px', borderRadius: '4px', fontSize: '11px', color: 'var(--text-secondary)', fontWeight: '500' }}>
                                    {portfolio.plan_type} • {portfolio.option_type}
                                </span>
                            </div>
                            <div className="shp-identity-right" style={{ display: 'flex', gap: '24px' }}>
                                <div className="shp-identity-stat" style={{ textAlign: 'right' }}>
                                    <div className="shp-stat-label" style={{ fontSize: '10px', color: 'var(--text-secondary)', fontWeight: '600', marginBottom: '4px' }}>AMC</div>
                                    <div className="shp-stat-val" style={{ color: 'var(--text-primary)', fontWeight: 'bold', fontSize: '14px' }}>{portfolio.amc_name}</div>
                                </div>
                                <div className="shp-identity-stat" style={{ paddingLeft: '24px', borderLeft: '1px solid var(--border-color)', textAlign: 'right' }}>
                                    <div className="shp-stat-label" style={{ fontSize: '10px', color: 'var(--text-secondary)', fontWeight: '600', marginBottom: '4px' }}>EQUITY COMPANIES</div>
                                    <div className="shp-stat-val" style={{ fontSize: '16px', fontWeight: 'bold', color: 'var(--accent-primary)' }}>{formatNumber(portfolio.total_holdings)}</div>
                                </div>
                            </div>
                        </div>

                        <div className="portfolio-table-section">
                            <div className="shp-table-wrap" style={{ borderTopLeftRadius: 8, borderTopRightRadius: 8 }}>
                                {/* ── Controls Header ── */}
                                <div className="shp-controls-header shp-controls-row">
                                    <div className="shp-section-title" style={{ margin: 0, color: 'var(--shp-header-color)' }}>Equity Holdings</div>
                                    <div className="shp-controls-right">
                                        <div className="shp-month-picker">
                                            <span className="shp-picker-label">Period</span>
                                            <select
                                                className="shp-picker-select"
                                                value={selectedMonth || ''}
                                                onChange={(e) => handleMonthChange(e.target.value)}
                                            >
                                                <option value="">Latest</option>
                                                {allAvailableMonths.map(m => (
                                                    <option key={m} value={m}>{m}</option>
                                                ))}
                                            </select>
                                        </div>
                                        <input
                                            type="text"
                                            className="shp-search-input"
                                            placeholder="Filter by Name, ISIN, Sector..."
                                            value={filterText}
                                            onChange={(e) => setFilterText(e.target.value)}
                                        />
                                    </div>
                                </div>
                                <table className="shp-table">
                                    <thead>
                                        <tr className="shp-head-row">
                                            <th rowSpan="2" className="shp-th shp-th-name" onClick={() => handleSort('company_name')} style={{ cursor: 'pointer' }}>
                                                COMPANY NAME {renderSortArrow('company_name')}
                                            </th>
                                            {displayMonths.map((monthLabel, idx) => {
                                                const mData = portfolio.monthly_aum.find(m => m.month === monthLabel);
                                                return (
                                                    <th key={idx} colSpan="2" className={`shp-th shp-th-month-group shp-month-${idx}`}>
                                                        <div className="shp-sortable" onClick={() => handleSort(`shares_${idx}`)}>
                                                            {monthLabel} {renderSortArrow(`shares_${idx}`)}
                                                        </div>
                                                        <div className="rv-aum-label" style={{ fontSize: '10px', marginTop: '2px', fontWeight: '400', opacity: 0.8 }}>
                                                            {mData && mData.aum_cr > 0 ? `AUM: ₹ ${formatNumber(mData.aum_cr)} (Cr.)` : 'AUM: ₹ - (Cr.)'}
                                                        </div>
                                                    </th>
                                                );
                                            })}
                                        </tr>
                                        <tr className="shp-head-row">
                                            {displayMonths.map((_, idx) => (
                                                <React.Fragment key={idx}>
                                                    <th className={`shp-th shp-th-num shp-th-sub shp-group-start shp-month-${idx}`}>% OF AUM</th>
                                                    <th className={`shp-th shp-th-num shp-th-sub shp-month-${idx}`}>SHARES HELD</th>
                                                </React.Fragment>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {getHoldingsToRender().map((holding, hIdx) => {
                                            const histMap = holding.monthly_data.reduce((acc, curr) => {
                                                acc[curr.month] = curr;
                                                return acc;
                                            }, {});

                                            return (
                                                <tr key={hIdx} className="shp-row">
                                                    <td className="shp-td shp-td-name">
                                                        <div className="shp-fund-name">{holding.company_name}</div>
                                                        <div className="shp-fund-sub">
                                                            {holding.isin} {holding.sector && `| ${holding.sector}`}
                                                        </div>
                                                    </td>
                                                    {displayMonths.map((monthLabel, mIdx) => {
                                                        const m = histMap[monthLabel];
                                                        // Previous month is conceptually the next index in our newest-first array
                                                        const prevMonthLabel = displayMonths[mIdx + 1];
                                                        const prevM = prevMonthLabel ? histMap[prevMonthLabel] : null;

                                                        return (
                                                            <React.Fragment key={mIdx}>
                                                                <td className={`shp-td shp-td-num shp-month-${mIdx} shp-group-start`}>
                                                                    {m && m.num_shares === null ? <span className="shp-not-uploaded" title="AMC Data Not Uploaded">N/A</span> : (m && m.num_shares > 0 ? fmtPct(m.percent_to_aum) : '-')}
                                                                </td>
                                                                <td className={`shp-td shp-td-num shp-month-${mIdx}`}>
                                                                    {m && m.num_shares === null ? <span className="shp-not-uploaded" title="AMC Data Not Uploaded">N/A</span> : (m && m.num_shares > 0 ? (
                                                                        <div className="rv-shares-cell" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px' }}>
                                                                            <span>{fmt(m.num_shares)}</span>
                                                                            <TrendIcon current={m.num_shares} previous={prevM?.num_shares} />
                                                                        </div>
                                                                    ) : '-')}
                                                                </td>
                                                            </React.Fragment>
                                                        );
                                                    })}
                                                </tr>
                                            );
                                        })}
                                        {getHoldingsToRender().length === 0 && (
                                            <tr>
                                                <td colSpan={1 + displayMonths.length * 2} className="shp-empty">
                                                    No holdings match your search filters.
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                )}

                {!schemeIdParam && !loading && !error && (
                    <div className="shp-empty-state">
                        <div className="shp-empty-icon">📊</div>
                        <p>Search for a scheme in the header above to view its portfolio holdings.</p>
                    </div>
                )}
            </div>
        </div>
    );
}
