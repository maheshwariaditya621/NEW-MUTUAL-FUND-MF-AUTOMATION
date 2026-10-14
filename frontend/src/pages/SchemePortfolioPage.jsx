import React, { useState, useEffect, useMemo } from 'react';
import { useSearchParams, Link } from 'react-router-dom';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import MissingData from '../components/common/MissingData';
import PageEmptyState from '../components/common/PageEmptyState';
import ExportButton from '../components/common/ExportButton';
import WatchButton from '../components/common/WatchButton';
import { searchSchemes, getSchemePortfolio } from '../api/schemes';
import { getBulkPrices } from '../api/stocks';
import { handleApiError } from '../api/client';
import { formatCrores, formatPercent, formatNumber } from '../utils/helpers';
import './SchemePortfolioPage.css';

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
    if (change === 0) return <span className="shp-change-neutral">—</span>;
    const isPos = change > 0;
    const isNeg = change < 0;
    const cls = isPos ? 'pos' : isNeg ? 'neg' : 'neutral';
    const sign = isPos ? '+' : '';

    return (
        <div className={`shp-change-cell ${cls}`}>
            <span className="shp-change-val">{sign}{fmt(change)}</span>
            {pct != null && (
                <span className="shp-change-pct">
                    ({sign}{pct.toFixed(2)}%)
                </span>
            )}
        </div>
    );
}

function MiniSparkline({ history }) {
    if (!history || history.length < 2) return null;
    const vals = history.map(h => h.num_shares).filter(v => v !== null && v !== undefined);
    if (vals.length < 2) return null;
    const rev = [...vals].reverse();
    const min = Math.min(...rev);
    const max = Math.max(...rev);
    const range = max - min || 1;
    const w = 38;
    const h = 16;
    const pointsArr = rev.map((val, idx) => {
        const x = (idx / (rev.length - 1)) * (w - 8) + 4;
        const y = h - 3 - ((val - min) / range) * (h - 6);
        return [x, y];
    });
    const pointsStr = pointsArr.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
    const lastPoint = pointsArr[pointsArr.length - 1];

    const isGrowing = rev[rev.length - 1] > rev[0];
    const isFlat = rev[rev.length - 1] === rev[0];
    const stroke = isGrowing ? '#10b981' : isFlat ? '#94a3b8' : '#f43f5e';

    return (
        <svg className="shp-sparkline" width={w} height={h} viewBox={`0 0 ${w} ${h}`} title="Holding trend trajectory">
            <polyline fill="none" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" points={pointsStr} />
            <circle cx={lastPoint[0].toFixed(1)} cy={lastPoint[1].toFixed(1)} r="2" fill={stroke} />
        </svg>
    );
}

export default function SchemePortfolioPage() {
    const [searchParams, setSearchParams] = useSearchParams();

    const schemeIdParam = searchParams.get('scheme_id');

    const [portfolio, setPortfolio] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [sortConfig, setSortConfig] = useState({ key: 'shares_0', direction: 'desc' });
    const [filterText, setFilterText] = useState('');
    const [selectedMonth, setSelectedMonth] = useState(null);
    const [aumViewMode, setAumViewMode] = useState('total'); // 'total' or 'equity'

    const [allAvailableMonths, setAllAvailableMonths] = useState([]);
    const [livePrices, setLivePrices] = useState({});

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

    // One-time bulk fetch for any holding missing EOD price
    useEffect(() => {
        if (!portfolio?.holdings?.length) return;

        const missingIsins = portfolio.holdings
            .filter(h => !h.live_price && !h.close_price && h.isin)
            .map(h => h.isin);

        if (missingIsins.length === 0) return;

        getBulkPrices(missingIsins)
            .then(priceMap => {
                setLivePrices(prev => ({ ...prev, ...priceMap }));
            })
            .catch(err => console.error("Failed to fetch bulk prices:", err));
    }, [portfolio?.holdings]);

    const handleSort = (key) => {
        setSortConfig(prev => {
            if (prev.key !== key) return { key, direction: 'desc' };
            if (prev.direction === 'desc') return { key, direction: 'asc' };
            return { key: null, direction: 'desc' }; // cycle: desc -> asc -> none
        });
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

        if (sortConfig.key) {
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
                } else if (sortConfig.key.startsWith('pnav_')) {
                    const monthIdx = parseInt(sortConfig.key.split('_')[1]);
                    const monthLabel = displayMonths[monthIdx];
                    const aM = a.monthly_data.find(m => m.month === monthLabel);
                    const bM = b.monthly_data.find(m => m.month === monthLabel);
                    aVal = aM ? aM.percent_to_aum || 0 : 0;
                    bVal = bM ? bM.percent_to_aum || 0 : 0;
                } else if (sortConfig.key.startsWith('change_')) {
                    const monthIdx = parseInt(sortConfig.key.split('_')[1]);
                    const currLabel = displayMonths[monthIdx];
                    const prevLabel = displayMonths[monthIdx + 1];
                    const aCurr = a.monthly_data.find(m => m.month === currLabel)?.num_shares || 0;
                    const aPrev = prevLabel ? (a.monthly_data.find(m => m.month === prevLabel)?.num_shares || 0) : 0;
                    const bCurr = b.monthly_data.find(m => m.month === currLabel)?.num_shares || 0;
                    const bPrev = prevLabel ? (b.monthly_data.find(m => m.month === prevLabel)?.num_shares || 0) : 0;
                    aVal = aCurr - aPrev;
                    bVal = bCurr - bPrev;
                }

                if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
                if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }

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
                                <WatchButton
                                    assetType="scheme"
                                    schemeId={Number(schemeIdParam)}
                                    name={portfolio.scheme_name}
                                    size="sm"
                                />
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
                                {/* ── Pending Month Pill ── */}
                                {portfolio.pending_month && (
                                    <div style={{
                                        display: 'flex', alignItems: 'center', gap: '8px',
                                        background: 'rgba(99, 102, 241, 0.08)',
                                        border: '1px solid rgba(99, 102, 241, 0.3)',
                                        borderRadius: '8px', padding: '10px 14px',
                                        marginBottom: '10px', fontSize: '12px',
                                        color: 'var(--text-primary)'
                                    }}>
                                        <span style={{ fontSize: '16px' }}>🕐</span>
                                        <div>
                                            <strong style={{ color: '#818cf8' }}>{portfolio.pending_month.month} — Data Pending</strong>
                                            <span style={{ color: 'var(--text-secondary)', marginLeft: '6px' }}>
                                                {portfolio.pending_month.message}
                                            </span>
                                        </div>
                                    </div>
                                )}

                                {/* ── Industry Partial Warning ── */}
                                {portfolio.data_warning && !portfolio.pending_month && (
                                    <div style={{
                                        display: 'flex', alignItems: 'center', gap: '8px',
                                        background: 'rgba(234, 179, 8, 0.08)',
                                        border: '1px solid rgba(234, 179, 8, 0.3)',
                                        borderRadius: '8px', padding: '10px 14px',
                                        marginBottom: '10px', fontSize: '12px',
                                        color: 'var(--text-primary)'
                                    }}>
                                        <span style={{ fontSize: '16px' }}>⚠️</span>
                                        <div>
                                            <strong style={{ color: '#eab308' }}>Partial Industry Data — {portfolio.data_warning.latest_label}</strong>
                                            <span style={{ color: 'var(--text-secondary)', marginLeft: '6px' }}>
                                                <strong style={{ color: 'var(--text-primary)' }}>
                                                    {portfolio.data_warning.amcs_uploaded} of {portfolio.data_warning.amcs_expected} AMCs
                                                </strong> have uploaded. This scheme's data is available.
                                                {portfolio.data_warning.amcs_pending > 0 && (
                                                    <span style={{ color: '#f87171' }}> ({portfolio.data_warning.amcs_pending} still pending)</span>
                                                )}.
                                            </span>
                                        </div>
                                    </div>
                                )}

                                {/* ── Controls Header ── */}
                                <div className="shp-controls-header shp-controls-row">
                                    <div className="shp-section-title" style={{ margin: 0, color: 'var(--shp-header-color)' }}>Equity Holdings</div>
                                    <div className="shp-controls-right" style={{ display: 'flex', gap: '16px', alignItems: 'center' }}>
                                        <div className="shp-aum-toggle" style={{ display: 'flex', background: 'var(--bg-tertiary)', padding: '4px', borderRadius: '8px', border: '1px solid var(--border-color)', fontSize: '11px', fontWeight: '600' }}>
                                            <button
                                                onClick={() => setAumViewMode('total')}
                                                style={{ padding: '6px 12px', borderRadius: '4px', border: 'none', background: aumViewMode === 'total' ? 'var(--accent-primary)' : 'transparent', color: aumViewMode === 'total' ? '#fff' : 'var(--text-secondary)', cursor: 'pointer', transition: 'all 0.2s' }}
                                            >
                                                TOTAL AUM
                                            </button>
                                            <button
                                                onClick={() => setAumViewMode('equity')}
                                                style={{ padding: '6px 12px', borderRadius: '4px', border: 'none', background: aumViewMode === 'equity' ? 'var(--accent-primary)' : 'transparent', color: aumViewMode === 'equity' ? '#fff' : 'var(--text-secondary)', cursor: 'pointer', transition: 'all 0.2s' }}
                                            >
                                                EQUITY AUM
                                            </button>
                                        </div>
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
                                        <ExportButton
                                            getData={() => {
                                                const holdings = getHoldingsToRender();
                                                return holdings.map(h => {
                                                    const histMap = h.monthly_data.reduce((acc, m) => { acc[m.month] = m; return acc; }, {});
                                                    const row = {
                                                        company_name: h.company_name,
                                                        isin: h.isin,
                                                        sector: h.sector,
                                                        close_price: h.close_price || h.live_price || livePrices[h.isin] || null,
                                                    };
                                                    displayMonths.forEach((m, mIdx) => {
                                                        const md = histMap[m];
                                                        const prevMonth = displayMonths[mIdx + 1];
                                                        const prevMd = prevMonth ? histMap[prevMonth] : null;
                                                        const currShares = md?.num_shares ?? null;
                                                        const prevShares = prevMd?.num_shares ?? null;
                                                        const chg = (currShares != null && prevShares != null) ? currShares - prevShares : null;

                                                        row[`pct_aum_${m}`] = md?.percent_to_aum ?? null;
                                                        row[`shares_${m}`] = currShares;
                                                        row[`change_${m}`] = chg;
                                                    });
                                                    return row;
                                                });
                                            }}
                                            columns={[
                                                { key: 'company_name', label: 'Company', exportFormat: 'string' },
                                                { key: 'isin', label: 'ISIN', exportFormat: 'string' },
                                                { key: 'sector', label: 'Sector', exportFormat: 'string' },
                                                { key: 'close_price', label: 'EOD Price (₹)', exportFormat: 'numeric' },
                                                ...displayMonths.flatMap(m => [
                                                    { key: `pct_aum_${m}`, label: `% of AUM (${m})`, exportFormat: 'numeric' },
                                                    { key: `shares_${m}`, label: `Shares Held (${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Net Change (${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]}
                                            pdfColumns={[
                                                { key: 'company_name', label: 'Company', exportFormat: 'string' },
                                                { key: 'sector', label: 'Sector', exportFormat: 'string' },
                                                ...displayMonths.flatMap(m => [
                                                    { key: `pct_aum_${m}`, label: `% AUM (${m})`, exportFormat: 'numeric' },
                                                    { key: `shares_${m}`, label: `Shares (${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Change (${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]}
                                            fileNameConfig={{
                                                page: 'scheme-portfolio',
                                                filters: {
                                                    scheme: portfolio?.scheme_name,
                                                    period: displayMonths[0],
                                                },
                                            }}
                                            metadata={{
                                                title: `Scheme Portfolio — ${portfolio?.scheme_name || ''}`,
                                                filters: {
                                                    Scheme: portfolio?.scheme_name,
                                                    AMC: portfolio?.amc_name,
                                                    Plan: portfolio?.plan_type,
                                                    Option: portfolio?.option_type,
                                                    Period: displayMonths[0],
                                                    Search: filterText || undefined,
                                                },
                                            }}
                                        />
                                    </div>
                                </div>
                                <table className="shp-table">
                                    <thead>
                                        <tr className="shp-thead-row-months">
                                            <th className="shp-th shp-th-asset-hdr sortable" onClick={() => handleSort('company_name')}>
                                                <div className="shp-th-sort-wrap">
                                                    <span>Company Name</span>
                                                    {renderSortArrow('company_name')}
                                                </div>
                                            </th>
                                            {displayMonths.map((monthLabel, idx) => {
                                                const mData = portfolio.monthly_aum?.find(m => m.month === monthLabel);
                                                return (
                                                    <th key={idx} colSpan={3} className={`shp-th shp-month-group shp-month-${idx}`}>
                                                        <span className="shp-month-title">{monthLabel}{idx === 0 ? ' (LATEST)' : ''}</span>
                                                        <div className="rv-aum-label" style={{ fontSize: '10px', marginTop: '2px', fontWeight: '400', opacity: 0.85 }}>
                                                            {mData ? (
                                                                aumViewMode === 'total'
                                                                    ? (mData.total_aum_cr > 0 ? `TOTAL: ₹ ${formatNumber(mData.total_aum_cr)} (Cr.)` : 'TOTAL: ₹ - (Cr.)')
                                                                    : (mData.equity_aum_cr > 0 ? `EQUITY: ₹ ${formatNumber(mData.equity_aum_cr)} (Cr.)` : 'EQUITY: ₹ - (Cr.)')
                                                            ) : 'AUM: ₹ - (Cr.)'}
                                                        </div>
                                                    </th>
                                                );
                                            })}
                                        </tr>
                                        <tr className="shp-thead-row-subs">
                                            <th className="shp-th shp-th-sub shp-th-asset-sub">
                                                <span>Equity Portfolio Allocation</span>
                                            </th>
                                            {displayMonths.map((monthLabel, idx) => (
                                                <React.Fragment key={idx}>
                                                    <th className={`shp-th shp-th-sub shp-month-${idx} sortable`} onClick={() => handleSort(`pnav_${idx}`)}>
                                                        <span>% AUM</span>
                                                        {renderSortArrow(`pnav_${idx}`)}
                                                    </th>
                                                    <th className={`shp-th shp-th-sub shp-month-${idx} sortable`} onClick={() => handleSort(`shares_${idx}`)}>
                                                        <span>QTY</span>
                                                        {renderSortArrow(`shares_${idx}`)}
                                                    </th>
                                                    <th className={`shp-th shp-th-sub shp-th-change-hdr shp-month-${idx} sortable`} onClick={() => handleSort(`change_${idx}`)}>
                                                        <span>CHANGE</span>
                                                        {renderSortArrow(`change_${idx}`)}
                                                    </th>
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
                                                        <div className="shp-fund-name" style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
                                                            <Link to={`/stocks?isin=${holding.isin}`} className="shp-fund-title-link">
                                                                {holding.company_name}
                                                            </Link>
                                                            {(holding.close_price || holding.live_price || livePrices[holding.isin]) && (
                                                                <span className="rv-eod-chip-small" title={holding.price_date ? `NSE Bhavcopy EOD Close as of ${holding.price_date}` : 'NSE Bhavcopy EOD Close'}>
                                                                    <span className="shp-eod-badge">EOD</span>
                                                                    <span>₹{formatNumber(holding.close_price || holding.live_price || livePrices[holding.isin])}</span>
                                                                </span>
                                                            )}
                                                        </div>
                                                        <div className="shp-fund-sub">
                                                            {holding.isin} {holding.sector && `• ${holding.sector}`}
                                                        </div>
                                                    </td>
                                                    {displayMonths.map((monthLabel, mIdx) => {
                                                        const m = histMap[monthLabel];
                                                        const prevMonthLabel = displayMonths[mIdx + 1];
                                                        const prevM = prevMonthLabel ? histMap[prevMonthLabel] : null;

                                                        const currShares = m?.num_shares;
                                                        const prevShares = prevM?.num_shares;
                                                        let change = null;
                                                        let changePct = null;
                                                        if (currShares != null && prevShares != null) {
                                                            change = currShares - prevShares;
                                                            if (prevShares > 0) {
                                                                changePct = ((currShares - prevShares) / prevShares) * 100;
                                                            } else if (currShares > 0) {
                                                                changePct = 100;
                                                            }
                                                        }

                                                        return (
                                                            <React.Fragment key={mIdx}>
                                                                <td className={`shp-td shp-td-num shp-month-${mIdx}`}>
                                                                    {fmtPct(m?.percent_to_aum) || <MissingData />}
                                                                </td>
                                                                <td className={`shp-td shp-td-num shp-month-${mIdx}`}>
                                                                    {currShares === null || currShares === undefined ? (
                                                                        <MissingData />
                                                                    ) : currShares === 0 ? (
                                                                        '-'
                                                                    ) : (
                                                                        <div className="rv-shares-cell" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '6px' }}>
                                                                            <span className="tabular-nums">{fmt(currShares)}</span>
                                                                            {mIdx === 0 && <MiniSparkline history={holding.monthly_data} />}
                                                                        </div>
                                                                    )}
                                                                </td>
                                                                <td className={`shp-td shp-td-num shp-month-${mIdx} shp-change-col`}>
                                                                    <ChangeCell change={change} pct={changePct} />
                                                                </td>
                                                            </React.Fragment>
                                                        );
                                                    })}
                                                </tr>
                                            );
                                        })}
                                        {getHoldingsToRender().length === 0 && (
                                            <tr>
                                                <td colSpan={1 + displayMonths.length * 3} className="shp-empty">
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
                    <PageEmptyState
                        title="Deep-Dive Portfolio Explorer"
                        description="Explore the DNA of your mutual fund investments. Deep-dive into any fund's equity portfolio to see exactly where your money is invested, discover new stocks they've added, and monitor how they are shifting their sectoral weights over time."
                        placeholder="Search by Scheme Name or AMC (e.g. Quant Small Cap, HDFC)"
                        type="scheme"
                        suggestions={["Quant Small Cap", "HDFC Flexi Cap", "Parag Parikh Flexi Cap", "SBI Bluechip", "ICICI Prudential Bluechip"]}
                        onSearch={(val) => {
                            const next = new URLSearchParams(searchParams);
                            next.set('scheme_id', String(val));
                            setSearchParams(next);
                        }}

                    />
                )}
            </div>
        </div>
    );
}
