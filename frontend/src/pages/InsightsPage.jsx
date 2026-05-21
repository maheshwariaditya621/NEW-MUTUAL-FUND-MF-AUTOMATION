import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { getStockActivity } from '../api/insights';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import MissingData from '../components/common/MissingData';
import ExportButton from '../components/common/ExportButton';
import { TrendingUp, TrendingDown, Sparkles, LogOut as LogOutIcon, Info } from 'lucide-react';
import './InsightsPage.css';

// ── Helpers ──
const fmt = (n) => (n === null || n === undefined) ? null : (n === 0 ? '-' : Number(n).toLocaleString('en-IN'));
const fmtCr = (n) => (n === null || n === undefined) ? null : (n === 0 ? '-' : Number(n).toLocaleString('en-IN', { maximumFractionDigits: 2 }));

export default function InsightsPage() {
    const [activityType, setActivityType] = useState('buying'); // 'buying' | 'selling'
    const [mcapCategory, setMcapCategory] = useState('All');
    const [filterText, setFilterText] = useState('');
    const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const fetchActivity = async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await getStockActivity(activityType, mcapCategory);
            setData(result);
        } catch (err) {
            setError("Failed to fetch analytical insights. Please try again later.");
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchActivity();
    }, [activityType, mcapCategory]);

    const filteredResults = useMemo(() => {
        if (!data?.results) return [];
        let items = [...data.results];

        // 1. Filter
        if (filterText) {
            const q = filterText.toLowerCase();
            items = items.filter(item =>
                item.company_name.toLowerCase().includes(q) ||
                (item.sector && item.sector.toLowerCase().includes(q)) ||
                item.isin.toLowerCase().includes(q)
            );
        }

        // 2. Sort
        if (sortConfig.key) {
            items.sort((a, b) => {
                let aVal = a[sortConfig.key];
                let bVal = b[sortConfig.key];

                const numericColumns = ['market_cap', 'net_qty_bought', 'num_funds_curr', 'buy_value_crore'];

                if (numericColumns.includes(sortConfig.key)) {
                    aVal = Number(aVal) || 0;
                    bVal = Number(bVal) || 0;
                } else {
                    // Handle nulls safely
                    if (aVal === null || aVal === undefined) aVal = '';
                    if (bVal === null || bVal === undefined) bVal = '';

                    if (typeof aVal === 'string') {
                        aVal = aVal.toLowerCase();
                        bVal = bVal.toLowerCase();
                    }
                }

                if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
                if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }

        return items;
    }, [data, filterText, sortConfig]);

    const handleSort = (key) => {
        setSortConfig(prev => {
            if (prev.key !== key) return { key, direction: 'desc' };
            if (prev.direction === 'desc') return { key, direction: 'asc' };
            return { key: null, direction: 'desc' };
        });
    };

    const SortIcon = ({ columnKey }) => {
        if (sortConfig.key !== columnKey) return null;
        return <span className="ins-sort-icon active">{sortConfig.direction === 'asc' ? '↑' : '↓'}</span>;
    };

    const summaryStats = useMemo(() => {
        if (!data?.results) return null;
        let capital = 0;
        let sectorCounts = {};
        
        data.results.forEach(item => {
            capital += Math.abs(Number(item.buy_value_crore || 0));
            if (item.sector) {
                sectorCounts[item.sector] = (sectorCounts[item.sector] || 0) + 1;
            }
        });
        
        let maxSec = 'None';
        let maxCount = 0;
        for (let s in sectorCounts) {
            if (sectorCounts[s] > maxCount) {
                maxCount = sectorCounts[s];
                maxSec = s;
            }
        }
        
        return {
            capital: capital,
            topSector: maxSec,
            topSectorCount: maxCount,
            totalStocks: data.results.length
        };
    }, [data]);

    return (
        <div className="ins-page">
            <div className="ins-container">

                {/* ── Header Section ── */}
                <div className="ins-header">
                    <div className="ins-title-row">
                        <div className="ins-title-group">
                            <h1>Institutional Activity Radar</h1>
                            <div className="ins-subtitle">
                                {data ? (
                                    <>Comparing MF portfolios for <strong>{data.month}</strong> vs <strong>{data.prev_month}</strong></>
                                ) : 'Analyzing latest portfolio movements...'}
                            </div>
                        </div>
                    </div>
                    
                    {/* ── Summary Stats ── */}
                    {summaryStats && !loading && !error && (
                        <div className="ins-summary-cards">
                            <div className="ins-stat-card">
                                <div className="ins-stat-label">Total Stocks Found</div>
                                <div className="ins-stat-value">{summaryStats.totalStocks}</div>
                            </div>
                            <div className="ins-stat-card">
                                <div className="ins-stat-label">Total Est. Value</div>
                                <div className={`ins-stat-value ${activityType === 'buying' || activityType === 'entrants' ? 'ins-buying' : 'ins-selling'}`}>
                                    ₹ {fmtCr(summaryStats.capital)} Cr
                                </div>
                            </div>
                            <div className="ins-stat-card">
                                <div className="ins-stat-label">Sector In Focus</div>
                                <div className="ins-stat-value" style={{ fontSize: '15px' }}>{summaryStats.topSector}</div>
                                <div className="ins-stat-subtext">{summaryStats.topSectorCount} stocks</div>
                            </div>
                        </div>
                    )}

                    {/* ── Main Activity Tabs ── */}
                    <div className="ins-tabs-wrap">
                        <div className="ins-tabs">
                            <button
                                className={`ins-tab-btn ${activityType === 'buying' ? 'active buying' : ''}`}
                                onClick={() => setActivityType('buying')}
                            >
                                <TrendingUp size={16} /> Top Buys
                            </button>
                            <button
                                className={`ins-tab-btn ${activityType === 'selling' ? 'active selling' : ''}`}
                                onClick={() => setActivityType('selling')}
                            >
                                <TrendingDown size={16} /> Top Sells
                            </button>
                            <div className="ins-tab-divider"></div>
                            <button
                                className={`ins-tab-btn ${activityType === 'entrants' ? 'active entrants' : ''}`}
                                onClick={() => setActivityType('entrants')}
                                title="Stocks that had 0 MF ownership last month but were bought this month."
                            >
                                <Sparkles size={16} /> New Entrants
                            </button>
                            <button
                                className={`ins-tab-btn ${activityType === 'exits' ? 'active exits' : ''}`}
                                onClick={() => setActivityType('exits')}
                                title="Stocks that were completely sold off by all MFs."
                            >
                                <LogOutIcon size={16} /> Complete Exits
                            </button>
                        </div>
                    </div>

                    {/* ── Secondary Filter Bar ── */}
                    <div className="ins-filters">
                        <div className="ins-filter-item">
                            <span className="ins-filter-label">Market Cap</span>
                            <select
                                className="ins-select"
                                value={mcapCategory}
                                onChange={(e) => setMcapCategory(e.target.value)}
                            >
                                <option value="All">All Categories</option>
                                <option value="Large Cap">Large Cap</option>
                                <option value="Mid Cap">Mid Cap</option>
                                <option value="Small Cap">Small Cap</option>
                            </select>
                        </div>
                        <div className="ins-filter-item">
                            <span className="ins-filter-label">Search</span>
                            <input
                                type="text"
                                className="ins-input"
                                placeholder="Search Stock/Sector..."
                                value={filterText}
                                onChange={(e) => setFilterText(e.target.value)}
                            />
                        </div>
                    </div>

                    {/* ── Partial Data Warning ── */}
                    {data?.data_warning && (
                        <div style={{
                            display: 'flex', alignItems: 'flex-start', gap: '10px',
                            background: 'rgba(234, 179, 8, 0.08)',
                            border: '1px solid rgba(234, 179, 8, 0.35)',
                            borderRadius: '8px', padding: '10px 14px',
                            marginTop: '12px', fontSize: '12px',
                            color: 'var(--text-primary)', lineHeight: '1.5'
                        }}>
                            <span style={{ fontSize: '16px', flexShrink: 0 }}>⚠️</span>
                            <div>
                                <strong style={{ color: '#eab308' }}>Showing {data.data_warning.complete_label} vs Previous</strong>
                                <span style={{ color: 'var(--text-secondary)', marginLeft: '6px' }}>
                                    Only <strong style={{ color: 'var(--text-primary)' }}>{data.data_warning.amcs_uploaded} of {data.data_warning.amcs_expected} AMCs</strong> have
                                    submitted {data.data_warning.latest_label} data
                                    {data.data_warning.amcs_pending > 0 && (
                                        <span style={{ color: '#f87171' }}> ({data.data_warning.amcs_pending} pending)</span>
                                    )}.
                                    {' '}Activity signals use <strong>{data.data_warning.complete_label}</strong> (last complete month) to avoid misleading insights.
                                </span>
                            </div>
                        </div>
                    )}

                    {/* ── Export Button ── */}
                    {data && !loading && filteredResults.length > 0 && (
                        <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '12px' }}>
                            <ExportButton
                                getData={() => filteredResults.map(item => ({
                                    company_name: item.company_name,
                                    isin: item.isin,
                                    sector: item.sector,
                                    // market_cap is already in Crores from the API (Pydantic field: "Market Cap in INR Crores")
                                    market_cap_cr: item.market_cap != null ? Number(item.market_cap) : null,
                                    classification: item.classification,
                                    period: data.month,
                                    prev_period: data.prev_month,
                                    total_qty_curr: item.total_qty_curr,
                                    total_qty_prev: item.total_qty_prev,
                                    // net_qty_bought is negative for selling stocks
                                    net_qty: Math.abs(item.net_qty_bought),
                                    num_funds: item.num_funds_curr,
                                    num_funds_prev: item.num_funds_prev,
                                    net_fund_change: item.net_fund_entrants,
                                    // buy_value_crore may come as Decimal string from Python
                                    value_crore: Math.abs(Number(item.buy_value_crore)),
                                }))}
                                columns={[
                                    { key: 'company_name', label: 'Stock', exportFormat: 'string' },
                                    { key: 'isin', label: 'ISIN', exportFormat: 'string' },
                                    { key: 'sector', label: 'Sector', exportFormat: 'string' },
                                    { key: 'market_cap_cr', label: 'Market Cap (Cr)', exportFormat: 'numeric' },
                                    { key: 'classification', label: 'Category', exportFormat: 'string' },
                                    { key: 'period', label: 'Period', exportFormat: 'string' },
                                    { key: 'total_qty_curr', label: 'Total Shares (Curr)', exportFormat: 'numeric' },
                                    { key: 'total_qty_prev', label: 'Total Shares (Prev)', exportFormat: 'numeric' },
                                    { key: 'net_qty', label: 'Net Qty', exportFormat: 'numeric' },
                                    { key: 'num_funds', label: 'Funds (Curr)', exportFormat: 'numeric' },
                                    { key: 'num_funds_prev', label: 'Funds (Prev)', exportFormat: 'numeric' },
                                    { key: 'net_fund_change', label: 'Net Fund Change', exportFormat: 'numeric' },
                                    { key: 'value_crore', label: 'Value (Cr)', exportFormat: 'numeric' },
                                ]}
                                pdfColumns={[
                                    { key: 'company_name', label: 'Stock', exportFormat: 'string' },
                                    { key: 'sector', label: 'Sector', exportFormat: 'string' },
                                    { key: 'market_cap_cr', label: 'Mkt Cap (Cr)', exportFormat: 'numeric' },
                                    { key: 'net_qty', label: 'Net Qty', exportFormat: 'numeric' },
                                    { key: 'num_funds', label: 'Funds', exportFormat: 'numeric' },
                                    { key: 'value_crore', label: 'Value (Cr)', exportFormat: 'numeric' },
                                ]}
                                fileNameConfig={{
                                    page: `mf-activity-${activityType}`,
                                    filters: {
                                        period: data?.month,
                                        mcap: mcapCategory !== 'All' ? mcapCategory : undefined,
                                    },
                                }}
                                metadata={{
                                    title: `MF Activity (${activityType.toUpperCase()}) \u2014 ${data?.month}`,
                                    filters: {
                                        Period: `${data?.month} vs ${data?.prev_month}`,
                                        'Market Cap': mcapCategory,
                                        Search: filterText || undefined,
                                    },
                                }}
                            />
                        </div>
                    )}
                </div>
            </div>

            {/* ── Data Views ── */}
            {loading && <Loading message="Calculating market movements..." />}
            {error && <ErrorMessage message={error} onRetry={fetchActivity} />}

            {data && !loading && !error && (
                <div className="ins-table-wrap">
                    <table className="ins-table">
                        <thead>
                            <tr>
                                <th className="ins-th sortable" onClick={() => handleSort('company_name')}>
                                    Stock Name <SortIcon columnKey="company_name" />
                                </th>
                                <th className="ins-th sortable" onClick={() => handleSort('sector')}>
                                    Sector <SortIcon columnKey="sector" />
                                </th>
                                <th className="ins-th sortable" onClick={() => handleSort('market_cap')}>
                                    Market Cap (Cr) <SortIcon columnKey="market_cap" />
                                </th>
                                <th className="ins-th sortable" onClick={() => handleSort('net_qty_bought')}>
                                    {activityType === 'buying' && 'Net Qty Bought'}
                                    {activityType === 'selling' && 'Net Qty Sold'}
                                    {activityType === 'entrants' && 'Qty Acquired'}
                                    {activityType === 'exits' && 'Qty Dumped'}
                                    <SortIcon columnKey="net_qty_bought" />
                                </th>
                                <th className="ins-th sortable" onClick={() => handleSort('num_funds_curr')}>
                                    Fund Conviction <Info size={12} style={{marginLeft: 4, verticalAlign: 'text-bottom'}} title="Number of mutual funds holding this stock" /> <SortIcon columnKey="num_funds_curr" />
                                </th>
                                <th className="ins-th sortable" onClick={() => handleSort('buy_value_crore')}>
                                    Est. Value (Cr) <SortIcon columnKey="buy_value_crore" />
                                </th>
                            </tr>
                        </thead>
                        <tbody>
                            {filteredResults.map((item, idx) => (
                                <tr key={idx} className="ins-row">
                                    <td className="ins-td">
                                        <div className="ins-stock-cell">
                                            <Link to={`/stocks?isin=${item.isin}`} className="ins-stock-name">
                                                {item.company_name}
                                            </Link>
                                            <span className="ins-isin">{item.isin}</span>
                                        </div>
                                    </td>
                                    <td className="ins-td">
                                        <span style={{ fontWeight: 500 }}>{item.sector || <MissingData inline />}</span>
                                    </td>
                                    <td className="ins-td">
                                        <div className="ins-mcap-cell">
                                            <span
                                                className="ins-mcap-val"
                                                title={item.market_cap ? `Exact: ${fmt(item.market_cap)} INR` : ''}
                                                style={{ cursor: 'help' }}
                                            >
                                                {fmtCr(item.market_cap / 10000000) || <MissingData inline />}
                                            </span>
                                            {item.classification && (
                                                <span className={`ins-badge mcap ${item.classification.toLowerCase().replace(' ', '-')}`}>
                                                    {item.classification}
                                                </span>
                                            )}
                                        </div>
                                    </td>
                                    <td className="ins-td ins-qty">
                                        <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
                                            <span>{fmt(Math.abs(item.net_qty_bought))}</span>
                                            {activityType !== 'entrants' && activityType !== 'exits' && item.holding_change_percent !== null && (
                                                <span style={{ fontSize: '10.5px', fontWeight: 600, color: 'var(--text-secondary)' }}>
                                                    {item.holding_change_percent > 0 ? '+' : ''}{item.holding_change_percent}% change
                                                </span>
                                            )}
                                            {item.ownership_change_percent !== null && Math.abs(item.ownership_change_percent) >= 0.01 && (
                                                <span style={{ fontSize: '10px', fontWeight: 700, color: activityType === 'buying' || activityType === 'entrants' ? '#10b981' : '#ef4444' }}>
                                                    {Math.abs(item.ownership_change_percent)}% of company
                                                </span>
                                            )}
                                        </div>
                                    </td>
                                    <td className="ins-td">
                                        <div className="ins-funds-count">
                                            <div className="conviction-bar-bg" title={`${item.num_funds_curr} funds`}>
                                                <div 
                                                    className={`conviction-bar-fill ${activityType === 'selling' || activityType === 'exits' ? 'neg' : 'pos'}`} 
                                                    style={{ width: `${Math.min(item.num_funds_curr * 2, 100)}%` }}
                                                ></div>
                                            </div>
                                            <Link to={`/stocks?isin=${item.isin}`} style={{ textDecoration: 'none', color: 'inherit' }}>
                                                <span style={{ fontWeight: 700 }}>{activityType === 'exits' ? item.num_funds_prev : item.num_funds_curr}</span>
                                            </Link>
                                            {item.net_fund_entrants !== 0 && (
                                                <Link to={`/stocks?isin=${item.isin}&filter=${item.net_fund_entrants > 0 ? 'entrants' : 'exits'}`} style={{ textDecoration: 'none' }}>
                                                    <span className={`ins-fund-change ${item.net_fund_entrants > 0 ? 'pos' : 'neg'}`} style={{ cursor: 'pointer' }}>
                                                        {item.net_fund_entrants > 0 ? '+' : ''}{item.net_fund_entrants}
                                                    </span>
                                                </Link>
                                            )}
                                        </div>
                                    </td>
                                    <td className="ins-td ins-value-cr">
                                        <span className={activityType === 'buying' || activityType === 'entrants' ? 'ins-buying' : 'ins-selling'}>
                                            ₹ {fmtCr(Math.abs(item.buy_value_crore))} Cr
                                        </span>
                                    </td>
                                </tr>
                            ))}
                            {filteredResults.length === 0 && (
                                <tr>
                                    <td colSpan="7" className="ins-empty">No stocks found matching your criteria.</td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
