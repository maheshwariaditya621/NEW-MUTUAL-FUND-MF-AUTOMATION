import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { getStockActivity } from '../api/insights';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import MissingData from '../components/common/MissingData';
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

    return (
        <div className="ins-page">
            <div className="ins-container">

                {/* ── Header Section ── */}
                <div className="ins-header">
                    <div className="ins-title-row">
                        <div className="ins-title-group">
                            <h1>Mutual Fund Activity Insights</h1>
                            <div className="ins-subtitle">
                                {data ? (
                                    <>Comparing <strong>{data.month}</strong> vs <strong>{data.prev_month}</strong> results</>
                                ) : 'Analyzing latest portfolio movements...'}
                            </div>
                        </div>

                        {/* ── Main Activity Tabs ── */}
                        <div className="ins-tabs">
                            <button
                                className={`ins-tab-btn ${activityType === 'buying' ? 'active' : ''}`}
                                onClick={() => setActivityType('buying')}
                            >
                                Stocks Attracting Fund Managers
                            </button>
                            <button
                                className={`ins-tab-btn ${activityType === 'selling' ? 'active' : ''}`}
                                onClick={() => setActivityType('selling')}
                            >
                                Stocks Seeing Selling Pressure
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
                                <th className="ins-th">Month</th>
                                <th className="ins-th sortable" onClick={() => handleSort('net_qty_bought')}>
                                    Net Qty {activityType === 'buying' ? 'Bought' : 'Sold'} <SortIcon columnKey="net_qty_bought" />
                                </th>
                                <th className="ins-th sortable" onClick={() => handleSort('num_funds_curr')}>
                                    Fund Participation <SortIcon columnKey="num_funds_curr" />
                                </th>
                                <th className="ins-th sortable" onClick={() => handleSort('buy_value_crore')}>
                                    Approx. {activityType === 'buying' ? 'Buy' : 'Sell'} Value <SortIcon columnKey="buy_value_crore" />
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
                                    <td className="ins-td" style={{ fontSize: '12px', fontWeight: 600 }}>
                                        {data.month}
                                    </td>
                                    <td className="ins-td ins-qty">
                                        {fmt(Math.abs(item.net_qty_bought))}
                                    </td>
                                    <td className="ins-td">
                                        <div className="ins-funds-count">
                                            <Link to={`/stocks?isin=${item.isin}`} style={{ textDecoration: 'none', color: 'inherit' }}>
                                                <span style={{ fontWeight: 700 }}>{item.num_funds_curr} Funds</span>
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
                                        <span className={activityType === 'buying' ? 'ins-buying' : 'ins-selling'}>
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
