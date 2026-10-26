import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Activity, TrendingUp, TrendingDown, ExternalLink, ShieldCheck, Filter } from 'lucide-react';
import { fetchDeals } from '../api/deals';
import './StockDealsSection.css';

const fmt = (num) => {
    if (num === null || num === undefined || isNaN(num)) return '—';
    return Number(num).toLocaleString('en-IN');
};

const fmtCr = (inr) => {
    if (inr === null || inr === undefined || isNaN(inr)) return '—';
    const val = Number(inr) / 10000000;
    return `₹${val.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} Cr`;
};

const fmtPrice = (p) => {
    if (p === null || p === undefined || isNaN(p)) return '—';
    return `₹${Number(p).toFixed(2)}`;
};

export default function StockDealsSection({ companyId, symbol, isin, companyName }) {
    const [deals, setDeals] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [typeFilter, setTypeFilter] = useState('ALL');
    const [searchQuery, setSearchQuery] = useState('');
    const [displayLimit, setDisplayLimit] = useState(10);

    useEffect(() => {
        if (!companyId && !symbol && !isin && !companyName) return;

        let isMounted = true;
        setLoading(true);
        setError(null);

        const params = {
            limit: 100,
            sort_by: 'trade_date',
            sort_order: 'desc'
        };
        if (isin) params.isin = isin;
        if (companyId) params.company_id = companyId;
        if (symbol) params.symbol = symbol;

        fetchDeals(params)
            .then(res => {
                if (isMounted) {
                    const dealsData = res?.deals || res?.items || [];
                    setDeals(dealsData);
                }
            })
            .catch(err => {
                if (isMounted) {
                    console.warn("StockDealsSection fetch error:", err);
                    setError("Failed to load institutional deals for this stock.");
                }
            })
            .finally(() => {
                if (isMounted) setLoading(false);
            });

        return () => { isMounted = false; };
    }, [companyId, symbol, isin, companyName]);

    // Aggregate statistics
    const stats = useMemo(() => {
        let buyVal = 0;
        let sellVal = 0;
        let buyCount = 0;
        let sellCount = 0;
        let bulkCount = 0;
        let blockCount = 0;

        for (const d of deals) {
            const val = parseFloat(d.trade_value_inr) || 0;
            if (d.action === 'BUY') {
                buyVal += val;
                buyCount++;
            } else if (d.action === 'SELL') {
                sellVal += val;
                sellCount++;
            }
            if (d.deal_type === 'BULK') bulkCount++;
            if (d.deal_type === 'BLOCK') blockCount++;
        }

        const netFlow = buyVal - sellVal;
        return {
            totalDeals: deals.length,
            buyVal,
            sellVal,
            netFlow,
            buyCount,
            sellCount,
            bulkCount,
            blockCount
        };
    }, [deals]);

    // Filter deals by tab and search input
    const filteredDeals = useMemo(() => {
        return deals.filter(d => {
            if (typeFilter !== 'ALL' && d.deal_type !== typeFilter) return false;
            if (searchQuery.trim()) {
                const q = searchQuery.toLowerCase();
                const client = (d.client_name || '').toLowerCase();
                const sec = (d.security_name || '').toLowerCase();
                return client.includes(q) || sec.includes(q);
            }
            return true;
        });
    }, [deals, typeFilter, searchQuery]);

    const visibleDeals = useMemo(() => {
        return filteredDeals.slice(0, displayLimit);
    }, [filteredDeals, displayLimit]);

    const getCatClass = (cat) => {
        switch (cat) {
            case 'MUTUAL_FUND': return 'sds-cat-mf';
            case 'FII_FPI': return 'sds-cat-fii';
            case 'AIF': return 'sds-cat-aif';
            case 'PROMOTER': return 'sds-cat-promoter';
            default: return 'sds-cat-other';
        }
    };

    const getCatLabel = (cat) => {
        switch (cat) {
            case 'MUTUAL_FUND': return 'Mutual Fund';
            case 'FII_FPI': return 'FII / FPI';
            case 'AIF': return 'AIF';
            case 'PROMOTER': return 'Promoter';
            case 'DII': return 'DII';
            default: return 'Other / HNI';
        }
    };

    if (loading) {
        return (
            <div className="stock-deals-container">
                <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-secondary)' }}>
                    Loading institutional market deals...
                </div>
            </div>
        );
    }

    if (!loading && deals.length === 0) {
        return (
            <div className="stock-deals-container">
                <div className="sds-header">
                    <div className="sds-title-group">
                        <h3 className="sds-title">
                            <Activity size={18} className="sds-title-icon" />
                            Institutional Market Deals (Bulk & Block Activity)
                        </h3>
                        <p className="sds-subtitle">
                            Large-volume market transactions executed across NSE & BSE
                        </p>
                    </div>
                </div>
                <div className="sds-empty">
                    <ShieldCheck size={36} className="sds-empty-icon" />
                    <h4 className="sds-empty-title">No Large Deals Reported</h4>
                    <p className="sds-empty-sub">
                        No bulk (&gt;0.5% equity) or block window trades have been recorded for {companyName || symbol} in our historical archives.
                    </p>
                    <Link to={`/deals?search=${encodeURIComponent(symbol || isin || companyName || '')}`} className="sds-explorer-link" style={{ marginTop: '8px' }}>
                        Open Market Deals Explorer <ExternalLink size={13} />
                    </Link>
                </div>
            </div>
        );
    }

    return (
        <div className="stock-deals-container" id="institutional-deals-section">
            {/* Header */}
            <div className="sds-header">
                <div className="sds-title-group">
                    <h3 className="sds-title">
                        <Activity size={18} className="sds-title-icon" />
                        Institutional Market Deals (Bulk & Block Activity)
                    </h3>
                    <p className="sds-subtitle">
                        Historical large block window trades and bulk disclosures for {companyName || symbol}
                    </p>
                </div>
                <div className="sds-header-actions">
                    <Link to={`/deals?search=${encodeURIComponent(symbol || isin || companyName || '')}`} className="sds-explorer-link">
                        Full Deals Hub <ExternalLink size={13} />
                    </Link>
                </div>
            </div>

            {/* Institutional Flow Summary Cards */}
            <div className="sds-flow-bar">
                <div className="sds-stat-card">
                    <span className="sds-stat-label">Net Institutional Flow</span>
                    <span className={`sds-stat-value ${stats.netFlow >= 0 ? 'positive' : 'negative'}`}>
                        {stats.netFlow >= 0 ? <TrendingUp size={16} /> : <TrendingDown size={16} />}
                        {stats.netFlow >= 0 ? '+' : ''}{fmtCr(stats.netFlow)}
                    </span>
                    <span className="sds-stat-sub">
                        {stats.netFlow >= 0 ? 'Net Accumulation' : 'Net Distribution'} across recorded deals
                    </span>
                </div>

                <div className="sds-stat-card">
                    <span className="sds-stat-label">Institutional Buy Action</span>
                    <span className="sds-stat-value positive">
                        {fmtCr(stats.buyVal)}
                    </span>
                    <span className="sds-stat-sub">{stats.buyCount} buyer filings</span>
                </div>

                <div className="sds-stat-card">
                    <span className="sds-stat-label">Institutional Sell Action</span>
                    <span className="sds-stat-value negative">
                        {fmtCr(stats.sellVal)}
                    </span>
                    <span className="sds-stat-sub">{stats.sellCount} seller filings</span>
                </div>

                <div className="sds-stat-card">
                    <span className="sds-stat-label">Deals Overview</span>
                    <span className="sds-stat-value" style={{ color: 'var(--text-primary)' }}>
                        {stats.totalDeals} Deals
                    </span>
                    <span className="sds-stat-sub">{stats.bulkCount} Bulk • {stats.blockCount} Block Window</span>
                </div>
            </div>

            {/* Filter Tabs & Search */}
            <div className="sds-controls">
                <div className="sds-type-tabs">
                    <button
                        type="button"
                        className={`sds-type-btn ${typeFilter === 'ALL' ? 'active' : ''}`}
                        onClick={() => { setTypeFilter('ALL'); setDisplayLimit(10); }}
                    >
                        All Deals ({stats.totalDeals})
                    </button>
                    <button
                        type="button"
                        className={`sds-type-btn ${typeFilter === 'BULK' ? 'active' : ''}`}
                        onClick={() => { setTypeFilter('BULK'); setDisplayLimit(10); }}
                    >
                        Bulk Deals ({stats.bulkCount})
                    </button>
                    <button
                        type="button"
                        className={`sds-type-btn ${typeFilter === 'BLOCK' ? 'active' : ''}`}
                        onClick={() => { setTypeFilter('BLOCK'); setDisplayLimit(10); }}
                    >
                        Block Window ({stats.blockCount})
                    </button>
                </div>

                <input
                    type="text"
                    className="sds-search-input"
                    placeholder="Search investor / fund..."
                    value={searchQuery}
                    onChange={(e) => { setSearchQuery(e.target.value); setDisplayLimit(10); }}
                />
            </div>

            {/* Deals Table */}
            <div className="sds-table-wrapper">
                <table className="sds-table">
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Type</th>
                            <th>Exch</th>
                            <th>Side</th>
                            <th>Client / Investor</th>
                            <th className="sds-num">Quantity</th>
                            <th className="sds-num">Price</th>
                            <th className="sds-num">Total Value</th>
                            <th className="sds-num">% Equity</th>
                        </tr>
                    </thead>
                    <tbody>
                        {visibleDeals.map((deal) => (
                            <tr key={deal.deal_id}>
                                <td style={{ fontWeight: 600 }}>{deal.trade_date}</td>
                                <td>
                                    <span className={`sds-badge-type ${deal.deal_type.toLowerCase()}`}>
                                        {deal.deal_type}
                                    </span>
                                </td>
                                <td>
                                    <span className="sds-badge-exch">{deal.exchange}</span>
                                </td>
                                <td>
                                    <span className={`sds-badge-side ${deal.action.toLowerCase()}`}>
                                        {deal.action === 'BUY' ? '↗ BUY' : '↘ SELL'}
                                    </span>
                                </td>
                                <td>
                                    <div className="sds-client-wrap">
                                        <span className="sds-client-name" title={deal.client_name}>
                                            {deal.client_name}
                                        </span>
                                        <span className={`sds-cat-pill ${getCatClass(deal.investor_category)}`}>
                                            {getCatLabel(deal.investor_category)}
                                        </span>
                                    </div>
                                </td>
                                <td className="sds-num">{fmt(deal.quantity)}</td>
                                <td className="sds-num">{fmtPrice(deal.trade_price)}</td>
                                <td className="sds-num sds-val-strong">{fmtCr(deal.trade_value_inr)}</td>
                                <td className="sds-num" style={{ color: 'var(--text-secondary)' }}>
                                    {deal.pct_equity_traded ? `${Number(deal.pct_equity_traded).toFixed(2)}%` : '—'}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            {/* Footer with Load More / Total */}
            <div className="sds-footer">
                <span>
                    Showing {visibleDeals.length} of {filteredDeals.length} recorded deals
                </span>
                {filteredDeals.length > visibleDeals.length && (
                    <button
                        type="button"
                        className="sds-more-btn"
                        onClick={() => setDisplayLimit(prev => prev + 15)}
                    >
                        Show More Deals ({filteredDeals.length - visibleDeals.length} remaining)
                    </button>
                )}
            </div>
        </div>
    );
}
