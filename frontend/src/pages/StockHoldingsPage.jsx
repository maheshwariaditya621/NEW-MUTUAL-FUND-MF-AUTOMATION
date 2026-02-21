import React, { useState } from 'react';
import SearchBox from '../components/common/SearchBox';
import Card from '../components/common/Card';
import Table from '../components/common/Table';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import { searchStocks, getStockHoldings } from '../api/stocks';
import { handleApiError } from '../api/client';
import { formatCrores, formatPercent, formatNumber } from '../utils/helpers';
import './StockHoldingsPage.css';
import '../components/common/RupeevestTable.css';

export default function StockHoldingsPage() {
    const [suggestions, setSuggestions] = useState([]);
    const [searchLoading, setSearchLoading] = useState(false);
    const [selectedStock, setSelectedStock] = useState(null);
    const [holdings, setHoldings] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [sortConfig, setSortConfig] = useState({ key: 'shares_latest', direction: 'desc' });
    const [filterText, setFilterText] = useState('');

    const handleSearch = async (query) => {
        setSearchLoading(true);
        setError(null);

        try {
            const result = await searchStocks(query, 1000);
            setSuggestions(result.results || []);
        } catch (err) {
            console.error('Search error:', err);
            setSuggestions([]);
        } finally {
            setSearchLoading(false);
        }
    };

    const handleSelectStock = async (stock) => {
        setSelectedStock(stock);
        setLoading(true);
        setError(null);
        setHoldings(null);

        try {
            const data = await getStockHoldings(stock.isin, 4);
            setHoldings(data);
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    const handleSort = (key) => {
        let direction = 'desc';
        if (sortConfig.key === key && sortConfig.direction === 'desc') {
            direction = 'asc';
        }
        setSortConfig({ key, direction });
    };

    const getHoldingsToRender = () => {
        if (!holdings || !holdings.holdings) return [];

        let items = [...holdings.holdings];

        // 1. Filter
        if (filterText) {
            const lowFilter = filterText.toLowerCase();
            items = items.filter(h =>
                h.scheme_name.toLowerCase().includes(lowFilter) ||
                h.amc_name.toLowerCase().includes(lowFilter)
            );
        }

        // 2. Sort
        items.sort((a, b) => {
            let aVal, bVal;

            if (sortConfig.key === 'scheme_name') {
                aVal = a.scheme_name;
                bVal = b.scheme_name;
            } else if (sortConfig.key === 'aum_cr') {
                aVal = a.aum_cr || 0;
                bVal = b.aum_cr || 0;
            } else if (sortConfig.key === 'pnav') {
                aVal = a.history[0]?.percent_to_aum || 0;
                bVal = b.history[0]?.percent_to_aum || 0;
            } else if (sortConfig.key.startsWith('shares_')) {
                const monthIdx = parseInt(sortConfig.key.split('_')[1]);
                aVal = a.history[monthIdx]?.num_shares || 0;
                bVal = b.history[monthIdx]?.num_shares || 0;
            }

            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });

        return items;
    };

    const renderSuggestion = (stock) => (
        <div className="stock-suggestion">
            <div className="stock-suggestion-main">
                <span className="stock-name">{stock.company_name}</span>
                {stock.nse_symbol && (
                    <span className="stock-symbol">{stock.nse_symbol}</span>
                )}
            </div>
            <div className="stock-suggestion-meta">
                <span className="stock-isin">{stock.isin}</span>
                {stock.sector && (
                    <span className="stock-sector">{stock.sector}</span>
                )}
            </div>
        </div>
    );

    const historyMonths = holdings && holdings.holdings.length > 0
        ? holdings.holdings[0].history.map(h => h.month)
        : [];

    const holdingsColumns = [
        {
            key: 'scheme_name',
            label: 'Scheme Name',
            sortable: true,
            render: (value, row) => (
                <div>
                    <div className="scheme-name">{value}</div>
                    <div className="scheme-meta">
                        {row.amc_name} • {row.plan_type} • {row.option_type}
                    </div>
                </div>
            )
        },
        {
            key: 'aum_cr',
            label: 'Eq. AUM (Cr)',
            sortable: true,
            className: 'text-right',
            render: (value) => formatCrores(value * 10000000)
        }
    ];

    if (historyMonths.length > 0) {
        // % to NAV Column (Latest)
        holdingsColumns.push({
            key: 'pnav_latest',
            label: `% to NAV (${historyMonths[0]})`,
            sortable: true,
            className: 'text-right',
            sortValue: (row) => parseFloat(row.history[0]?.percent_to_aum || 0),
            render: (_, row) => formatPercent(row.history[0]?.percent_to_aum || 0)
        });

        // Shares Columns (Dynamic)
        historyMonths.forEach((month, idx) => {
            holdingsColumns.push({
                key: `shares_${month}`,
                label: `Shares (${month})`,
                sortable: true,
                className: 'text-right',
                sortValue: (row) => row.history[idx]?.num_shares || 0,
                render: (_, row) => {
                    const h = row.history[idx];
                    if (!h) return <span className="text-muted">-</span>;
                    return (
                        <div className="shares-cell">
                            <div className="shares-wrapper">
                                <span>{formatNumber(h.num_shares)}</span>
                                {h.is_adjusted && (
                                    <i className="info-icon" title="Quantity adjusted for corporate actions (splits/bonuses)">i</i>
                                )}
                            </div>
                            {h.trend === 'up' && <span className="trend-up">▲</span>}
                            {h.trend === 'down' && <span className="trend-down">▼</span>}
                        </div>
                    );
                }
            });
        });
    }


    return (
        <div className="stock-holdings-page">
            <div className="container" style={{ padding: '20px' }}>
                <div className="page-header" style={{ marginBottom: '20px' }}>
                    <h2 style={{ fontSize: '24px', fontWeight: 'bold' }}>Stock Holdings Search</h2>
                    <p className="text-muted" style={{ fontSize: '14px' }}>
                        Search for a stock to see which mutual fund schemes hold it
                    </p>
                </div>

                <div className="search-section" style={{ marginBottom: '30px' }}>
                    <SearchBox
                        placeholder="Search by company name, ISIN, or NSE symbol..."
                        onSearch={handleSearch}
                        suggestions={suggestions}
                        onSelect={handleSelectStock}
                        loading={searchLoading}
                        renderSuggestion={renderSuggestion}
                        minChars={2}
                    />
                </div>

                {loading && <Loading message="Loading stock holdings..." />}

                {error && (
                    <ErrorMessage
                        message={error}
                        onRetry={() => selectedStock && handleSelectStock(selectedStock)}
                    />
                )}

                {holdings && !loading && !error && (
                    <div className="holdings-section">
                        <div className="rv-meta-bar">
                            <div className="rv-meta-item">
                                <span className="rv-meta-label">Stock Name :</span>
                                <span className="rv-meta-value">
                                    {holdings.company_name}
                                    <span style={{ fontWeight: 'normal', fontSize: '12px', color: 'var(--text-secondary)', marginLeft: '8px' }}>
                                        (As on {holdings.as_of_date})
                                    </span>
                                </span>
                            </div>
                            <div className="rv-meta-item">
                                <span className="rv-meta-label">ISIN :</span>
                                <span className="rv-meta-value" style={{ fontFamily: 'monospace' }}>{holdings.isin}</span>
                            </div>
                            {holdings.market_cap && (
                                <div className="rv-meta-item">
                                    <span className="rv-meta-label">M-Cap :</span>
                                    <span className="rv-meta-value">
                                        ₹ {formatNumber(holdings.market_cap)} Cr
                                        {holdings.mcap_type && <span className={`rv-mcap-type ${holdings.mcap_type.toLowerCase().replace(' ', '-')}`} style={{ marginLeft: '8px' }}>{holdings.mcap_type}</span>}
                                    </span>
                                </div>
                            )}
                        </div>

                        <div className="rv-summary-box" style={{ marginBottom: '30px' }}>
                            <div className="stock-summary-table-wrapper">
                                <table className="rv-table">
                                    <thead>
                                        <tr>
                                            <th rowSpan="2" className="text-center">Sector</th>
                                            <th rowSpan="2" className="text-center">No. of Funds</th>
                                            <th colSpan={holdings.monthly_trend.length} className="text-center">No. of Shares</th>
                                        </tr>
                                        <tr>
                                            {holdings.monthly_trend.map(m => (
                                                <th key={m.month} className="rv-sub-header text-center">{m.month}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td className="text-center">{holdings.sector || '-'}</td>
                                            <td className="text-center">{holdings.total_funds}</td>
                                            {holdings.monthly_trend.map((m, idx) => (
                                                <td key={m.month} className="text-right">
                                                    <div className="rv-shares-cell">
                                                        <span>{formatNumber(m.total_shares)}</span>
                                                        {m.trend === 'up' && <span className="rv-trend up">▲</span>}
                                                        {m.trend === 'down' && <span className="rv-trend down">▼</span>}
                                                        {m.trend === 'same' && <span className="rv-trend neutral">-</span>}
                                                    </div>
                                                </td>
                                            ))}
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        <div className="holdings-table-section">
                            <div className="section-header-row" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
                                <h4 style={{ margin: 0, fontSize: '18px' }}>Mutual Fund Holdings</h4>
                                <div className="search-holdings-container" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                    <span style={{ fontSize: '12px', color: '#666' }}>Search Here</span>
                                    <input
                                        type="text"
                                        placeholder="Fund or AMC name..."
                                        value={filterText}
                                        onChange={(e) => setFilterText(e.target.value)}
                                        style={{
                                            border: '1px solid var(--border-color)',
                                            padding: '4px 8px',
                                            borderRadius: '4px',
                                            fontSize: '13px',
                                            background: 'var(--bg-primary)',
                                            color: 'var(--text-primary)',
                                            width: '200px'
                                        }}
                                    />
                                </div>
                            </div>

                            <div className="rv-table-container">
                                <table className="rv-table">
                                    <thead>
                                        <tr>
                                            <th rowSpan="2" style={{ width: '25%' }}>
                                                <div className="rv-sortable-header" onClick={() => handleSort('scheme_name')}>
                                                    Fund Name {sortConfig.key === 'scheme_name' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                                </div>
                                            </th>
                                            <th rowSpan="2" className="text-center">Fund Manager</th>
                                            <th rowSpan="2" className="text-center">
                                                <div className="rv-sortable-header" onClick={() => handleSort('aum_cr')}>
                                                    Fund AUM (Cr) {sortConfig.key === 'aum_cr' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                                </div>
                                            </th>
                                            <th rowSpan="2" className="text-center">
                                                <div className="rv-sortable-header" onClick={() => handleSort('pnav')}>
                                                    % of AUM {sortConfig.key === 'pnav' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                                </div>
                                            </th>
                                            <th colSpan={historyMonths.length} className="text-center">No. of Shares</th>
                                        </tr>
                                        <tr>
                                            {historyMonths.map((m, idx) => (
                                                <th key={idx} className="rv-sub-header text-center">
                                                    <div className="rv-sortable-header" onClick={() => handleSort(`shares_${idx}`)}>
                                                        {m} {sortConfig.key === `shares_${idx}` && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                                    </div>
                                                </th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {getHoldingsToRender().map((scheme, sIdx) => (
                                            <tr key={sIdx}>
                                                <td className="text-left">
                                                    <div className="rv-company-name">{scheme.scheme_name}</div>
                                                    <div className="rv-company-meta">
                                                        <span className="rv-sector">{scheme.amc_name}</span>
                                                    </div>
                                                </td>
                                                <td className="text-center">-</td>
                                                <td className="text-right">{formatNumber(scheme.aum_cr)}</td>
                                                <td className="text-center">{formatPercent(scheme.history[0]?.percent_to_aum || 0, 2)}</td>
                                                {scheme.history.map((h, hIdx) => (
                                                    <td key={hIdx} className="text-right">
                                                        {h.num_shares > 0 ? (
                                                            <div className="rv-shares-cell">
                                                                <span>{formatNumber(h.num_shares)}</span>
                                                                {h.trend === 'up' && <span className="rv-trend up">▲</span>}
                                                                {h.trend === 'down' && <span className="rv-trend down">▼</span>}
                                                            </div>
                                                        ) : '-'}
                                                    </td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                )}

                {!selectedStock && !loading && !error && (
                    <div className="empty-state">
                        <p className="text-muted">
                            Search for a stock to view its mutual fund holdings
                        </p>
                    </div>
                )}
            </div>
        </div>
    );
}
