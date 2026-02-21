import React, { useState } from 'react';
import SearchBox from '../components/common/SearchBox';
import Card from '../components/common/Card';
import Table from '../components/common/Table';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import { searchSchemes, getSchemePortfolio } from '../api/schemes';
import { handleApiError } from '../api/client';
import { formatCrores, formatPercent, formatNumber } from '../utils/helpers';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import './SchemePortfolioPage.css';
import '../components/common/RupeevestTable.css';

export default function SchemePortfolioPage() {
    const [suggestions, setSuggestions] = useState([]);
    const [searchLoading, setSearchLoading] = useState([]);
    const [selectedScheme, setSelectedScheme] = useState(null);
    const [portfolio, setPortfolio] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [sortConfig, setSortConfig] = useState({ key: 'month_3', direction: 'desc' });
    const [filterText, setFilterText] = useState('');

    const handleSearch = async (query) => {
        setSearchLoading(true);
        setError(null);

        try {
            const result = await searchSchemes(query, 1000);
            setSuggestions(result.results || []);
        } catch (err) {
            console.error('Search error:', err);
            setSuggestions([]);
        } finally {
            setSearchLoading(false);
        }
    };

    const handleSelectScheme = async (scheme) => {
        setSelectedScheme(scheme);
        setLoading(true);
        setError(null);
        setPortfolio(null);

        try {
            const data = await getSchemePortfolio(scheme.scheme_id, 4);
            setPortfolio(data);
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    const renderSuggestion = (scheme) => (
        <div className="scheme-suggestion">
            <div className="scheme-suggestion-main">
                <span className="scheme-name-text">{scheme.scheme_name}</span>
            </div>
            <div className="scheme-suggestion-meta">
                <span>{scheme.amc_name}</span>
                <span>•</span>
                <span>{scheme.plan_type}</span>
                <span>•</span>
                <span>{scheme.option_type}</span>
            </div>
        </div>
    );

    const getTrendIcon = (current, previous) => {
        if (!previous) return <Minus size={16} className="trend-neutral" />;
        if (current > previous) return <TrendingUp size={16} className="trend-up" />;
        if (current < previous) return <TrendingDown size={16} className="trend-down" />;
        return <Minus size={16} className="trend-neutral" />;
    };

    const handleSort = (key) => {
        let direction = 'desc';
        if (sortConfig.key === key && sortConfig.direction === 'desc') {
            direction = 'asc';
        }
        setSortConfig({ key, direction });
    };

    const getHoldingsToRender = () => {
        if (!portfolio || !portfolio.holdings) return [];

        let items = [...portfolio.holdings];

        // 1. Filter
        if (filterText) {
            const lowFilter = filterText.toLowerCase();
            items = items.filter(h =>
                h.company_name.toLowerCase().includes(lowFilter) ||
                (h.sector && h.sector.toLowerCase().includes(lowFilter)) ||
                (h.isin && h.isin.toLowerCase().includes(lowFilter))
            );
        }

        // 2. Sort
        items.sort((a, b) => {
            let aVal, bVal;

            if (sortConfig.key === 'company_name') {
                aVal = a.company_name;
                bVal = b.company_name;
            } else if (sortConfig.key.startsWith('month_')) {
                const monthIdx = parseInt(sortConfig.key.split('_')[1]);
                aVal = a.monthly_data[monthIdx]?.percent_to_aum || 0;
                bVal = b.monthly_data[monthIdx]?.percent_to_aum || 0;
            } else if (sortConfig.key === 'market_cap') {
                aVal = a.market_cap || 0;
                bVal = b.market_cap || 0;
            }

            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });

        return items;
    };

    // Build columns dynamically based on monthly data
    const getPortfolioColumns = () => {
        if (!portfolio || !portfolio.monthly_aum || portfolio.monthly_aum.length === 0) {
            return [];
        }

        const columns = [
            {
                key: 'company_name',
                label: 'Company',
                sortable: true,
                render: (value, row) => (
                    <div>
                        <div className="company-name">{value}</div>
                        {row.sector && (
                            <div className="company-sector">{row.sector}</div>
                        )}
                    </div>
                )
            }
        ];

        // Add column for each month
        portfolio.monthly_aum.forEach((monthData, index) => {
            columns.push({
                key: `month_${index}`,
                label: monthData.month,
                sortable: true,
                render: (value, row) => {
                    const monthlyData = row.monthly_data[index];
                    if (!monthlyData) return '-';

                    const prevMonthlyData = index > 0 ? row.monthly_data[index - 1] : null;
                    const trend = prevMonthlyData
                        ? getTrendIcon(monthlyData.percent_to_aum, prevMonthlyData.percent_to_aum)
                        : null;

                    return (
                        <div className="monthly-cell">
                            <div className="monthly-percent">
                                {formatPercent(monthlyData.percent_to_aum, 2)}
                                {trend && <span className="trend-icon">{trend}</span>}
                            </div>
                            <div className="monthly-shares">
                                {formatNumber(monthlyData.num_shares)} shares
                            </div>
                        </div>
                    );
                }
            });
        });

        return columns;
    };

    // Transform portfolio data for table
    const getTableData = () => {
        if (!portfolio || !portfolio.holdings) return [];

        return portfolio.holdings.map(holding => ({
            company_name: holding.company_name,
            sector: holding.sector,
            monthly_data: holding.monthly_data,
            // Add sortable values for each month
            ...holding.monthly_data.reduce((acc, data, index) => ({
                ...acc,
                [`month_${index}`]: data.percent_to_aum
            }), {})
        }));
    };

    return (
        <div className="scheme-portfolio-page">
            <div className="container-wide" style={{ padding: '20px' }}>
                <div className="page-header" style={{ marginBottom: '20px' }}>
                    <h2 style={{ fontSize: '24px', fontWeight: 'bold' }}>Scheme Portfolio Tracker</h2>
                    <p className="text-muted" style={{ fontSize: '14px' }}>
                        Search for a mutual fund scheme to view its portfolio holdings
                    </p>
                </div>

                <div className="search-section" style={{ marginBottom: '30px' }}>
                    <SearchBox
                        placeholder="Search by scheme name or AMC..."
                        onSearch={handleSearch}
                        suggestions={suggestions}
                        onSelect={handleSelectScheme}
                        loading={searchLoading}
                        renderSuggestion={renderSuggestion}
                        minChars={2}
                    />
                </div>

                {loading && <Loading message="Loading portfolio data..." />}

                {error && (
                    <ErrorMessage
                        message={error}
                        onRetry={() => selectedScheme && handleSelectScheme(selectedScheme)}
                    />
                )}

                {portfolio && !loading && !error && (
                    <div className="portfolio-section">
                        <div className="rv-meta-bar">
                            <div className="rv-meta-item">
                                <span className="rv-meta-label">Fund Name :</span>
                                <span className="rv-meta-value">{portfolio.scheme_name}</span>
                            </div>
                            <div className="rv-meta-item">
                                <span className="rv-meta-label">Category :</span>
                                <span className="rv-meta-value">{portfolio.category || 'Portfolio'}</span>
                            </div>
                        </div>

                        <div className="portfolio-table-section">
                            <div className="section-header-row" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
                                <h4 style={{ margin: 0, fontSize: '18px' }}>Equity Holdings</h4>
                                <div className="search-holdings-container" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                    <span style={{ fontSize: '12px', color: '#666' }}>Search Here</span>
                                    <input
                                        type="text"
                                        placeholder="Company, ISIN, Sector..."
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
                                                <div className="rv-sortable-header" onClick={() => handleSort('company_name')}>
                                                    Company {sortConfig.key === 'company_name' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                                </div>
                                            </th>
                                            {/* Market Cap removed */}
                                            {portfolio.monthly_aum.map((m, idx) => (
                                                <th key={idx} colSpan="2" className="text-center">
                                                    <div className="rv-sortable-header" onClick={() => handleSort(`month_${idx}`)}>
                                                        {m.month} {sortConfig.key === `month_${idx}` && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                                    </div>
                                                    <div className="rv-aum-label">
                                                        AUM: {m.aum_cr > 0 ? `₹ ${formatNumber(m.aum_cr)} (Cr.)` : '₹ - (Cr.)'}
                                                    </div>
                                                </th>
                                            ))}
                                        </tr>
                                        <tr>
                                            {portfolio.monthly_aum.map((_, idx) => (
                                                <React.Fragment key={idx}>
                                                    <th className="rv-sub-header text-center">% of AUM</th>
                                                    <th className="rv-sub-header text-center">No. of Shares</th>
                                                </React.Fragment>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {getHoldingsToRender().map((holding, hIdx) => (
                                            <tr key={hIdx}>
                                                <td className="text-left">
                                                    <div className="rv-company-name">{holding.company_name}</div>
                                                    <div className="rv-company-meta">
                                                        <span className="rv-isin">{holding.isin}</span>
                                                        {holding.sector && <span className="rv-meta-sep">|</span>}
                                                        {holding.sector && <span className="rv-sector">{holding.sector}</span>}
                                                    </div>
                                                </td>
                                                {/* Market Cap cell removed */}
                                                {holding.monthly_data.map((m, mIdx) => {
                                                    const prevM = mIdx > 0 ? holding.monthly_data[mIdx - 1] : null;
                                                    const hasData = m.num_shares > 0;

                                                    return (
                                                        <React.Fragment key={mIdx}>
                                                            <td className="text-center">
                                                                {hasData ? formatPercent(m.percent_to_aum, 2) : '-'}
                                                            </td>
                                                            <td className="text-right">
                                                                {hasData ? (
                                                                    <div className="rv-shares-cell">
                                                                        <span>{formatNumber(m.num_shares)}</span>
                                                                        {prevM && prevM.num_shares > 0 && (
                                                                            <span className={`rv-trend ${m.num_shares > prevM.num_shares ? 'up' : m.num_shares < prevM.num_shares ? 'down' : ''}`}>
                                                                                {m.num_shares > prevM.num_shares ? '▲' : m.num_shares < prevM.num_shares ? '▼' : ''}
                                                                            </span>
                                                                        )}
                                                                    </div>
                                                                ) : '-'}
                                                            </td>
                                                        </React.Fragment>
                                                    );
                                                })}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                )}

                {!selectedScheme && !loading && !error && (
                    <div className="empty-state">
                        <p className="text-muted">
                            Search for a scheme to view its portfolio holdings
                        </p>
                    </div>
                )}
            </div>
        </div>
    );
}
