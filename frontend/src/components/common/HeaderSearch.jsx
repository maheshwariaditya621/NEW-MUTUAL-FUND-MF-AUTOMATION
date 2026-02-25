import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { LayoutGrid, TrendingUp, Landmark, FileText, PiggyBank } from 'lucide-react';
import SearchBox from './SearchBox';
import { searchStocks } from '../../api/stocks';
import { searchSchemes } from '../../api/schemes';
import './HeaderSearch.css';

export default function HeaderSearch() {
    const [suggestions, setSuggestions] = useState([]);
    const [loading, setLoading] = useState(false);
    const [activeFilter, setActiveFilter] = useState('all'); // 'all' | 'stocks' | 'schemes'
    const [lastQuery, setLastQuery] = useState('');
    const navigate = useNavigate();

    const handleSearch = async (query) => {
        setLastQuery(query);
        setLoading(true);
        try {
            // "Show all results" - pass a high limit
            const limit = 500;

            let stockPromise = Promise.resolve({ results: [] });
            let schemePromise = Promise.resolve({ results: [] });

            if (activeFilter === 'all' || activeFilter === 'stocks') {
                stockPromise = searchStocks(query, limit);
            }
            if (activeFilter === 'all' || activeFilter === 'schemes') {
                schemePromise = searchSchemes(query, limit);
            }

            const [stockRes, schemeRes] = await Promise.all([stockPromise, schemePromise]);

            const stocks = (stockRes.results || []).map(s => ({ ...s, type: 'stock' }));
            const schemes = (schemeRes.results || []).map(s => ({ ...s, type: 'scheme' }));

            setSuggestions([...stocks, ...schemes]);
        } catch (err) {
            console.error('Header search error:', err);
            setSuggestions([]);
        } finally {
            setLoading(false);
        }
    };

    // Re-trigger search when filter changes
    useEffect(() => {
        if (lastQuery.length >= 2) {
            handleSearch(lastQuery);
        }
    }, [activeFilter]);

    const handleSelect = (item) => {
        if (item.type === 'stock') {
            navigate(`/stocks?isin=${item.isin}`);
        } else {
            navigate(`/schemes?scheme_id=${item.scheme_id}`);
        }
    };

    const renderSuggestion = (item) => (
        <div className={`header-suggestion ${item.type}`}>
            <div className="header-suggestion-main">
                <span className="stock-name">{item.type === 'stock' ? item.company_name : item.scheme_name}</span>
                <span className={`search-type-badge ${item.type}`}>
                    {item.type === 'stock' ? 'STOCK' : 'SCHEME'}
                </span>
            </div>
            <div className="header-suggestion-meta">
                {item.type === 'stock' ? (
                    <>
                        <span className="stock-isin">{item.isin}</span>
                        {item.nse_symbol && <span className="meta-sep">|</span>}
                        {item.nse_symbol && <span className="stock-symbol-mini">{item.nse_symbol}</span>}
                    </>
                ) : (
                    <>
                        <span>{item.amc_name}</span>
                        <span className="meta-sep">|</span>
                        <span>{item.plan_type} • {item.option_type}</span>
                    </>
                )}
            </div>
        </div>
    );

    const renderTabs = () => (
        <div className="search-tabs-container">
            <div className="search-tabs">
                <button
                    className={`search-tab ${activeFilter === 'all' ? 'active' : ''}`}
                    onClick={(e) => { e.stopPropagation(); setActiveFilter('all'); }}
                >
                    <LayoutGrid size={14} />
                    <span>All</span>
                </button>
                <button
                    className={`search-tab ${activeFilter === 'schemes' ? 'active' : ''}`}
                    onClick={(e) => { e.stopPropagation(); setActiveFilter('schemes'); }}
                >
                    <Landmark size={14} />
                    <span>Mutual Funds</span>
                </button>
                <button
                    className={`search-tab ${activeFilter === 'stocks' ? 'active' : ''}`}
                    onClick={(e) => { e.stopPropagation(); setActiveFilter('stocks'); }}
                >
                    <TrendingUp size={14} />
                    <span>Stocks</span>
                </button>
            </div>
        </div>
    );

    return (
        <div className="header-search">
            <SearchBox
                placeholder={`Search ${activeFilter === 'all' ? 'anything' : activeFilter}...`}
                onSearch={handleSearch}
                suggestions={suggestions}
                onSelect={handleSelect}
                loading={loading}
                renderSuggestion={renderSuggestion}
                dropdownHeader={renderTabs()}
                minChars={2}
            />
        </div>
    );
}
