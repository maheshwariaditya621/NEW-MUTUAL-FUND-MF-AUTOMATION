import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { LayoutGrid, TrendingUp, Landmark, Bookmark, BookmarkCheck } from 'lucide-react';
import SearchBox from './SearchBox';
import { searchStocks } from '../../api/stocks';
import { searchSchemes } from '../../api/schemes';
import { useWatchlist } from '../../contexts/WatchlistContext';
import './HeaderSearch.css';

// ── Small inline watch button rendered inside each suggestion ──────────────────
function WatchBtn({ item, onClick }) {
    const { isWatched } = useWatchlist();

    const identifier = item.type === 'stock' ? item.isin : String(item.scheme_id);
    const watched = isWatched(item.type, identifier);

    return (
        <button
            id={`search-watch-${item.type}-${identifier}`}
            className={`search-watch-btn ${watched ? 'watched' : ''}`}
            title={watched ? 'Remove from watchlist' : 'Add to watchlist'}
            onClick={(e) => {
                e.stopPropagation(); // don't navigate
                onClick(e, item, watched);
            }}
            aria-label={watched ? 'Remove from watchlist' : 'Add to watchlist'}
        >
            {watched
                ? <BookmarkCheck size={13} strokeWidth={2.5} />
                : <Bookmark size={13} strokeWidth={2} />
            }
        </button>
    );
}

export default function HeaderSearch() {
    const [suggestions, setSuggestions] = useState([]);
    const [loading, setLoading] = useState(false);
    const [activeFilter, setActiveFilter] = useState('all');
    const [lastQuery, setLastQuery] = useState('');
    const navigate = useNavigate();
    const { addToWatchlist, removeFromWatchlist, getWatchlistId } = useWatchlist();

    const handleSearch = async (query) => {
        setLastQuery(query);
        setLoading(true);
        try {
            const limit = 500;
            let stockPromise = Promise.resolve({ results: [] });
            let schemePromise = Promise.resolve({ results: [] });

            if (activeFilter === 'all' || activeFilter === 'stocks')
                stockPromise = searchStocks(query, limit);
            if (activeFilter === 'all' || activeFilter === 'schemes')
                schemePromise = searchSchemes(query, limit);

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

    useEffect(() => {
        if (lastQuery.length >= 2) handleSearch(lastQuery);
    }, [activeFilter]);

    const handleSelect = (item) => {
        if (item.type === 'stock') navigate(`/stocks?isin=${item.isin}`);
        else navigate(`/schemes?scheme_id=${item.scheme_id}`);
    };

    const handleWatchToggle = useCallback(async (e, item, alreadyWatched) => {
        if (alreadyWatched) {
            const identifier = item.type === 'stock' ? item.isin : String(item.scheme_id);
            await removeFromWatchlist(item.type, identifier);
        } else {
            await addToWatchlist({
                assetType: item.type,
                isin: item.type === 'stock' ? item.isin : undefined,
                schemeId: item.type === 'scheme' ? item.scheme_id : undefined,
                name: item.type === 'stock' ? item.company_name : item.scheme_name,
            });
        }
    }, [addToWatchlist, removeFromWatchlist]);

    const renderSuggestion = (item) => (
        <div className={`header-suggestion ${item.type}`}>
            <div className="header-suggestion-main">
                <span className="stock-name">
                    {item.type === 'stock' ? item.company_name : item.scheme_name}
                </span>
                <div className="header-suggestion-right">
                    <span className={`search-type-badge ${item.type}`}>
                        {item.type === 'stock' ? 'STOCK' : 'SCHEME'}
                    </span>
                    <WatchBtn item={item} onClick={handleWatchToggle} />
                </div>
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
