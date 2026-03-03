import React, { useState, useEffect, useRef } from 'react';
import { Loader2 } from 'lucide-react';
import { debounce } from '../../utils/helpers';
import { searchStocks } from '../../api/stocks';
import { searchSchemes } from '../../api/schemes';
import './PageEmptyState.css';


/**
 * A premium, high-fidelity empty state component for main pages with fuzzy autocomplete.
 */
const PageEmptyState = ({
    title,
    description,
    placeholder,
    onSearch,
    type = 'stock',
    suggestions = []
}) => {
    const [inputValue, setInputValue] = useState('');
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);
    const [showDropdown, setShowDropdown] = useState(false);
    const [selectedIndex, setSelectedIndex] = useState(-1);
    const searchRef = useRef(null);
    const inputRef = useRef(null);

    // Debounced fetch function to match fuzzy search behavior
    const debouncedFetch = useRef(
        debounce(async (query) => {
            if (query.trim().length < 2) {
                setResults([]);
                return;
            }
            setLoading(true);
            try {
                let res;
                if (type === 'stock') {
                    res = await searchStocks(query, 10);
                } else {
                    res = await searchSchemes(query, 10);
                }
                setResults(res.results || []);
            } catch (err) {
                console.error('Empty state search error:', err);
                setResults([]);
            } finally {
                setLoading(false);
            }
        }, 300)
    ).current;

    useEffect(() => {
        if (inputValue.length >= 2) {
            debouncedFetch(inputValue);
            setShowDropdown(true);
        } else {
            setResults([]);
            setShowDropdown(false);
        }
    }, [inputValue, debouncedFetch]);

    // Close on click outside
    useEffect(() => {
        const handleClickOutside = (e) => {
            if (searchRef.current && !searchRef.current.contains(e.target)) {
                setShowDropdown(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const handleSubmit = (e) => {
        if (e) e.preventDefault();
        if (inputValue.trim()) {
            onSearch(inputValue.trim());
            setShowDropdown(false);
        }
    };

    const handleSelect = (item) => {
        const val = type === 'stock' ? item.isin : item.scheme_id;
        onSearch(val);
        setInputValue('');
        setShowDropdown(false);
        setSelectedIndex(-1);
    };

    const handleKeyDown = (e) => {
        if (!showDropdown || results.length === 0) return;

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                setSelectedIndex(prev => (prev < results.length - 1 ? prev + 1 : prev));
                break;
            case 'ArrowUp':
                e.preventDefault();
                setSelectedIndex(prev => (prev > 0 ? prev - 1 : -1));
                break;
            case 'Enter':
                e.preventDefault();
                if (selectedIndex >= 0 && results[selectedIndex]) {
                    handleSelect(results[selectedIndex]);
                } else {
                    handleSubmit();
                }
                break;
            case 'Escape':
                setShowDropdown(false);
                setSelectedIndex(-1);
                break;
            default:
                break;
        }
    };

    return (
        <div className="pes-container animate-fade-in">
            <div className="pes-bg-blob blob-1"></div>
            <div className="pes-bg-blob blob-2"></div>

            <div className="pes-content">
                <div className="pes-icon-wrapper">
                    {type === 'stock' ? '📈' : '🏢'}
                </div>

                <h1 className="pes-title">{title}</h1>
                <p className="pes-description">{description}</p>

                <div className="pes-search-form" ref={searchRef}>
                    <div className="pes-search-input-wrapper">
                        <span className="pes-search-icon">🔍</span>
                        <input
                            ref={inputRef}
                            type="text"
                            className="pes-search-input"
                            placeholder={placeholder}
                            value={inputValue}
                            onChange={(e) => setInputValue(e.target.value)}
                            onKeyDown={handleKeyDown}
                            autoComplete="off"
                        />
                        {loading && <Loader2 className="pes-search-loader spin" size={16} />}
                    </div>

                    {showDropdown && (inputValue.length >= 2) && (
                        <div className="pes-autocomplete-dropdown">
                            {results.length > 0 ? (
                                results.map((item, index) => (
                                    <div
                                        key={index}
                                        className={`pes-result-item ${index === selectedIndex ? 'selected' : ''}`}
                                        onClick={() => handleSelect(item)}
                                        onMouseEnter={() => setSelectedIndex(index)}
                                    >
                                        <div className="pes-result-main">
                                            <span className="pes-result-name">
                                                {type === 'stock' ? item.company_name : item.scheme_name}
                                            </span>
                                            <span className={`pes-badge ${type}`}>
                                                {type === 'stock' ? 'STOCK' : 'SCHEME'}
                                            </span>
                                        </div>
                                        <div className="pes-result-meta">
                                            {type === 'stock' ? (
                                                <>
                                                    <span>{item.isin}</span>
                                                    {item.nse_symbol && <span className="meta-sep">|</span>}
                                                    {item.nse_symbol && <span>{item.nse_symbol}</span>}
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
                                ))
                            ) : (
                                !loading && <div className="pes-no-results">No results found for "{inputValue}"</div>
                            )}
                        </div>
                    )}
                </div>

                {suggestions.length > 0 && (
                    <div className="pes-suggestions">
                        <span className="pes-suggestion-label">Popular:</span>
                        <div className="pes-suggestion-chips">
                            {suggestions.map((suggestion, index) => (
                                <button
                                    key={index}
                                    className="pes-suggestion-chip"
                                    onClick={() => onSearch(suggestion)}
                                >
                                    {suggestion}
                                </button>
                            ))}
                        </div>
                    </div>
                )}

                <div className="pes-discoverability-hint">
                    <div className="pes-hint-arrow">↑</div>
                    <p>You can also use the global search at the top anytime.</p>
                </div>
            </div>

            <div className="pes-footer">
                <div className="pes-footer-left">
                    Mutual Fund Analytics Platform © 2026
                </div>
                <div className="pes-footer-right">
                    <span className="pes-admin-icon">🔐</span> Admin Vault
                </div>
            </div>
        </div>
    );
};

export default PageEmptyState;
