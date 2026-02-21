import { useState, useEffect, useRef } from 'react';
import { Search, X, Loader2 } from 'lucide-react';
import { debounce } from '../../utils/helpers';
import './SearchBox.css';

export default function SearchBox({
    placeholder = 'Search...',
    onSearch,
    suggestions = [],
    onSelect,
    loading = false,
    renderSuggestion,
    minChars = 2,
    debounceMs = 300
}) {
    const [query, setQuery] = useState('');
    const [showDropdown, setShowDropdown] = useState(false);
    const [selectedIndex, setSelectedIndex] = useState(-1);
    const searchRef = useRef(null);
    const inputRef = useRef(null);

    // Debounced search function
    const debouncedSearch = useRef(
        debounce((value) => {
            if (value.length >= minChars && onSearch) {
                onSearch(value);
            }
        }, debounceMs)
    ).current;

    useEffect(() => {
        if (query.length >= minChars) {
            debouncedSearch(query);
            setShowDropdown(true);
        } else {
            setShowDropdown(false);
        }
    }, [query, minChars, debouncedSearch]);

    // Close dropdown when clicking outside
    useEffect(() => {
        function handleClickOutside(event) {
            if (searchRef.current && !searchRef.current.contains(event.target)) {
                setShowDropdown(false);
            }
        }

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    // Keyboard navigation
    const handleKeyDown = (e) => {
        if (!showDropdown || suggestions.length === 0) return;

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                setSelectedIndex(prev =>
                    prev < suggestions.length - 1 ? prev + 1 : prev
                );
                break;
            case 'ArrowUp':
                e.preventDefault();
                setSelectedIndex(prev => prev > 0 ? prev - 1 : -1);
                break;
            case 'Enter':
                e.preventDefault();
                if (selectedIndex >= 0 && suggestions[selectedIndex]) {
                    handleSelect(suggestions[selectedIndex]);
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

    const handleSelect = (item) => {
        if (onSelect) {
            onSelect(item);
        }
        setQuery('');
        setShowDropdown(false);
        setSelectedIndex(-1);
        inputRef.current?.blur();
    };

    const handleClear = () => {
        setQuery('');
        setShowDropdown(false);
        setSelectedIndex(-1);
        inputRef.current?.focus();
    };

    return (
        <div className="search-box" ref={searchRef}>
            <div className="search-input-wrapper">
                <Search className="search-icon" size={20} />

                <input
                    ref={inputRef}
                    type="text"
                    className="search-input"
                    placeholder={placeholder}
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    onKeyDown={handleKeyDown}
                    onFocus={() => query.length >= minChars && setShowDropdown(true)}
                />

                {loading && (
                    <Loader2 className="search-loading" size={20} />
                )}

                {query && !loading && (
                    <button
                        className="search-clear"
                        onClick={handleClear}
                        aria-label="Clear search"
                    >
                        <X size={18} />
                    </button>
                )}
            </div>

            {showDropdown && suggestions.length > 0 && (
                <div className="search-dropdown">
                    {suggestions.map((item, index) => (
                        <div
                            key={index}
                            className={`search-suggestion ${index === selectedIndex ? 'selected' : ''}`}
                            onClick={() => handleSelect(item)}
                            onMouseEnter={() => setSelectedIndex(index)}
                        >
                            {renderSuggestion ? renderSuggestion(item) : (
                                <span>{item.name || item.label || String(item)}</span>
                            )}
                        </div>
                    ))}
                </div>
            )}

            {showDropdown && !loading && suggestions.length === 0 && query.length >= minChars && (
                <div className="search-dropdown">
                    <div className="search-empty">
                        No results found for "{query}"
                    </div>
                </div>
            )}
        </div>
    );
}
