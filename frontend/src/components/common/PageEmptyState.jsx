import React from 'react';
import './PageEmptyState.css';

/**
 * A premium, high-fidelity empty state component for main pages.
 * 
 * @param {Object} props
 * @param {string} props.title - The prominent heading
 * @param {string} props.description - Engaging informative copy
 * @param {string} props.placeholder - Placeholder for the search input
 * @param {Function} props.onSearch - Callback for when search is submitted
 * @param {string} props.type - 'stock' or 'scheme' to customize icons/suggestions
 * @param {Array} props.suggestions - List of strings for quick-start searches
 */
const PageEmptyState = ({
    title,
    description,
    placeholder,
    onSearch,
    type = 'stock',
    suggestions = []
}) => {
    const [inputValue, setInputValue] = React.useState('');

    const handleSubmit = (e) => {
        e.preventDefault();
        if (inputValue.trim()) {
            onSearch(inputValue.trim());
        }
    };

    return (
        <div className="pes-container animate-fade-in">
            {/* Background Decorative Elements - Now at top level for full coverage */}
            <div className="pes-bg-blob blob-1"></div>
            <div className="pes-bg-blob blob-2"></div>

            <div className="pes-content">
                <div className="pes-icon-wrapper">
                    {type === 'stock' ? '📈' : '🏢'}
                </div>

                <h1 className="pes-title">{title}</h1>
                <p className="pes-description">{description}</p>

                <form className="pes-search-form" onSubmit={handleSubmit}>
                    <div className="pes-search-input-wrapper">
                        <span className="pes-search-icon">🔍</span>
                        <input
                            type="text"
                            className="pes-search-input"
                            placeholder={placeholder}
                            value={inputValue}
                            onChange={(e) => setInputValue(e.target.value)}
                        />
                        <button type="submit" className="pes-search-btn">
                            Search
                        </button>
                    </div>
                </form>

                {suggestions.length > 0 && (
                    <div className="pes-suggestions">
                        <span className="pes-suggestion-label">Popular:</span>
                        <div className="pes-suggestion-chips">
                            {suggestions.map((suggestion, index) => (
                                <button
                                    key={index}
                                    className="pes-suggestion-chip"
                                    onClick={() => {
                                        setInputValue(suggestion);
                                        onSearch(suggestion);
                                    }}
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

            {/* Footer Area */}
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
