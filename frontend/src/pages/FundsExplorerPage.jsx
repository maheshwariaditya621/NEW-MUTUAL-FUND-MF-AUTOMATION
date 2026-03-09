import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { apiGet, handleApiError } from '../api/client';
import ExportButton from '../components/common/ExportButton';
import './FundsExplorerPage.css';

const Categories = [
    { id: 'equity', label: 'Equity Funds', icon: '🚀' },
    { id: 'debt', label: 'Debt Funds', icon: '🛡️' },
    { id: 'hybrid', label: 'Hybrid Funds', icon: '⚖️' },
    { id: 'index', label: 'Index Funds', icon: '📉' },
    { id: 'tax', label: 'Tax-Saving Funds (ELSS)', icon: '💰' },
    { id: 'thematic', label: 'Thematic Funds', icon: '🏛️' },
    { id: 'other', label: 'Other', icon: '📁' }
];

const SubCategories = {
    equity: ['Large Cap', 'Mid Cap', 'Small Cap', 'Flexi Cap', 'Multi Cap', 'Dividend Yield', 'Value/Contra', 'Other Equity'],
    debt: ['Liquid', 'Money Market', 'Corporate Bond', 'Gilt', 'Dynamic Bond', 'Debt'],
    hybrid: ['Balanced Advantage', 'Arbitrage Fund', 'Multi Asset Allocation', 'Aggressive Hybrid', 'Equity Savings', 'Conservative Hybrid', 'Hybrid'],
    index: ['Index Fund', 'ETF'],
    tax: ['ELSS'],
    thematic: ['Sectoral/Thematic'],
    other: ['Uncategorized']
};

// Utility to format ALL-CAPS names to Title Case for better readability
const formatSchemeName = (name) => {
    if (!name) return "";
    return name
        .toLowerCase()
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ')
        .replace(/\b(Amc|Mf|Absl|Sbi|Hdfc|Icici|Idfc|Kotak|Nippon|Utl|Uti|Canara|Robeco|Quant|Tata|L&t)\b/g, (m) => m.toUpperCase());
};

const FundsExplorerPage = () => {
    const [schemes, setSchemes] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [searchTerm, setSearchTerm] = useState('');
    const [activeCategory, setActiveCategory] = useState('equity');
    const [activeSubCategory, setActiveSubCategory] = useState('All');
    const [isSidebarOpen, setIsSidebarOpen] = useState(true);

    useEffect(() => {
        fetchSchemes();
    }, []);

    const fetchSchemes = async () => {
        setLoading(true);
        try {
            // Use empty query for initial load to get top 5000 schemes
            const data = await apiGet('/schemes/search?q=' + (searchTerm || ''));
            setSchemes(data.results);
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    // Filter logic
    const filteredSchemes = useMemo(() => {
        return schemes.filter(s => {
            const matchesCat = s.website_category === (Categories.find(c => c.id === activeCategory)?.label);
            const matchesSub = activeSubCategory === 'All' || s.website_sub_category === activeSubCategory;
            const matchesSearch = s.scheme_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                s.amc_name.toLowerCase().includes(searchTerm.toLowerCase());
            return matchesCat && matchesSub && matchesSearch;
        });
    }, [schemes, activeCategory, activeSubCategory, searchTerm]);

    const handleCategoryChange = (catId) => {
        setActiveCategory(catId);
        setActiveSubCategory('All');
    };

    if (loading) {
        return (
            <div className="loading-container">
                <div className="loading-spinner"></div>
                <p>Loading Funds Explorer...</p>
            </div>
        );
    }

    return (
        <div className="funds-explorer-wrapper">
            {/* Sidebar for Filtering */}
            <aside className={`explorer-sidebar glass-effect ${isSidebarOpen ? 'open' : 'closed'}`}>
                <div className="sidebar-header">
                    <h3>Categories</h3>
                    <button className="toggle-sidebar" onClick={() => setIsSidebarOpen(!isSidebarOpen)}>
                        {isSidebarOpen ? '⇠' : '⇢'}
                    </button>
                </div>

                <div className="category-list">
                    {Categories.map(cat => (
                        <div
                            key={cat.id}
                            className={`category-item ${activeCategory === cat.id ? 'active' : ''}`}
                            onClick={() => handleCategoryChange(cat.id)}
                        >
                            <span className="cat-icon">{cat.icon}</span>
                            <span className="cat-label">{cat.label}</span>
                        </div>
                    ))}
                </div>

                <div className="sidebar-divider"></div>

                <div className="sub-category-section">
                    <h4>Sub-Categories</h4>
                    <div className="sub-category-list">
                        <button
                            className={`sub-cat-pill ${activeSubCategory === 'All' ? 'active' : ''}`}
                            onClick={() => setActiveSubCategory('All')}
                        >
                            All
                        </button>
                        {SubCategories[activeCategory].map(sub => (
                            <button
                                key={sub}
                                className={`sub-cat-pill ${activeSubCategory === sub ? 'active' : ''}`}
                                onClick={() => setActiveSubCategory(sub)}
                            >
                                {sub}
                            </button>
                        ))}
                    </div>
                </div>
            </aside>

            {/* Main Content Area */}
            <main className="explorer-content">
                <header className="explorer-header">
                    <div className="header-text">
                        <h1>Funds Explorer</h1>
                        <p>Discover funds by category and strategy</p>
                    </div>
                    <div className="header-search">
                        <input
                            type="text"
                            placeholder="Search schemes or AMCs..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                        <span className="search-icon">🔍</span>
                    </div>
                </header>

                <div className="explorer-results">
                    <div className="results-meta" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                        <span>
                            Found <strong>{filteredSchemes.length}</strong> funds in
                            <span> {Categories.find(c => c.id === activeCategory)?.label}</span>
                            {activeSubCategory !== 'All' && <span> › {activeSubCategory}</span>}
                        </span>
                        <ExportButton
                            getData={() => filteredSchemes.map(s => ({
                                scheme_name: s.scheme_name,
                                amc_name: s.amc_name,
                                plan_type: s.plan_type,
                                option_type: s.option_type,
                                category: s.website_category,
                                sub_category: s.website_sub_category,
                            }))}
                            columns={[
                                { key: 'scheme_name', label: 'Scheme Name', exportFormat: 'string' },
                                { key: 'amc_name', label: 'AMC', exportFormat: 'string' },
                                { key: 'plan_type', label: 'Plan Type', exportFormat: 'string' },
                                { key: 'option_type', label: 'Option', exportFormat: 'string' },
                                { key: 'category', label: 'Category', exportFormat: 'string' },
                                { key: 'sub_category', label: 'Sub-Category', exportFormat: 'string' },
                            ]}
                            fileNameConfig={{
                                page: 'funds-explorer',
                                filters: {
                                    category: Categories.find(c => c.id === activeCategory)?.label,
                                    subCategory: activeSubCategory !== 'All' ? activeSubCategory : undefined,
                                    search: searchTerm || undefined,
                                },
                            }}
                            metadata={{
                                title: `Funds Explorer — ${Categories.find(c => c.id === activeCategory)?.label || ''}`,
                                filters: {
                                    Category: Categories.find(c => c.id === activeCategory)?.label,
                                    'Sub-Category': activeSubCategory !== 'All' ? activeSubCategory : 'All',
                                    Search: searchTerm || undefined,
                                },
                            }}
                        />
                    </div>

                    <div className="scheme-grid">
                        {filteredSchemes.length > 0 ? (
                            filteredSchemes.map(scheme => (
                                <div key={scheme.scheme_id} className={`scheme-card-sleek glass-effect cat-${activeCategory}`}>
                                    <div className="card-accent-bar"></div>
                                    <div className="card-main-content">
                                        <div className="card-info-header">
                                            <span className="amc-label">{scheme.amc_name}</span>
                                            <span className="sub-cat-badge">{scheme.website_sub_category}</span>
                                        </div>
                                        <h3 className="scheme-title">{formatSchemeName(scheme.scheme_name)}</h3>
                                        <div className="card-info-footer">
                                            <span className="plan-type">{scheme.plan_type} • {scheme.option_type}</span>
                                            <Link
                                                to={`/schemes?scheme_id=${scheme.scheme_id}`}
                                                className="action-link"
                                            >
                                                Explore Portfolio
                                            </Link>
                                        </div>
                                    </div>
                                </div>
                            ))
                        ) : (
                            <div className="empty-results glass-effect">
                                <div className="empty-icon">📂</div>
                                <h3>No funds found</h3>
                                <p>Try adjusting your filters or search term.</p>
                                <button onClick={() => { setActiveSubCategory('All'); setSearchTerm(''); }} className="reset-btn">
                                    Reset Filters
                                </button>
                            </div>
                        )}
                    </div>
                </div>
            </main>
        </div>
    );
};

export default FundsExplorerPage;
