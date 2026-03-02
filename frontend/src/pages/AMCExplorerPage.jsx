import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { apiGet, handleApiError } from '../api/client';
import MissingData from '../components/common/MissingData';
import './AMCExplorerPage.css';

const AMCExplorerPage = () => {
    const [amcs, setAmcs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedAMC, setSelectedAMC] = useState(null);
    const [amcDetails, setAmcDetails] = useState(null);
    const [detailsLoading, setDetailsLoading] = useState(false);
    const [lastUpdated, setLastUpdated] = useState('');

    useEffect(() => {
        fetchAMCs();
    }, []);

    const fetchAMCs = async () => {
        setLoading(true);
        try {
            const data = await apiGet('/amcs');
            setAmcs(data.amcs);
            setLastUpdated(data.last_updated_month);
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    const fetchAMCDetails = async (amcId) => {
        setDetailsLoading(true);
        try {
            const data = await apiGet(`/amcs/${amcId}`);
            setAmcDetails(data);
        } catch (err) {
            console.error('AMC details fetch error:', err);
        } finally {
            setDetailsLoading(false);
        }
    };

    const handleAMCClick = (amc) => {
        if (selectedAMC?.amc_id === amc.amc_id) {
            setSelectedAMC(null);
            setAmcDetails(null);
        } else {
            setSelectedAMC(amc);
            fetchAMCDetails(amc.amc_id);
            // Scroll to details after a short delay to allow render
            setTimeout(() => {
                document.getElementById('amc-details-section')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }, 100);
        }
    };

    const filteredAMCs = useMemo(() => {
        return amcs.filter(amc =>
            amc.amc_name.toLowerCase().includes(searchTerm.toLowerCase())
        );
    }, [amcs, searchTerm]);

    if (loading) {
        return (
            <div className="loading-container">
                <div className="loading-spinner"></div>
                <p>Loading Fund Houses...</p>
            </div>
        );
    }

    if (error) {
        return <div className="error-container">Error: {error}</div>;
    }

    return (
        <div className="container amc-explorer-container">
            <header className="page-header">
                <div className="header-titles">
                    <h1>Funds Explorer</h1>
                    <p className="subtitle">Discover Mutual Fund houses and their equity strategies</p>
                </div>
                <div className="header-meta">
                    <span className="last-updated">Data as of: <strong>{lastUpdated || <MissingData inline />}</strong></span>
                </div>
            </header>

            <div className="explorer-controls">
                <div className="search-box">
                    <span className="search-icon">🔍</span>
                    <input
                        type="text"
                        placeholder="Search for an AMC (e.g. Quant, HDFC, SBI)..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                    />
                    {searchTerm && (
                        <button className="clear-search" onClick={() => setSearchTerm('')}>✕</button>
                    )}
                </div>
                <div className="stats-summary">
                    Showing <strong>{filteredAMCs.length}</strong> Fund Houses
                </div>
            </div>

            <div className="amc-grid">
                {filteredAMCs.map(amc => (
                    <div
                        key={amc.amc_id}
                        className={`amc-card glass-effect ${selectedAMC?.amc_id === amc.amc_id ? 'selected' : ''}`}
                        onClick={() => handleAMCClick(amc)}
                    >
                        <div className="amc-card-header">
                            <h3>{amc.amc_name}</h3>
                            <button className="expand-btn">
                                {selectedAMC?.amc_id === amc.amc_id ? '−' : '+'}
                            </button>
                        </div>

                        <div className="amc-stats">
                            <div className="amc-stat-item">
                                <label>Total AUM</label>
                                <div className="value">₹{amc.total_aum_cr.toLocaleString()} Cr.</div>
                            </div>
                            <div className="amc-stat-item">
                                <label>Schemes</label>
                                <div className="value">{amc.scheme_count}</div>
                            </div>
                        </div>

                        {amc.top_holdings && amc.top_holdings.length > 0 && (
                            <div className="top-holdings-preview">
                                <label>Top Strategy</label>
                                <div className="holdings-pills">
                                    {amc.top_holdings.map((h, i) => (
                                        <span key={i} className="holding-pill">
                                            {h.company_name} <small>{h.percent_of_amc_equity}%</small>
                                        </span>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                ))}
            </div>

            {selectedAMC && (
                <div id="amc-details-section" className="amc-details-view glass-effect animate-slide-up">
                    <div className="details-header">
                        <h2>{selectedAMC.amc_name} — Portfolio Breakdown</h2>
                        <button className="close-details" onClick={() => setSelectedAMC(null)}>✕ Close</button>
                    </div>

                    {detailsLoading ? (
                        <div className="details-loading">
                            <div className="loading-spinner small"></div>
                            <span>Fetching Scheme Details...</span>
                        </div>
                    ) : amcDetails ? (
                        <div className="scheme-list-container">
                            <table className="scheme-table">
                                <thead>
                                    <tr>
                                        <th>Scheme Name</th>
                                        <th>Category</th>
                                        <th>Type</th>
                                        <th className="text-right">Total AUM (Cr.)</th>
                                        <th className="text-right">Equity AUM (Cr.)</th>
                                        <th className="text-center">Action</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {amcDetails.schemes.map(scheme => (
                                        <tr key={scheme.scheme_id}>
                                            <td className="scheme-name-cell">
                                                <strong>{scheme.scheme_name}</strong>
                                            </td>
                                            <td>
                                                <span className="category-badge">{scheme.category || 'Equity'}</span>
                                            </td>
                                            <td className="type-cell">
                                                {scheme.plan_type} | {scheme.option_type}
                                            </td>
                                            <td className="text-right font-mono">
                                                {scheme.total_aum_cr === null || scheme.total_aum_cr === undefined ? <MissingData /> : (scheme.total_aum_cr === 0 ? '-' : `₹${scheme.total_aum_cr.toLocaleString()}`)}
                                            </td>
                                            <td className="text-right font-mono" style={{ color: 'var(--text-secondary)' }}>
                                                {scheme.equity_aum_cr === null || scheme.equity_aum_cr === undefined ? <MissingData /> : (scheme.equity_aum_cr === 0 ? '-' : `₹${scheme.equity_aum_cr.toLocaleString()}`)}
                                            </td>
                                            <td className="text-center">
                                                <Link
                                                    to={`/schemes?scheme_id=${scheme.scheme_id}`}
                                                    className="view-btn"
                                                >
                                                    View Portfolio →
                                                </Link>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    ) : (
                        <p>No details found for this AMC.</p>
                    )}
                </div>
            )}
        </div>
    );
};

export default AMCExplorerPage;
