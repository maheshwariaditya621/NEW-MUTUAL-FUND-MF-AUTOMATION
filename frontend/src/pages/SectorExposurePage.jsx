import React, { useState, useEffect } from 'react';
import { Search, Plus, Trash2, PieChart, Activity, Info, Calendar, X, ExternalLink, ChevronRight } from 'lucide-react';
import { searchSchemes } from '../api/schemes';
import { getSectorComparison, getSectorCompanies, getAvailablePeriods } from '../api/comparison';
import SearchBox from '../components/common/SearchBox';
import './SectorExposurePage.css';

const SectorExposurePage = () => {
    const [selectedFunds, setSelectedFunds] = useState([null, null]); // Start with 2 slots
    const [comparisonData, setComparisonData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [searchingIndex, setSearchingIndex] = useState(null);
    const [suggestions, setSuggestions] = useState([]);
    const [searchLoading, setSearchLoading] = useState(false);
    const [drilldownSector, setDrilldownSector] = useState(null);
    const [drilldownData, setDrilldownData] = useState([]);
    const [drilldownLoading, setDrilldownLoading] = useState(false);
    const [aumType, setAumType] = useState('total'); // 'total' or 'equity'
    const [availablePeriods, setAvailablePeriods] = useState([]);
    const [selectedPeriodId, setSelectedPeriodId] = useState("");

    // Fetch available periods on mount
    useEffect(() => {
        const fetchPeriods = async () => {
            try {
                const data = await getAvailablePeriods();
                setAvailablePeriods(data);
            } catch (err) {
                console.error("Failed to fetch periods", err);
            }
        };
        fetchPeriods();
    }, []);

    const handleAddFund = () => {
        if (selectedFunds.length < 5) {
            setSelectedFunds([...selectedFunds, null]);
        }
    };

    const handleRemoveFund = (index) => {
        const newFunds = [...selectedFunds];
        newFunds.splice(index, 1);
        setSelectedFunds(newFunds.length < 1 ? [null] : newFunds);
    };

    const handleSelectFund = (index, fund) => {
        const newFunds = [...selectedFunds];
        newFunds[index] = fund;
        setSelectedFunds(newFunds);
        setSearchingIndex(null);
    };

    const handleSearch = async (query) => {
        setSearchLoading(true);
        try {
            const data = await searchSchemes(query, 10);
            setSuggestions(data.results || []);
        } catch (err) {
            console.error("Search error", err);
        } finally {
            setSearchLoading(false);
        }
    };

    const handleDrilldown = async (sectorName) => {
        const ids = selectedFunds.filter(f => f !== null).map(f => f.scheme_id);
        if (ids.length === 0) return;

        setDrilldownSector(sectorName);
        setDrilldownLoading(true);
        try {
            const data = await getSectorCompanies(ids, sectorName, selectedPeriodId || null);
            setDrilldownData(data);
        } catch (err) {
            console.error("Drilldown error", err);
        } finally {
            setDrilldownLoading(false);
        }
    };

    const handleCompare = async () => {
        const ids = selectedFunds.filter(f => f !== null).map(f => f.scheme_id);
        if (ids.length === 0) return;

        setLoading(true);
        try {
            const data = await getSectorComparison(ids, selectedPeriodId || null);
            setComparisonData(data);
        } catch (err) {
            console.error("Comparison error", err);
        } finally {
            setLoading(false);
        }
    };

    const fundColors = ['#6366f1', '#ef4444', '#10b981', '#f59e0b', '#3b82f6'];

    return (
        <div className="sector-exposure-wrapper">
            <header className="page-header-premium">
                <div className="header-icon"><PieChart size={32} /></div>
                <div className="header-text">
                    <h1>Indian Sector Exposure</h1>
                    <p>Compare sector-wise allocation across up to 5 mutual funds</p>
                </div>
            </header>

            <section className="selection-card glass-effect">
                <div className="fund-inputs-grid">
                    {selectedFunds.map((fund, idx) => (
                        <div key={idx} className="fund-input-row">
                            <div className="fund-label-wrapper">
                                <span className="fund-dot" style={{ backgroundColor: fundColors[idx] }}></span>
                                <label>Fund {idx + 1}</label>
                            </div>
                            <div className="search-container">
                                {fund ? (
                                    <div className="selected-fund-pill">
                                        <span className="fund-name">{fund.scheme_name}</span>
                                        <button className="remove-btn" onClick={() => handleSelectFund(idx, null)}>
                                            <Trash2 size={14} />
                                        </button>
                                    </div>
                                ) : (
                                    <SearchBox
                                        placeholder="Search scheme name..."
                                        onSearch={handleSearch}
                                        suggestions={suggestions}
                                        onSelect={(f) => handleSelectFund(idx, f)}
                                        loading={searchLoading}
                                        renderSuggestion={(f) => (
                                            <div className="suggestion-item">
                                                <div className="suggestion-main">{f.scheme_name}</div>
                                                <div className="suggestion-meta">{f.amc_name} | {f.website_sub_category}</div>
                                            </div>
                                        )}
                                    />
                                )}
                            </div>
                            {selectedFunds.length > 1 && (
                                <button className="delete-row-btn" onClick={() => handleRemoveFund(idx)}>
                                    <Trash2 size={18} />
                                </button>
                            )}
                        </div>
                    ))}
                </div>

                <div className="selection-footer">
                    <div className="selection-footer-left">
                        {selectedFunds.length < 5 && (
                            <button className="add-fund-link" onClick={handleAddFund}>
                                <Plus size={18} /> Add another fund (upto 5)
                            </button>
                        )}

                        <div className="period-selector-wrapper glass-effect">
                            <Calendar size={16} />
                            <select
                                value={selectedPeriodId}
                                onChange={(e) => setSelectedPeriodId(e.target.value)}
                                className="period-select"
                            >
                                <option value="">Latest Available</option>
                                {availablePeriods.map(p => (
                                    <option key={p.period_id} value={p.period_id}>
                                        {p.label}
                                    </option>
                                ))}
                            </select>
                        </div>
                    </div>

                    <button
                        className={`compare-btn ${selectedFunds.some(f => f) ? 'active' : ''}`}
                        onClick={handleCompare}
                        disabled={loading || !selectedFunds.some(f => f)}
                    >
                        {loading ? 'Comparing...' : 'Go'}
                    </button>
                </div>
            </section>

            {comparisonData && (
                <div className="results-container fade-in">
                    <section className="result-section">
                        <div className="section-header">
                            <div className="section-title-group">
                                <Activity size={20} />
                                <h2>Portfolio Summary</h2>
                            </div>
                            <div className="aum-toggle-wrapper glass-effect">
                                <button
                                    className={`aum-toggle-btn ${aumType === 'total' ? 'active' : ''}`}
                                    onClick={() => setAumType('total')}
                                >
                                    Total AUM
                                </button>
                                <button
                                    className={`aum-toggle-btn ${aumType === 'equity' ? 'active' : ''}`}
                                    onClick={() => setAumType('equity')}
                                >
                                    Equity AUM
                                </button>
                            </div>
                        </div>
                        <div className="table-responsive glass-effect">
                            <table className="summary-table">
                                <thead>
                                    <tr>
                                        <th className="text-left">Fund Name</th>
                                        <th className="text-left">Category</th>
                                        <th className="text-right">Top 10 Equity Holding (%)</th>
                                        <th className="text-right">
                                            {aumType === 'total' ? 'Total AUM (Cr)' : 'Equity AUM (Cr)'}
                                        </th>
                                        <th className="text-right">Date</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {comparisonData.summary.map((s, idx) => (
                                        <tr key={idx}>
                                            <td className="fund-name-cell">
                                                <span className="fund-dot" style={{ backgroundColor: fundColors[idx] }}></span>
                                                {s.fund_name}
                                            </td>
                                            <td>{s.category}</td>
                                            <td className="text-right font-mono">{s.top_10_pct.toFixed(2)}%</td>
                                            <td className="text-right font-mono">
                                                {aumType === 'total'
                                                    ? s.total_aum.toLocaleString('en-IN', { maximumFractionDigits: 2 })
                                                    : s.equity_aum.toLocaleString('en-IN', { maximumFractionDigits: 2 })
                                                }
                                            </td>
                                            <td className="text-right text-muted">{s.date}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </section>

                    <section className="result-section">
                        <div className="section-header">
                            <div className="section-title-group">
                                <PieChart size={20} />
                                <h2>Sector Exposure</h2>
                                <span className="exposure-basis-pill">as % of Net Assets</span>
                            </div>
                        </div>
                        <div className="table-responsive glass-effect">
                            <table className="exposure-table">
                                <thead>
                                    <tr>
                                        <th className="sticky-col">Sector</th>
                                        {comparisonData.summary.map((s, idx) => (
                                            <th key={idx} className="text-right">
                                                <div className="fund-header">
                                                    <span className="fund-dot" style={{ backgroundColor: fundColors[idx] }}></span>
                                                    Fund {idx + 1}
                                                </div>
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {comparisonData.sectors.map((row, idx) => (
                                        <tr key={idx}>
                                            <td
                                                className="sticky-col sector-name clickable"
                                                onClick={() => handleDrilldown(row.sector)}
                                            >
                                                {row.sector}
                                                <ChevronRight size={14} className="drill-icon" />
                                            </td>
                                            {comparisonData.summary.map((_, fIdx) => {
                                                const val = row[`fund_${fIdx + 1}`];
                                                return (
                                                    <td key={fIdx} className={`text-right font-mono ${val > 10 ? 'high-exposure' : ''}`}>
                                                        {val > 0 ? `${val.toFixed(2)}%` : '-'}
                                                    </td>
                                                );
                                            })}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </section>
                </div>
            )}
            {drilldownSector && (
                <div className="drilldown-overlay" onClick={() => setDrilldownSector(null)}>
                    <div className="drilldown-modal glass-effect" onClick={e => e.stopPropagation()}>
                        <div className="modal-header">
                            <div className="modal-title-group">
                                <PieChart size={24} className="modal-icon" />
                                <div>
                                    <h3>{drilldownSector}</h3>
                                    <p>Company-wise breakdown across selected funds</p>
                                </div>
                            </div>
                            <button className="close-modal-btn" onClick={() => setDrilldownSector(null)}>
                                <X size={24} />
                            </button>
                        </div>

                        <div className="modal-content">
                            {drilldownLoading ? (
                                <div className="modal-loader">
                                    <div className="pulse-loader"></div>
                                    <p>Analyzing holdings...</p>
                                </div>
                            ) : (
                                <div className="table-responsive">
                                    <table className="summary-table drilldown-table">
                                        <thead>
                                            <tr>
                                                <th className="text-left">Company Name</th>
                                                {comparisonData.summary.map((s, idx) => (
                                                    <th key={idx} className="text-right">
                                                        <div className="fund-dot" style={{ backgroundColor: fundColors[idx], display: 'inline-block', marginRight: '8px' }}></div>
                                                        Fund {idx + 1}
                                                    </th>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {drilldownData.map((row, idx) => (
                                                <tr key={idx}>
                                                    <td className="text-left font-bold">{row.company}</td>
                                                    {comparisonData.summary.map((_, fIdx) => {
                                                        const val = row[`fund_${fIdx + 1}`];
                                                        return (
                                                            <td key={fIdx} className="text-right font-mono">
                                                                {val > 0 ? `${val.toFixed(2)}%` : '-'}
                                                            </td>
                                                        );
                                                    })}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default SectorExposurePage;
