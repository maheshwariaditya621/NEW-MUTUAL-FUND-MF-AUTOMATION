import React, { useState, useEffect } from 'react';
import { Search, Plus, Trash2, PieChart, Activity, Info, Calendar, X, ChevronRight, Building2 } from 'lucide-react';
import { searchAMCs } from '../api/amcs';
import { getAMCSectorComparison, getAMCSectorCompanies, getAvailablePeriods } from '../api/comparison';
import SearchBox from '../components/common/SearchBox';
import './SectorExposurePage.css'; // Reuse the same premium styling

const AMCSectorExposurePage = () => {
    const [selectedAMCs, setSelectedAMCs] = useState([null, null]);
    const [comparisonData, setComparisonData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [suggestions, setSuggestions] = useState([]);
    const [searchLoading, setSearchLoading] = useState(false);
    const [drilldownSector, setDrilldownSector] = useState(null);
    const [drilldownData, setDrilldownData] = useState([]);
    const [drilldownLoading, setDrilldownLoading] = useState(false);
    const [aumType, setAumType] = useState('equity'); // Default to equity for AMCs
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

    const handleAddAMC = () => {
        if (selectedAMCs.length < 5) {
            setSelectedAMCs([...selectedAMCs, null]);
        }
    };

    const handleRemoveAMC = (index) => {
        const newAMCs = [...selectedAMCs];
        newAMCs.splice(index, 1);
        setSelectedAMCs(newAMCs.length < 1 ? [null] : newAMCs);
    };

    const handleSelectAMC = (index, amc) => {
        const newAMCs = [...selectedAMCs];
        newAMCs[index] = amc;
        setSelectedAMCs(newAMCs);
    };

    const handleSearch = async (query) => {
        setSearchLoading(true);
        try {
            const data = await searchAMCs(query);
            // Map keys for SearchBox compatibility
            const results = data.map(a => ({
                ...a,
                scheme_name: a.amc_name // SearchBox uses scheme_name for rendering by default in our current setup
            }));
            setSuggestions(results);
        } catch (err) {
            console.error("AMC Search error", err);
        } finally {
            setSearchLoading(false);
        }
    };

    const handleCompare = async () => {
        const ids = selectedAMCs.filter(a => a !== null).map(a => a.amc_id);
        if (ids.length === 0) return;

        setLoading(true);
        try {
            const data = await getAMCSectorComparison(ids, selectedPeriodId || null);
            setComparisonData(data);
        } catch (err) {
            console.error("Comparison error", err);
        } finally {
            setLoading(false);
        }
    };

    const handleDrilldown = async (sectorName) => {
        const ids = selectedAMCs.filter(a => a !== null).map(a => a.amc_id);
        if (ids.length === 0) return;

        setDrilldownSector(sectorName);
        setDrilldownLoading(true);
        try {
            const data = await getAMCSectorCompanies(ids, sectorName, selectedPeriodId || null);
            setDrilldownData(data);
        } catch (err) {
            console.error("Drilldown error", err);
        } finally {
            setDrilldownLoading(false);
        }
    };

    const amcColors = ['#6366f1', '#ef4444', '#10b981', '#f59e0b', '#3b82f6'];

    return (
        <div className="sector-exposure-wrapper">
            <header className="page-header-premium">
                <div className="header-icon"><Building2 size={32} /></div>
                <div className="header-text">
                    <h1>AMC Sector Exposure</h1>
                    <p>Compare consolidated sector-wise allocation across up to 5 Fund Houses</p>
                </div>
            </header>

            <section className="selection-card glass-effect">
                <div className="fund-inputs-grid">
                    {selectedAMCs.map((amc, idx) => (
                        <div key={idx} className="fund-input-row">
                            <div className="fund-label-wrapper">
                                <span className="fund-dot" style={{ backgroundColor: amcColors[idx] }}></span>
                                <label>AMC {idx + 1}</label>
                            </div>
                            <div className="search-container">
                                {amc ? (
                                    <div className="selected-fund-pill">
                                        <span className="fund-name">{amc.amc_name}</span>
                                        <button className="remove-btn" onClick={() => handleSelectAMC(idx, null)}>
                                            <Trash2 size={16} />
                                        </button>
                                    </div>
                                ) : (
                                    <SearchBox
                                        placeholder="Search AMC name (e.g. SBI, HDFC)..."
                                        onSearch={handleSearch}
                                        suggestions={suggestions}
                                        onSelect={(a) => handleSelectAMC(idx, a)}
                                        loading={searchLoading}
                                        renderSuggestion={(a) => (
                                            <div className="suggestion-item">
                                                <div className="suggestion-main">{a.amc_name}</div>
                                            </div>
                                        )}
                                    />
                                )}
                            </div>
                            {selectedAMCs.length > 1 && (
                                <button className="delete-row-btn" onClick={() => handleRemoveAMC(idx)}>
                                    <Trash2 size={18} />
                                </button>
                            )}
                        </div>
                    ))}
                </div>

                <div className="selection-footer">
                    <div className="selection-footer-left">
                        {selectedAMCs.length < 5 && (
                            <button className="add-fund-link" onClick={handleAddAMC}>
                                <Plus size={18} /> Add another AMC
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
                        className={`compare-btn ${selectedAMCs.some(a => a) ? 'active' : ''}`}
                        onClick={handleCompare}
                        disabled={loading || !selectedAMCs.some(a => a)}
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
                                <h2>AMC Summary</h2>
                            </div>
                            <div className="aum-toggle-wrapper glass-effect">
                                <button
                                    className={`aum-toggle-btn ${aumType === 'equity' ? 'active' : ''}`}
                                    onClick={() => setAumType('equity')}
                                >
                                    Equity AUM
                                </button>
                                <button
                                    className={`aum-toggle-btn ${aumType === 'total' ? 'active' : ''}`}
                                    onClick={() => setAumType('total')}
                                >
                                    Total AUM
                                </button>
                            </div>
                        </div>
                        <div className="table-responsive glass-effect">
                            <table className="summary-table">
                                <thead>
                                    <tr>
                                        <th className="text-left">AMC Name</th>
                                        <th className="text-right">Schemes</th>
                                        <th className="text-right">Top 10 Equity (%)</th>
                                        <th className="text-right">
                                            {aumType === 'equity' ? 'Total Equity AUM (Cr)' : 'Total Net AUM (Cr)'}
                                        </th>
                                        <th className="text-right">Date</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {comparisonData.summary.map((s, idx) => (
                                        <tr key={idx}>
                                            <td className="fund-name-cell">
                                                <span className="fund-dot" style={{ backgroundColor: amcColors[idx] }}></span>
                                                {s.amc_name}
                                            </td>
                                            <td className="text-right font-mono">{s.scheme_count}</td>
                                            <td className="text-right font-mono">{s.top_10_pct.toFixed(2)}%</td>
                                            <td className="text-right font-mono">
                                                {aumType === 'equity'
                                                    ? s.equity_aum.toLocaleString('en-IN', { maximumFractionDigits: 2 })
                                                    : s.total_aum.toLocaleString('en-IN', { maximumFractionDigits: 2 })
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
                                <h2>Consolidated Sector Exposure</h2>
                                <span className="exposure-basis-pill">as % of Equity Portfolio</span>
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
                                                    <div className="fund-dot" style={{ backgroundColor: amcColors[idx] }}></div>
                                                    AMC {idx + 1}
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
                                    <p>Consolidated company breakdown across selected AMCs</p>
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
                                    <p>Aggregating AMC holdings...</p>
                                </div>
                            ) : (
                                <div className="table-responsive">
                                    <table className="summary-table drilldown-table">
                                        <thead>
                                            <tr>
                                                <th className="text-left">Company Name</th>
                                                {comparisonData.summary.map((s, idx) => (
                                                    <th key={idx} className="text-right">
                                                        <div className="fund-dot" style={{ backgroundColor: amcColors[idx], display: 'inline-block', marginRight: '8px' }}></div>
                                                        AMC {idx + 1}
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

export default AMCSectorExposurePage;
