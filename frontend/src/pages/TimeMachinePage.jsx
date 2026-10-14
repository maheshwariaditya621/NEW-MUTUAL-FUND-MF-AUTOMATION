import React, { useState, useEffect, useMemo } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import MissingData from '../components/common/MissingData';
import WatchButton from '../components/common/WatchButton';
import ExportButton from '../components/common/ExportButton';
import { getTimeMachineComparison } from '../api/insights';
import { handleApiError } from '../api/client';
import { formatNumber } from '../utils/helpers';
import './TimeMachinePage.css';

// ── Helpers ──────────────────────────────────────────────────────────────────
const fmt = (n) => (n === null || n === undefined) ? '-' : Number(n).toLocaleString('en-IN');
const fmtCr = (n) => (n === null || n === undefined) ? '-' : `₹${Number(n).toLocaleString('en-IN', { maximumFractionDigits: 2 })} Cr`;
const fmtPct = (n) => (n === null || n === undefined) ? '-' : `${Number(n) > 0 ? '+' : ''}${Number(n).toFixed(2)}%`;

const MCAP_BADGE_CLASS = {
    'Large Cap': 'mcap-large',
    'Mid Cap': 'mcap-mid',
    'Small Cap': 'mcap-small',
    'Micro Cap': 'mcap-small'
};

const SECTOR_PALETTE = [
    '#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', 
    '#06b6d4', '#14b8a6', '#f97316', '#64748b', '#a855f7'
];

export default function TimeMachinePage() {
    const [searchParams, setSearchParams] = useSearchParams();
    const baseParam = searchParams.get('base');
    const targetParam = searchParams.get('target');

    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const [selectedBase, setSelectedBase] = useState(baseParam || '');
    const [selectedTarget, setSelectedTarget] = useState(targetParam || '');
    const [activeTab, setActiveTab] = useState('accumulations'); // 'accumulations' | 'distributions' | 'entrants' | 'exits'
    const [mcapFilter, setMcapFilter] = useState('All');
    const [searchQuery, setSearchQuery] = useState('');
    const [sortConfig, setSortConfig] = useState({ key: 'net_qty_bought', direction: 'desc' });

    useEffect(() => {
        fetchComparison(selectedBase, selectedTarget);
    }, []);

    const fetchComparison = async (base = null, target = null) => {
        setLoading(true);
        setError(null);
        try {
            const res = await getTimeMachineComparison(base || null, target || null, 100);
            setData(res);
            setSelectedBase(res.summary.base_period);
            setSelectedTarget(res.summary.target_period);

            // Update URL params
            const nextParams = new URLSearchParams(searchParams);
            nextParams.set('base', res.summary.base_period);
            nextParams.set('target', res.summary.target_period);
            setSearchParams(nextParams, { replace: true });
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    const handleApplyPeriods = () => {
        fetchComparison(selectedBase, selectedTarget);
    };

    const handlePreset = (monthsGap) => {
        if (!data?.available_periods || data.available_periods.length === 0) return;
        const target = data.available_periods[0]; // latest
        const baseIdx = Math.min(monthsGap, data.available_periods.length - 1);
        const base = data.available_periods[baseIdx];
        setSelectedTarget(target);
        setSelectedBase(base);
        fetchComparison(base, target);
    };

    const handleSort = (key) => {
        setSortConfig(prev => {
            if (prev.key === key) {
                return { key, direction: prev.direction === 'asc' ? 'desc' : 'asc' };
            }
            return { key, direction: 'desc' };
        });
    };

    // Filter & Sort table items
    const displayedItems = useMemo(() => {
        if (!data) return [];
        let list = [];
        if (activeTab === 'accumulations') list = [...data.top_accumulations];
        else if (activeTab === 'distributions') list = [...data.top_distributions];
        else if (activeTab === 'entrants') list = [...data.new_entrants];
        else if (activeTab === 'exits') list = [...data.complete_exits];

        if (mcapFilter !== 'All') {
            list = list.filter(item => item.classification === mcapFilter);
        }

        if (searchQuery.trim()) {
            const q = searchQuery.toLowerCase();
            list = list.filter(item => 
                item.company_name?.toLowerCase().includes(q) || 
                item.isin?.toLowerCase().includes(q) ||
                item.sector?.toLowerCase().includes(q)
            );
        }

        if (sortConfig.key) {
            list.sort((a, b) => {
                let aVal = a[sortConfig.key];
                let bVal = b[sortConfig.key];
                if (aVal === null || aVal === undefined) aVal = 0;
                if (bVal === null || bVal === undefined) bVal = 0;
                if (sortConfig.key === 'company_name') {
                    return sortConfig.direction === 'asc' 
                        ? String(aVal).localeCompare(String(bVal)) 
                        : String(bVal).localeCompare(String(aVal));
                }
                const diff = Number(aVal) - Number(bVal);
                return sortConfig.direction === 'asc' ? diff : -diff;
            });
        }

        return list;
    }, [data, activeTab, mcapFilter, searchQuery, sortConfig]);

    const sortArrow = (key) => sortConfig.key === key ? (sortConfig.direction === 'asc' ? ' ↑' : ' ↓') : null;

    return (
        <div className="tm-page">
            <div className="tm-container">
                {/* ── Top Hero & Time Controls ── */}
                <div className="tm-hero-card">
                    <div className="tm-hero-header">
                        <div className="tm-hero-title-group">
                            <div className="tm-hero-badge">
                                <span className="tm-pulse-dot"></span>
                                <span>INSTITUTIONAL TIME-MACHINE</span>
                            </div>
                            <h1 className="tm-hero-title">Portfolio Time-Machine Comparison</h1>
                            <p className="tm-hero-desc">
                                Travel between any two historical periods to uncover macroeconomic mutual fund rotations, 
                                heavy stake accumulations, and complete portfolio exits across the industry.
                            </p>
                        </div>

                        {/* Quick Presets */}
                        <div className="tm-presets-bar">
                            <span className="tm-presets-label">Quick Jump:</span>
                            <button type="button" className="tm-preset-btn" onClick={() => handlePreset(1)}>1M (MoM)</button>
                            <button type="button" className="tm-preset-btn" onClick={() => handlePreset(3)}>3M (Quarter)</button>
                            <button type="button" className="tm-preset-btn" onClick={() => handlePreset(6)}>6M (Half-Year)</button>
                            <button type="button" className="tm-preset-btn" onClick={() => handlePreset(12)}>1Y (Annual)</button>
                        </div>
                    </div>

                    {/* Dual Period Selector Controls */}
                    <div className="tm-period-control-deck">
                        <div className="tm-period-block">
                            <span className="tm-period-icon">🕒</span>
                            <div className="tm-period-info">
                                <span className="tm-period-label">ORIGIN PERIOD (BASE)</span>
                                <select 
                                    className="tm-period-select"
                                    value={selectedBase}
                                    onChange={(e) => setSelectedBase(e.target.value)}
                                    disabled={loading || !data?.available_periods}
                                >
                                    {data?.available_periods?.map(p => (
                                        <option key={p} value={p}>{p}</option>
                                    ))}
                                </select>
                            </div>
                        </div>

                        <div className="tm-jump-arrow">
                            <span className="tm-arrow-icon">➔</span>
                            <span className="tm-gap-badge">{data?.summary ? `${data.summary.months_gap} Mo Shift` : 'Time Jump'}</span>
                        </div>

                        <div className="tm-period-block">
                            <span className="tm-period-icon">🚀</span>
                            <div className="tm-period-info">
                                <span className="tm-period-label">DESTINATION PERIOD (TARGET)</span>
                                <select 
                                    className="tm-period-select"
                                    value={selectedTarget}
                                    onChange={(e) => setSelectedTarget(e.target.value)}
                                    disabled={loading || !data?.available_periods}
                                >
                                    {data?.available_periods?.map(p => (
                                        <option key={p} value={p}>{p}</option>
                                    ))}
                                </select>
                            </div>
                        </div>

                        <button 
                            type="button" 
                            className="tm-launch-btn"
                            onClick={handleApplyPeriods}
                            disabled={loading || selectedBase === selectedTarget}
                        >
                            {loading ? 'Traveling...' : 'Compare Shift'}
                        </button>
                    </div>
                </div>

                {loading && <Loading message="Calculating portfolio deltas across time periods..." />}
                {error && <ErrorMessage message={error} />}

                {data && !loading && !error && (
                    <>
                        {/* ── Executive Shift Metrics Bento Grid ── */}
                        <div className="tm-bento-grid">
                            <div className="tm-bento-card tm-shift-card">
                                <div className="tm-card-eyebrow">INDUSTRY AGGREGATE VOLUME</div>
                                <div className="tm-shift-numbers">
                                    <div className="tm-shift-val">
                                        <span className="tm-metric-sub">{data.summary.base_period}:</span> {fmt(data.summary.total_shares_base)}
                                    </div>
                                    <div className="tm-shift-val">
                                        <span className="tm-metric-sub">{data.summary.target_period}:</span> {fmt(data.summary.total_shares_target)}
                                    </div>
                                </div>
                                <div className={`tm-shift-delta-pill ${data.summary.net_shares_delta >= 0 ? 'pos' : 'neg'}`}>
                                    {data.summary.net_shares_delta >= 0 ? '▲ Net Inflow: +' : '▼ Net Outflow: '}
                                    {fmt(Math.abs(data.summary.net_shares_delta))} shares ({fmtPct(data.summary.net_shares_delta_pct)})
                                </div>
                            </div>

                            <div className="tm-bento-card tm-rotation-card">
                                <div className="tm-card-eyebrow">POSITION ROTATIONS</div>
                                <div className="tm-rotation-stats">
                                    <div className="tm-rot-item">
                                        <span className="tm-rot-count pos">{data.summary.accumulated_stocks_count}</span>
                                        <span className="tm-rot-label">Accumulated</span>
                                    </div>
                                    <div className="tm-rot-divider">vs</div>
                                    <div className="tm-rot-item">
                                        <span className="tm-rot-count neg">{data.summary.distributed_stocks_count}</span>
                                        <span className="tm-rot-label">Trimmed</span>
                                    </div>
                                </div>
                                <div className="tm-rot-progress">
                                    <div 
                                        className="tm-rot-bar pos" 
                                        style={{ 
                                            width: `${(data.summary.accumulated_stocks_count / (data.summary.accumulated_stocks_count + data.summary.distributed_stocks_count || 1)) * 100}%` 
                                        }}
                                    ></div>
                                </div>
                            </div>

                            <div className="tm-bento-card tm-lifecycle-card">
                                <div className="tm-card-eyebrow">PORTFOLIO LIFECYCLE</div>
                                <div className="tm-lifecycle-row">
                                    <div className="tm-lifecycle-item">
                                        <span className="tm-lifecycle-badge entrants">✨ {data.summary.new_entrants_count}</span>
                                        <span className="tm-lifecycle-label">Fresh Entrants</span>
                                    </div>
                                    <div className="tm-lifecycle-item">
                                        <span className="tm-lifecycle-badge exits">🚪 {data.summary.complete_exits_count}</span>
                                        <span className="tm-lifecycle-label">Complete Exits</span>
                                    </div>
                                </div>
                                <span className="tm-lifecycle-subtext">Positions initiated or 100% liquidated between periods</span>
                            </div>
                        </div>

                        {/* ── Sector Rotation Shift Bar ── */}
                        {data.sector_shifts && data.sector_shifts.length > 0 && (
                            <div className="tm-sector-rotation-card">
                                <div className="tm-sector-header">
                                    <div>
                                        <span className="tm-card-eyebrow">MACRO CAPITAL ROTATION</span>
                                        <h3 className="tm-sector-title">Top Sector Holding Shifts</h3>
                                    </div>
                                    <span className="tm-sector-badge">{data.sector_shifts.length} Sectors Tracked</span>
                                </div>

                                <div className="tm-sector-chips-grid">
                                    {data.sector_shifts.map((sec, idx) => {
                                        const isPos = sec.net_shares > 0;
                                        return (
                                            <div key={sec.sector} className={`tm-sector-chip ${isPos ? 'pos' : 'neg'}`}>
                                                <span className="tm-sec-color-dot" style={{ background: SECTOR_PALETTE[idx % SECTOR_PALETTE.length] }}></span>
                                                <span className="tm-sec-name">{sec.sector}</span>
                                                <span className={`tm-sec-delta ${isPos ? 'pos' : 'neg'}`}>
                                                    {isPos ? '+' : ''}{fmt(sec.net_shares)} ({sec.percent_change > 0 ? '+' : ''}{sec.percent_change.toFixed(1)}%)
                                                </span>
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>
                        )}

                        {/* ── Interactive Stock Activity Tables ── */}
                        <div className="tm-table-section">
                            {/* Toolbar & Filter Deck */}
                            <div className="tm-table-toolbar">
                                <div className="tm-tabs-group">
                                    <button 
                                        type="button" 
                                        className={`tm-tab-btn ${activeTab === 'accumulations' ? 'active' : ''}`}
                                        onClick={() => setActiveTab('accumulations')}
                                    >
                                        <span>🔥 Top Buys</span>
                                        <span className="tm-tab-badge">{data.top_accumulations.length}</span>
                                    </button>
                                    <button 
                                        type="button" 
                                        className={`tm-tab-btn ${activeTab === 'distributions' ? 'active' : ''}`}
                                        onClick={() => setActiveTab('distributions')}
                                    >
                                        <span>🔻 Heavy Sells</span>
                                        <span className="tm-tab-badge">{data.top_distributions.length}</span>
                                    </button>
                                    <button 
                                        type="button" 
                                        className={`tm-tab-btn ${activeTab === 'entrants' ? 'active' : ''}`}
                                        onClick={() => setActiveTab('entrants')}
                                    >
                                        <span>✨ New Entrants</span>
                                        <span className="tm-tab-badge">{data.new_entrants.length}</span>
                                    </button>
                                    <button 
                                        type="button" 
                                        className={`tm-tab-btn ${activeTab === 'exits' ? 'active' : ''}`}
                                        onClick={() => setActiveTab('exits')}
                                    >
                                        <span>🚪 Complete Exits</span>
                                        <span className="tm-tab-badge">{data.complete_exits.length}</span>
                                    </button>
                                </div>

                                <div className="tm-filters-right">
                                    <div className="tm-mcap-pills">
                                        {['All', 'Large Cap', 'Mid Cap', 'Small Cap'].map(cap => (
                                            <button 
                                                key={cap}
                                                type="button" 
                                                className={`tm-mcap-pill ${mcapFilter === cap ? 'active' : ''}`}
                                                onClick={() => setMcapFilter(cap)}
                                            >
                                                {cap}
                                            </button>
                                        ))}
                                    </div>

                                    <div className="tm-search-wrap">
                                        <input 
                                            type="text" 
                                            className="tm-search-input"
                                            placeholder="Search company, ISIN, sector..."
                                            value={searchQuery}
                                            onChange={(e) => setSearchQuery(e.target.value)}
                                        />
                                        {searchQuery && (
                                            <button type="button" className="tm-clear-btn" onClick={() => setSearchQuery('')}>✕</button>
                                        )}
                                    </div>

                                    <ExportButton 
                                        getData={() => displayedItems.map(item => ({
                                            company_name: item.company_name,
                                            isin: item.isin,
                                            sector: item.sector,
                                            classification: item.classification,
                                            base_period: data.summary.base_period,
                                            base_shares: item.total_qty_prev,
                                            target_period: data.summary.target_period,
                                            target_shares: item.total_qty_curr,
                                            net_shares_delta: item.net_qty_bought,
                                            percent_change: item.holding_change_percent,
                                            est_value_crores: item.buy_value_crore,
                                            funds_base: item.num_funds_prev,
                                            funds_target: item.num_funds_curr
                                        }))}
                                        filename={`time_machine_${data.summary.base_period}_to_${data.summary.target_period}_${activeTab}`}
                                    />
                                </div>
                            </div>

                            {/* Table container with fixed 2-tier sticky headers */}
                            <div className="shp-table-wrap tm-table-wrap">
                                <table className="shp-table tm-table">
                                    <thead>
                                        {/* Tier 1 Header */}
                                        <tr className="shp-tr-tier-1">
                                            <th className="shp-th-fund" style={{ width: '300px' }}>COMPANY & ASSET</th>
                                            <th className="shp-th-num" style={{ width: '130px' }}>SECTOR</th>
                                            <th className="shp-th-num" style={{ width: '110px' }}>MCAP TYPE</th>
                                            <th className="shp-th-num" style={{ width: '140px' }}>{data.summary.base_period} SHARES</th>
                                            <th className="shp-th-num" style={{ width: '140px' }}>{data.summary.target_period} SHARES</th>
                                            <th className="shp-th-num" style={{ width: '150px' }}>NET CHANGE</th>
                                            <th className="shp-th-num" style={{ width: '120px' }}>EST. VALUE</th>
                                            <th className="shp-th-num" style={{ width: '120px' }}>FUNDS (MoM)</th>
                                            <th className="shp-th-num" style={{ width: '70px' }}>WATCH</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {displayedItems.length === 0 ? (
                                            <tr>
                                                <td colSpan={9} className="shp-no-data">
                                                    No stocks match the selected criteria for this time-machine window.
                                                </td>
                                            </tr>
                                        ) : (
                                            displayedItems.map((item, idx) => {
                                                const isPos = item.net_qty_bought > 0;
                                                const isNeg = item.net_qty_bought < 0;

                                                return (
                                                    <tr key={`${item.isin}-${idx}`} className={idx % 2 === 0 ? 'shp-even-row' : 'shp-odd-row'}>
                                                        <td className="shp-fund-col">
                                                            <div className="tm-stock-cell">
                                                                <Link to={`/stocks?isin=${item.isin}`} className="shp-fund-name" title={`View ${item.company_name} holdings`}>
                                                                    {item.company_name}
                                                                </Link>
                                                                <span className="shp-fund-meta">{item.isin}</span>
                                                            </div>
                                                        </td>
                                                        <td className="shp-num-col">
                                                            <span className="tm-sector-tag">{item.sector || 'Other'}</span>
                                                        </td>
                                                        <td className="shp-num-col">
                                                            <span className={`tm-mcap-badge ${MCAP_BADGE_CLASS[item.classification] || 'mcap-other'}`}>
                                                                {item.classification || '-'}
                                                            </span>
                                                        </td>
                                                        <td className="shp-num-col tabular-nums">
                                                            {fmt(item.total_qty_prev)}
                                                        </td>
                                                        <td className="shp-num-col tabular-nums">
                                                            <strong>{fmt(item.total_qty_curr)}</strong>
                                                        </td>
                                                        <td className="shp-num-col">
                                                            <div className={`tm-delta-cell ${isPos ? 'pos' : isNeg ? 'neg' : 'neutral'}`}>
                                                                <span className="tm-delta-shares">
                                                                    {isPos ? '+' : ''}{fmt(item.net_qty_bought)}
                                                                </span>
                                                                {item.holding_change_percent !== null && (
                                                                    <span className="tm-delta-pct">
                                                                        ({item.holding_change_percent > 0 ? '+' : ''}{item.holding_change_percent.toFixed(2)}%)
                                                                    </span>
                                                                )}
                                                            </div>
                                                        </td>
                                                        <td className="shp-num-col tabular-nums">
                                                            {fmtCr(item.buy_value_crore)}
                                                        </td>
                                                        <td className="shp-num-col">
                                                            <div className="tm-funds-cell">
                                                                <span>{item.num_funds_prev} ➔ {item.num_funds_curr}</span>
                                                                {item.net_fund_entrants !== 0 && (
                                                                    <span className={`tm-fund-diff ${item.net_fund_entrants > 0 ? 'pos' : 'neg'}`}>
                                                                        ({item.net_fund_entrants > 0 ? '+' : ''}{item.net_fund_entrants})
                                                                    </span>
                                                                )}
                                                            </div>
                                                        </td>
                                                        <td className="shp-num-col" style={{ textAlign: 'center' }}>
                                                            <WatchButton
                                                                assetType="stock"
                                                                isin={item.isin}
                                                                name={item.company_name}
                                                                size="sm"
                                                            />
                                                        </td>
                                                    </tr>
                                                );
                                            })
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
