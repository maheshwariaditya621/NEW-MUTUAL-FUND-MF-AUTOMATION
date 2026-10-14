import React, { useState, useMemo, useEffect, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';
import Loading from '../components/common/Loading';
import ErrorMessage from '../components/common/ErrorMessage';
import MissingData from '../components/common/MissingData';
import PageEmptyState from '../components/common/PageEmptyState';
import ExportButton from '../components/common/ExportButton';
import WatchButton from '../components/common/WatchButton';
import { getStockHoldings, getStockPrice } from '../api/stocks';
import { handleApiError } from '../api/client';
import { formatNumber } from '../utils/helpers';
import './StockHoldingsPage.css';

// ── helpers ──────────────────────────────────────────────────────────────────
const fmt = (n) => (n === null || n === undefined) ? null : (n === 0 ? '-' : Number(n).toLocaleString('en-IN'));
const fmtCr = (n) => (n === null || n === undefined) ? null : (n === 0 ? '-' : Number(n).toLocaleString('en-IN', { maximumFractionDigits: 2 }));
const fmtPct = (n, digits = 2) => (n === null || n === undefined) ? null : (n === 0 ? '-' : `${Number(n).toFixed(digits)}%`);

const MONTH_NAMES = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];

function labelToDate(label) {
    if (!label) return new Date();
    const [mon, yr] = label.split('-');
    const m = MONTH_NAMES.indexOf(mon.toUpperCase());
    const y = parseInt(yr, 10) + 2000;
    return new Date(y, m);
}

function dateToLabel(date) {
    return `${MONTH_NAMES[date.getMonth()]}-${String(date.getFullYear()).slice(2)}`;
}

function computeDisplayMonths(endLabel, n = 3) {
    const end = labelToDate(endLabel);
    const result = [];
    for (let i = 0; i < n; i++) {
        const d = new Date(end.getFullYear(), end.getMonth() - i);
        result.push(dateToLabel(d));
    }
    return result;
}

function ChangeCell({ change, pct }) {
    if (change == null) return <MissingData inline />;
    if (change === 0) return <span className="shp-change-neutral">—</span>;
    const isPos = change > 0;
    const isNeg = change < 0;
    const cls = isPos ? 'pos' : isNeg ? 'neg' : 'neutral';
    const sign = isPos ? '+' : '';

    return (
        <div className={`shp-change-cell ${cls}`}>
            <span className="shp-change-val">{sign}{fmt(change)}</span>
            {pct != null && (
                <span className="shp-change-pct">
                    ({sign}{pct.toFixed(2)}%)
                </span>
            )}
        </div>
    );
}

// ── AMC Branding & Logo Monograms ─────────────────────────────────────────────
const AMC_BRANDS = {
    'HDFC': { bg: '#dc2626', color: '#ffffff', initial: 'HDFC' },
    'NIPPON': { bg: '#d90429', color: '#ffffff', initial: 'NIPPON' },
    'ICICI': { bg: '#b91c1c', color: '#ffffff', initial: 'ICICI' },
    'SBI': { bg: '#0284c7', color: '#ffffff', initial: 'SBI' },
    'KOTAK': { bg: '#e11d48', color: '#ffffff', initial: 'KOTAK' },
    'AXIS': { bg: '#9d174d', color: '#ffffff', initial: 'AXIS' },
    'TATA': { bg: '#2563eb', color: '#ffffff', initial: 'TATA' },
    'UTI': { bg: '#d97706', color: '#ffffff', initial: 'UTI' },
    'JM FINANCIAL': { bg: '#b45309', color: '#ffffff', initial: 'JM' },
    'MOTILAL': { bg: '#f59e0b', color: '#0f172a', initial: 'MO' },
    'FRANKLIN': { bg: '#0f766e', color: '#ffffff', initial: 'FT' },
    'BANDHAN': { bg: '#ea580c', color: '#ffffff', initial: 'BANDHAN' },
    'MIRAE': { bg: '#0369a1', color: '#ffffff', initial: 'MIRAE' },
    'DSP': { bg: '#4f46e5', color: '#ffffff', initial: 'DSP' },
    'QUANT': { bg: '#7c3aed', color: '#ffffff', initial: 'QUANT' },
    'PPFAS': { bg: '#15803d', color: '#ffffff', initial: 'PPFAS' },
    'PARAG PARIKH': { bg: '#15803d', color: '#ffffff', initial: 'PPFAS' },
    'BAJAJ': { bg: '#0891b2', color: '#ffffff', initial: 'BAJAJ' },
    'CANARA': { bg: '#1d4ed8', color: '#ffffff', initial: 'CANARA' },
    'EDELWEISS': { bg: '#0284c7', color: '#ffffff', initial: 'EDEL' },
    'SUNDARAM': { bg: '#059669', color: '#ffffff', initial: 'SUNDARAM' },
    'HSBC': { bg: '#db2777', color: '#ffffff', initial: 'HSBC' },
    'BARODA': { bg: '#c2410c', color: '#ffffff', initial: 'BOB' },
    'UNION': { bg: '#1e40af', color: '#ffffff', initial: 'UNION' },
};

function getAmcBrand(name) {
    if (!name) return { bg: '#475569', color: '#ffffff', label: 'MF' };
    const upper = name.toUpperCase();
    for (const [key, val] of Object.entries(AMC_BRANDS)) {
        if (upper.includes(key)) {
            return { bg: val.bg, color: val.color, label: val.initial };
        }
    }
    const parts = name.trim().split(/\s+/);
    const initials = (parts[0][0] + (parts[1]?.[0] || '')).toUpperCase();
    return { bg: '#334155', color: '#ffffff', label: initials };
}

// ── Lightweight In-Row Vector Sparkline ───────────────────────────────────────
function MiniSparkline({ history }) {
    if (!history || history.length < 2) return null;
    const vals = history.map(h => h.num_shares).filter(v => v !== null && v !== undefined);
    if (vals.length < 2) return null;
    const rev = [...vals].reverse();
    const min = Math.min(...rev);
    const max = Math.max(...rev);
    const range = max - min || 1;
    const w = 38;
    const h = 16;
    const pointsArr = rev.map((val, idx) => {
        const x = (idx / (rev.length - 1)) * (w - 8) + 4;
        const y = h - 3 - ((val - min) / range) * (h - 6);
        return [x, y];
    });
    const pointsStr = pointsArr.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
    const lastPoint = pointsArr[pointsArr.length - 1];

    const isGrowing = rev[rev.length - 1] > rev[0];
    const isFlat = rev[rev.length - 1] === rev[0];
    const stroke = isGrowing ? '#10b981' : isFlat ? '#94a3b8' : '#f43f5e';

    return (
        <svg className="shp-sparkline" width={w} height={h} viewBox={`0 0 ${w} ${h}`} title="Holding trend trajectory">
            <polyline fill="none" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" points={pointsStr} />
            <circle cx={lastPoint[0].toFixed(1)} cy={lastPoint[1].toFixed(1)} r="2" fill={stroke} />
        </svg>
    );
}

function formatEodDate(dateStr) {
    if (!dateStr) return '';
    try {
        const parts = String(dateStr).split('-');
        if (parts.length === 3) {
            const [y, m, d] = parts;
            const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            const mi = parseInt(m, 10) - 1;
            return `${parseInt(d, 10)} ${months[mi] || m} ${y}`;
        }
        const dt = new Date(dateStr);
        return isNaN(dt.getTime()) ? dateStr : dt.toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: 'numeric' });
    } catch {
        return dateStr;
    }
}

// ── Institutional Ownership Trajectory Chart Component ───────────────────────
function InstitutionalTrajectoryChart({ trendData, closePrice, companyName, onClose }) {
    const [hoverIdx, setHoverIdx] = useState(null);
    const [rangeLimit, setRangeLimit] = useState(6);

    // Reverse to chronological order (oldest -> newest, left -> right)
    const points = useMemo(() => {
        if (!trendData || trendData.length === 0) return [];
        const reversed = [...trendData].reverse().slice(-rangeLimit);
        return reversed.map((d, i) => {
            const price = d.avg_price || (i === reversed.length - 1 ? closePrice : null);
            return {
                month: d.month,
                shares: d.total_shares || 0,
                price: price || 0,
                numFunds: d.num_funds || 0,
                monthChange: d.month_change || 0,
                percentChange: d.percent_change || 0
            };
        });
    }, [trendData, closePrice, rangeLimit]);

    if (!points || points.length < 2) {
        return (
            <div className="shp-trajectory-empty-card">
                <div className="shp-traj-header">
                    <span className="shp-card-eyebrow">INSTITUTIONAL TRAJECTORY</span>
                    <button type="button" className="shp-traj-close-btn" onClick={onClose}>✕</button>
                </div>
                <p className="shp-traj-empty-text">Need at least 2 historical months of data to render trajectory chart.</p>
            </div>
        );
    }

    const width = 880;
    const height = 240;
    const padL = 70;
    const padR = 70;
    const padT = 25;
    const padB = 40;
    const plotW = width - padL - padR;
    const plotH = height - padT - padB;

    const sharesArr = points.map(p => p.shares);
    const minShares = Math.min(...sharesArr) * 0.95;
    const maxShares = Math.max(...sharesArr) * 1.05;
    const sharesRange = (maxShares - minShares) || 1;

    const pricesArr = points.map(p => p.price).filter(p => p > 0);
    const minPrice = pricesArr.length > 0 ? Math.min(...pricesArr) * 0.95 : 0;
    const maxPrice = pricesArr.length > 0 ? Math.max(...pricesArr) * 1.05 : 100;
    const priceRange = (maxPrice - minPrice) || 1;

    const coords = points.map((p, i) => {
        const x = padL + (i / (points.length - 1)) * plotW;
        const yShares = height - padB - ((p.shares - minShares) / sharesRange) * plotH;
        const yPrice = p.price > 0 
            ? height - padB - ((p.price - minPrice) / priceRange) * plotH
            : height - padB;
        return { ...p, x, yShares, yPrice, idx: i };
    });

    const sharesLinePath = coords.map((c, i) => `${i === 0 ? 'M' : 'L'} ${c.x.toFixed(1)} ${c.yShares.toFixed(1)}`).join(' ');
    const sharesAreaPath = `${sharesLinePath} L ${coords[coords.length - 1].x.toFixed(1)} ${(height - padB).toFixed(1)} L ${coords[0].x.toFixed(1)} ${(height - padB).toFixed(1)} Z`;

    const validPriceCoords = coords.filter(c => c.price > 0);
    const priceLinePath = validPriceCoords.map((c, i) => `${i === 0 ? 'M' : 'L'} ${c.x.toFixed(1)} ${c.yPrice.toFixed(1)}`).join(' ');

    const activePoint = hoverIdx !== null ? coords[hoverIdx] : coords[coords.length - 1];

    const oldest = coords[0];
    const newest = coords[coords.length - 1];
    const netSharesChg = newest.shares - oldest.shares;
    const netSharesPct = oldest.shares > 0 ? ((netSharesChg / oldest.shares) * 100) : 0;
    const netPriceChg = (newest.price && oldest.price) ? (newest.price - oldest.price) : 0;
    const netPricePct = oldest.price > 0 ? ((netPriceChg / oldest.price) * 100) : 0;

    let verdict = "NEUTRAL CONSOLIDATION";
    let verdictCls = "neutral";
    let verdictDesc = "Mutual fund aggregate holdings remained stable across the observed historical window.";

    if (netSharesChg > 0 && netPriceChg <= 0) {
        verdict = "BUYING THE DIP (ACCUMULATION)";
        verdictCls = "pos";
        verdictDesc = `Mutual funds actively accumulated shares (+${netSharesPct.toFixed(1)}%) while the stock price softened (${netPricePct.toFixed(1)}%), signaling strong value conviction.`;
    } else if (netSharesChg < 0 && netPriceChg >= 0) {
        verdict = "SELLING INTO RALLY (DISTRIBUTION)";
        verdictCls = "neg";
        verdictDesc = `Mutual funds booked profits and trimmed holdings (${netSharesPct.toFixed(1)}%) into market strength (+${netPricePct.toFixed(1)}%).`;
    } else if (netSharesChg > 0 && netPriceChg > 0) {
        verdict = "MOMENTUM ACCUMULATION";
        verdictCls = "pos";
        verdictDesc = `Mutual funds supported the upward price trend (+${netPricePct.toFixed(1)}%) with consistent net buying (+${netSharesPct.toFixed(1)}%).`;
    } else if (netSharesChg < 0 && netPriceChg < 0) {
        verdict = "RISK-OFF REDUCTION";
        verdictCls = "neg";
        verdictDesc = `Mutual funds trimmed aggregate exposure (${netSharesPct.toFixed(1)}%) alongside price contraction (${netPricePct.toFixed(1)}%).`;
    }

    return (
        <div className="shp-trajectory-card">
            <div className="shp-traj-header">
                <div className="shp-traj-title-wrap">
                    <span className="shp-card-eyebrow">INSTITUTIONAL OWNERSHIP TRAJECTORY</span>
                    <h3 className="shp-traj-title">
                        Total MF Shares vs EOD Stock Price
                    </h3>
                    <div className="shp-traj-sublabel">
                        Visualizing institutional accumulation/distribution correlation against stock valuation
                    </div>
                </div>

                <div className="shp-traj-header-actions">
                    <div className="shp-traj-legend">
                        <span className="shp-legend-item shares">
                            <span className="shp-legend-line shares-line"></span> Total MF Shares (Left)
                        </span>
                        <span className="shp-legend-item price">
                            <span className="shp-legend-line price-line"></span> Stock Price (Right)
                        </span>
                    </div>

                    <div className="shp-traj-range-pills">
                        <button 
                            type="button" 
                            className={`shp-traj-pill ${rangeLimit === 6 ? 'active' : ''}`}
                            onClick={() => setRangeLimit(6)}
                        >
                            6M
                        </button>
                        <button 
                            type="button" 
                            className={`shp-traj-pill ${rangeLimit === 12 ? 'active' : ''}`}
                            onClick={() => setRangeLimit(12)}
                        >
                            12M
                        </button>
                    </div>

                    <button 
                        type="button" 
                        className="shp-traj-close-btn" 
                        onClick={onClose}
                        title="Close Trajectory Chart"
                    >
                        ✕
                    </button>
                </div>
            </div>

            <div className="shp-traj-chart-container">
                <svg 
                    className="shp-traj-svg" 
                    viewBox={`0 0 ${width} ${height}`} 
                    preserveAspectRatio="xMidYMid meet"
                    onMouseLeave={() => setHoverIdx(null)}
                >
                    <defs>
                        <linearGradient id="mfSharesGradient" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="#10b981" stopOpacity="0.32" />
                            <stop offset="100%" stopColor="#10b981" stopOpacity="0.01" />
                        </linearGradient>
                    </defs>

                    {[0, 0.25, 0.5, 0.75, 1].map((pct, i) => {
                        const y = padT + pct * plotH;
                        const sVal = maxShares - pct * sharesRange;
                        const pVal = maxPrice - pct * priceRange;
                        return (
                            <g key={i}>
                                <line 
                                    x1={padL} 
                                    y1={y} 
                                    x2={width - padR} 
                                    y2={y} 
                                    stroke="var(--border-color)" 
                                    strokeDasharray="3 3" 
                                    strokeOpacity="0.45"
                                />
                                <text 
                                    x={padL - 10} 
                                    y={y + 4} 
                                    className="shp-traj-axis-text shares" 
                                    textAnchor="end"
                                >
                                    {sVal >= 10000000 ? `${(sVal / 10000000).toFixed(1)}Cr` : `${(sVal / 1000000).toFixed(1)}M`}
                                </text>
                                {pVal > 0 && (
                                    <text 
                                        x={width - padR + 10} 
                                        y={y + 4} 
                                        className="shp-traj-axis-text price" 
                                        textAnchor="start"
                                    >
                                        ₹{Math.round(pVal)}
                                    </text>
                                )}
                            </g>
                        );
                    })}

                    <path d={sharesAreaPath} fill="url(#mfSharesGradient)" />

                    <path 
                        d={sharesLinePath} 
                        fill="none" 
                        stroke="#10b981" 
                        strokeWidth="2.5" 
                        strokeLinecap="round" 
                        strokeLinejoin="round" 
                    />

                    {validPriceCoords.length > 1 && (
                        <path 
                            d={priceLinePath} 
                            fill="none" 
                            stroke="#f59e0b" 
                            strokeWidth="2.5" 
                            strokeDasharray="4 3" 
                            strokeLinecap="round" 
                            strokeLinejoin="round" 
                        />
                    )}

                    {coords.map((c, i) => (
                        <g key={c.month}>
                            <text 
                                x={c.x} 
                                y={height - 12} 
                                className={`shp-traj-x-text ${hoverIdx === i ? 'active' : ''}`}
                                textAnchor="middle"
                            >
                                {c.month}
                            </text>

                            <circle cx={c.x} cy={c.yShares} r={hoverIdx === i ? 5.5 : 3.5} fill="#10b981" stroke="var(--bg-card)" strokeWidth="1.5" />
                            {c.price > 0 && (
                                <circle cx={c.x} cy={c.yPrice} r={hoverIdx === i ? 5.5 : 3.5} fill="#f59e0b" stroke="var(--bg-card)" strokeWidth="1.5" />
                            )}

                            <rect 
                                x={c.x - (plotW / points.length) / 2} 
                                y={padT} 
                                width={plotW / points.length} 
                                height={plotH} 
                                fill="transparent"
                                style={{ cursor: 'pointer' }}
                                onMouseEnter={() => setHoverIdx(i)}
                            />
                        </g>
                    ))}

                    {hoverIdx !== null && (
                        <line 
                            x1={activePoint.x} 
                            y1={padT} 
                            x2={activePoint.x} 
                            y2={height - padB} 
                            stroke="rgba(255, 255, 255, 0.4)" 
                            strokeDasharray="2 2" 
                        />
                    )}
                </svg>

                {activePoint && (
                    <div 
                        className="shp-traj-tooltip"
                        style={{
                            left: `${(activePoint.x / width) * 100}%`
                        }}
                    >
                        <div className="shp-tt-month">{activePoint.month}</div>
                        <div className="shp-tt-row shares">
                            <span>MF Shares:</span>
                            <strong>{fmt(activePoint.shares)}</strong>
                        </div>
                        {activePoint.monthChange !== 0 && (
                            <div className="shp-tt-sub">
                                MoM: {activePoint.monthChange > 0 ? '+' : ''}{fmt(activePoint.monthChange)} ({activePoint.percentChange > 0 ? '+' : ''}{activePoint.percentChange.toFixed(2)}%)
                            </div>
                        )}
                        {activePoint.price > 0 && (
                            <div className="shp-tt-row price">
                                <span>EOD Price:</span>
                                <strong>₹{activePoint.price.toLocaleString('en-IN', { maximumFractionDigits: 2 })}</strong>
                            </div>
                        )}
                        <div className="shp-tt-funds">
                            {activePoint.numFunds} Mutual Fund Schemes
                        </div>
                    </div>
                )}
            </div>

            <div className={`shp-traj-verdict-banner ${verdictCls}`}>
                <div className="shp-verdict-badge">{verdict}</div>
                <div className="shp-verdict-desc">{verdictDesc}</div>
            </div>
        </div>
    );
}

// ── Main Component ────────────────────────────────────────────────────────────
export default function StockHoldingsPage() {
    const [searchParams, setSearchParams] = useSearchParams();
    const identifierParam = searchParams.get('isin') || searchParams.get('identifier');
    const filterParam = searchParams.get('filter'); // 'entrants' | 'exits'

    const [summary, setSummary] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [livePrice, setLivePrice] = useState(null);
    const [viewMode, setViewMode] = useState('scheme'); // 'scheme' | 'amc' | 'category'
    const [filterText, setFilterText] = useState('');
    const [sortConfig, setSortConfig] = useState({ key: 'shares_0', direction: 'desc' });
    const [selectedMonth, setSelectedMonth] = useState(null);
    const [aumViewMode, setAumViewMode] = useState('total'); // 'total' or 'equity'
    const [allAvailableMonths, setAllAvailableMonths] = useState([]);
    const [showAmcStatus, setShowAmcStatus] = useState(false);
    const [showTrajectoryChart, setShowTrajectoryChart] = useState(false);
    const [expandedCats, setExpandedCats] = useState({}); // sub_category -> bool
    const [schemeTypeFilter, setSchemeTypeFilter] = useState('all'); // 'all' | 'pure_equity' | 'arbitrage'
    const tableWrapRef = useRef(null);

    const displayMonths = useMemo(() => {
        if (selectedMonth) return computeDisplayMonths(selectedMonth, 3);
        const latest = summary?.holdings?.[0]?.history?.[0]?.month;
        if (latest) return computeDisplayMonths(latest, 3);
        return [];
    }, [selectedMonth, summary]);

    useEffect(() => {
        if (!summary?.holdings?.[0]?.history) return;
        const seen = new Set(summary.holdings[0].history.map(h => h.month));
        setAllAvailableMonths(prev => {
            const merged = Array.from(new Set([...prev, ...seen]));
            merged.sort((a, b) => labelToDate(b) - labelToDate(a));
            return merged;
        });
    }, [summary]);

    useEffect(() => {
        if (identifierParam) {
            handleFetchHoldings(identifierParam, 6, null);
        }
    }, [identifierParam]);

    const handleFetchHoldings = async (identifier, months, endMonth) => {
        setLoading(true);
        setError(null);
        setSummary(null);
        try {
            const data = await getStockHoldings(identifier, months, endMonth);
            setSummary(data);

            const p = data.close_price || data.live_price;
            if (p) {
                setLivePrice(p);
            } else if (data.isin) {
                getStockPrice(data.isin).then(res => {
                    if (res?.price) setLivePrice(res.price);
                }).catch(e => console.error("Price fetch fallback error:", e));
            }
        } catch (err) {
            setError(handleApiError(err));
        } finally {
            setLoading(false);
        }
    };

    const handleMonthChange = (monthLabel) => {
        const val = monthLabel || null;
        setSelectedMonth(val);
        if (identifierParam) handleFetchHoldings(identifierParam, 6, val);
    };

    const handleSort = (key) => {
        setSortConfig(prev => {
            if (prev.key !== key) return { key, direction: 'desc' };
            if (prev.direction === 'desc') return { key, direction: 'asc' };
            return { key: null, direction: 'desc' };
        });
    };

    const sortArrow = (key) =>
        sortConfig.key === key ? (sortConfig.direction === 'asc' ? ' ↑' : ' ↓') : null;

    // Helper: get CSS class from category name
    const catClass = (cat) => {
        if (!cat) return 'other';
        const c = cat.toLowerCase();
        if (c.includes('equity')) return 'equity';
        if (c.includes('arbitrage')) return 'arbitrage';
        if (c.includes('hybrid')) return 'hybrid';
        if (c.includes('debt')) return 'debt';
        if (c.includes('index')) return 'index';
        if (c.includes('thematic')) return 'thematic';
        if (c.includes('elss') || c.includes('tax')) return 'elss';
        return 'other';
    };

    // Helper: Identify Arbitrage schemes
    const isArbitrageScheme = (s) => {
        if (!s) return false;
        const cat = (s.website_category || '').toLowerCase();
        const sub = (s.website_sub_category || '').toLowerCase();
        const name = (s.scheme_name || '').toUpperCase();
        return cat.includes('arbitrage') || sub.includes('arbitrage') || name.includes('ARBITRAGE');
    };

    // Scheme counts by type
    const schemeTypeCounts = useMemo(() => {
        if (!summary?.holdings) return { all: 0, pureEquity: 0, arbitrage: 0 };
        let pureEquity = 0;
        let arbitrage = 0;
        summary.holdings.forEach(s => {
            if (isArbitrageScheme(s)) {
                arbitrage++;
            } else {
                pureEquity++;
            }
        });
        return { all: summary.holdings.length, pureEquity, arbitrage };
    }, [summary]);

    // Schemes filtered by schemeTypeFilter
    const filteredHoldings = useMemo(() => {
        if (!summary?.holdings) return [];
        if (schemeTypeFilter === 'pure_equity') {
            return summary.holdings.filter(s => !isArbitrageScheme(s));
        }
        if (schemeTypeFilter === 'arbitrage') {
            return summary.holdings.filter(s => isArbitrageScheme(s));
        }
        return summary.holdings;
    }, [summary, schemeTypeFilter]);

    // Dynamic Monthly Trend for Top Cards (reflects active filter)
    const displayTrend = useMemo(() => {
        if (!summary?.monthly_trend) return [];
        if (schemeTypeFilter === 'all') return summary.monthly_trend;
        return displayMonths.map((m) => {
            let total_shares = 0;
            let month_change = 0;
            let activeFunds = 0;
            filteredHoldings.forEach(scheme => {
                const h = scheme.history?.find(x => x.month === m);
                if (h && h.num_shares !== null && h.num_shares > 0) {
                    total_shares += h.num_shares;
                    month_change += (h.month_change || 0);
                    activeFunds += 1;
                }
            });
            const ownership_percent = summary.shares_outstanding && total_shares > 0
                ? (total_shares / summary.shares_outstanding) * 100
                : null;
            return {
                month: m,
                total_shares,
                num_funds: activeFunds,
                month_change,
                ownership_percent,
            };
        });
    }, [summary, schemeTypeFilter, filteredHoldings, displayMonths]);

    // Overall Institutional Conviction Ratio (Pure Active vs Arbitrage)
    const convictionRatio = useMemo(() => {
        if (!summary?.holdings) return { equityPct: 100, arbPct: 0, equityShares: 0, arbShares: 0 };
        let equityShares = 0;
        let arbShares = 0;
        summary.holdings.forEach(s => {
            const latest = s.history?.[0]?.num_shares || 0;
            if (isArbitrageScheme(s)) {
                arbShares += latest;
            } else {
                equityShares += latest;
            }
        });
        const total = equityShares + arbShares;
        const equityPct = total > 0 ? Math.round((equityShares / total) * 100) : 100;
        const arbPct = 100 - equityPct;
        return { equityPct, arbPct, equityShares, arbShares };
    }, [summary]);

    const amcAggregated = useMemo(() => {
        if (!summary?.holdings) return [];

        // Track total composition per AMC across all schemes
        const amcComposition = {};
        for (const scheme of summary.holdings) {
            const amc = scheme.amc_name;
            if (!amcComposition[amc]) {
                amcComposition[amc] = { total: 0, pureEquity: 0, arbitrage: 0 };
            }
            amcComposition[amc].total += 1;
            if (isArbitrageScheme(scheme)) {
                amcComposition[amc].arbitrage += 1;
            } else {
                amcComposition[amc].pureEquity += 1;
            }
        }

        const map = {};
        for (const scheme of filteredHoldings) {
            const amc = scheme.amc_name;
            if (!map[amc]) {
                const comp = amcComposition[amc] || { total: 0, pureEquity: 0, arbitrage: 0 };
                map[amc] = {
                    amc_name: amc,
                    scheme_count: 0,
                    total_schemes_all: comp.total,
                    equity_schemes_all: comp.pureEquity,
                    arb_schemes_all: comp.arbitrage,
                    has_arbitrage: comp.arbitrage > 0,
                    equity_aum_cr: 0,
                    total_aum_cr: 0,
                    history: scheme.history.map(h => ({ ...h, num_shares: null, month_change: 0 })),
                };
            }
            const entry = map[amc];
            entry.scheme_count += 1;
            entry.equity_aum_cr += (parseFloat(scheme.equity_aum_cr) || 0);
            entry.total_aum_cr += (parseFloat(scheme.total_aum_cr) || 0);
            scheme.history.forEach((h, idx) => {
                if (entry.history[idx]) {
                    if (h.num_shares !== null) {
                        if (entry.history[idx].num_shares === null) entry.history[idx].num_shares = 0;
                        entry.history[idx].num_shares += h.num_shares;
                    }
                    entry.history[idx].month_change = (entry.history[idx].month_change || 0) + (h.month_change || 0);
                    if (summary.shares_outstanding) {
                        entry.history[idx].ownership_percent = (entry.history[idx].num_shares / summary.shares_outstanding) * 100;
                    }
                }
            });
        }
        return Object.values(map);
    }, [summary, filteredHoldings]);

    // ── Category grouped data (for Category-wise view) ──
    const { broadGroups, subGroups } = useMemo(() => {
        if (!filteredHoldings) return { broadGroups: [], subGroups: [] };
        
        const broadMap = {};
        const subMap = {};
        
        for (const scheme of filteredHoldings) {
            const cat  = scheme.website_category     || 'Other';
            const sub  = scheme.website_sub_category || 'Uncategorized';
            const subKey  = `${cat}||${sub}`;
            
            // Broad category aggregation
            if (!broadMap[cat]) {
                broadMap[cat] = { category: cat, totalShares: 0, schemeCount: 0 };
            }
            // Sub category aggregation
            if (!subMap[subKey]) {
                subMap[subKey] = { category: cat, sub_category: sub, schemes: [], totalShares: 0 };
            }
            
            const latestShares = scheme.history[0]?.num_shares ?? 0;
            if (latestShares > 0) {
                broadMap[cat].totalShares += latestShares;
                subMap[subKey].totalShares += latestShares;
            }
            broadMap[cat].schemeCount += 1;
            subMap[subKey].schemes.push(scheme);
        }
        
        // Sort broad groups by shares descending
        const broadGroupsArr = Object.values(broadMap).sort((a, b) => b.totalShares - a.totalShares);
        
        // Sort sub groups by Broad Category name, then shares descending
        const subGroupsArr = Object.values(subMap).sort((a, b) => {
            if (a.category !== b.category) return a.category.localeCompare(b.category);
            return b.totalShares - a.totalShares;
        });
        
        return { broadGroups: broadGroupsArr, subGroups: subGroupsArr };
    }, [filteredHoldings]);

    // toggle expand/collapse of one category
    const toggleCat = (key) => setExpandedCats(prev => ({ ...prev, [key]: !prev[key] }));
    const expandAll  = () => {
        const next = {};
        subGroups.forEach(g => { next[`${g.category}||${g.sub_category}`] = true; });
        setExpandedCats(next);
    };
    const collapseAll = () => setExpandedCats({});

    const schemesToRender = useMemo(() => {
        if (!filteredHoldings) return [];
        let items = [...filteredHoldings];

        if (filterParam === 'entrants') {
            items = items.filter(h => (h.history[0]?.num_shares > 0 && h.history[1]?.num_shares === 0));
        } else if (filterParam === 'exits') {
            items = items.filter(h => (h.history[1]?.num_shares > 0 && h.history[0]?.num_shares === 0));
        }

        if (filterText) {
            const low = filterText.toLowerCase();
            items = items.filter(h => h.scheme_name.toLowerCase().includes(low) || h.amc_name.toLowerCase().includes(low));
        }

        if (sortConfig.key) {
            items.sort((a, b) => {
                let aVal, bVal;
                if (sortConfig.key === 'scheme_name') { aVal = a.scheme_name; bVal = b.scheme_name; }
                else if (sortConfig.key === 'amc_name') { aVal = a.amc_name; bVal = b.amc_name; }
                else if (sortConfig.key === 'pnav') { aVal = parseFloat(a.history[0]?.percent_to_aum) || 0; bVal = parseFloat(b.history[0]?.percent_to_aum) || 0; }
                else if (sortConfig.key.startsWith('shares_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.num_shares || 0; bVal = b.history[idx]?.num_shares || 0;
                }
                else if (sortConfig.key.startsWith('change_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.month_change || 0; bVal = b.history[idx]?.month_change || 0;
                }
                else if (sortConfig.key.startsWith('own_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.ownership_percent || 0; bVal = b.history[idx]?.ownership_percent || 0;
                }

                if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
                if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }
        return items;
    }, [summary, filterText, sortConfig, filterParam]);

    const amcToRender = useMemo(() => {
        let items = [...amcAggregated];
        if (filterParam === 'entrants') {
            items = items.filter(h => (h.history[0]?.num_shares > 0 && h.history[1]?.num_shares === 0));
        } else if (filterParam === 'exits') {
            items = items.filter(h => (h.history[1]?.num_shares > 0 && h.history[0]?.num_shares === 0));
        }
        if (filterText) {
            const low = filterText.toLowerCase();
            items = items.filter(h => h.amc_name.toLowerCase().includes(low));
        }

        if (sortConfig.key) {
            items.sort((a, b) => {
                let aVal, bVal;
                if (sortConfig.key === 'amc_name') { aVal = a.amc_name; bVal = b.amc_name; }
                else if (sortConfig.key === 'equity_aum_cr') { aVal = a.equity_aum_cr; bVal = b.equity_aum_cr; }
                else if (sortConfig.key === 'total_aum_cr') { aVal = a.total_aum_cr; bVal = b.total_aum_cr; }
                else if (sortConfig.key.startsWith('shares_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.num_shares || 0; bVal = b.history[idx]?.num_shares || 0;
                }
                else if (sortConfig.key.startsWith('change_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.month_change || 0; bVal = b.history[idx]?.month_change || 0;
                }
                else if (sortConfig.key.startsWith('own_')) {
                    const idx = parseInt(sortConfig.key.split('_')[1]);
                    aVal = a.history[idx]?.ownership_percent || 0; bVal = b.history[idx]?.ownership_percent || 0;
                }
                if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
                if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }
        return items;
    }, [amcAggregated, filterText, sortConfig, filterParam]);

    return (
        <div className="shp-page">
            <div className="shp-container">
                {loading && <Loading message="Loading stock holdings..." />}
                {error && (
                    <ErrorMessage
                        message={error}
                        onRetry={() => identifierParam && handleFetchHoldings(identifierParam, 4, selectedMonth)}
                    />
                )}

                {!identifierParam && !loading && !error && (
                    <PageEmptyState
                        title="Institutional Ownership Insights"
                        description="Unlock the secrets of institutional stock ownership. Track which mutual funds are backing your favorite stocks, monitor exactly how much they are buying or selling each month, and analyze ownership trends to make smarter investment decisions."
                        placeholder="Search by Stock Name or ISIN (e.g. Reliance, INE002A01018)"
                        type="stock"
                        suggestions={["Reliance", "HDFC Bank", "ICICI Bank", "Infosys", "ITC"]}
                        onSearch={(val) => {
                            searchParams.set('isin', val);
                            setSearchParams(searchParams);
                        }}
                    />
                )}

                {summary && !loading && !error && (
                    <>
                        {/* ── Institutional Bento Grid Overview ── */}
                        <div className="shp-bento-grid">
                            {/* Card 1: Stock Identity & Snapshot */}
                            <div className="shp-bento-card shp-stock-hero-card">
                                <div className="shp-card-header">
                                    <span className="shp-card-eyebrow">EQUITY ASSET</span>
                                    <div className="rv-live-price-wrapper">
                                        {(summary.close_price || livePrice) ? (
                                            <div 
                                                className="rv-eod-price-chip" 
                                                title={summary.price_date ? `NSE Bhavcopy EOD Close as of ${formatEodDate(summary.price_date)}` : 'NSE Bhavcopy EOD Close'}
                                            >
                                                <span className="shp-eod-badge">EOD</span>
                                                <span className="shp-eod-price">₹{formatNumber(summary.close_price || livePrice)}</span>
                                                {summary.price_change_pct !== null && summary.price_change_pct !== undefined && (
                                                    <span className={`shp-eod-change ${Number(summary.price_change_pct) >= 0 ? 'pos' : 'neg'}`}>
                                                        {Number(summary.price_change_pct) >= 0 ? '+' : ''}{Number(summary.price_change_pct).toFixed(2)}%
                                                    </span>
                                                )}
                                                {summary.price_date && (
                                                    <span className="shp-eod-date">
                                                        • {formatEodDate(summary.price_date)}
                                                    </span>
                                                )}
                                            </div>
                                        ) : (
                                            <div className="shp-eod-unavailable">
                                                <span>Price: N/A</span>
                                            </div>
                                        )}
                                    </div>
                                </div>

                                <div className="shp-stock-title-row">
                                    <h1 className="shp-company-name">{summary.company_name}</h1>
                                    <WatchButton
                                        assetType="stock"
                                        isin={summary.isin}
                                        name={summary.company_name}
                                        size="sm"
                                    />
                                </div>

                                <div className="shp-meta-pills">
                                    <span className="shp-meta-pill shp-isin-pill">{summary.isin}</span>
                                    {summary.sector && <span className="shp-meta-pill shp-sector-pill">{summary.sector}</span>}
                                    {summary.market_cap && (
                                        <span className="shp-meta-pill shp-mcap-pill">
                                            ₹{fmtCr(summary.market_cap) || <MissingData inline />} Cr
                                            {summary.mcap_type && (
                                                <span className={`shp-mcap-type-badge ${summary.mcap_type.toLowerCase().replace(' ', '-')}`}>
                                                    {summary.mcap_type}
                                                </span>
                                            )}
                                        </span>
                                    )}
                                    {summary.shares_outstanding && (
                                        <span className="shp-meta-pill shp-shares-pill">
                                            O/S: {fmt(summary.shares_outstanding)}
                                        </span>
                                    )}
                                </div>

                                <div className="shp-hero-metrics-row">
                                    <div className="shp-metric-block">
                                        <span className="shp-metric-label">MF Ownership</span>
                                        <span className="shp-metric-val shp-emerald-glow">
                                            {summary.ownership_percent !== null ? `${summary.ownership_percent.toFixed(2)}%` : <MissingData inline />}
                                        </span>
                                    </div>
                                    <div className="shp-metric-block">
                                        <span className="shp-metric-label">Total MF Shares</span>
                                        <span className="shp-metric-val tabular-nums">{fmt(summary.total_shares)}</span>
                                    </div>
                                    <div className="shp-metric-block">
                                        <span className="shp-metric-label">Active Funds</span>
                                        <span className="shp-metric-val tabular-nums">{summary.total_funds}</span>
                                    </div>
                                </div>
                            </div>

                            {/* Card 2: 3-Month Flow Momentum */}
                            <div className="shp-bento-card shp-flow-momentum-card">
                                <div className="shp-card-header">
                                    <div>
                                        <span className="shp-card-eyebrow">INSTITUTIONAL NET FLOW MOMENTUM</span>
                                        <div className="shp-card-sublabel">MoM Holdings & Accumulation Trajectory</div>
                                    </div>
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                        <button
                                            type="button"
                                            className={`shp-traj-toggle-pill ${showTrajectoryChart ? 'active' : ''}`}
                                            onClick={() => setShowTrajectoryChart(v => !v)}
                                            title="Toggle Institutional Ownership Trajectory Chart"
                                        >
                                            📈 {showTrajectoryChart ? 'Hide Chart' : 'Trajectory Chart'}
                                        </button>
                                        <span className="shp-flow-trend-badge">
                                            {displayMonths.length} Months Trend
                                        </span>
                                    </div>
                                </div>

                                <div className="shp-flow-months-grid">
                                    {displayMonths.map((m, i) => {
                                        const trendData = displayTrend.find(t => t.month === m);
                                        const isPartialMonth = summary.data_warning && m === summary.data_warning.latest_label;
                                        const isPos = trendData && trendData.month_change > 0;
                                        const isNeg = trendData && trendData.month_change < 0;

                                        return (
                                            <div key={m} className={`shp-flow-month-item ${i === 0 ? 'is-latest' : ''}`}>
                                                <div className="shp-flow-month-head">
                                                    <span className="shp-flow-month-label">{m}</span>
                                                    {isPartialMonth && (
                                                        <span className="shp-partial-badge" title="Partial data — not all AMCs have uploaded yet">⚠️ Partial</span>
                                                    )}
                                                </div>
                                                {trendData ? (
                                                    <div className="shp-flow-month-body">
                                                        <div className="shp-flow-shares-val tabular-nums">
                                                            {fmt(trendData.total_shares)}
                                                        </div>
                                                        <div className="shp-flow-delta-row">
                                                            {trendData.month_change !== 0 ? (
                                                                <span className={`shp-flow-delta-pill ${isPos ? 'pos' : 'neg'}`}>
                                                                    {isPos ? '▲ +' : '▼ '}{fmt(trendData.month_change)}
                                                                </span>
                                                            ) : (
                                                                <span className="shp-flow-delta-pill neutral">— 0</span>
                                                            )}
                                                        </div>
                                                        {trendData.ownership_percent !== null && (
                                                            <div className="shp-flow-stake">
                                                                <span>Stake:</span> <strong>{trendData.ownership_percent.toFixed(2)}%</strong>
                                                            </div>
                                                        )}
                                                    </div>
                                                ) : (
                                                    <div className="shp-flow-empty"><MissingData /></div>
                                                )}
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>

                            {/* Card 3: Active Conviction vs Arbitrage Meter */}
                            <div className="shp-bento-card shp-conviction-meter-card">
                                <div className="shp-card-header">
                                    <div>
                                        <span className="shp-card-eyebrow">PORTFOLIO DYNAMICS</span>
                                        <div className="shp-card-sublabel">Fundamental Conviction vs Hedged Arbitrage</div>
                                    </div>
                                    <span className={`shp-conviction-pill ${convictionRatio.equityPct >= 75 ? 'high-conviction' : 'spread-play'}`}>
                                        {convictionRatio.equityPct >= 75 ? 'HIGH ACTIVE CONVICTION' : convictionRatio.arbPct >= 40 ? 'SIGNIFICANT ARBITRAGE' : 'BALANCED SPREAD'}
                                    </span>
                                </div>

                                <div className="shp-meter-wrap">
                                    <div className="shp-meter-stats-row">
                                        <div className="shp-meter-stat">
                                            <span className="shp-meter-dot equity-dot"></span>
                                            <span className="shp-meter-name">Pure Equity</span>
                                            <strong className="shp-meter-pct">{convictionRatio.equityPct}%</strong>
                                        </div>
                                        <div className="shp-meter-stat">
                                            <span className="shp-meter-dot arb-dot"></span>
                                            <span className="shp-meter-name">Arbitrage</span>
                                            <strong className="shp-meter-pct">{convictionRatio.arbPct}%</strong>
                                        </div>
                                    </div>

                                    <div className="shp-meter-bar-track">
                                        <div
                                            className="shp-meter-bar-fill equity-fill"
                                            style={{ width: `${convictionRatio.equityPct}%` }}
                                            title={`Pure Equity: ${convictionRatio.equityPct}% (${fmt(convictionRatio.equityShares)} shares)`}
                                        />
                                        <div
                                            className="shp-meter-bar-fill arb-fill"
                                            style={{ width: `${convictionRatio.arbPct}%` }}
                                            title={`Arbitrage: ${convictionRatio.arbPct}% (${fmt(convictionRatio.arbShares)} shares)`}
                                        />
                                    </div>

                                    <div className="shp-conviction-subtext">
                                        <strong>{fmt(convictionRatio.equityShares)}</strong> fundamental shares vs <strong>{fmt(convictionRatio.arbShares)}</strong> hedged arbitrage desk shares.
                                    </div>
                                </div>

                                <div className="shp-meter-quick-toggles">
                                    <button
                                        type="button"
                                        className={`shp-meter-btn ${schemeTypeFilter === 'all' ? 'active' : ''}`}
                                        onClick={() => setSchemeTypeFilter('all')}
                                    >
                                        All Funds
                                    </button>
                                    <button
                                        type="button"
                                        className={`shp-meter-btn equity-btn ${schemeTypeFilter === 'pure_equity' ? 'active' : ''}`}
                                        onClick={() => setSchemeTypeFilter('pure_equity')}
                                    >
                                        Pure Equity Only
                                    </button>
                                    <button
                                        type="button"
                                        className={`shp-meter-btn arb-btn ${schemeTypeFilter === 'arbitrage' ? 'active' : ''}`}
                                        onClick={() => setSchemeTypeFilter('arbitrage')}
                                    >
                                        Arbitrage Only
                                    </button>
                                </div>
                            </div>
                        </div>

                        {/* ── Partial Data Warning Banner ── */}
                        {summary.data_warning && (
                            <div style={{
                                background: 'rgba(234, 179, 8, 0.08)',
                                border: '1px solid rgba(234, 179, 8, 0.35)',
                                borderRadius: '8px', padding: '10px 14px',
                                marginBottom: '16px', fontSize: '12px',
                                color: 'var(--text-primary)', lineHeight: '1.5'
                            }}>
                                <div style={{ display: 'flex', alignItems: 'flex-start', gap: '10px' }}>
                                    <span style={{ fontSize: '16px', flexShrink: 0 }}>⚠️</span>
                                    <div style={{ flex: 1 }}>
                                        <strong style={{ color: '#eab308' }}>Partial Data — {summary.data_warning.latest_label}</strong>
                                        <span style={{ color: 'var(--text-secondary)', marginLeft: '6px' }}>
                                            <strong style={{ color: 'var(--text-primary)' }}>
                                                {summary.data_warning.amcs_uploaded} of {summary.data_warning.amcs_expected} AMCs
                                            </strong> have submitted {summary.data_warning.latest_label} data
                                            {summary.data_warning.amcs_pending > 0 && (
                                                <span style={{ color: '#f87171' }}> ({summary.data_warning.amcs_pending} still pending)</span>
                                            )}.
                                            {' '}Summary figures reflect
                                            {' '}<strong>{summary.data_warning.complete_label}</strong> — the last complete month.
                                            Banner disappears automatically once all AMCs upload.
                                        </span>
                                    </div>
                                    <button
                                        onClick={() => setShowAmcStatus(s => !s)}
                                        style={{
                                            flexShrink: 0, fontSize: '11px', fontWeight: '600',
                                            padding: '3px 10px', borderRadius: '4px', cursor: 'pointer',
                                            background: 'rgba(234,179,8,0.15)',
                                            border: '1px solid rgba(234,179,8,0.4)',
                                            color: '#eab308', whiteSpace: 'nowrap'
                                        }}
                                    >
                                        {showAmcStatus ? 'Hide' : 'View AMC Status'}
                                    </button>
                                </div>

                                {/* ── Expandable AMC Status Panel ── */}
                                {showAmcStatus && (
                                    <div style={{
                                        display: 'grid', gridTemplateColumns: '1fr 1fr',
                                        gap: '12px', marginTop: '12px', paddingTop: '12px',
                                        borderTop: '1px solid rgba(234,179,8,0.2)'
                                    }}>
                                        {/* Pending AMCs */}
                                        <div>
                                            <div style={{ fontSize: '11px', fontWeight: '700', color: '#f87171', marginBottom: '6px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                                                ⏳ Pending ({summary.data_warning.amcs_pending})
                                            </div>
                                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                                {(summary.data_warning.pending_amc_names || []).map(amc => (
                                                    <span key={amc} style={{
                                                        fontSize: '10px', padding: '2px 7px',
                                                        borderRadius: '3px', fontWeight: '500',
                                                        background: 'rgba(248,113,113,0.1)',
                                                        border: '1px solid rgba(248,113,113,0.3)',
                                                        color: '#f87171'
                                                    }}>{amc}</span>
                                                ))}
                                                {(!summary.data_warning.pending_amc_names?.length) && (
                                                    <span style={{ color: 'var(--text-secondary)', fontSize: '11px' }}>None — all uploaded!</span>
                                                )}
                                            </div>
                                        </div>
                                        {/* Uploaded AMCs */}
                                        <div>
                                            <div style={{ fontSize: '11px', fontWeight: '700', color: '#10b981', marginBottom: '6px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                                                ✅ Uploaded ({summary.data_warning.amcs_uploaded})
                                            </div>
                                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                                {(summary.data_warning.uploaded_amc_names || []).map(amc => (
                                                    <span key={amc} style={{
                                                        fontSize: '10px', padding: '2px 7px',
                                                        borderRadius: '3px', fontWeight: '500',
                                                        background: 'rgba(16,185,129,0.08)',
                                                        border: '1px solid rgba(16,185,129,0.25)',
                                                        color: '#10b981'
                                                    }}>{amc}</span>
                                                ))}
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        )}

                        {/* ── Institutional Ownership Trajectory Chart (Collapsible/View Option) ── */}
                        {showTrajectoryChart && summary?.monthly_trend && (
                            <InstitutionalTrajectoryChart
                                trendData={summary.monthly_trend}
                                closePrice={summary.close_price || livePrice}
                                companyName={summary.company_name}
                                onClose={() => setShowTrajectoryChart(false)}
                            />
                        )}

                        {/* ── Terminal Controls Panel (Deck 1 & Deck 2) ── */}
                        <div className="shp-terminal-toolbar-card">
                            <div className="shp-toolbar-deck-1">
                                <div className="shp-toolbar-left-group">
                                    <div className="shp-segmented-control">
                                        <button
                                            type="button"
                                            className={`shp-segment-btn ${viewMode === 'scheme' ? 'active' : ''}`}
                                            onClick={() => setViewMode('scheme')}
                                        >
                                            <span>Scheme-wise</span>
                                            <span className="shp-segment-badge">{schemesToRender.length}</span>
                                        </button>
                                        <button
                                            type="button"
                                            className={`shp-segment-btn ${viewMode === 'amc' ? 'active' : ''}`}
                                            onClick={() => setViewMode('amc')}
                                        >
                                            <span>AMC-wise</span>
                                            <span className="shp-segment-badge">{amcToRender.length}</span>
                                        </button>
                                        <button
                                            type="button"
                                            className={`shp-segment-btn ${viewMode === 'category' ? 'active' : ''}`}
                                            onClick={() => setViewMode('category')}
                                        >
                                            <span>Category-wise</span>
                                        </button>
                                    </div>
                                    <div className="shp-flow-filter-group">
                                        <button
                                            type="button"
                                            className={`shp-flow-btn ${!filterParam ? 'active' : ''}`}
                                            onClick={() => { searchParams.delete('filter'); setSearchParams(searchParams); }}
                                        >
                                            All
                                        </button>
                                        <button
                                            type="button"
                                            className={`shp-flow-btn entrants ${filterParam === 'entrants' ? 'active' : ''}`}
                                            onClick={() => { searchParams.set('filter', 'entrants'); setSearchParams(searchParams); }}
                                            title="New entrants that added this stock"
                                        >
                                            ✦ Entrants
                                        </button>
                                        <button
                                            type="button"
                                            className={`shp-flow-btn exits ${filterParam === 'exits' ? 'active' : ''}`}
                                            onClick={() => { searchParams.set('filter', 'exits'); setSearchParams(searchParams); }}
                                            title="Exited funds that sold out"
                                        >
                                            ✕ Exits
                                        </button>
                                    </div>
                                </div>
                                <div className="shp-toolbar-right-group">
                                    <div className="shp-fund-type-pills">
                                        <button
                                            type="button"
                                            className={`shp-type-pill ${schemeTypeFilter === 'all' ? 'active' : ''}`}
                                            onClick={() => setSchemeTypeFilter('all')}
                                            title="Show all funds (Pure Equity + Arbitrage combined)"
                                        >
                                            All Funds <span className="shp-pill-count">{schemeTypeCounts.all}</span>
                                        </button>
                                        <button
                                            type="button"
                                            className={`shp-type-pill pure-eq ${schemeTypeFilter === 'pure_equity' ? 'active' : ''}`}
                                            onClick={() => setSchemeTypeFilter('pure_equity')}
                                            title="Exclude Arbitrage schemes — shows pure fundamental equity conviction"
                                        >
                                            Pure Equity <span className="shp-pill-count">{schemeTypeCounts.pureEquity}</span>
                                        </button>
                                        <button
                                            type="button"
                                            className={`shp-type-pill arb ${schemeTypeFilter === 'arbitrage' ? 'active' : ''}`}
                                            onClick={() => setSchemeTypeFilter('arbitrage')}
                                            title="Arbitrage schemes only — hedged cash-futures trading flow"
                                        >
                                            Arbitrage <span className="shp-pill-count">{schemeTypeCounts.arbitrage}</span>
                                        </button>
                                    </div>
                                    <div className="shp-aum-pill-toggle">
                                        <button
                                            type="button"
                                            className={`shp-aum-btn ${aumViewMode === 'total' ? 'active' : ''}`}
                                            onClick={() => setAumViewMode('total')}
                                        >
                                            Total AUM
                                        </button>
                                        <button
                                            type="button"
                                            className={`shp-aum-btn ${aumViewMode === 'equity' ? 'active' : ''}`}
                                            onClick={() => setAumViewMode('equity')}
                                        >
                                            EQ AUM
                                        </button>
                                    </div>
                                </div>
                            </div>
                            <div className="shp-toolbar-deck-2">
                                <div className="shp-search-container">
                                    <span className="shp-search-icon">🔍</span>
                                    <input
                                        type="text"
                                        className="shp-terminal-search"
                                        placeholder={`Filter ${viewMode === 'scheme' ? schemesToRender.length : amcToRender.length} ${viewMode === 'scheme' ? 'schemes' : 'AMCs'} by name...`}
                                        value={filterText}
                                        onChange={(e) => setFilterText(e.target.value)}
                                    />
                                    {filterText && (
                                        <button type="button" className="shp-clear-btn" onClick={() => setFilterText('')}>✕</button>
                                    )}
                                </div>
                                <div className="shp-toolbar-actions-group">
                                    <div className="shp-picker-wrapper">
                                        <span className="shp-picker-icon">📅</span>
                                        <select
                                            className="shp-terminal-select"
                                            value={selectedMonth || ''}
                                            onChange={(e) => handleMonthChange(e.target.value)}
                                        >
                                            <option value="">Latest Period</option>
                                            {allAvailableMonths.map(m => <option key={m} value={m}>{m}</option>)}
                                        </select>
                                    </div>
                                    <ExportButton
                                        getData={() => {
                                            if (viewMode === 'scheme') {
                                                // Scheme-wise: mirrors UI table exactly
                                                return schemesToRender.map(item => {
                                                    const row = {
                                                        scheme_name: item.scheme_name,
                                                        amc_name: item.amc_name,
                                                        plan_type: item.plan_type ?? '',
                                                        option_type: item.option_type ?? '',
                                                        // AUM column respects the Total/Equity toggle
                                                        aum_cr: aumViewMode === 'equity'
                                                            ? (item.equity_aum_cr ?? null)
                                                            : (item.total_aum_cr ?? null),
                                                    };
                                                    displayMonths.forEach((m, idx) => {
                                                        const h = (item.history || []).find(x => x.month === m);
                                                        // % AUM only for the first (latest) month, matching the UI
                                                        if (idx === 0) row[`pct_aum_${m}`] = h?.percent_to_aum ?? null;
                                                        row[`own_pct_${m}`] = h?.ownership_percent ?? null;
                                                        row[`qty_${m}`] = h?.num_shares ?? null;
                                                        row[`change_${m}`] = h?.month_change ?? null;
                                                        // Change % only for latest month
                                                        if (idx === 0) {
                                                            const prev = h?.num_shares != null && h?.month_change != null
                                                                ? (h.num_shares - h.month_change) : null;
                                                            row[`change_pct_${m}`] = prev && prev !== 0
                                                                ? (h.month_change / prev) * 100 : null;
                                                        }
                                                    });
                                                    return row;
                                                });
                                            } else {
                                                // AMC-wise: mirrors UI table exactly
                                                return amcToRender.map(item => {
                                                    const row = {
                                                        amc_name: item.amc_name,
                                                        scheme_count: item.scheme_count,
                                                        aum_cr: aumViewMode === 'equity'
                                                            ? (item.equity_aum_cr ?? null)
                                                            : (item.total_aum_cr ?? null),
                                                    };
                                                    displayMonths.forEach((m, idx) => {
                                                        const h = (item.history || []).find(x => x.month === m);
                                                        row[`own_pct_${m}`] = h?.ownership_percent ?? null;
                                                        row[`qty_${m}`] = h?.num_shares ?? null;
                                                        row[`change_${m}`] = h?.month_change ?? null;
                                                    });
                                                    return row;
                                                });
                                            }
                                        }}
                                        columns={viewMode === 'scheme'
                                            ? [
                                                { key: 'scheme_name', label: 'Scheme Name', exportFormat: 'string' },
                                                { key: 'amc_name', label: 'AMC', exportFormat: 'string' },
                                                { key: 'plan_type', label: 'Plan', exportFormat: 'string' },
                                                { key: 'option_type', label: 'Option', exportFormat: 'string' },
                                                { key: 'aum_cr', label: aumViewMode === 'equity' ? 'Equity AUM (Cr)' : 'Total AUM (Cr)', exportFormat: 'numeric' },
                                                // Latest month has % AUM + Own% + Qty + Change + Change%
                                                ...(displayMonths[0] ? [
                                                    { key: `pct_aum_${displayMonths[0]}`, label: `% AUM (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                    { key: `own_pct_${displayMonths[0]}`, label: `Ownership % (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                    { key: `qty_${displayMonths[0]}`, label: `Qty / Shares (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                    { key: `change_${displayMonths[0]}`, label: `Change in Qty (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                    { key: `change_pct_${displayMonths[0]}`, label: `Change % (${displayMonths[0]})`, exportFormat: 'numeric' },
                                                ] : []),
                                                // Older months: Own%, Qty, Change only (matches UI)
                                                ...displayMonths.slice(1).flatMap(m => [
                                                    { key: `own_pct_${m}`, label: `Ownership % (${m})`, exportFormat: 'numeric' },
                                                    { key: `qty_${m}`, label: `Qty / Shares (${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Change in Qty (${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]
                                            : [
                                                { key: 'amc_name', label: 'AMC Name', exportFormat: 'string' },
                                                { key: 'scheme_count', label: 'Schemes', exportFormat: 'numeric' },
                                                { key: 'aum_cr', label: aumViewMode === 'equity' ? 'Equity AUM (Cr)' : 'Total AUM (Cr)', exportFormat: 'numeric' },
                                                ...displayMonths.flatMap(m => [
                                                    { key: `own_pct_${m}`, label: `Ownership % (${m})`, exportFormat: 'numeric' },
                                                    { key: `qty_${m}`, label: `Qty / Shares (${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Change in Qty (${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]
                                        }
                                        pdfColumns={viewMode === 'scheme'
                                            ? [
                                                { key: 'scheme_name', label: 'Scheme Name', exportFormat: 'string' },
                                                { key: 'amc_name', label: 'AMC', exportFormat: 'string' },
                                                { key: 'plan_type', label: 'Plan', exportFormat: 'string' },
                                                { key: 'aum_cr', label: aumViewMode === 'equity' ? 'Eq.AUM(Cr)' : 'Tot.AUM(Cr)', exportFormat: 'numeric' },
                                                ...displayMonths.flatMap((m, idx) => [
                                                    ...(idx === 0 ? [{ key: `pct_aum_${m}`, label: `%AUM(${m})`, exportFormat: 'numeric' }] : []),
                                                    { key: `own_pct_${m}`, label: `Own%(${m})`, exportFormat: 'numeric' },
                                                    { key: `qty_${m}`, label: `Qty(${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Chg(${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]
                                            : [
                                                { key: 'amc_name', label: 'AMC', exportFormat: 'string' },
                                                { key: 'scheme_count', label: 'Schemes', exportFormat: 'numeric' },
                                                { key: 'aum_cr', label: aumViewMode === 'equity' ? 'Eq.AUM(Cr)' : 'Tot.AUM(Cr)', exportFormat: 'numeric' },
                                                ...displayMonths.flatMap(m => [
                                                    { key: `own_pct_${m}`, label: `Own%(${m})`, exportFormat: 'numeric' },
                                                    { key: `qty_${m}`, label: `Qty(${m})`, exportFormat: 'numeric' },
                                                    { key: `change_${m}`, label: `Chg(${m})`, exportFormat: 'numeric' },
                                                ]),
                                            ]
                                        }
                                        fileNameConfig={{
                                            page: 'stock-holdings',
                                            filters: {
                                                stock: summary?.company_name,
                                                period: displayMonths[0],
                                                view: viewMode,
                                            },
                                        }}
                                        metadata={{
                                            title: `Stock Holdings \u2014 ${summary?.company_name || ''}`,
                                            filters: {
                                                'Stock': summary?.company_name,
                                                'ISIN': summary?.isin,
                                                'Sector': summary?.sector,
                                                'LTP (\u20b9)': livePrice ? `\u20b9${livePrice}` : undefined,
                                                'Market Cap': summary?.market_cap ? `\u20b9${Number(summary.market_cap).toLocaleString('en-IN', { maximumFractionDigits: 2 })} Cr (${summary.mcap_type || ''})` : undefined,
                                                'Total Shares (Company)': summary?.shares_outstanding ? Number(summary.shares_outstanding).toLocaleString('en-IN') : undefined,
                                                'MF Ownership %': summary?.ownership_percent ? `${summary.ownership_percent.toFixed(2)}%` : undefined,
                                                'Total MF Funds': summary?.total_funds,
                                                'Total MF Shares': summary?.total_shares ? Number(summary.total_shares).toLocaleString('en-IN') : undefined,
                                                'Displayed Periods': displayMonths.join(', '),
                                                'View': viewMode === 'scheme' ? 'Scheme-wise' : 'AMC-wise',
                                                'Scheme Type': schemeTypeFilter === 'pure_equity' ? 'Pure Equity (Excl. Arbitrage)' : schemeTypeFilter === 'arbitrage' ? 'Arbitrage Only' : 'All Funds',
                                                'AUM Mode': aumViewMode === 'total' ? 'Total AUM' : 'Equity AUM',
                                                'Fund Filter': filterParam === 'entrants' ? 'New Entrants Only' : filterParam === 'exits' ? 'Exits Only' : undefined,
                                                'Search': filterText || undefined,
                                            },
                                        }}
                                    />
                                    <button
                                        type="button"
                                        className={`shp-terminal-chart-btn ${showTrajectoryChart ? 'active' : ''}`}
                                        onClick={() => setShowTrajectoryChart(v => !v)}
                                        title="Toggle Institutional Accumulation vs Price Trajectory Chart"
                                    >
                                        📈 {showTrajectoryChart ? 'Hide Chart' : 'Trajectory Chart'}
                                    </button>
                                </div>
                            </div>
                        </div>

                        {/* ── Active Filter Notification Banner ── */}
                        {schemeTypeFilter !== 'all' && (
                            <div className="shp-filter-active-banner">
                                <span>
                                    {schemeTypeFilter === 'pure_equity' ? (
                                        <>
                                            <strong style={{ color: '#10b981' }}>Pure Equity View:</strong> Arbitrage funds are excluded to show genuine institutional fundamental conviction.
                                        </>
                                    ) : (
                                        <>
                                            <strong style={{ color: '#f59e0b' }}>Arbitrage Only View:</strong> Showing hedged cash-futures arbitrage desk positions and spread rollovers only.
                                        </>
                                    )}
                                </span>
                                <button type="button" onClick={() => setSchemeTypeFilter('all')} className="shp-filter-clear-btn">
                                    Reset to All Funds
                                </button>
                            </div>
                        )}

                        {viewMode === 'category' && (
                            <>
                                {/* Partial Data Warning for Category View */}
                                {summary.data_warning && (
                                    <div style={{ 
                                        marginBottom: '12px', padding: '10px 14px', 
                                        backgroundColor: 'rgba(234, 179, 8, 0.1)', 
                                        border: '1px solid rgba(234, 179, 8, 0.3)', 
                                        borderRadius: '8px', color: '#d97706', 
                                        fontSize: '12px', display: 'flex', alignItems: 'center', gap: '8px' 
                                    }}>
                                        <span style={{ fontSize: '16px' }}>⚠️</span>
                                        <span>
                                            <strong>Incomplete Data for {summary.data_warning.latest_label}:</strong> The totals below only include data from {summary.data_warning.amcs_uploaded} out of {summary.data_warning.amcs_expected} AMCs.
                                        </span>
                                    </div>
                                )}

                                {/* Summary cards strip (Broad Categories) */}
                                <div className="shp-cat-summary-strip">
                                    {broadGroups.map(g => {
                                        const cls = catClass(g.category);
                                        return (
                                            <div key={g.category} className={`shp-cat-summary-card theme-${cls}`}>
                                                <div className="shp-cat-card-label">{g.category}</div>
                                                <div className="shp-cat-card-value">
                                                    {fmt(g.totalShares)}
                                                    {summary.data_warning && <span style={{ color: '#eab308', marginLeft: '3px' }} title="Partial Data">*</span>}
                                                </div>
                                                <div className="shp-cat-card-sub">{g.schemeCount} scheme{g.schemeCount !== 1 ? 's' : ''}</div>
                                            </div>
                                        );
                                    })}
                                </div>

                                {/* Expand / Collapse all controls */}
                                <div style={{ display:'flex', gap:'8px', marginBottom:'10px', alignItems:'center' }}>
                                    <span style={{ fontSize:'11px', color:'var(--text-secondary)', fontWeight:600 }}>Groups:</span>
                                    <button onClick={expandAll}  style={{ fontSize:'11px', padding:'3px 10px', borderRadius:'5px', border:'1px solid var(--border-color)', background:'var(--bg-tertiary)', color:'var(--text-secondary)', cursor:'pointer' }}>Expand All</button>
                                    <button onClick={collapseAll} style={{ fontSize:'11px', padding:'3px 10px', borderRadius:'5px', border:'1px solid var(--border-color)', background:'var(--bg-tertiary)', color:'var(--text-secondary)', cursor:'pointer' }}>Collapse All</button>
                                </div>
                            </>
                        )}

                        <div className="shp-table-wrap" ref={tableWrapRef}>
                            {viewMode === 'category' ? (
                                /* ── CATEGORY-WISE VIEW ── */
                                <table className="shp-table">
                                            <thead>
                                                <tr className="shp-thead-row-months">
                                                    <th className="shp-th shp-th-asset-hdr" style={{ textAlign:'left', minWidth:260 }}>Category / Scheme</th>
                                                    {displayMonths.map((m, i) => (
                                                        <th key={m} className={`shp-th shp-month-group shp-month-${i}`} colSpan={i === 0 ? 3 : 2}>
                                                            <span className="shp-month-title">{m}{i === 0 ? ' (LATEST)' : ''}</span>
                                                        </th>
                                                    ))}
                                                </tr>
                                                <tr className="shp-thead-row-subs">
                                                    <th className="shp-th shp-th-sub shp-th-asset-sub" style={{ textAlign:'left' }}>
                                                        <span>Classification</span>
                                                    </th>
                                                    {displayMonths.map((m, i) => (
                                                        <React.Fragment key={m}>
                                                            {i === 0 && <th className={`shp-th shp-th-sub shp-month-${i}`}>% AUM</th>}
                                                            <th className={`shp-th shp-th-sub shp-month-${i}`}>QTY</th>
                                                            <th className={`shp-th shp-th-sub shp-th-change-hdr shp-month-${i}`}>CHANGE</th>
                                                        </React.Fragment>
                                                    ))}
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {subGroups.map(g => {
                                                    const key = `${g.category}||${g.sub_category}`;
                                                    const isOpen = !!expandedCats[key];
                                                    const cls = catClass(g.category);
                                                    return (
                                                        <React.Fragment key={key}>
                                                            {/* Category header row */}
                                                            <tr className="shp-cat-header-row" onClick={() => toggleCat(key)}>
                                                                <td className="shp-td shp-td-name" colSpan={1 + displayMonths.reduce((acc, _, i) => acc + (i === 0 ? 3 : 2), 0)}>
                                                                    <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', width:'100%' }}>
                                                                        <div style={{ display:'flex', alignItems:'center', gap:'8px' }}>
                                                                            <span className={`shp-chevron ${isOpen ? 'open' : ''}`}>›</span>
                                                                            <span className={`shp-cat-badge ${cls}`}>{g.category}</span>
                                                                            <span className="shp-subcat-chip">{g.sub_category}</span>
                                                                        </div>
                                                                        <div style={{ display:'flex', alignItems:'center', gap:'16px', marginRight:'8px' }}>
                                                                            <span className="shp-cat-schemes-count">{g.schemes.length} scheme{g.schemes.length !== 1 ? 's' : ''}</span>
                                                                            <div style={{ textAlign:'right' }}>
                                                                                <div style={{ fontSize:'13px', fontWeight:700, fontVariantNumeric:'tabular-nums' }}>{fmt(g.totalShares)}</div>
                                                                                <div style={{ fontSize:'10px', color:'var(--text-secondary)', fontWeight:600 }}>TOTAL SHARES</div>
                                                                            </div>
                                                                        </div>
                                                                    </div>
                                                                </td>
                                                            </tr>

                                                            {/* Scheme drill-down rows */}
                                                            {isOpen && g.schemes.map((scheme, si) => (
                                                                <tr key={si} className="shp-cat-scheme-row">
                                                                    <td className="shp-td shp-td-name">
                                                                        <div className="shp-fund-name">{scheme.scheme_name}</div>
                                                                        <div className="shp-fund-sub">{scheme.amc_name}</div>
                                                                    </td>
                                                                    {displayMonths.map((m, i) => {
                                                                        const h = (scheme.history || []).find(x => x.month === m);
                                                                        return (
                                                                            <React.Fragment key={m}>
                                                                                {i === 0 && <td className={`shp-td shp-num shp-month-${i}`}>{fmtPct(h?.percent_to_aum) || <MissingData />}</td>}
                                                                                <td className={`shp-td shp-num shp-month-${i}`}>
                                                                                    <div className="shp-qty-cell-content">
                                                                                        <span>{fmt(h?.num_shares) || <MissingData />}</span>
                                                                                        {i === 0 && <MiniSparkline history={scheme.history} />}
                                                                                    </div>
                                                                                </td>
                                                                                <td className={`shp-td shp-num shp-month-${i}`}><ChangeCell change={h?.month_change} pct={h?.percent_change} /></td>
                                                                            </React.Fragment>
                                                                        );
                                                                    })}
                                                                </tr>
                                                            ))}
                                                        </React.Fragment>
                                                    );
                                                })}
                                            </tbody>
                                        </table>
                                ) : (
                                /* ── SCHEME / AMC TABLE ── */
                                <table className="shp-table">
                                    <thead>
                                        <tr className="shp-thead-row-months">
                                            <th className="shp-th shp-th-asset-hdr sortable" onClick={() => handleSort(viewMode === 'scheme' ? 'scheme_name' : 'amc_name')}>
                                                <div className="shp-th-sort-wrap">
                                                    <span>{viewMode === 'scheme' ? 'Mutual Fund Scheme' : 'Asset Management Company (AMC)'}</span>
                                                    {sortArrow(viewMode === 'scheme' ? 'scheme_name' : 'amc_name')}
                                                </div>
                                            </th>
                                            {displayMonths.map((m, i) => (
                                                <th key={m} className={`shp-th shp-month-group shp-month-${i}`} colSpan={i === 0 ? 4 : 3}>
                                                    <span className="shp-month-title">{m}{i === 0 ? ' (LATEST)' : ''}</span>
                                                </th>
                                            ))}
                                        </tr>
                                        <tr className="shp-thead-row-subs">
                                            <th className="shp-th shp-th-sub shp-th-asset-sub">
                                                <span>Portfolio Allocation</span>
                                            </th>
                                            {displayMonths.map((m, i) => (
                                                <React.Fragment key={m}>
                                                    {i === 0 && (
                                                        <th className="shp-th shp-th-sub shp-month-0 sortable" onClick={() => handleSort('pnav')}>
                                                            <span>% AUM</span>
                                                            {sortArrow('pnav')}
                                                        </th>
                                                    )}
                                                    <th className={`shp-th shp-th-sub shp-month-${i} sortable`} onClick={() => handleSort(`own_${i}`)}>
                                                        <span>OWN %</span>
                                                        {sortArrow(`own_${i}`)}
                                                    </th>
                                                    <th className={`shp-th shp-th-sub shp-month-${i} sortable`} onClick={() => handleSort(`shares_${i}`)}>
                                                        <span>QTY</span>
                                                        {sortArrow(`shares_${i}`)}
                                                    </th>
                                                    <th className={`shp-th shp-th-sub shp-th-change-hdr shp-month-${i} sortable`} onClick={() => handleSort(`change_${i}`)}>
                                                        <span>CHANGE{schemeTypeFilter === 'pure_equity' ? ' (EQ)' : schemeTypeFilter === 'arbitrage' ? ' (ARB)' : ''}</span>
                                                        {sortArrow(`change_${i}`)}
                                                    </th>
                                                </React.Fragment>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {(viewMode === 'scheme' ? schemesToRender : amcToRender).map((item, idx) => {
                                            const brand = viewMode === 'amc' ? getAmcBrand(item.amc_name) : null;
                                            return (
                                                <tr key={idx} className="shp-row">
                                                    <td className="shp-td shp-td-name">
                                                        <div className="shp-fund-name">
                                                            {viewMode === 'amc' && brand && (
                                                                <span
                                                                    className="shp-amc-brand-badge"
                                                                    style={{
                                                                        backgroundColor: brand.bg,
                                                                        color: brand.color
                                                                    }}
                                                                    title={item.amc_name}
                                                                >
                                                                    {brand.label}
                                                                </span>
                                                            )}
                                                            <span>{viewMode === 'scheme' ? item.scheme_name : item.amc_name}</span>
                                                            {viewMode === 'amc' && schemeTypeFilter === 'all' && item.has_arbitrage && (
                                                                <span className="shp-arb-tag" title="This AMC holds this stock across both Active Equity and Arbitrage schemes">
                                                                    ⚡ Arb Incl.
                                                                </span>
                                                            )}
                                                            {viewMode === 'amc' && schemeTypeFilter === 'arbitrage' && (
                                                                <span className="shp-arb-tag" title="Showing Arbitrage scheme holdings only">
                                                                    Arbitrage
                                                                </span>
                                                            )}
                                                            {viewMode === 'amc' && schemeTypeFilter === 'pure_equity' && (
                                                                <span className="shp-equity-tag" title="Arbitrage excluded — pure fundamental equity conviction">
                                                                    Pure Eq
                                                                </span>
                                                            )}
                                                        </div>
                                                        <div className="shp-fund-sub">
                                                            {viewMode === 'scheme'
                                                                ? item.amc_name
                                                                : (
                                                                    schemeTypeFilter === 'all' && item.has_arbitrage
                                                                        ? `${item.scheme_count} schemes (${item.equity_schemes_all} Equity, ${item.arb_schemes_all} Arb) | ${aumViewMode === 'total' ? 'Total' : 'EQ'} AUM: ₹${fmtCr(aumViewMode === 'total' ? item.total_aum_cr : item.equity_aum_cr)} Cr`
                                                                        : `${item.scheme_count} scheme${item.scheme_count !== 1 ? 's' : ''} | ${aumViewMode === 'total' ? 'Total' : 'EQ'} AUM: ₹${fmtCr(aumViewMode === 'total' ? item.total_aum_cr : item.equity_aum_cr)} Cr`
                                                                )
                                                            }
                                                        </div>
                                                    </td>
                                                    {displayMonths.map((m, i) => {
                                                        const history = (item.history || []).find(h => h.month === m);
                                                        return (
                                                            <React.Fragment key={m}>
                                                                {i === 0 && <td className="shp-td shp-num shp-month-0">{fmtPct(history?.percent_to_aum) || <MissingData />}</td>}
                                                                <td className={`shp-td shp-num shp-month-${i} shp-own-cell`}>
                                                                    {fmtPct(history?.ownership_percent) || <MissingData />}
                                                                </td>
                                                                <td className={`shp-td shp-num shp-month-${i}`}>
                                                                    <div className="shp-qty-cell-content">
                                                                        <span>{fmt(history?.num_shares) || <MissingData />}</span>
                                                                        {i === 0 && <MiniSparkline history={item.history} />}
                                                                    </div>
                                                                </td>
                                                                <td className={`shp-td shp-num shp-month-${i}`}><ChangeCell change={history?.month_change} pct={history?.percent_change} /></td>
                                                            </React.Fragment>
                                                        );
                                                    })}
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                                )}
                            </div>
                    </>
                )}
            </div>
        </div >
    );
}
