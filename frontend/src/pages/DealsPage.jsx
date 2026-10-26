import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { Link } from 'react-router-dom';
import {
    Activity, ArrowDownRight, ArrowUpRight, CheckCircle2, ChevronLeft,
    ChevronRight, Download, Filter, HelpCircle, Layers, RefreshCw,
    Search, ShieldAlert, SlidersHorizontal, UploadCloud, X, AlertCircle
} from 'lucide-react';
import {
    fetchDeals, fetchDealsSummary, fetchDealsStatus,
    triggerDealsSync, uploadDealsCsv
} from '../api/deals';
import { useAuth } from '../contexts/AuthContext';
import './DealsPage.css';

export default function DealsPage() {
    const { user, hasPermission } = useAuth();
    const isAdmin = hasPermission('admin') || user?.role === 'admin';

    // Filter states
    const [dealType, setDealType] = useState('ALL'); // ALL, BULK, BLOCK
    const [exchange, setExchange] = useState('ALL'); // ALL, NSE, BSE
    const [action, setAction] = useState('ALL');     // ALL, BUY, SELL
    const [category, setCategory] = useState('ALL'); // ALL, MUTUAL_FUND, FII_FPI, DII, PROMOTER, AIF, OTHER
    const [search, setSearch] = useState('');
    const [fromDate, setFromDate] = useState('');
    const [toDate, setToDate] = useState('');
    const [sortBy, setSortBy] = useState('trade_date');
    const [sortOrder, setSortOrder] = useState('desc');
    const [page, setPage] = useState(1);
    const limit = 50;

    // Data states
    const [deals, setDeals] = useState([]);
    const [totalDeals, setTotalDeals] = useState(0);
    const [totalPages, setTotalPages] = useState(1);
    const [summary, setSummary] = useState(null);
    const [statusData, setStatusData] = useState(null);

    // UI Loading & Modal states
    const [loading, setLoading] = useState(true);
    const [summaryLoading, setSummaryLoading] = useState(true);
    const [syncing, setSyncing] = useState(false);
    const [error, setError] = useState(null);
    const [showStatusModal, setShowStatusModal] = useState(false);
    const [showUploadModal, setShowUploadModal] = useState(false);

    // CSV Upload state
    const [csvInput, setCsvInput] = useState('');
    const [csvFilename, setCsvFilename] = useState('manual_deals.csv');
    const [csvDryRun, setCsvDryRun] = useState(true);
    const [uploading, setUploading] = useState(false);
    const [uploadResult, setUploadResult] = useState(null);

    // Load Deals
    const loadDeals = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const params = {
                page,
                limit,
                sort_by: sortBy,
                sort_order: sortOrder
            };
            if (dealType !== 'ALL') params.deal_type = dealType;
            if (exchange !== 'ALL') params.exchange = exchange;
            if (action !== 'ALL') params.action = action;
            if (category !== 'ALL') params.investor_category = category;
            if (search.trim()) params.search = search.trim();
            if (fromDate) params.from_date = fromDate;
            if (toDate) params.to_date = toDate;

            const res = await fetchDeals(params);
            setDeals(res.deals || []);
            setTotalDeals(res.total || 0);
            setTotalPages(res.total_pages || 1);
        } catch (err) {
            console.error('Failed to load deals:', err);
            setError(err.message || 'Failed to fetch deals');
        } finally {
            setLoading(false);
        }
    }, [page, limit, dealType, exchange, action, category, search, fromDate, toDate, sortBy, sortOrder]);

    // Load Summary Stats
    const loadSummary = useCallback(async () => {
        setSummaryLoading(true);
        try {
            const params = {};
            if (dealType !== 'ALL') params.deal_type = dealType;
            if (fromDate) params.from_date = fromDate;
            if (toDate) params.to_date = toDate;

            const res = await fetchDealsSummary(params);
            setSummary(res);
        } catch (err) {
            console.warn('Failed to load deals summary:', err);
        } finally {
            setSummaryLoading(false);
        }
    }, [dealType, fromDate, toDate]);

    // Load Status
    const loadStatus = useCallback(async () => {
        try {
            const res = await fetchDealsStatus();
            setStatusData(res);
        } catch (err) {
            console.warn('Failed to load deals status:', err);
        }
    }, []);

    // Initial load & trigger on filter change
    useEffect(() => {
        loadDeals();
    }, [loadDeals]);

    useEffect(() => {
        loadSummary();
    }, [loadSummary]);

    useEffect(() => {
        loadStatus();
    }, [loadStatus]);

    // Reset pagination on filter change
    const handleDealTypeChange = (type) => {
        setDealType(type);
        setPage(1);
    };

    const handleSort = (field) => {
        if (sortBy === field) {
            setSortOrder(prev => prev === 'desc' ? 'asc' : 'desc');
        } else {
            setSortBy(field);
            setSortOrder('desc');
        }
        setPage(1);
    };

    // Trigger background sync
    const handleSync = async () => {
        if (syncing) return;
        setSyncing(true);
        try {
            await triggerDealsSync(5, dealType);
            await Promise.all([loadDeals(), loadSummary(), loadStatus()]);
        } catch (err) {
            alert(`Sync failed: ${err.message}`);
        } finally {
            setSyncing(false);
        }
    };

    // Handle CSV Submit
    const handleCsvSubmit = async () => {
        if (!csvInput.trim()) return;
        setUploading(true);
        setUploadResult(null);
        try {
            const res = await uploadDealsCsv({
                csv_content: csvInput,
                filename: csvFilename,
                deal_type: dealType !== 'ALL' ? dealType : null,
                dry_run: csvDryRun
            });
            setUploadResult(res);
            if (!csvDryRun && res.inserted > 0) {
                await Promise.all([loadDeals(), loadSummary(), loadStatus()]);
            }
        } catch (err) {
            setUploadResult({ error: err.message });
        } finally {
            setUploading(false);
        }
    };

    // Category badge helpers
    const getCategoryBadge = (cat) => {
        switch (cat) {
            case 'MUTUAL_FUND':
                return <span className="badge-cat cat-mf">Mutual Fund</span>;
            case 'FII_FPI':
                return <span className="badge-cat cat-fii">FII / FPI</span>;
            case 'DII':
                return <span className="badge-cat cat-dii">DII / Insurer</span>;
            case 'PROMOTER':
                return <span className="badge-cat cat-promoter">Promoter</span>;
            case 'AIF':
                return <span className="badge-cat cat-aif">AIF</span>;
            default:
                return <span className="badge-cat cat-other">Other / HNI</span>;
        }
    };

    const formatCurrency = (val) => {
        if (!val && val !== 0) return '—';
        if (val >= 10000000) {
            return `₹${(val / 10000000).toFixed(2)} Cr`;
        } else if (val >= 100000) {
            return `₹${(val / 100000).toFixed(2)} L`;
        }
        return `₹${val.toLocaleString('en-IN')}`;
    };

    return (
        <div className="deals-page">
            <div className="container">
                {/* ── Top Header Toolbar ── */}
                <header className="deals-header">
                    <div>
                        <div className="deals-title-group">
                            <h1 className="deals-title">Bulk & Block Deals</h1>
                            <div className="deals-live-pill">
                                <span className="deals-live-dot"></span>
                                Live Disclosures
                            </div>
                        </div>
                        <p className="deals-subtitle">
                            Tracking institutional large-volume transactions and block window trades across NSE & BSE with investor intelligence and mutual fund cross-referencing.
                        </p>
                    </div>

                    <div className="deals-header-actions">
                        <button
                            className="deals-btn deals-btn-outline"
                            onClick={() => setShowStatusModal(true)}
                            title="View Data Health & Ingestion Status"
                        >
                            <Activity size={15} />
                            <span>System Status</span>
                        </button>

                        <button
                            className="deals-btn deals-btn-outline"
                            onClick={() => setShowUploadModal(true)}
                            title="Manual CSV Import Fallback"
                        >
                            <UploadCloud size={15} />
                            <span>Import CSV</span>
                        </button>

                        {isAdmin && (
                            <button
                                className="deals-btn deals-btn-primary"
                                onClick={handleSync}
                                disabled={syncing}
                                title="Run Immediate Sync Cycle"
                            >
                                <RefreshCw size={15} className={syncing ? 'deals-spin' : ''} />
                                <span>{syncing ? 'Syncing...' : 'Sync Now'}</span>
                            </button>
                        )}
                    </div>
                </header>

                {/* ── Summary Stats Cards ── */}
                <section className="deals-summary-grid">
                    <div className="deals-card">
                        <div className="deals-card-header">
                            <span className="deals-card-label">Total Deals</span>
                            <div className="deals-card-icon"><Layers size={18} /></div>
                        </div>
                        <div className="deals-card-value">
                            {summaryLoading ? '...' : (summary?.total_deals?.toLocaleString('en-IN') || 0)}
                        </div>
                        <div className="deals-card-sub">
                            Across {summary?.unique_symbols || 0} unique securities
                        </div>
                    </div>

                    <div className="deals-card">
                        <div className="deals-card-header">
                            <span className="deals-card-label">Reported Turnover</span>
                            <div className="deals-card-icon"><Activity size={18} /></div>
                        </div>
                        <div className="deals-card-value">
                            {summaryLoading ? '...' : `₹${summary?.total_value_crores || 0} Cr`}
                        </div>
                        <div className="deals-card-sub">
                            Total transaction gross value
                        </div>
                    </div>

                    <div className="deals-card">
                        <div className="deals-card-header">
                            <span className="deals-card-label">Buy Action</span>
                            <div className="deals-card-icon" style={{ background: 'rgba(16, 185, 129, 0.1)', color: '#10b981' }}>
                                <ArrowUpRight size={18} />
                            </div>
                        </div>
                        <div className="deals-card-value deals-val-green">
                            {summaryLoading ? '...' : `₹${summary?.buy_value_crores || 0} Cr`}
                        </div>
                        <div className="deals-card-sub">
                            {summary?.buy_count || 0} buyer filings
                        </div>
                    </div>

                    <div className="deals-card">
                        <div className="deals-card-header">
                            <span className="deals-card-label">Sell Action</span>
                            <div className="deals-card-icon" style={{ background: 'rgba(239, 68, 68, 0.1)', color: '#ef4444' }}>
                                <ArrowDownRight size={18} />
                            </div>
                        </div>
                        <div className="deals-card-value deals-val-red">
                            {summaryLoading ? '...' : `₹${summary?.sell_value_crores || 0} Cr`}
                        </div>
                        <div className="deals-card-sub">
                            {summary?.sell_count || 0} seller filings
                        </div>
                    </div>
                </section>

                {/* ── Filter Bar ── */}
                <section className="deals-filter-panel">
                    <div className="deals-tabs-row">
                        {/* Tab Switcher */}
                        <div className="deals-dealtype-tabs">
                            <button
                                className={`deals-tab-btn ${dealType === 'ALL' ? 'active' : ''}`}
                                onClick={() => handleDealTypeChange('ALL')}
                            >
                                All Deals
                            </button>
                            <button
                                className={`deals-tab-btn ${dealType === 'BULK' ? 'active' : ''}`}
                                onClick={() => handleDealTypeChange('BULK')}
                            >
                                Bulk Deals (&gt;0.5%)
                            </button>
                            <button
                                className={`deals-tab-btn ${dealType === 'BLOCK' ? 'active' : ''}`}
                                onClick={() => handleDealTypeChange('BLOCK')}
                            >
                                Block Window Deals
                            </button>
                        </div>

                        {/* Search Input */}
                        <div className="deals-search-box">
                            <Search size={16} className="deals-search-icon" />
                            <input
                                type="text"
                                className="deals-search-input"
                                placeholder="Search symbol, company, or client..."
                                value={search}
                                onChange={(e) => { setSearch(e.target.value); setPage(1); }}
                            />
                        </div>
                    </div>

                    <div className="deals-filter-controls">
                        {/* Exchange */}
                        <select
                            className="deals-select"
                            value={exchange}
                            onChange={(e) => { setExchange(e.target.value); setPage(1); }}
                        >
                            <option value="ALL">All Exchanges</option>
                            <option value="NSE">NSE Only</option>
                            <option value="BSE">BSE Only</option>
                        </select>

                        {/* Action */}
                        <select
                            className="deals-select"
                            value={action}
                            onChange={(e) => { setAction(e.target.value); setPage(1); }}
                        >
                            <option value="ALL">All Actions (Buy/Sell)</option>
                            <option value="BUY">Buy Only</option>
                            <option value="SELL">Sell Only</option>
                        </select>

                        {/* Category */}
                        <select
                            className="deals-select"
                            value={category}
                            onChange={(e) => { setCategory(e.target.value); setPage(1); }}
                        >
                            <option value="ALL">All Investor Classes</option>
                            <option value="MUTUAL_FUND">Mutual Funds Only</option>
                            <option value="FII_FPI">FII / FPI</option>
                            <option value="DII">DII / Insurance</option>
                            <option value="PROMOTER">Promoter / Insiders</option>
                            <option value="AIF">AIF Funds</option>
                            <option value="OTHER">Other / HNI</option>
                        </select>

                        {/* Date Range */}
                        <div className="deals-date-inputs">
                            <input
                                type="date"
                                className="deals-date-input"
                                value={fromDate}
                                onChange={(e) => { setFromDate(e.target.value); setPage(1); }}
                                title="From Date"
                            />
                            <span style={{ color: 'var(--text-secondary)' }}>to</span>
                            <input
                                type="date"
                                className="deals-date-input"
                                value={toDate}
                                onChange={(e) => { setToDate(e.target.value); setPage(1); }}
                                title="To Date"
                            />
                        </div>

                        {(search || exchange !== 'ALL' || action !== 'ALL' || category !== 'ALL' || fromDate || toDate) && (
                            <button
                                className="deals-btn deals-btn-outline"
                                onClick={() => {
                                    setSearch('');
                                    setExchange('ALL');
                                    setAction('ALL');
                                    setCategory('ALL');
                                    setFromDate('');
                                    setToDate('');
                                    setPage(1);
                                }}
                            >
                                Reset Filters
                            </button>
                        )}
                    </div>
                </section>

                {/* ── Deals Table ── */}
                <section className="deals-table-container">
                    <div className="deals-table-scroll">
                        <table className="deals-table">
                            <thead>
                                <tr>
                                    <th onClick={() => handleSort('trade_date')}>Date {sortBy === 'trade_date' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</th>
                                    <th>Type</th>
                                    <th>Exch</th>
                                    <th onClick={() => handleSort('symbol')}>Security / Company {sortBy === 'symbol' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</th>
                                    <th onClick={() => handleSort('client_name')}>Client / Investor {sortBy === 'client_name' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</th>
                                    <th>Class</th>
                                    <th>Side</th>
                                    <th onClick={() => handleSort('quantity')} style={{ textAlign: 'right' }}>Quantity {sortBy === 'quantity' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</th>
                                    <th onClick={() => handleSort('trade_price')} style={{ textAlign: 'right' }}>Price (₹) {sortBy === 'trade_price' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</th>
                                    <th onClick={() => handleSort('trade_value_inr')} style={{ textAlign: 'right' }}>Total Value {sortBy === 'trade_value_inr' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</th>
                                    <th onClick={() => handleSort('pct_equity_traded')} style={{ textAlign: 'right' }}>% Equity {sortBy === 'pct_equity_traded' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</th>
                                </tr>
                            </thead>
                            <tbody>
                                {loading ? (
                                    <tr>
                                        <td colSpan="11">
                                            <div className="deals-loading">
                                                <div className="deals-spinner"></div>
                                                <p>Loading bulk & block deals...</p>
                                            </div>
                                        </td>
                                    </tr>
                                ) : deals.length === 0 ? (
                                    <tr>
                                        <td colSpan="11">
                                            <div className="deals-empty">
                                                <AlertCircle size={32} style={{ margin: '0 auto 12px', opacity: 0.5 }} />
                                                <p style={{ fontWeight: 600 }}>No deals found matching your selected filters.</p>
                                                <p style={{ fontSize: '0.8rem', marginTop: 4 }}>Try broadening your date range or clearing search criteria.</p>
                                            </div>
                                        </td>
                                    </tr>
                                ) : (
                                    deals.map((deal) => (
                                        <tr key={deal.deal_id}>
                                            <td style={{ whiteSpace: 'nowrap', fontWeight: 600 }}>
                                                {deal.trade_date}
                                            </td>
                                            <td>
                                                <span className={deal.deal_type === 'BULK' ? 'badge-bulk' : 'badge-block'}>
                                                    {deal.deal_type}
                                                </span>
                                            </td>
                                            <td>
                                                <span className="badge-exchange">{deal.exchange}</span>
                                            </td>
                                            <td>
                                                <div className="deals-company-cell">
                                                    <Link to={`/stocks/${deal.symbol}`} className="deals-symbol-link">
                                                        {deal.symbol}
                                                    </Link>
                                                    <span className="deals-sec-name" title={deal.security_name || deal.company_name}>
                                                        {deal.company_name || deal.security_name}
                                                    </span>
                                                </div>
                                            </td>
                                            <td>
                                                <div className="deals-client-cell">
                                                    <span className="deals-client-name">{deal.client_name}</span>
                                                </div>
                                            </td>
                                            <td>
                                                {getCategoryBadge(deal.investor_category)}
                                            </td>
                                            <td>
                                                <span className={deal.action === 'BUY' ? 'badge-buy' : 'badge-sell'}>
                                                    {deal.action === 'BUY' ? <ArrowUpRight size={13} /> : <ArrowDownRight size={13} />}
                                                    {deal.action}
                                                </span>
                                            </td>
                                            <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                                                {deal.quantity.toLocaleString('en-IN')}
                                            </td>
                                            <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                                                ₹{deal.trade_price.toFixed(2)}
                                            </td>
                                            <td style={{ textAlign: 'right', fontWeight: 700, fontVariantNumeric: 'tabular-nums' }}>
                                                {formatCurrency(deal.trade_value_inr)}
                                            </td>
                                            <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: 'var(--text-secondary)' }}>
                                                {deal.pct_equity_traded ? `${deal.pct_equity_traded.toFixed(2)}%` : '—'}
                                            </td>
                                        </tr>
                                    ))
                                )}
                            </tbody>
                        </table>
                    </div>

                    {/* Pagination */}
                    <div className="deals-pagination">
                        <div>
                            Showing {deals.length > 0 ? (page - 1) * limit + 1 : 0} to {Math.min(page * limit, totalDeals)} of {totalDeals} deals
                        </div>
                        <div className="deals-page-controls">
                            <button
                                className="deals-page-btn"
                                onClick={() => setPage(p => Math.max(p - 1, 1))}
                                disabled={page <= 1}
                            >
                                <ChevronLeft size={16} />
                            </button>
                            <span>Page {page} of {totalPages}</span>
                            <button
                                className="deals-page-btn"
                                onClick={() => setPage(p => Math.min(p + 1, totalPages))}
                                disabled={page >= totalPages}
                            >
                                <ChevronRight size={16} />
                            </button>
                        </div>
                    </div>
                </section>
            </div>

            {/* ── System Status Modal ── */}
            {showStatusModal && (
                <div className="deals-modal-backdrop" onClick={() => setShowStatusModal(false)}>
                    <div className="deals-modal" onClick={e => e.stopPropagation()}>
                        <div className="deals-modal-header">
                            <h3 className="deals-modal-title">Operational Health & Sync Checkpoints</h3>
                            <button className="deals-modal-close" onClick={() => setShowStatusModal(false)}>
                                <X size={20} />
                            </button>
                        </div>
                        <div className="deals-modal-body">
                            <div style={{ fontSize: '0.88rem', color: 'var(--text-secondary)' }}>
                                Total Deals Persisted in Database: <strong style={{ color: 'var(--text-primary)' }}>{statusData?.total_deals_in_db || 0}</strong>
                            </div>

                            <h4 style={{ margin: '12px 0 6px', fontSize: '0.9rem' }}>Collector Sync Checkpoints</h4>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                {statusData?.sync_states?.map(s => (
                                    <div key={s.source_key} style={{ padding: '10px 14px', background: 'var(--bg-primary)', borderRadius: 8, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                        <div>
                                            <div style={{ fontWeight: 600 }}>{s.source_key}</div>
                                            <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                                                Last Success: {s.last_successful_at ? new Date(s.last_successful_at).toLocaleString('en-IN') : 'Never'}
                                            </div>
                                        </div>
                                        <div style={{ textAlign: 'right' }}>
                                            <span style={{ fontSize: '0.75rem', padding: '2px 8px', borderRadius: 4, background: s.status === 'OK' ? 'rgba(16, 185, 129, 0.2)' : 'rgba(239, 68, 68, 0.2)', color: s.status === 'OK' ? '#10b981' : '#ef4444', fontWeight: 700 }}>
                                                {s.status}
                                            </span>
                                        </div>
                                    </div>
                                ))}
                            </div>

                            <h4 style={{ margin: '16px 0 6px', fontSize: '0.9rem' }}>Recent Ingestion Runs</h4>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 6, maxHeight: 180, overflowY: 'auto' }}>
                                {statusData?.recent_runs?.map(r => (
                                    <div key={r.run_id} style={{ padding: '8px 12px', background: 'var(--bg-primary)', borderRadius: 6, fontSize: '0.78rem', display: 'flex', justifyContent: 'space-between' }}>
                                        <span>[{r.exchange}] {r.deal_type} ({r.run_mode})</span>
                                        <span>+{r.records_inserted} inserted, {r.records_skipped} skipped ({r.status})</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* ── Manual CSV Upload Modal ── */}
            {showUploadModal && (
                <div className="deals-modal-backdrop" onClick={() => setShowUploadModal(false)}>
                    <div className="deals-modal" onClick={e => e.stopPropagation()}>
                        <div className="deals-modal-header">
                            <h3 className="deals-modal-title">Manual CSV Import Fallback</h3>
                            <button className="deals-modal-close" onClick={() => setShowUploadModal(false)}>
                                <X size={20} />
                            </button>
                        </div>
                        <div className="deals-modal-body">
                            <p style={{ fontSize: '0.84rem', color: 'var(--text-secondary)', margin: 0 }}>
                                Paste official NSE or BSE Bulk/Block Deal CSV content below. The parser verifies columns, validates dates and numeric values, and handles deduplication deterministically.
                            </p>

                            <textarea
                                className="deals-textarea"
                                placeholder={`Date,Symbol,Security Name,Client Name,Buy/Sell,Quantity Traded,Trade Price / Wgt. Avg. Price\n26-Oct-2026,RELIANCE,Reliance Industries Ltd,NIPPON INDIA MUTUAL FUND,BUY,500000,2950.50`}
                                value={csvInput}
                                onChange={(e) => setCsvInput(e.target.value)}
                            />

                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: '0.82rem', cursor: 'pointer' }}>
                                    <input
                                        type="checkbox"
                                        checked={csvDryRun}
                                        onChange={(e) => setCsvDryRun(e.target.checked)}
                                    />
                                    <span>Dry Run (Validate & Preview without writing to DB)</span>
                                </label>

                                <button
                                    className="deals-btn deals-btn-primary"
                                    onClick={handleCsvSubmit}
                                    disabled={uploading || !csvInput.trim()}
                                >
                                    {uploading ? 'Processing...' : (csvDryRun ? 'Validate Preview' : 'Import Deals')}
                                </button>
                            </div>

                            {uploadResult && (
                                <div style={{ marginTop: 12, padding: 14, background: 'var(--bg-primary)', borderRadius: 8, fontSize: '0.8rem' }}>
                                    {uploadResult.error ? (
                                        <div style={{ color: '#ef4444', fontWeight: 600 }}>Error: {uploadResult.error}</div>
                                    ) : (
                                        <div>
                                            <div style={{ fontWeight: 700, color: '#10b981', marginBottom: 4 }}>
                                                {uploadResult.dry_run ? 'Dry Run Validation Complete' : 'Deals Imported Successfully!'}
                                            </div>
                                            <div>Valid records detected: {uploadResult.valid_records_count}</div>
                                            {!uploadResult.dry_run && (
                                                <div>Inserted: {uploadResult.inserted}, Skipped/Duplicates: {uploadResult.skipped}</div>
                                            )}
                                            {uploadResult.rejected_records_count > 0 && (
                                                <div style={{ color: '#f59e0b', marginTop: 4 }}>
                                                    Malformed/Rejected rows: {uploadResult.rejected_records_count}
                                                </div>
                                            )}
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
