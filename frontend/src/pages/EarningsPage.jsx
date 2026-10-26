import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
    Calendar as CalendarIcon,
    Clock,
    FileText,
    ExternalLink,
    Search,
    RefreshCw,
    Filter,
    CheckCircle2,
    AlertCircle,
    Building2,
    ChevronLeft,
    ChevronRight,
    TrendingUp,
    Shield,
    X,
    Eye,
    BarChart3,
    History,
    FileSpreadsheet,
    Activity,
    Layers,
    SlidersHorizontal,
    Bell
} from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import {
    fetchEarningsCalendar,
    fetchTodayResultsDesk,
    fetchPublishedResults,
    fetchCompanyEarningsTimeline,
    fetchEventDetails,
    fetchEarningsSyncStatus,
    triggerEarningsSyncNow
} from '../api/earnings';
import './EarningsPage.css';

export default function EarningsPage() {
    const { user, hasPermission } = useAuth();
    const isAdmin = user?.role === 'admin';

    // Active View Tab: 'today' | 'calendar' | 'results' | 'health'
    const [activeTab, setActiveTab] = useState('today');

    // Calendar View Sub-mode: 'agenda' | 'month' | 'week'
    const [calendarViewMode, setCalendarViewMode] = useState('agenda');

    // Today's Desk Data
    const [todayData, setTodayData] = useState(null);
    const [todayLoading, setTodayLoading] = useState(true);

    // Calendar Events Data
    const [calendarEvents, setCalendarEvents] = useState([]);
    const [calendarTotal, setCalendarTotal] = useState(0);
    const [calendarPage, setCalendarPage] = useState(1);
    const [calendarLimit] = useState(40);
    const [calendarLoading, setCalendarLoading] = useState(false);

    // Filters for Calendar
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearch, setDebouncedSearch] = useState('');
    const [statusFilter, setStatusFilter] = useState('ALL');
    const [periodFilter, setPeriodFilter] = useState('ALL');
    const [watchlistOnly, setWatchlistOnly] = useState(false);

    // Published Results Data
    const [resultsFilings, setResultsFilings] = useState([]);
    const [resultsTotal, setResultsTotal] = useState(0);
    const [resultsPage, setResultsPage] = useState(1);
    const [resultsLimit] = useState(25);
    const [resultsExchange, setResultsExchange] = useState('ALL');
    const [resultsType, setResultsType] = useState('ALL');
    const [resultsLoading, setResultsLoading] = useState(false);

    // Sync Diagnostics Data
    const [syncStatus, setSyncStatus] = useState(null);
    const [syncingNow, setSyncingNow] = useState(false);

    // Drawer / Modal for Event Details & Audit History
    const [selectedEventId, setSelectedEventId] = useState(null);
    const [eventDetail, setEventDetail] = useState(null);
    const [detailLoading, setDetailLoading] = useState(false);

    // Company Dossier Drawer
    const [selectedCompanyId, setSelectedCompanyId] = useState(null);
    const [companyDossier, setCompanyDossier] = useState(null);
    const [dossierLoading, setDossierLoading] = useState(false);

    // Auto-refresh state (every 30 seconds when tab is active)
    const [autoRefresh, setAutoRefresh] = useState(true);
    const [lastRefreshTime, setLastRefreshTime] = useState(new Date());

    // Debounce search input
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearch(searchQuery);
            setCalendarPage(1);
            setResultsPage(1);
        }, 300);
        return () => clearTimeout(timer);
    }, [searchQuery]);

    // 1. Load Today's Desk
    const loadTodayDesk = useCallback(async () => {
        try {
            setTodayLoading(true);
            const data = await fetchTodayResultsDesk();
            setTodayData(data);
            setLastRefreshTime(new Date());
        } catch (err) {
            console.error('Failed to load today earnings desk:', err);
        } finally {
            setTodayLoading(false);
        }
    }, []);

    // 2. Load Calendar Events
    const loadCalendarEvents = useCallback(async () => {
        try {
            setCalendarLoading(true);
            const params = {
                page: calendarPage,
                limit: calendarLimit,
                search: debouncedSearch || undefined,
                event_status: statusFilter !== 'ALL' ? statusFilter : undefined,
                fiscal_period: periodFilter !== 'ALL' ? periodFilter : undefined,
                watchlist_only: watchlistOnly ? true : undefined
            };
            const data = await fetchEarningsCalendar(params);
            setCalendarEvents(data.events || []);
            setCalendarTotal(data.total || 0);
        } catch (err) {
            console.error('Failed to load earnings calendar:', err);
        } finally {
            setCalendarLoading(false);
        }
    }, [calendarPage, calendarLimit, debouncedSearch, statusFilter, periodFilter, watchlistOnly]);

    // 3. Load Results Filings
    const loadPublishedResults = useCallback(async () => {
        try {
            setResultsLoading(true);
            const params = {
                page: resultsPage,
                limit: resultsLimit,
                search: debouncedSearch || undefined,
                exchange: resultsExchange !== 'ALL' ? resultsExchange : undefined,
                result_type: resultsType !== 'ALL' ? resultsType : undefined
            };
            const data = await fetchPublishedResults(params);
            setResultsFilings(data.filings || []);
            setResultsTotal(data.total || 0);
        } catch (err) {
            console.error('Failed to load published results filings:', err);
        } finally {
            setResultsLoading(false);
        }
    }, [resultsPage, resultsLimit, debouncedSearch, resultsExchange, resultsType]);

    // 4. Load Sync Status
    const loadSyncStatus = useCallback(async () => {
        try {
            const data = await fetchEarningsSyncStatus();
            setSyncStatus(data);
        } catch (err) {
            console.error('Failed to load earnings sync diagnostics:', err);
        }
    }, []);

    // Initial load & Tab switching
    useEffect(() => {
        if (activeTab === 'today') {
            loadTodayDesk();
        } else if (activeTab === 'calendar') {
            loadCalendarEvents();
        } else if (activeTab === 'results') {
            loadPublishedResults();
        } else if (activeTab === 'health') {
            loadSyncStatus();
        }
    }, [activeTab, loadTodayDesk, loadCalendarEvents, loadPublishedResults, loadSyncStatus]);

    // Auto-refresh interval (30s)
    useEffect(() => {
        if (!autoRefresh) return;
        const interval = setInterval(() => {
            if (document.visibilityState === 'visible') {
                if (activeTab === 'today') loadTodayDesk();
                else if (activeTab === 'calendar') loadCalendarEvents();
                else if (activeTab === 'results') loadPublishedResults();
                else if (activeTab === 'health') loadSyncStatus();
            }
        }, 30000);
        return () => clearInterval(interval);
    }, [autoRefresh, activeTab, loadTodayDesk, loadCalendarEvents, loadPublishedResults, loadSyncStatus]);

    // Load Event Details Drawer
    useEffect(() => {
        if (!selectedEventId) {
            setEventDetail(null);
            return;
        }
        let isMounted = true;
        setDetailLoading(true);
        fetchEventDetails(selectedEventId)
            .then(data => {
                if (isMounted) setEventDetail(data);
            })
            .catch(err => console.error('Error fetching event details:', err))
            .finally(() => {
                if (isMounted) setDetailLoading(false);
            });
        return () => { isMounted = false; };
    }, [selectedEventId]);

    // Load Company Dossier Drawer
    useEffect(() => {
        if (!selectedCompanyId) {
            setCompanyDossier(null);
            return;
        }
        let isMounted = true;
        setDossierLoading(true);
        fetchCompanyEarningsTimeline(selectedCompanyId)
            .then(data => {
                if (isMounted) setCompanyDossier(data);
            })
            .catch(err => console.error('Error fetching company timeline:', err))
            .finally(() => {
                if (isMounted) setDossierLoading(false);
            });
        return () => { isMounted = false; };
    }, [selectedCompanyId]);

    // Manual Sync Handler
    const handleManualSync = async () => {
        if (syncingNow) return;
        setSyncingNow(true);
        try {
            await triggerEarningsSyncNow();
            await Promise.all([loadTodayDesk(), loadSyncStatus(), loadCalendarEvents(), loadPublishedResults()]);
        } catch (err) {
            console.error('Manual sync failed:', err);
        } finally {
            setSyncingNow(false);
        }
    };

    // Format IST Date & Time
    const formatISTDateTime = (isoStr) => {
        if (!isoStr) return 'Time not available';
        try {
            const dt = new Date(isoStr);
            return dt.toLocaleString('en-IN', {
                timeZone: 'Asia/Kolkata',
                day: '2-digit',
                month: 'short',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: true
            });
        } catch {
            return isoStr;
        }
    };

    const formatISTDateOnly = (dateStr) => {
        if (!dateStr) return 'N/A';
        try {
            const [y, m, d] = dateStr.split('-');
            const dateObj = new Date(y, m - 1, d);
            return dateObj.toLocaleDateString('en-IN', {
                day: '2-digit',
                month: 'short',
                year: 'numeric'
            });
        } catch {
            return dateStr;
        }
    };

    // Status Badge Helper
    const renderStatusBadge = (status) => {
        const s = (status || '').toUpperCase();
        if (s === 'RESULTS_PUBLISHED') {
            return <span className="earn-badge badge-published"><CheckCircle2 size={12} /> Published</span>;
        }
        if (s === 'RESCHEDULED') {
            return <span className="earn-badge badge-rescheduled"><History size={12} /> Rescheduled</span>;
        }
        if (s === 'POSTPONED') {
            return <span className="earn-badge badge-postponed"><AlertCircle size={12} /> Postponed</span>;
        }
        if (s === 'CANCELLED') {
            return <span className="earn-badge badge-cancelled"><X size={12} /> Cancelled</span>;
        }
        if (s === 'CONCLUDED') {
            return <span className="earn-badge badge-concluded">Concluded</span>;
        }
        return <span className="earn-badge badge-scheduled"><CalendarIcon size={12} /> Scheduled</span>;
    };

    // Type Badge Helper
    const renderTypeBadge = (resType, auditStatus) => {
        const rt = (resType || '').toUpperCase();
        const as = (auditStatus || '').toUpperCase();
        return (
            <div className="earn-type-badges">
                {rt !== 'UNKNOWN' && (
                    <span className={`earn-pill ${rt === 'BOTH' ? 'pill-both' : rt === 'CONSOLIDATED' ? 'pill-cons' : 'pill-stand'}`}>
                        {rt}
                    </span>
                )}
                {as !== 'UNKNOWN' && (
                    <span className={`earn-pill ${as === 'AUDITED' ? 'pill-audited' : 'pill-unaudited'}`}>
                        {as}
                    </span>
                )}
            </div>
        );
    };

    return (
        <div className="earn-page">
            <div className="container">
                {/* ── Top Header Toolbar ── */}
                <header className="earn-header">
                    <div className="earn-header-left">
                        <div className="earn-title-group">
                            <h1 className="earn-title">Earnings Desk & Calendar</h1>
                            <div className="earn-live-pill">
                                <span className="pulse-dot"></span>
                                <span>Real-Time Exchange Radar</span>
                            </div>
                        </div>
                        <p className="earn-subtitle">
                            Automated tracking of SEBI Regulation 29 Board Meetings, analyst consensus estimates, and official BSE/NSE results dissemination.
                        </p>
                    </div>

                    <div className="earn-header-actions">
                        <button
                            className={`earn-btn-refresh ${todayLoading || calendarLoading ? 'spinning' : ''}`}
                            onClick={() => {
                                if (activeTab === 'today') loadTodayDesk();
                                else if (activeTab === 'calendar') loadCalendarEvents();
                                else if (activeTab === 'results') loadPublishedResults();
                                else loadSyncStatus();
                            }}
                            title="Refresh data now"
                        >
                            <RefreshCw size={15} />
                            <span>Refresh</span>
                        </button>

                        <div className="earn-refresh-info">
                            <span>Updated {lastRefreshTime.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</span>
                        </div>

                        {isAdmin && (
                            <button
                                className={`earn-btn-sync ${syncingNow ? 'loading' : ''}`}
                                onClick={handleManualSync}
                                disabled={syncingNow}
                                title="Trigger exchange scan and reconciliation daemon"
                            >
                                <Activity size={15} />
                                <span>{syncingNow ? 'Ingesting Feeds...' : 'Sync Now'}</span>
                            </button>
                        )}
                    </div>
                </header>

                {/* ── Metric Summary Cards ── */}
                <section className="earn-metrics-grid">
                    <div className="earn-metric-card metric-scheduled" onClick={() => { setActiveTab('today'); }}>
                        <div className="metric-icon-wrap">
                            <Clock size={20} />
                        </div>
                        <div className="metric-content">
                            <span className="metric-label">Scheduled Today</span>
                            <span className="metric-val">{todayData?.summary?.scheduled_today ?? '—'}</span>
                            <span className="metric-sub">Meetings to consider results</span>
                        </div>
                    </div>

                    <div className="earn-metric-card metric-published" onClick={() => { setActiveTab('today'); }}>
                        <div className="metric-icon-wrap">
                            <CheckCircle2 size={20} />
                        </div>
                        <div className="metric-content">
                            <span className="metric-label">Results Published Today</span>
                            <span className="metric-val">{todayData?.summary?.published_today ?? '—'}</span>
                            <span className="metric-sub">Disseminated by exchanges</span>
                        </div>
                    </div>

                    <div className="earn-metric-card metric-week" onClick={() => { setActiveTab('calendar'); setStatusFilter('SCHEDULED'); }}>
                        <div className="metric-icon-wrap">
                            <CalendarIcon size={20} />
                        </div>
                        <div className="metric-content">
                            <span className="metric-label">Upcoming (Next 7 Days)</span>
                            <span className="metric-val">{todayData?.summary?.upcoming_this_week ?? '—'}</span>
                            <span className="metric-sub">Scheduled board meetings</span>
                        </div>
                    </div>

                    <div className="earn-metric-card metric-active" onClick={() => { setActiveTab('calendar'); }}>
                        <div className="metric-icon-wrap">
                            <Layers size={20} />
                        </div>
                        <div className="metric-content">
                            <span className="metric-label">Active Tracked Events</span>
                            <span className="metric-val">{todayData?.summary?.total_scheduled_active ?? '—'}</span>
                            <span className="metric-sub">Across 1,329 MF portfolio stocks</span>
                        </div>
                    </div>
                </section>

                {/* ── Main Tab Navigation ── */}
                <div className="earn-tab-nav">
                    <button
                        className={`earn-tab-btn ${activeTab === 'today' ? 'active' : ''}`}
                        onClick={() => setActiveTab('today')}
                    >
                        <Clock size={16} />
                        <span>Today's Results Desk</span>
                        {todayData?.summary?.published_today > 0 && (
                            <span className="earn-tab-count">{todayData.summary.published_today}</span>
                        )}
                    </button>

                    <button
                        className={`earn-tab-btn ${activeTab === 'calendar' ? 'active' : ''}`}
                        onClick={() => setActiveTab('calendar')}
                    >
                        <CalendarIcon size={16} />
                        <span>Earnings Calendar</span>
                        {calendarTotal > 0 && <span className="earn-tab-count">{calendarTotal}</span>}
                    </button>

                    <button
                        className={`earn-tab-btn ${activeTab === 'results' ? 'active' : ''}`}
                        onClick={() => setActiveTab('results')}
                    >
                        <FileText size={16} />
                        <span>Published Results Ledger</span>
                    </button>

                    <button
                        className={`earn-tab-btn ${activeTab === 'health' ? 'active' : ''}`}
                        onClick={() => setActiveTab('health')}
                    >
                        <Activity size={16} />
                        <span>Exchange Feed Health</span>
                    </button>
                </div>

                {/* ══════════════════════════════════════════════════════════════════════════
                    TAB 1: TODAY'S RESULTS DESK
                   ══════════════════════════════════════════════════════════════════════════ */}
                {activeTab === 'today' && (
                    <div className="earn-today-section">
                        {todayLoading ? (
                            <div className="earn-loading-box">
                                <RefreshCw size={28} className="spinning" />
                                <p>Scanning exchange dissemination desks for today's financial results...</p>
                            </div>
                        ) : (
                            <div className="earn-today-split-grid">
                                {/* Left Panel: Results Published Today */}
                                <div className="earn-desk-panel desk-published">
                                    <div className="desk-panel-header">
                                        <div className="desk-panel-title">
                                            <CheckCircle2 size={18} className="text-emerald" />
                                            <h3>Results Published Today</h3>
                                            <span className="desk-badge-count">{todayData?.results_published?.length || 0}</span>
                                        </div>
                                        <span className="desk-date-tag">Official Exchange Dissemination</span>
                                    </div>

                                    {todayData?.results_published?.length === 0 ? (
                                        <div className="earn-empty-state">
                                            <FileSpreadsheet size={36} className="text-muted" />
                                            <h4>No financial results filed today yet</h4>
                                            <p>As listed companies disseminate quarterly results, they will appear here with direct document links and exact timestamps.</p>
                                        </div>
                                    ) : (
                                        <div className="earn-filings-list">
                                            {todayData.results_published.map(f => (
                                                <div key={f.filing_id} className="earn-filing-card">
                                                    <div className="filing-card-top">
                                                        <div className="filing-company-info">
                                                            <button
                                                                className="filing-comp-name"
                                                                onClick={() => setSelectedCompanyId(f.company_id)}
                                                            >
                                                                {f.company_name}
                                                            </button>
                                                            <div className="filing-meta-chips">
                                                                {f.exchange_symbol && <span className="chip-symbol">{f.exchange_symbol}</span>}
                                                                {f.bse_code && <span className="chip-bse">{f.bse_code}</span>}
                                                                <span className="chip-exchange">{f.exchange}</span>
                                                                {f.sector && <span className="chip-sector">{f.sector}</span>}
                                                            </div>
                                                        </div>

                                                        {renderTypeBadge(f.result_type, f.audit_status)}
                                                    </div>

                                                    <p className="filing-title-text">{f.filing_title}</p>

                                                    <div className="filing-card-footer">
                                                        <div className="filing-timestamps">
                                                            <span title="Exact exchange dissemination instant">
                                                                <strong>Disseminated:</strong> {formatISTDateTime(f.dissemination_timestamp)}
                                                            </span>
                                                            {f.submission_timestamp && (
                                                                <span title="Company submission instant">
                                                                    <strong>Submitted:</strong> {formatISTDateTime(f.submission_timestamp)}
                                                                </span>
                                                            )}
                                                        </div>

                                                        {f.attachment_url && (
                                                            <a
                                                                href={f.attachment_url}
                                                                target="_blank"
                                                                rel="noopener noreferrer"
                                                                className="btn-view-pdf"
                                                                title="Open official exchange filing PDF"
                                                            >
                                                                <FileText size={14} />
                                                                <span>View PDF</span>
                                                                <ExternalLink size={12} />
                                                            </a>
                                                        )}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>

                                {/* Right Panel: Awaiting Results Today */}
                                <div className="earn-desk-panel desk-awaiting">
                                    <div className="desk-panel-header">
                                        <div className="desk-panel-title">
                                            <Clock size={18} className="text-amber" />
                                            <h3>Awaiting Results Today</h3>
                                            <span className="desk-badge-count">{todayData?.awaiting_results?.length || 0}</span>
                                        </div>
                                        <span className="desk-date-tag">SEBI Reg 29 Advance Intimations</span>
                                    </div>

                                    {todayData?.awaiting_results?.length === 0 ? (
                                        <div className="earn-empty-state">
                                            <CheckCircle2 size={36} className="text-muted" />
                                            <h4>No pending scheduled meetings for today</h4>
                                            <p>All meetings scheduled for today have either disseminated their outcome or no results meetings were slated for this date.</p>
                                        </div>
                                    ) : (
                                        <div className="earn-awaiting-list">
                                            {todayData.awaiting_results.map(ev => (
                                                <div
                                                    key={ev.event_id}
                                                    className="earn-awaiting-card"
                                                    onClick={() => setSelectedEventId(ev.event_id)}
                                                >
                                                    <div className="awaiting-top">
                                                        <div>
                                                            <span className="awaiting-name">{ev.company_name}</span>
                                                            <div className="awaiting-chips">
                                                                {ev.exchange_symbol && <span className="chip-symbol">{ev.exchange_symbol}</span>}
                                                                {ev.bse_code && <span className="chip-bse">{ev.bse_code}</span>}
                                                                {ev.fiscal_period && <span className="chip-period">{ev.fiscal_period}</span>}
                                                            </div>
                                                        </div>
                                                        {renderStatusBadge(ev.event_status)}
                                                    </div>

                                                    <p className="awaiting-purpose-snippet">{ev.raw_purpose}</p>

                                                    <div className="awaiting-footer">
                                                        <span className="awaiting-source-tag">Source: {ev.source_name} ({ev.verification_status})</span>
                                                        <span className="awaiting-inspect-hint">Click for audit history &rarr;</span>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            </div>
                        )}
                    </div>
                )}

                {/* ══════════════════════════════════════════════════════════════════════════
                    TAB 2: EARNINGS CALENDAR
                   ══════════════════════════════════════════════════════════════════════════ */}
                {activeTab === 'calendar' && (
                    <div className="earn-calendar-section">
                        {/* Filter Bar */}
                        <div className="earn-filter-bar">
                            <div className="filter-search-box">
                                <Search size={16} />
                                <input
                                    type="text"
                                    placeholder="Search company, ticker, BSE code..."
                                    value={searchQuery}
                                    onChange={(e) => setSearchQuery(e.target.value)}
                                    className="earn-search-input"
                                />
                                {searchQuery && (
                                    <button className="clear-search-btn" onClick={() => setSearchQuery('')}>
                                        <X size={14} />
                                    </button>
                                )}
                            </div>

                            <div className="filter-select-group">
                                <div className="filter-select-wrap">
                                    <SlidersHorizontal size={14} />
                                    <select
                                        value={statusFilter}
                                        onChange={(e) => { setStatusFilter(e.target.value); setCalendarPage(1); }}
                                        className="earn-select"
                                    >
                                        <option value="ALL">All Statuses</option>
                                        <option value="SCHEDULED">Scheduled</option>
                                        <option value="RESULTS_PUBLISHED">Results Published</option>
                                        <option value="RESCHEDULED">Rescheduled</option>
                                        <option value="CONCLUDED">Concluded</option>
                                        <option value="POSTPONED">Postponed</option>
                                        <option value="CANCELLED">Cancelled</option>
                                    </select>
                                </div>

                                <div className="filter-select-wrap">
                                    <select
                                        value={periodFilter}
                                        onChange={(e) => { setPeriodFilter(e.target.value); setCalendarPage(1); }}
                                        className="earn-select"
                                    >
                                        <option value="ALL">All Periods</option>
                                        <option value="Q1">Q1 (June Quarter)</option>
                                        <option value="Q2">Q2 (September Quarter)</option>
                                        <option value="Q3">Q3 (December Quarter)</option>
                                        <option value="Q4">Q4 (March Quarter)</option>
                                        <option value="FY">Full Year (Annual)</option>
                                    </select>
                                </div>

                                <label className="earn-checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={watchlistOnly}
                                        onChange={(e) => { setWatchlistOnly(e.target.checked); setCalendarPage(1); }}
                                    />
                                    <span>Watchlist Only</span>
                                </label>
                            </div>
                        </div>

                        {/* Calendar Events List */}
                        {calendarLoading ? (
                            <div className="earn-loading-box">
                                <RefreshCw size={28} className="spinning" />
                                <p>Loading earnings schedule...</p>
                            </div>
                        ) : calendarEvents.length === 0 ? (
                            <div className="earn-empty-state">
                                <CalendarIcon size={40} className="text-muted" />
                                <h3>No scheduled events match your filters</h3>
                                <p>Try clearing filters or searching for another company or ticker.</p>
                            </div>
                        ) : (
                            <div className="earn-events-grid">
                                {calendarEvents.map(ev => (
                                    <div
                                        key={ev.event_id}
                                        className="earn-calendar-card"
                                        onClick={() => setSelectedEventId(ev.event_id)}
                                    >
                                        <div className="card-header-row">
                                            <div className="card-date-badge">
                                                <CalendarIcon size={14} />
                                                <span>{formatISTDateOnly(ev.meeting_date)}</span>
                                            </div>
                                            {renderStatusBadge(ev.event_status)}
                                        </div>

                                        <div className="card-company-title">
                                            <h4>{ev.company_name}</h4>
                                            <div className="card-tags-row">
                                                {ev.exchange_symbol && <span className="chip-symbol">{ev.exchange_symbol}</span>}
                                                {ev.bse_code && <span className="chip-bse">{ev.bse_code}</span>}
                                                {ev.fiscal_period && <span className="chip-period">{ev.fiscal_period}</span>}
                                                {ev.is_in_watchlist && <span className="chip-wl">Watchlist</span>}
                                            </div>
                                        </div>

                                        <p className="card-purpose-text">{ev.raw_purpose}</p>

                                        {/* Consensus Estimates if available */}
                                        {(ev.consensus_eps || ev.consensus_revenue_inr) && (
                                            <div className="card-consensus-box">
                                                <div className="consensus-title">
                                                    <TrendingUp size={12} />
                                                    <span>Consensus Estimates</span>
                                                </div>
                                                <div className="consensus-vals">
                                                    {ev.consensus_eps && (
                                                        <span>EPS: <strong>₹{Number(ev.consensus_eps).toFixed(2)}</strong></span>
                                                    )}
                                                    {ev.consensus_revenue_inr && (
                                                        <span>Rev: <strong>₹{(Number(ev.consensus_revenue_inr) / 1e7).toFixed(1)} Cr</strong></span>
                                                    )}
                                                </div>
                                            </div>
                                        )}

                                        <div className="card-footer-row">
                                            <span className="card-source-tag">Source: {ev.source_name}</span>
                                            <span className="card-details-cta">
                                                <Eye size={12} />
                                                <span>Details & History</span>
                                            </span>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}

                        {/* Pagination */}
                        {calendarTotal > calendarLimit && (
                            <div className="earn-pagination">
                                <button
                                    disabled={calendarPage <= 1}
                                    onClick={() => setCalendarPage(p => Math.max(1, p - 1))}
                                    className="page-nav-btn"
                                >
                                    <ChevronLeft size={16} /> Previous
                                </button>
                                <span className="page-indicator">
                                    Page {calendarPage} of {Math.ceil(calendarTotal / calendarLimit)} ({calendarTotal} total events)
                                </span>
                                <button
                                    disabled={calendarPage >= Math.ceil(calendarTotal / calendarLimit)}
                                    onClick={() => setCalendarPage(p => p + 1)}
                                    className="page-nav-btn"
                                >
                                    Next <ChevronRight size={16} />
                                </button>
                            </div>
                        )}
                    </div>
                )}

                {/* ══════════════════════════════════════════════════════════════════════════
                    TAB 3: PUBLISHED RESULTS LEDGER
                   ══════════════════════════════════════════════════════════════════════════ */}
                {activeTab === 'results' && (
                    <div className="earn-ledger-section">
                        {/* Filters */}
                        <div className="earn-filter-bar">
                            <div className="filter-search-box">
                                <Search size={16} />
                                <input
                                    type="text"
                                    placeholder="Filter by company, ticker, filing keywords..."
                                    value={searchQuery}
                                    onChange={(e) => setSearchQuery(e.target.value)}
                                    className="earn-search-input"
                                />
                            </div>

                            <div className="filter-select-group">
                                <select
                                    value={resultsExchange}
                                    onChange={(e) => { setResultsExchange(e.target.value); setResultsPage(1); }}
                                    className="earn-select"
                                >
                                    <option value="ALL">All Exchanges</option>
                                    <option value="BSE">BSE India</option>
                                    <option value="NSE">NSE India</option>
                                </select>

                                <select
                                    value={resultsType}
                                    onChange={(e) => { setResultsType(e.target.value); setResultsPage(1); }}
                                    className="earn-select"
                                >
                                    <option value="ALL">All Result Types</option>
                                    <option value="STANDALONE">Standalone</option>
                                    <option value="CONSOLIDATED">Consolidated</option>
                                    <option value="BOTH">Standalone & Consolidated</option>
                                </select>
                            </div>
                        </div>

                        {resultsLoading ? (
                            <div className="earn-loading-box">
                                <RefreshCw size={28} className="spinning" />
                                <p>Loading historical results ledger...</p>
                            </div>
                        ) : resultsFilings.length === 0 ? (
                            <div className="earn-empty-state">
                                <FileText size={36} className="text-muted" />
                                <h3>No results filings found</h3>
                                <p>No filings matched your current query criteria.</p>
                            </div>
                        ) : (
                            <div className="earn-ledger-table-wrap">
                                <table className="earn-ledger-table">
                                    <thead>
                                        <tr>
                                            <th>Company / Symbol</th>
                                            <th>Exchange</th>
                                            <th>Period</th>
                                            <th>Type & Audit</th>
                                            <th>Disseminated At (IST)</th>
                                            <th>Filing Title</th>
                                            <th>Document</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {resultsFilings.map(f => (
                                            <tr key={f.filing_id}>
                                                <td>
                                                    <div className="table-comp-cell">
                                                        <button
                                                            className="table-comp-btn"
                                                            onClick={() => setSelectedCompanyId(f.company_id)}
                                                        >
                                                            {f.company_name}
                                                        </button>
                                                        <span className="table-symbol">{f.exchange_symbol || f.bse_code}</span>
                                                    </div>
                                                </td>
                                                <td>
                                                    <span className={`chip-exchange-mini ${f.exchange.toLowerCase()}`}>{f.exchange}</span>
                                                </td>
                                                <td>
                                                    <span className="table-period-badge">
                                                        {f.financial_year || ''} {f.fiscal_period || ''}
                                                    </span>
                                                </td>
                                                <td>
                                                    {renderTypeBadge(f.result_type, f.audit_status)}
                                                </td>
                                                <td className="table-dissem-time">
                                                    {formatISTDateTime(f.dissemination_timestamp)}
                                                </td>
                                                <td className="table-title-cell" title={f.filing_title}>
                                                    {f.filing_title}
                                                </td>
                                                <td>
                                                    {f.attachment_url ? (
                                                        <a
                                                            href={f.attachment_url}
                                                            target="_blank"
                                                            rel="noopener noreferrer"
                                                            className="table-pdf-link"
                                                            title="View original exchange filing"
                                                        >
                                                            <FileText size={14} /> PDF
                                                        </a>
                                                    ) : (
                                                        <span className="text-muted text-xs">No PDF</span>
                                                    )}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}

                        {resultsTotal > resultsLimit && (
                            <div className="earn-pagination">
                                <button
                                    disabled={resultsPage <= 1}
                                    onClick={() => setResultsPage(p => Math.max(1, p - 1))}
                                    className="page-nav-btn"
                                >
                                    <ChevronLeft size={16} /> Previous
                                </button>
                                <span className="page-indicator">
                                    Page {resultsPage} of {Math.ceil(resultsTotal / resultsLimit)} ({resultsTotal} total filings)
                                </span>
                                <button
                                    disabled={resultsPage >= Math.ceil(resultsTotal / resultsLimit)}
                                    onClick={() => setResultsPage(p => p + 1)}
                                    className="page-nav-btn"
                                >
                                    Next <ChevronRight size={16} />
                                </button>
                            </div>
                        )}
                    </div>
                )}

                {/* ══════════════════════════════════════════════════════════════════════════
                    TAB 4: FEED HEALTH & OBSERVABILITY
                   ══════════════════════════════════════════════════════════════════════════ */}
                {activeTab === 'health' && (
                    <div className="earn-health-section">
                        <div className="health-overview-bar">
                            <div className="health-stat">
                                <span className="stat-label">Total Calendar Events Tracked</span>
                                <span className="stat-value">{syncStatus?.total_calendar_events ?? '—'}</span>
                            </div>
                            <div className="health-stat">
                                <span className="stat-label">Total Published Filings Disseminated</span>
                                <span className="stat-value">{syncStatus?.total_results_filings ?? '—'}</span>
                            </div>
                            <div className="health-stat">
                                <span className="stat-label">Append-Only Audit Records Logged</span>
                                <span className="stat-value">{syncStatus?.total_history_records ?? '—'}</span>
                            </div>
                        </div>

                        <div className="health-sources-grid">
                            {(syncStatus?.sources || []).map(src => (
                                <div key={src.source_key} className="health-source-card">
                                    <div className="source-card-top">
                                        <span className="source-title">{src.source_key}</span>
                                        <span className={`source-status-badge ${src.status.toLowerCase()}`}>
                                            {src.status}
                                        </span>
                                    </div>

                                    <div className="source-details-list">
                                        <div className="source-row">
                                            <span>Last Successful Sync:</span>
                                            <strong>{formatISTDateTime(src.last_successful_at)}</strong>
                                        </div>
                                        <div className="source-row">
                                            <span>Last Attempted:</span>
                                            <strong>{formatISTDateTime(src.last_attempted_at)}</strong>
                                        </div>
                                        <div className="source-row">
                                            <span>Records Processed:</span>
                                            <strong>{src.events_count + src.results_count}</strong>
                                        </div>
                                        {src.last_error && (
                                            <div className="source-error-box">
                                                <AlertCircle size={14} />
                                                <span>{src.last_error}</span>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </div>

            {/* ══════════════════════════════════════════════════════════════════════════
                DRAWER 1: SINGLE EVENT DETAILS & AUDIT LOG
               ══════════════════════════════════════════════════════════════════════════ */}
            {selectedEventId && (
                <div className="earn-drawer-overlay" onClick={() => setSelectedEventId(null)}>
                    <div className="earn-drawer-content" onClick={(e) => e.stopPropagation()}>
                        <div className="drawer-header">
                            <div>
                                <span className="drawer-subheading">Event Inspector & Historical Audit</span>
                                <h3 className="drawer-title">{eventDetail?.event?.company_name || 'Loading event...'}</h3>
                            </div>
                            <button className="drawer-close-btn" onClick={() => setSelectedEventId(null)}>
                                <X size={18} />
                            </button>
                        </div>

                        {detailLoading ? (
                            <div className="earn-loading-box">
                                <RefreshCw size={24} className="spinning" />
                                <p>Loading event history...</p>
                            </div>
                        ) : eventDetail ? (
                            <div className="drawer-body">
                                {/* Event Summary Box */}
                                <div className="drawer-info-card">
                                    <div className="drawer-info-row">
                                        <span>Scheduled Meeting Date:</span>
                                        <strong>{formatISTDateOnly(eventDetail.event.meeting_date)}</strong>
                                    </div>
                                    <div className="drawer-info-row">
                                        <span>Current Status:</span>
                                        {renderStatusBadge(eventDetail.event.event_status)}
                                    </div>
                                    <div className="drawer-info-row">
                                        <span>Fiscal Period:</span>
                                        <strong>{eventDetail.event.financial_year || ''} {eventDetail.event.fiscal_period || ''}</strong>
                                    </div>
                                    <div className="drawer-info-row">
                                        <span>Source Provider:</span>
                                        <strong>{eventDetail.event.source_name} ({eventDetail.event.verification_status})</strong>
                                    </div>
                                    <div className="drawer-info-row">
                                        <span>First Detected Ingestion Instant:</span>
                                        <strong>{formatISTDateTime(eventDetail.event.first_detected_at)}</strong>
                                    </div>
                                    <div className="drawer-info-row">
                                        <span>Last Verified:</span>
                                        <strong>{formatISTDateTime(eventDetail.event.last_verified_at)}</strong>
                                    </div>
                                </div>

                                <div className="drawer-section">
                                    <h4 className="drawer-section-title">Raw Statutory Meeting Purpose</h4>
                                    <div className="drawer-purpose-box">
                                        {eventDetail.event.raw_purpose}
                                    </div>
                                </div>

                                {/* Linked Results Filings */}
                                <div className="drawer-section">
                                    <h4 className="drawer-section-title">
                                        Linked Published Results Filings ({eventDetail.linked_filings?.length || 0})
                                    </h4>
                                    {eventDetail.linked_filings?.length === 0 ? (
                                        <p className="text-muted text-sm">No results filings linked to this meeting yet.</p>
                                    ) : (
                                        <div className="drawer-filings-list">
                                            {eventDetail.linked_filings.map(lf => (
                                                <div key={lf.filing_id} className="drawer-filing-card">
                                                    <div className="drawer-filing-top">
                                                        <span className="chip-exchange">{lf.exchange}</span>
                                                        {renderTypeBadge(lf.result_type, lf.audit_status)}
                                                    </div>
                                                    <p className="drawer-filing-title">{lf.filing_title}</p>
                                                    <div className="drawer-filing-footer">
                                                        <span>Disseminated: {formatISTDateTime(lf.dissemination_timestamp)}</span>
                                                        {lf.attachment_url && (
                                                            <a
                                                                href={lf.attachment_url}
                                                                target="_blank"
                                                                rel="noopener noreferrer"
                                                                className="btn-view-pdf"
                                                            >
                                                                <FileText size={12} /> View PDF
                                                            </a>
                                                        )}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>

                                {/* Append-Only Audit History Log */}
                                <div className="drawer-section">
                                    <h4 className="drawer-section-title">
                                        Audit Change Ledger ({eventDetail.history?.length || 0})
                                    </h4>
                                    {eventDetail.history?.length === 0 ? (
                                        <p className="text-muted text-sm">No date reschedules or status modifications recorded.</p>
                                    ) : (
                                        <div className="drawer-history-timeline">
                                            {eventDetail.history.map(h => (
                                                <div key={h.history_id} className="history-item">
                                                    <div className="history-dot"></div>
                                                    <div className="history-content">
                                                        <span className="history-date">{formatISTDateTime(h.detected_at)}</span>
                                                        <p className="history-reason">
                                                            <strong>Status Transition:</strong> {h.old_status} &rarr; {h.new_status}
                                                        </p>
                                                        {h.change_reason && <p className="history-detail">{h.change_reason}</p>}
                                                        {h.source_evidence && <span className="history-evidence">Evidence: {h.source_evidence}</span>}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            </div>
                        ) : null}
                    </div>
                </div>
            )}

            {/* ══════════════════════════════════════════════════════════════════════════
                DRAWER 2: COMPANY EARNINGS DOSSIER
               ══════════════════════════════════════════════════════════════════════════ */}
            {selectedCompanyId && (
                <div className="earn-drawer-overlay" onClick={() => setSelectedCompanyId(null)}>
                    <div className="earn-drawer-content" onClick={(e) => e.stopPropagation()}>
                        <div className="drawer-header">
                            <div>
                                <span className="drawer-subheading">Company Earnings Dossier</span>
                                <h3 className="drawer-title">{companyDossier?.company?.company_name || 'Loading company...'}</h3>
                            </div>
                            <button className="drawer-close-btn" onClick={() => setSelectedCompanyId(null)}>
                                <X size={18} />
                            </button>
                        </div>

                        {dossierLoading ? (
                            <div className="earn-loading-box">
                                <RefreshCw size={24} className="spinning" />
                                <p>Loading company earnings dossier...</p>
                            </div>
                        ) : companyDossier ? (
                            <div className="drawer-body">
                                <div className="drawer-info-card">
                                    <div className="drawer-info-row">
                                        <span>NSE Symbol / BSE Code:</span>
                                        <strong>{companyDossier.company.exchange_symbol || '—'} / {companyDossier.company.bse_code || '—'}</strong>
                                    </div>
                                    <div className="drawer-info-row">
                                        <span>ISIN:</span>
                                        <strong>{companyDossier.company.isin}</strong>
                                    </div>
                                    <div className="drawer-info-row">
                                        <span>Sector / Industry:</span>
                                        <strong>{companyDossier.company.sector || '—'} ({companyDossier.company.industry || '—'})</strong>
                                    </div>
                                </div>

                                <div className="drawer-section">
                                    <h4 className="drawer-section-title">
                                        Upcoming Scheduled Meetings ({companyDossier.upcoming_events?.length || 0})
                                    </h4>
                                    {companyDossier.upcoming_events?.length === 0 ? (
                                        <p className="text-muted text-sm">No upcoming results board meetings currently scheduled.</p>
                                    ) : (
                                        <div className="drawer-filings-list">
                                            {companyDossier.upcoming_events.map(ev => (
                                                <div key={ev.event_id} className="drawer-filing-card">
                                                    <div className="drawer-filing-top">
                                                        <span>Meeting Date: <strong>{formatISTDateOnly(ev.meeting_date)}</strong></span>
                                                        {renderStatusBadge(ev.event_status)}
                                                    </div>
                                                    <p className="drawer-filing-title">{ev.raw_purpose}</p>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>

                                <div className="drawer-section">
                                    <h4 className="drawer-section-title">
                                        Published Financial Results Filings ({companyDossier.filings?.length || 0})
                                    </h4>
                                    {companyDossier.filings?.length === 0 ? (
                                        <p className="text-muted text-sm">No historical results filings available for this company.</p>
                                    ) : (
                                        <div className="drawer-filings-list">
                                            {companyDossier.filings.map(f => (
                                                <div key={f.filing_id} className="drawer-filing-card">
                                                    <div className="drawer-filing-top">
                                                        <span className="chip-exchange">{f.exchange}</span>
                                                        {renderTypeBadge(f.result_type, f.audit_status)}
                                                    </div>
                                                    <p className="drawer-filing-title">{f.filing_title}</p>
                                                    <div className="drawer-filing-footer">
                                                        <span>Disseminated: {formatISTDateTime(f.dissemination_timestamp)}</span>
                                                        {f.attachment_url && (
                                                            <a
                                                                href={f.attachment_url}
                                                                target="_blank"
                                                                rel="noopener noreferrer"
                                                                className="btn-view-pdf"
                                                            >
                                                                <FileText size={12} /> View PDF
                                                            </a>
                                                        )}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            </div>
                        ) : null}
                    </div>
                </div>
            )}
        </div>
    );
}
