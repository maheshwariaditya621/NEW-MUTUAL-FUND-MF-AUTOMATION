import React, { useState, useEffect, useCallback } from 'react';
import {
    Search,
    RefreshCw,
    Filter,
    FileText,
    ExternalLink,
    Building2,
    Calendar,
    Clock,
    AlertCircle,
    CheckCircle2,
    SlidersHorizontal,
    Plus,
    Trash2,
    X,
    Eye,
    ChevronLeft,
    ChevronRight,
    Sparkles,
    ShieldAlert,
    Radio
} from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import {
    fetchAnnouncements,
    fetchAnnouncementDetails,
    fetchAnnouncementCategories,
    fetchActiveMasterWatchlist,
    addToMasterWatchlist,
    removeFromMasterWatchlist,
    searchCompaniesForWatchlist,
    fetchSyncHealth,
    triggerManualSync,
    getAttachmentViewUrl
} from '../api/announcements';
import './AnnouncementsPage.css';

export default function AnnouncementsPage() {
    const { user, hasPermission } = useAuth();
    const canManageWatchlist = hasPermission('manage_master_watchlist') || user?.role === 'admin';

    // Announcements state
    const [announcements, setAnnouncements] = useState([]);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [limit] = useState(25);
    const [totalPages, setTotalPages] = useState(1);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    // Filters state
    const [search, setSearch] = useState('');
    const [debouncedSearch, setDebouncedSearch] = useState('');
    const [selectedCategory, setSelectedCategory] = useState('All');
    const [selectedExchange, setSelectedExchange] = useState('ALL');
    const [onlyRevisions, setOnlyRevisions] = useState(false);
    const [categories, setCategories] = useState([]);

    // Master Watchlist modal & data
    const [showWatchlistModal, setShowWatchlistModal] = useState(false);
    const [watchlistCompanies, setWatchlistCompanies] = useState([]);
    const [watchlistSearch, setWatchlistSearch] = useState('');
    const [searchCandidates, setSearchCandidates] = useState([]);
    const [searchingCandidates, setSearchingCandidates] = useState(false);
    const [watchlistActionLoading, setWatchlistActionLoading] = useState(false);
    const [watchlistNotes, setWatchlistNotes] = useState('');

    // Detail & PDF Preview Modal
    const [selectedAnnouncement, setSelectedAnnouncement] = useState(null);
    const [detailLoading, setDetailLoading] = useState(false);
    const [previewAttachment, setPreviewAttachment] = useState(null);

    // Sync State & Health
    const [syncHealth, setSyncHealth] = useState(null);
    const [syncingNow, setSyncingNow] = useState(false);
    const [autoRefresh, setAutoRefresh] = useState(true);

    // Debounce search
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearch(search);
            setPage(1);
        }, 350);
        return () => clearTimeout(timer);
    }, [search]);

    // Load Categories & Health
    const loadMetadata = useCallback(async () => {
        try {
            const [catData, healthData, wlData] = await Promise.all([
                fetchAnnouncementCategories(),
                fetchSyncHealth(),
                fetchActiveMasterWatchlist()
            ]);
            setCategories(catData || []);
            setSyncHealth(healthData || null);
            setWatchlistCompanies(wlData || []);
        } catch (err) {
            console.error('Failed to load announcements metadata:', err);
        }
    }, []);

    useEffect(() => {
        loadMetadata();
    }, [loadMetadata]);

    // Fetch Announcements Feed
    const loadAnnouncements = useCallback(async (isSilent = false) => {
        if (!isSilent) setLoading(true);
        setError(null);
        try {
            const params = {
                page,
                limit,
                search: debouncedSearch || undefined,
                category: selectedCategory !== 'All' ? selectedCategory : undefined,
                exchange: selectedExchange !== 'ALL' ? selectedExchange : undefined,
                is_revision: onlyRevisions ? true : undefined
            };
            const data = await fetchAnnouncements(params);
            setAnnouncements(data.items || []);
            setTotal(data.total || 0);
            setTotalPages(data.total_pages || 1);
        } catch (err) {
            console.error('Failed to load announcements:', err);
            setError(err.message || 'Failed to load corporate announcements');
        } finally {
            if (!isSilent) setLoading(false);
        }
    }, [page, limit, debouncedSearch, selectedCategory, selectedExchange, onlyRevisions]);

    useEffect(() => {
        loadAnnouncements();
    }, [loadAnnouncements]);

    // Auto-refresh every 60 seconds
    useEffect(() => {
        if (!autoRefresh) return;
        const interval = setInterval(() => {
            loadAnnouncements(true);
            fetchSyncHealth().then(data => setSyncHealth(data)).catch(() => {});
        }, 60000);
        return () => clearInterval(interval);
    }, [autoRefresh, loadAnnouncements]);

    // Manual Sync Trigger
    const handleTriggerSync = async () => {
        setSyncingNow(true);
        try {
            await triggerManualSync();
            await loadMetadata();
            await loadAnnouncements();
        } catch (err) {
            alert(`Manual sync failed: ${err.message}`);
        } finally {
            setSyncingNow(false);
        }
    };

    // Watchlist Company Search
    useEffect(() => {
        if (!watchlistSearch || watchlistSearch.length < 2) {
            setSearchCandidates([]);
            return;
        }
        const timer = setTimeout(async () => {
            setSearchingCandidates(true);
            try {
                const results = await searchCompaniesForWatchlist(watchlistSearch);
                setSearchCandidates(results || []);
            } catch (err) {
                console.error('Failed searching companies:', err);
            } finally {
                setSearchingCandidates(false);
            }
        }, 300);
        return () => clearTimeout(timer);
    }, [watchlistSearch]);

    // Add company to watchlist
    const handleAddCompany = async (companyId) => {
        setWatchlistActionLoading(true);
        try {
            await addToMasterWatchlist(companyId, watchlistNotes);
            setWatchlistNotes('');
            setWatchlistSearch('');
            setSearchCandidates([]);
            const updatedWL = await fetchActiveMasterWatchlist();
            setWatchlistCompanies(updatedWL || []);
            loadMetadata();
        } catch (err) {
            alert(`Failed adding company to watchlist: ${err.message}`);
        } finally {
            setWatchlistActionLoading(false);
        }
    };

    // Remove company from watchlist
    const handleRemoveCompany = async (companyId) => {
        if (!window.confirm('Remove this company from the Master Office Watchlist?')) return;
        setWatchlistActionLoading(true);
        try {
            await removeFromMasterWatchlist(companyId);
            const updatedWL = await fetchActiveMasterWatchlist();
            setWatchlistCompanies(updatedWL || []);
            loadMetadata();
        } catch (err) {
            alert(`Failed removing company: ${err.message}`);
        } finally {
            setWatchlistActionLoading(false);
        }
    };

    // Open Announcement Detail Modal
    const handleOpenDetail = async (canonicalId) => {
        setDetailLoading(true);
        setSelectedAnnouncement(null);
        setPreviewAttachment(null);
        try {
            const data = await fetchAnnouncementDetails(canonicalId);
            setSelectedAnnouncement(data);
            if (data.attachments && data.attachments.length > 0) {
                setPreviewAttachment(data.attachments[0]);
            }
        } catch (err) {
            alert(`Failed to load announcement details: ${err.message}`);
        } finally {
            setDetailLoading(false);
        }
    };

    // Format IST Date
    const formatDateTime = (isoString) => {
        if (!isoString) return '--';
        try {
            const d = new Date(isoString);
            return d.toLocaleString('en-IN', {
                timeZone: 'Asia/Kolkata',
                day: '2-digit',
                month: 'short',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                hour12: true
            });
        } catch {
            return isoString;
        }
    };

    return (
        <div className="ann-page">
            <div className="container">
                {/* ── Top Header Toolbar ── */}
                <div className="ann-toolbar">
                    <div className="ann-toolbar-left">
                        <div className="ann-title-wrap">
                            <h1 className="ann-title">Corporate Announcements</h1>
                            <span className="ann-live-pill">
                                <span className="pulse-dot"></span> Live Office Feed
                            </span>
                        </div>
                        <div className="ann-stats-bar">
                            <span className="ann-stat-chip">
                                <Building2 size={14} /> {watchlistCompanies.length} Tracked Stocks
                            </span>
                            <span className="ann-stat-chip">
                                <FileText size={14} /> {total} Filings
                            </span>
                        </div>
                    </div>

                    <div className="ann-toolbar-right">
                        <button
                            className="ann-btn ann-btn-secondary"
                            onClick={() => setShowWatchlistModal(true)}
                        >
                            <SlidersHorizontal size={15} />
                            Master Watchlist
                            <span className="ann-count-badge">{watchlistCompanies.length}</span>
                        </button>

                        <button
                            className={`ann-btn ann-btn-primary ${syncingNow ? 'syncing' : ''}`}
                            onClick={handleTriggerSync}
                            disabled={syncingNow}
                        >
                            <RefreshCw size={15} className={syncingNow ? 'spin-anim' : ''} />
                            {syncingNow ? 'Polling Feeds...' : 'Sync Now'}
                        </button>
                    </div>
                </div>

                {/* ── Controls / Search & Filters ── */}
                <div className="ann-filters-card">
                    <div className="ann-search-row">
                        <div className="ann-search-input-wrap">
                            <Search size={18} className="search-icon" />
                            <input
                                type="text"
                                placeholder="Search by stock name, NSE symbol (e.g., RELIANCE), or subject keywords..."
                                value={search}
                                onChange={(e) => setSearch(e.target.value)}
                                className="ann-search-input"
                            />
                            {search && (
                                <button className="clear-btn" onClick={() => setSearch('')}>
                                    <X size={16} />
                                </button>
                            )}
                        </div>

                        {/* Exchange Filter Segment */}
                        <div className="ann-exchange-toggles">
                            {['ALL', 'NSE', 'BSE', 'BOTH'].map((ex) => (
                                <button
                                    key={ex}
                                    className={`ex-toggle-btn ${selectedExchange === ex ? 'active' : ''}`}
                                    onClick={() => {
                                        setSelectedExchange(ex);
                                        setPage(1);
                                    }}
                                >
                                    {ex === 'ALL' ? 'All Exchanges' : ex === 'BOTH' ? 'NSE & BSE' : ex}
                                </button>
                            ))}
                        </div>

                        {/* Revisions Filter Toggle */}
                        <label className={`ann-revision-toggle ${onlyRevisions ? 'active' : ''}`}>
                            <input
                                type="checkbox"
                                checked={onlyRevisions}
                                onChange={(e) => {
                                    setOnlyRevisions(e.target.checked);
                                    setPage(1);
                                }}
                            />
                            <ShieldAlert size={15} />
                            <span>Revisions / Corrigendums Only</span>
                        </label>
                    </div>

                    {/* Category Filter Pills */}
                    <div className="ann-category-scroller">
                        <button
                            className={`cat-pill ${selectedCategory === 'All' ? 'active' : ''}`}
                            onClick={() => {
                                setSelectedCategory('All');
                                setPage(1);
                            }}
                        >
                            All Categories
                        </button>
                        {categories.map((cat) => (
                            <button
                                key={cat.name}
                                className={`cat-pill ${selectedCategory === cat.name ? 'active' : ''}`}
                                onClick={() => {
                                    setSelectedCategory(cat.name);
                                    setPage(1);
                                }}
                            >
                                {cat.name}
                                {cat.count > 0 && <span className="cat-count">{cat.count}</span>}
                            </button>
                        ))}
                    </div>
                </div>

                {/* ── Main Content / Feed ── */}
                {loading ? (
                    <div className="ann-loading-state">
                        <div className="ann-spinner"></div>
                        <p>Fetching corporate announcements from NSE & BSE...</p>
                    </div>
                ) : error ? (
                    <div className="ann-error-state">
                        <AlertCircle size={32} />
                        <h3>Failed to load announcements</h3>
                        <p>{error}</p>
                        <button className="ann-btn ann-btn-secondary" onClick={() => loadAnnouncements()}>
                            Try Again
                        </button>
                    </div>
                ) : announcements.length === 0 ? (
                    <div className="ann-empty-state">
                        <Radio size={48} className="empty-icon" />
                        <h3>No announcements found</h3>
                        <p>
                            {watchlistCompanies.length === 0
                                ? 'Your Master Office Watchlist is currently empty. Add stocks to start monitoring announcements.'
                                : 'No announcements match your search filters or selected categories.'}
                        </p>
                        {watchlistCompanies.length === 0 && (
                            <button
                                className="ann-btn ann-btn-primary"
                                onClick={() => setShowWatchlistModal(true)}
                            >
                                <Plus size={16} /> Open Master Watchlist
                            </button>
                        )}
                    </div>
                ) : (
                    <div className="ann-feed-container">
                        <div className="ann-cards-list">
                            {announcements.map((item) => (
                                <div
                                    key={item.canonical_id}
                                    className={`ann-card ${item.is_revision ? 'is-revision-card' : ''}`}
                                >
                                    <div className="ann-card-header">
                                        <div className="ann-company-meta">
                                            <div className="ann-stock-tag">
                                                <span className="ann-symbol">{item.nse_symbol || item.bse_code}</span>
                                                <span className="ann-cname">{item.company_name}</span>
                                            </div>
                                            {item.sector && <span className="ann-sector">{item.sector}</span>}
                                        </div>

                                        <div className="ann-badges-group">
                                            {/* Exchange badges */}
                                            {item.has_nse && item.has_bse ? (
                                                <span className="badge-dual">NSE + BSE Unified</span>
                                            ) : item.has_nse ? (
                                                <span className="badge-nse">NSE</span>
                                            ) : (
                                                <span className="badge-bse">BSE</span>
                                            )}

                                            {/* Revision badge */}
                                            {item.is_revision && (
                                                <span className="badge-revision">
                                                    ⚠️ {item.revision_type || 'REVISION'}
                                                </span>
                                            )}
                                        </div>
                                    </div>

                                    <div className="ann-card-body">
                                        <h3
                                            className="ann-subject-title"
                                            onClick={() => handleOpenDetail(item.canonical_id)}
                                        >
                                            {item.title}
                                        </h3>

                                        {item.summary_text && item.summary_text !== item.title && (
                                            <p className="ann-summary-snippet">
                                                {item.summary_text.slice(0, 180)}
                                                {item.summary_text.length > 180 ? '...' : ''}
                                            </p>
                                        )}

                                        {/* Categories & Metadata */}
                                        <div className="ann-tags-row">
                                            {item.categories &&
                                                item.categories.map((c) => (
                                                    <span key={c} className="ann-cat-tag">
                                                        {c}
                                                    </span>
                                                ))}
                                        </div>
                                    </div>

                                    <div className="ann-card-footer">
                                        <div className="ann-footer-left">
                                            <span className="ann-time">
                                                <Clock size={13} /> {formatDateTime(item.primary_timestamp)}
                                            </span>
                                            <span className="ann-isin-code">ISIN: {item.isin}</span>
                                        </div>

                                        <div className="ann-footer-right">
                                            {item.attachments && item.attachments.length > 0 && (
                                                <a
                                                    href={getAttachmentViewUrl(item.attachments[0].attachment_id)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="ann-pdf-btn"
                                                    title={item.attachments[0].original_file_name}
                                                >
                                                    <FileText size={14} />
                                                    View PDF Filing
                                                    <ExternalLink size={12} />
                                                </a>
                                            )}

                                            <button
                                                className="ann-detail-btn"
                                                onClick={() => handleOpenDetail(item.canonical_id)}
                                            >
                                                <Eye size={14} /> Full Details
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>

                        {/* Pagination */}
                        {totalPages > 1 && (
                            <div className="ann-pagination">
                                <button
                                    className="page-btn"
                                    disabled={page <= 1}
                                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                                >
                                    <ChevronLeft size={16} /> Previous
                                </button>
                                <span className="page-info">
                                    Page <strong>{page}</strong> of <strong>{totalPages}</strong> ({total} announcements)
                                </span>
                                <button
                                    className="page-btn"
                                    disabled={page >= totalPages}
                                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                                >
                                    Next <ChevronRight size={16} />
                                </button>
                            </div>
                        )}
                    </div>
                )}
            </div>

            {/* ============================================================ */}
            {/* MASTER WATCHLIST MODAL */}
            {/* ============================================================ */}
            {showWatchlistModal && (
                <div className="modal-overlay">
                    <div className="modal-content ann-modal-wide">
                        <div className="modal-header">
                            <div className="modal-title-wrap">
                                <h3>Master Office Watchlist</h3>
                                <p className="modal-subtitle">
                                    Only companies in this list are actively monitored by the 60s NSE/BSE collector.
                                </p>
                            </div>
                            <button className="icon-btn" onClick={() => setShowWatchlistModal(false)}>
                                <X size={20} />
                            </button>
                        </div>

                        <div className="ann-modal-body">
                            {/* Search & Add New Stock */}
                            {canManageWatchlist && (
                                <div className="wl-add-section">
                                    <label className="wl-section-label">Add Stock to Watchlist</label>
                                    <div className="wl-search-input-box">
                                        <Search size={16} />
                                        <input
                                            type="text"
                                            placeholder="Search listed companies by name, NSE symbol or BSE code..."
                                            value={watchlistSearch}
                                            onChange={(e) => setWatchlistSearch(e.target.value)}
                                            className="admin-input"
                                        />
                                        {searchingCandidates && <div className="mini-spinner"></div>}
                                    </div>

                                    {/* Candidates Dropdown */}
                                    {searchCandidates.length > 0 && (
                                        <div className="wl-candidates-dropdown">
                                            {searchCandidates.map((c) => (
                                                <div key={c.company_id} className="wl-candidate-item">
                                                    <div className="cand-info">
                                                        <span className="cand-name">{c.company_name}</span>
                                                        <span className="cand-sub">
                                                            NSE: <strong>{c.nse_symbol || '--'}</strong> | BSE: <strong>{c.bse_code || '--'}</strong> | {c.sector}
                                                        </span>
                                                    </div>
                                                    {c.in_master_watchlist ? (
                                                        <span className="cand-already-tag">Already Monitored</span>
                                                    ) : (
                                                        <button
                                                            className="cand-add-btn"
                                                            disabled={watchlistActionLoading}
                                                            onClick={() => handleAddCompany(c.company_id)}
                                                        >
                                                            <Plus size={14} /> Add
                                                        </button>
                                                    )}
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            )}

                            {/* Active Watchlist List */}
                            <div className="wl-active-section">
                                <div className="wl-active-header">
                                    <h4>Monitored Companies ({watchlistCompanies.length})</h4>
                                    <span className="wl-subtext">50-250 companies recommended</span>
                                </div>

                                <div className="wl-table-wrap">
                                    <table className="wl-table">
                                        <thead>
                                            <tr>
                                                <th>Company Name</th>
                                                <th>NSE Symbol</th>
                                                <th>BSE Code</th>
                                                <th>Sector</th>
                                                <th>ISIN</th>
                                                {canManageWatchlist && <th>Actions</th>}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {watchlistCompanies.map((w) => (
                                                <tr key={w.company_id}>
                                                    <td className="font-semibold">{w.company_name}</td>
                                                    <td>
                                                        <span className="nse-code-pill">{w.nse_symbol || '--'}</span>
                                                    </td>
                                                    <td>
                                                        <span className="bse-code-pill">{w.bse_code || '--'}</span>
                                                    </td>
                                                    <td>{w.sector || '--'}</td>
                                                    <td className="text-muted font-mono">{w.isin}</td>
                                                    {canManageWatchlist && (
                                                        <td>
                                                            <button
                                                                className="wl-delete-btn"
                                                                title="Remove from watchlist"
                                                                disabled={watchlistActionLoading}
                                                                onClick={() => handleRemoveCompany(w.company_id)}
                                                            >
                                                                <Trash2 size={15} />
                                                            </button>
                                                        </td>
                                                    )}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>

                        <div className="modal-footer">
                            <button className="btn-secondary" onClick={() => setShowWatchlistModal(false)}>
                                Close
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* ============================================================ */}
            {/* ANNOUNCEMENT DETAIL & PDF VIEWER MODAL */}
            {/* ============================================================ */}
            {selectedAnnouncement && (
                <div className="modal-overlay">
                    <div className="modal-content ann-detail-modal">
                        <div className="modal-header">
                            <div>
                                <span className="modal-tag">
                                    {selectedAnnouncement.company_name} ({selectedAnnouncement.exchange_symbol || selectedAnnouncement.bse_code})
                                </span>
                                <h3 className="ann-detail-headline">{selectedAnnouncement.title}</h3>
                            </div>
                            <button className="icon-btn" onClick={() => setSelectedAnnouncement(null)}>
                                <X size={20} />
                            </button>
                        </div>

                        <div className="ann-detail-body">
                            <div className="ann-meta-grid">
                                <div className="meta-block">
                                    <span className="meta-lbl">Dissemination Time</span>
                                    <span className="meta-val">
                                        <Clock size={14} /> {formatDateTime(selectedAnnouncement.primary_timestamp)}
                                    </span>
                                </div>
                                <div className="meta-block">
                                    <span className="meta-lbl">Exchanges</span>
                                    <span className="meta-val">
                                        {selectedAnnouncement.has_nse && <span className="badge-nse">NSE</span>}
                                        {selectedAnnouncement.has_bse && <span className="badge-bse">BSE</span>}
                                    </span>
                                </div>
                                <div className="meta-block">
                                    <span className="meta-lbl">ISIN</span>
                                    <span className="meta-val font-mono">{selectedAnnouncement.isin}</span>
                                </div>
                                <div className="meta-block">
                                    <span className="meta-lbl">Categories</span>
                                    <span className="meta-val">
                                        {selectedAnnouncement.categories.join(', ')}
                                    </span>
                                </div>
                            </div>

                            {/* Summary Text / Body */}
                            {selectedAnnouncement.summary_text && (
                                <div className="ann-detail-text-box">
                                    <h4>Filing Summary / Details</h4>
                                    <p>{selectedAnnouncement.summary_text}</p>
                                </div>
                            )}

                            {/* Child Revisions / Corrigendums */}
                            {selectedAnnouncement.child_revisions && selectedAnnouncement.child_revisions.length > 0 && (
                                <div className="ann-revisions-chain-box">
                                    <h4>⚠️ Corrigendums / Amendments Linked to this Filing</h4>
                                    <ul>
                                        {selectedAnnouncement.child_revisions.map((rev) => (
                                            <li key={rev.canonical_id}>
                                                <strong>[{rev.revision_type}]</strong> {rev.title} (
                                                {formatDateTime(rev.primary_timestamp)})
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}

                            {/* Raw Exchange Sources Audit Table */}
                            <div className="ann-sources-audit-box">
                                <h4>Exchange Ingestions & Timestamps</h4>
                                <table className="audit-table">
                                    <thead>
                                        <tr>
                                            <th>Exchange</th>
                                            <th>Filing / Source ID</th>
                                            <th>Dissemination Time (IST)</th>
                                            <th>Time Difference</th>
                                            <th>XBRL</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {selectedAnnouncement.sources.map((src) => (
                                            <tr key={src.source_id}>
                                                <td>
                                                    <span className={src.exchange === 'NSE' ? 'badge-nse' : 'badge-bse'}>
                                                        {src.exchange}
                                                    </span>
                                                </td>
                                                <td className="font-mono">{src.source_announcement_id}</td>
                                                <td>{formatDateTime(src.dissemination_timestamp)}</td>
                                                <td>{src.time_difference || '--'}</td>
                                                <td>{src.has_xbrl ? 'Yes' : 'No'}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>

                            {/* PDF Attachments Section */}
                            {selectedAnnouncement.attachments && selectedAnnouncement.attachments.length > 0 && (
                                <div className="ann-pdf-preview-box">
                                    <div className="pdf-box-header">
                                        <h4>Official Exchange PDF Attachments</h4>
                                        <div className="pdf-switch-tabs">
                                            {selectedAnnouncement.attachments.map((att, idx) => (
                                                <button
                                                    key={att.attachment_id}
                                                    className={`pdf-tab-btn ${previewAttachment?.attachment_id === att.attachment_id ? 'active' : ''}`}
                                                    onClick={() => setPreviewAttachment(att)}
                                                >
                                                    {att.exchange} Attachment #{idx + 1}
                                                </button>
                                            ))}
                                        </div>
                                    </div>

                                    {previewAttachment && (
                                        <div className="pdf-frame-container">
                                            <div className="pdf-actions-bar">
                                                <span>
                                                    <strong>File:</strong> {previewAttachment.original_file_name} ({previewAttachment.lifecycle_stage})
                                                </span>
                                                <a
                                                    href={getAttachmentViewUrl(previewAttachment.attachment_id)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="ann-btn ann-btn-secondary"
                                                >
                                                    <ExternalLink size={14} /> Open in New Tab
                                                </a>
                                            </div>
                                            <iframe
                                                src={getAttachmentViewUrl(previewAttachment.attachment_id)}
                                                title="Filing Attachment Preview"
                                                className="pdf-iframe-view"
                                            />
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>

                        <div className="modal-footer">
                            <button className="btn-secondary" onClick={() => setSelectedAnnouncement(null)}>
                                Close
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
