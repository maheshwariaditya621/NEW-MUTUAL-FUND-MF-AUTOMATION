import React, { useState, useEffect } from 'react';
import './FundManagerAdmin.css';
import { 
    Users, RefreshCw, CheckCircle, AlertTriangle, ShieldCheck, 
    ArrowUpRight, Clock, Database, Check, X, FileText, Play, ExternalLink
} from 'lucide-react';

const FundManagerAdmin = ({ password }) => {
    const [statusData, setStatusData] = useState(null);
    const [loadingStatus, setLoadingStatus] = useState(false);
    const [activeSubTab, setActiveSubTab] = useState('queue'); // 'queue' | 'history'
    
    // Queue state
    const [queueFilter, setQueueFilter] = useState('pending'); // 'pending' | 'approved' | 'rejected'
    const [queueItems, setQueueItems] = useState([]);
    const [loadingQueue, setLoadingQueue] = useState(false);
    
    // Trigger Sync state
    const [syncing, setSyncing] = useState(false);
    const [syncResult, setSyncResult] = useState(null);

    // Review Modal State
    const [reviewItem, setReviewItem] = useState(null);
    const [targetAmfiCode, setTargetAmfiCode] = useState('');
    const [reviewerNotes, setReviewerNotes] = useState('');
    const [isConfirmed, setIsConfirmed] = useState(false);
    const [submittingReview, setSubmittingReview] = useState(false);

    const API_BASE = '/api/v1/admin/fund-manager-sync';

    const fetchStatus = async () => {
        setLoadingStatus(true);
        try {
            const resp = await fetch(`${API_BASE}/status`, {
                headers: { 'X-Admin-Secret': password }
            });
            if (resp.ok) {
                const data = await resp.json();
                setStatusData(data);
            }
        } catch (err) {
            console.error('Failed to fetch FM sync status:', err);
        } finally {
            setLoadingStatus(false);
        }
    };

    const fetchQueue = async (filter = queueFilter) => {
        setLoadingQueue(true);
        try {
            const resp = await fetch(`${API_BASE}/unmatched?status=${filter}&limit=100`, {
                headers: { 'X-Admin-Secret': password }
            });
            if (resp.ok) {
                const data = await resp.json();
                setQueueItems(data.items || []);
            }
        } catch (err) {
            console.error('Failed to fetch unmatched queue:', err);
        } finally {
            setLoadingQueue(false);
        }
    };

    useEffect(() => {
        fetchStatus();
    }, [password]);

    useEffect(() => {
        if (activeSubTab === 'queue') {
            fetchQueue(queueFilter);
        }
    }, [queueFilter, activeSubTab]);

    const handleRunSync = async (dryRun = false) => {
        const confirmMsg = dryRun 
            ? 'Run dry-run comparison against AMFI SSD? (No database changes will be written)'
            : 'Trigger automated fund manager synchronization? This will download the latest AMFI SSD and update scheme assignments.';
        
        if (!window.confirm(confirmMsg)) return;

        setSyncing(true);
        setSyncResult(null);
        try {
            const resp = await fetch(`${API_BASE}/run?dry_run=${dryRun}`, {
                method: 'POST',
                headers: { 'X-Admin-Secret': password }
            });
            const data = await resp.json();
            setSyncResult({ dryRun, data });
            // Refresh status after 2 seconds
            setTimeout(() => {
                fetchStatus();
                if (activeSubTab === 'queue') fetchQueue(queueFilter);
            }, 2000);
        } catch (err) {
            alert('Failed to trigger synchronization: ' + err.message);
        } finally {
            setSyncing(false);
        }
    };

    const openReviewModal = (item) => {
        setReviewItem(item);
        setTargetAmfiCode(item.candidate_source_code || '');
        setReviewerNotes('');
        setIsConfirmed(false);
    };

    const handleReviewSubmit = async (action) => {
        if (action === 'approve' && !isConfirmed) {
            alert('Please check the confirmation box before approving.');
            return;
        }

        setSubmittingReview(true);
        try {
            const resp = await fetch(`${API_BASE}/review`, {
                method: 'POST',
                headers: {
                    'X-Admin-Secret': password,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    queue_id: reviewItem.queue_id,
                    action: action,
                    target_amfi_code: targetAmfiCode || null,
                    reviewer_notes: reviewerNotes || `Action: ${action} via Admin Console`
                })
            });

            if (resp.ok) {
                setReviewItem(null);
                fetchQueue(queueFilter);
                fetchStatus();
            } else {
                const err = await resp.json();
                alert(`Review submission failed: ${err.detail || 'Unknown error'}`);
            }
        } catch (err) {
            alert('Failed to submit review: ' + err.message);
        } finally {
            setSubmittingReview(false);
        }
    };

    const lastRun = statusData?.last_run;

    return (
        <div className="fm-admin-container">
            {/* Header & Primary Actions */}
            <div className="fm-admin-header">
                <div className="fm-admin-title-group">
                    <h2><Users size={22} color="#0ea5e9" /> Fund Manager Intelligence Console</h2>
                    <p>Automated weekly synchronization & verified scheme manager mapping</p>
                </div>
                <div className="fm-admin-actions">
                    <button 
                        className="fm-sync-btn secondary" 
                        onClick={() => { fetchStatus(); if (activeSubTab === 'queue') fetchQueue(); }}
                        disabled={loadingStatus || syncing}
                    >
                        <RefreshCw size={15} className={loadingStatus ? 'spin' : ''} />
                        Refresh
                    </button>
                    <button 
                        className="fm-sync-btn secondary" 
                        onClick={() => handleRunSync(true)}
                        disabled={syncing}
                    >
                        <Play size={15} />
                        Dry-Run Sync
                    </button>
                    <button 
                        className="fm-sync-btn primary" 
                        onClick={() => handleRunSync(false)}
                        disabled={syncing}
                    >
                        <Database size={15} />
                        {syncing ? 'Running Sync...' : 'Run Sync Now'}
                    </button>
                </div>
            </div>

            {/* Sync Result Banner */}
            {syncResult && (
                <div style={{
                    padding: '1rem',
                    borderRadius: '8px',
                    background: syncResult.dryRun ? 'rgba(14, 165, 233, 0.15)' : 'rgba(34, 197, 94, 0.15)',
                    border: `1px solid ${syncResult.dryRun ? '#0ea5e9' : '#22c55e'}`,
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center'
                }}>
                    <div>
                        <strong>{syncResult.dryRun ? '🔍 Dry-Run Output:' : '⚡ Background Sync Triggered:'}</strong>
                        <span style={{ marginLeft: '8px' }}>
                            {syncResult.data.message || JSON.stringify(syncResult.data)}
                        </span>
                        {syncResult.data.report && (
                            <div style={{ fontSize: '0.85rem', marginTop: '4px', opacity: 0.9 }}>
                                Matched: {syncResult.data.report.matched_schemes} schemes | 
                                Exact: {syncResult.data.report.exact_identifier_matches} | 
                                Unmatched: {syncResult.data.report.unmatched_schemes}
                            </div>
                        )}
                    </div>
                    <button 
                        className="icon-btn" 
                        onClick={() => setSyncResult(null)}
                        style={{ background: 'none', border: 'none', color: 'inherit', cursor: 'pointer' }}
                    >
                        <X size={18} />
                    </button>
                </div>
            )}

            {/* Metrics Overview Grid */}
            <div className="fm-stats-grid">
                <div className={`fm-stat-card ${statusData?.health === 'healthy' ? 'health-ok' : 'health-warn'}`}>
                    <span className="label">Scheme Coverage</span>
                    <div className="val">{statusData?.coverage_percent || 0}%</div>
                    <span className="sub">
                        {statusData?.matched_schemes || 0} / {statusData?.total_local_schemes || 0} schemes matched
                    </span>
                </div>

                <div className="fm-stat-card">
                    <span className="label">Fund Managers Tracked</span>
                    <div className="val">{statusData?.total_fund_managers || 0}</div>
                    <span className="sub">Canonical manager identities</span>
                </div>

                <div className="fm-stat-card">
                    <span className="label">Review Queue</span>
                    <div className="val" style={{ color: statusData?.review_queue?.pending > 0 ? '#f59e0b' : 'inherit' }}>
                        {statusData?.review_queue?.pending || 0}
                    </div>
                    <span className="sub">
                        {statusData?.review_queue?.approved || 0} approved | {statusData?.review_queue?.rejected || 0} rejected
                    </span>
                </div>

                <div className="fm-stat-card">
                    <span className="label">Last Synchronization</span>
                    <div className="val" style={{ fontSize: '1.25rem', color: lastRun?.status === 'success' ? '#22c55e' : '#f59e0b' }}>
                        {lastRun ? lastRun.status.toUpperCase() : 'NEVER'}
                    </div>
                    <span className="sub">
                        {lastRun ? new Date(lastRun.started_at).toLocaleString() : 'No execution logged'}
                    </span>
                </div>
            </div>

            {/* Sub-Navigation Tabs */}
            <div className="fm-subnav">
                <button 
                    className={`fm-subnav-btn ${activeSubTab === 'queue' ? 'active' : ''}`}
                    onClick={() => setActiveSubTab('queue')}
                >
                    <AlertTriangle size={16} />
                    Unmatched Review Queue
                    {(statusData?.review_queue?.pending || 0) > 0 && (
                        <span className="fm-badge-count">{statusData.review_queue.pending}</span>
                    )}
                </button>
                <button 
                    className={`fm-subnav-btn ${activeSubTab === 'history' ? 'active' : ''}`}
                    onClick={() => setActiveSubTab('history')}
                >
                    <Clock size={16} />
                    Sync Execution History
                </button>
            </div>

            {/* View 1: Review Queue */}
            {activeSubTab === 'queue' && (
                <div>
                    <div className="fm-queue-header">
                        <div className="fm-queue-filters">
                            {['pending', 'approved', 'rejected'].map(f => (
                                <button
                                    key={f}
                                    className={`fm-pill-btn ${queueFilter === f ? 'active' : ''}`}
                                    onClick={() => setQueueFilter(f)}
                                >
                                    {f.charAt(0).toUpperCase() + f.slice(1)}
                                </button>
                            ))}
                        </div>
                        <span style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>
                            Showing {queueItems.length} records
                        </span>
                    </div>

                    <div className="fm-table-wrap">
                        {loadingQueue ? (
                            <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-secondary)' }}>
                                Loading review queue...
                            </div>
                        ) : queueItems.length === 0 ? (
                            <div style={{ padding: '3rem', textAlign: 'center', color: 'var(--text-secondary)' }}>
                                <CheckCircle size={32} color="#22c55e" style={{ marginBottom: '0.5rem' }} />
                                <div>No records in the {queueFilter} queue.</div>
                            </div>
                        ) : (
                            <table className="fm-table">
                                <thead>
                                    <tr>
                                        <th>Local Scheme</th>
                                        <th>Candidate AMFI Scheme</th>
                                        <th>Candidate AMFI Code</th>
                                        <th>Match Reason</th>
                                        <th>Confidence</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {queueItems.map(item => (
                                        <tr key={item.queue_id}>
                                            <td>
                                                <div className="fm-scheme-title">{item.local_scheme_name || `Scheme #${item.scheme_id}`}</div>
                                                <div className="fm-scheme-sub">
                                                    {item.plan_type} • {item.option_type}
                                                </div>
                                            </td>
                                            <td>
                                                <div className="fm-source-cand">
                                                    {item.candidate_source_name || 'No Candidate Found'}
                                                </div>
                                                <div className="fm-scheme-sub">
                                                    AMC: {item.source_amc || 'N/A'} {item.candidate_isin ? `• ISIN: ${item.candidate_isin}` : ''}
                                                </div>
                                            </td>
                                            <td>
                                                <span style={{ fontFamily: 'monospace', fontWeight: 600 }}>
                                                    {item.candidate_source_code || '—'}
                                                </span>
                                            </td>
                                            <td>
                                                <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                                                    {item.match_reason}
                                                </span>
                                            </td>
                                            <td>
                                                <span className={`fm-conf-pill ${item.confidence_score >= 0.7 ? 'fm-conf-high' : 'fm-conf-med'}`}>
                                                    {Math.round((item.confidence_score || 0) * 100)}%
                                                </span>
                                            </td>
                                            <td>
                                                {item.review_status === 'pending' ? (
                                                    <div className="fm-action-group">
                                                        <button 
                                                            className="fm-btn-review"
                                                            onClick={() => openReviewModal(item)}
                                                        >
                                                            Review & Map
                                                        </button>
                                                        <button 
                                                            className="fm-btn-reject"
                                                            onClick={() => {
                                                                if (window.confirm('Reject this candidate match?')) {
                                                                    setReviewItem(item);
                                                                    handleReviewSubmit('reject');
                                                                }
                                                            }}
                                                        >
                                                            Reject
                                                        </button>
                                                    </div>
                                                ) : (
                                                    <span style={{ 
                                                        textTransform: 'uppercase', 
                                                        fontSize: '0.75rem', 
                                                        fontWeight: 700,
                                                        color: item.review_status === 'approved' ? '#22c55e' : '#ef4444'
                                                    }}>
                                                        {item.review_status}
                                                    </span>
                                                )}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        )}
                    </div>
                </div>
            )}

            {/* View 2: Sync Execution History */}
            {activeSubTab === 'history' && (
                <div className="fm-table-wrap">
                    <table className="fm-table">
                        <thead>
                            <tr>
                                <th>Execution ID</th>
                                <th>Timestamp</th>
                                <th>Status</th>
                                <th>Source Records</th>
                                <th>Exact Matches</th>
                                <th>Verified Matches</th>
                                <th>Queue</th>
                                <th>Additions / Dep.</th>
                            </tr>
                        </thead>
                        <tbody>
                            {(statusData?.recent_runs || []).map(run => (
                                <tr key={run.job_id}>
                                    <td style={{ fontFamily: 'monospace', fontWeight: 600 }}>
                                        #{run.job_id}
                                    </td>
                                    <td>
                                        <div>{new Date(run.started_at).toLocaleString()}</div>
                                        {run.finished_at && (
                                            <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                                                Duration: {Math.max(1, Math.round((new Date(run.finished_at) - new Date(run.started_at)) / 1000))}s
                                            </div>
                                        )}
                                    </td>
                                    <td>
                                        <span style={{
                                            padding: '0.2rem 0.5rem',
                                            borderRadius: '4px',
                                            fontSize: '0.75rem',
                                            fontWeight: 700,
                                            background: run.status === 'success' ? 'rgba(34, 197, 94, 0.15)' : 'rgba(239, 68, 68, 0.15)',
                                            color: run.status === 'success' ? '#22c55e' : '#ef4444'
                                        }}>
                                            {run.status.toUpperCase()}
                                        </span>
                                    </td>
                                    <td>{(run.records_processed || 0).toLocaleString()}</td>
                                    <td>{(run.exact_matches || 0).toLocaleString()}</td>
                                    <td>{(run.verified_matches || 0).toLocaleString()}</td>
                                    <td>
                                        <span style={{ color: run.unmatched_records > 0 ? '#f59e0b' : 'inherit' }}>
                                            {run.unmatched_records || 0}
                                        </span>
                                    </td>
                                    <td>
                                        <span style={{ color: '#22c55e' }}>+{run.manager_additions || 0}</span> / 
                                        <span style={{ color: '#ef4444', marginLeft: '4px' }}>-{run.manager_departures || 0}</span>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}

            {/* Review & Approval Modal */}
            {reviewItem && (
                <div className="fm-modal-backdrop" onClick={() => !submittingReview && setReviewItem(null)}>
                    <div className="fm-modal-card" onClick={e => e.stopPropagation()}>
                        <div className="fm-modal-header">
                            <h3><ShieldCheck size={18} color="#0ea5e9" /> Confirm Scheme Mapping</h3>
                            <button 
                                className="icon-btn" 
                                onClick={() => setReviewItem(null)}
                                disabled={submittingReview}
                                style={{ background: 'none', border: 'none', color: 'inherit', cursor: 'pointer' }}
                            >
                                <X size={20} />
                            </button>
                        </div>

                        <div className="fm-modal-body">
                            <div className="fm-compare-grid">
                                <div className="fm-compare-side">
                                    <label>Local Database Scheme</label>
                                    <div className="title">{reviewItem.local_scheme_name || `Scheme #${reviewItem.scheme_id}`}</div>
                                    <div className="item">Scheme ID: #{reviewItem.scheme_id}</div>
                                    <div className="item">Plan: {reviewItem.plan_type}</div>
                                    <div className="item">Option: {reviewItem.option_type}</div>
                                </div>

                                <div className="fm-compare-side">
                                    <label>AMFI Candidate Record</label>
                                    <div className="title" style={{ color: '#38bdf8' }}>
                                        {reviewItem.candidate_source_name || 'Manual Mapping'}
                                    </div>
                                    <div className="item">AMC: {reviewItem.source_amc || 'N/A'}</div>
                                    <div className="item">Candidate AMFI Code: {reviewItem.candidate_source_code || 'None'}</div>
                                    <div className="item">Candidate ISIN: {reviewItem.candidate_isin || 'None'}</div>
                                </div>
                            </div>

                            <div>
                                <label style={{ display: 'block', fontSize: '0.85rem', fontWeight: 600, marginBottom: '0.4rem' }}>
                                    Verified AMFI Scheme Code to Assign:
                                </label>
                                <input 
                                    type="text" 
                                    className="admin-input"
                                    value={targetAmfiCode}
                                    onChange={e => setTargetAmfiCode(e.target.value)}
                                    placeholder="e.g. 118989"
                                    style={{
                                        width: '100%',
                                        padding: '0.6rem 0.8rem',
                                        background: 'rgba(255,255,255,0.05)',
                                        border: '1px solid var(--border-color)',
                                        borderRadius: '6px',
                                        color: 'var(--text-primary)',
                                        fontFamily: 'monospace',
                                        fontSize: '0.95rem'
                                    }}
                                />
                                <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '0.25rem', display: 'block' }}>
                                    Assigning this code will link the scheme to official AMFI Scheme Summary Document records.
                                </span>
                            </div>

                            <div>
                                <label style={{ display: 'block', fontSize: '0.85rem', fontWeight: 600, marginBottom: '0.4rem' }}>
                                    Reviewer Audit Notes:
                                </label>
                                <textarea 
                                    className="admin-input"
                                    rows="2"
                                    value={reviewerNotes}
                                    onChange={e => setReviewerNotes(e.target.value)}
                                    placeholder="e.g. Verified against AMFI official scheme master portal"
                                    style={{
                                        width: '100%',
                                        padding: '0.6rem 0.8rem',
                                        background: 'rgba(255,255,255,0.05)',
                                        border: '1px solid var(--border-color)',
                                        borderRadius: '6px',
                                        color: 'var(--text-primary)',
                                        fontSize: '0.85rem'
                                    }}
                                />
                            </div>

                            <label className="fm-confirm-box" style={{ cursor: 'pointer' }}>
                                <input 
                                    type="checkbox" 
                                    checked={isConfirmed}
                                    onChange={e => setIsConfirmed(e.target.checked)}
                                    style={{ marginTop: '2px' }}
                                />
                                <span>
                                    <strong>Explicit Audit Confirmation:</strong> I have verified that this local scheme corresponds to the official AMFI scheme code and approve importing its fund manager disclosures.
                                </span>
                            </label>
                        </div>

                        <div className="fm-modal-footer">
                            <button 
                                className="fm-sync-btn secondary" 
                                onClick={() => setReviewItem(null)}
                                disabled={submittingReview}
                            >
                                Cancel
                            </button>
                            <button 
                                className="fm-sync-btn primary" 
                                onClick={() => handleReviewSubmit('approve')}
                                disabled={!isConfirmed || submittingReview || !targetAmfiCode}
                            >
                                {submittingReview ? 'Approving...' : 'Approve & Map'}
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default FundManagerAdmin;
