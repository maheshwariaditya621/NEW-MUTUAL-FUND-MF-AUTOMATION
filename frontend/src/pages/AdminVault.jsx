import React, { useState, useEffect, useMemo } from 'react';
import './AdminVault.css';
import FileManagement from './admin/FileManagement';

const AdminVault = () => {
    const [password, setPassword] = useState('');
    const [isAuthenticated, setIsAuthenticated] = useState(false);
    const [stats, setStats] = useState(null);
    const [merges, setMerges] = useState([]);
    const [alerts, setAlerts] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [activeTab, setActiveTab] = useState('merges');
    const [uploading, setUploading] = useState(false);
    const [syncing, setSyncing] = useState(false);
    const [uploadResult, setUploadResult] = useState(null);
    const [syncResult, setSyncResult] = useState(null);
    const [sortConfig, setSortConfig] = useState({ key: 'confidence', direction: 'desc' });

    // Extraction state
    const ALL_SLUGS = [
        'abakkus', 'absl', 'angelone', 'axis', 'bajaj', 'bandhan', 'baroda',
        'boi', 'canara', 'capitalmind', 'choice', 'dsp', 'edelweiss', 'franklin',
        'groww', 'hdfc', 'helios', 'hsbc', 'icici', 'invesco', 'iti', 'jio_br',
        'jmfinancial', 'kotak', 'lic', 'mahindra', 'mirae_asset', 'motilal',
        'navi', 'nippon', 'nj', 'old_bridge', 'pgim_india', 'ppfas', 'quant',
        'quantum', 'samco', 'sbi', 'shriram', 'sundaram', 'tata', 'taurus',
        'threesixtyone', 'trust', 'unifi', 'union', 'uti', 'wealth_company',
        'whiteoak', 'zerodha'
    ];
    const now = new Date();
    const [selectedSlugs, setSelectedSlugs] = useState([]);
    const [selectedSteps, setSelectedSteps] = useState(['download', 'merge', 'extract']);
    const [extYear, setExtYear] = useState(now.getFullYear());
    const [extMonth, setExtMonth] = useState(now.getMonth() + 1);
    const [isDryRun, setIsDryRun] = useState(true);
    const [isRedo, setIsRedo] = useState(false);
    const [extJobs, setExtJobs] = useState([]);
    const [triggering, setTriggering] = useState(false);
    const [activeJobId, setActiveJobId] = useState(null);

    const API_BASE = '/api/v1/admin';

    const handleLogin = (e) => {
        e.preventDefault();
        // We'll verify by trying to fetch stats
        fetchStats(password);
    };

    const fetchStats = async (pass) => {
        setLoading(true);
        try {
            const resp = await fetch(`${API_BASE}/stats`, {
                headers: { 'X-Admin-Secret': pass }
            });
            if (resp.ok) {
                const data = await resp.json();
                setStats(data);
                setIsAuthenticated(true);
                setError(null);
            } else {
                setError('Invalid Secret Key');
            }
        } catch (err) {
            setError('Failed to connect to API');
        } finally {
            setLoading(false);
        }
    };

    const fetchData = async () => {
        if (!isAuthenticated) return;
        const pass = password;
        setLoading(true);
        try {
            if (activeTab === 'merges') {
                const resp = await fetch(`${API_BASE}/pending-merges`, {
                    headers: { 'X-Admin-Secret': pass }
                });
                const data = await resp.json();
                setMerges(data);
            } else if (activeTab === 'alerts') {
                const resp = await fetch(`${API_BASE}/alerts`, {
                    headers: { 'X-Admin-Secret': pass }
                });
                const data = await resp.json();
                setAlerts(data);
            }
            // Also refresh stats
            const statsResp = await fetch(`${API_BASE}/stats`, {
                headers: { 'X-Admin-Secret': pass }
            });
            const statsData = await statsResp.json();
            setStats(statsData);
        } catch (err) {
            setError('Refresh failed');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        if (isAuthenticated) fetchData();
    }, [isAuthenticated, activeTab]);

    // Poll active extraction job
    useEffect(() => {
        if (!activeJobId) return;
        const interval = setInterval(async () => {
            try {
                const resp = await fetch(`${API_BASE}/extraction-jobs/${activeJobId}`, {
                    headers: { 'X-Admin-Secret': password }
                });
                const data = await resp.json();
                setExtJobs(prev => prev.map(j => j.job_id === activeJobId ? data : j));
                if (data.status === 'completed') {
                    setActiveJobId(null);
                    clearInterval(interval);
                }
            } catch { }
        }, 2000);
        return () => clearInterval(interval);
    }, [activeJobId, password]);

    const handleApprove = async (mergeId) => {
        const pass = password;
        try {
            const resp = await fetch(`${API_BASE}/approve-merge/${mergeId}`, {
                method: 'POST',
                headers: { 'X-Admin-Secret': pass }
            });
            if (resp.ok) {
                alert('Approved successfully!');
                fetchData();
            }
        } catch (err) {
            alert('Approval failed');
        }
    };

    const handleReject = async (mergeId) => {
        const pass = password;
        try {
            const resp = await fetch(`${API_BASE}/reject-merge/${mergeId}`, {
                method: 'POST',
                headers: { 'X-Admin-Secret': pass }
            });
            if (resp.ok) {
                alert('Rejected');
                fetchData();
            }
        } catch (err) {
            alert('Rejection failed');
        }
    };

    const handleSort = (key) => {
        setSortConfig(prev => {
            if (prev.key !== key) return { key, direction: 'desc' };
            if (prev.direction === 'desc') return { key, direction: 'asc' };
            return { key: null, direction: 'desc' };
        });
    };

    const sortedMerges = useMemo(() => {
        let items = [...merges];
        if (!sortConfig.key) return items;

        items.sort((a, b) => {
            let aVal, bVal;
            if (sortConfig.key === 'confidence') {
                aVal = a.confidence_score;
                bVal = b.confidence_score;
            } else if (sortConfig.key === 'amc') {
                aVal = a.amc_name;
                bVal = b.amc_name;
            } else if (sortConfig.key === 'old_name') {
                aVal = a.old_scheme_name;
                bVal = b.old_scheme_name;
            } else if (sortConfig.key === 'new_name') {
                aVal = a.new_scheme_name;
                bVal = b.new_scheme_name;
            }

            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });
        return items;
    }, [merges, sortConfig]);

    const renderSortArrow = (key) => {
        if (sortConfig.key !== key) return null;
        return <span style={{ marginLeft: '4px' }}>{sortConfig.direction === 'asc' ? '↑' : '↓'}</span>;
    };

    if (!isAuthenticated) {
        return (
            <div className="admin-login-overlay">
                <div className="admin-login-box">
                    <div className="admin-icon">🔐</div>
                    <h2>Admin Vault</h2>
                    <p>Protected Management Console</p>
                    <form onSubmit={handleLogin}>
                        <input
                            type="password"
                            placeholder="Enter Secret Key"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            autoFocus
                        />
                        <button type="submit" disabled={loading}>
                            {loading ? 'Verifying...' : 'Unlock Vault'}
                        </button>
                    </form>
                    {error && <div className="error-msg">{error}</div>}
                </div>
            </div>
        );
    }

    return (
        <div className="admin-container">
            <header className="admin-vault-header">
                <div className="header-left">
                    <h1>Admin Command Center</h1>
                    <span className="badge status-live">SYSTEM LIVE</span>
                </div>
                <div className="header-right">
                    <button className="btn-secondary" onClick={() => {
                        setIsAuthenticated(false);
                    }}>Lock Vault</button>
                </div>
            </header>

            <div className="admin-stats-grid">
                <div className="stat-card">
                    <label>Pending Merges</label>
                    <div className="value">{stats?.pending_merges_count || 0}</div>
                </div>
                <div className="stat-card">
                    <label>Total Schemes</label>
                    <div className="value">{stats?.total_schemes || 0}</div>
                </div>
                <div className="stat-card">
                    <label>Last Load Status</label>
                    <div className={`value ${stats?.last_extraction_status === 'SUCCESS' ? 'text-success' : 'text-danger'}`}>
                        {stats?.last_extraction_status || 'NONE'}
                    </div>
                </div>
                <div className="stat-card">
                    <label>24h Alerts</label>
                    <div className="value">{stats?.error_count_24h || 0}</div>
                </div>
            </div>

            <div className="admin-tabs">
                <button className={`tab-btn ${activeTab === 'merges' ? 'active' : ''}`} onClick={() => setActiveTab('merges')}>Scheme Merges</button>
                <button className={`tab-btn ${activeTab === 'alerts' ? 'active' : ''}`} onClick={() => setActiveTab('alerts')}>System Alerts</button>
                <button className={`tab-btn ${activeTab === 'files' ? 'active' : ''}`} onClick={() => setActiveTab('files')}>📁 File Manager</button>
                <button className={`tab-btn ${activeTab === 'extraction' ? 'active' : ''}`} onClick={() => setActiveTab('extraction')}>🚀 Pipeline Control</button>
            </div>

            <div className="tab-content">
                {activeTab === 'merges' && (
                    <div className="merges-list">
                        {merges.length === 0 ? (
                            <div className="empty-state">No pending renames detected.</div>
                        ) : (
                            <table className="admin-table">
                                <thead>
                                    <tr>
                                        <th className="sortable" onClick={() => handleSort('amc')}>AMC & Type {renderSortArrow('amc')}</th>
                                        <th className="sortable" onClick={() => handleSort('old_name')}>Old Scheme {renderSortArrow('old_name')}</th>
                                        <th className="sortable" onClick={() => handleSort('new_name')}>New Name detected {renderSortArrow('new_name')}</th>
                                        <th className="sortable" onClick={() => handleSort('confidence')}>Confidence {renderSortArrow('confidence')}</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {sortedMerges.map(m => (
                                        <tr key={m.merge_id}>
                                            <td>
                                                <div className="amc-label">{m.amc_name}</div>
                                                <div className="type-label">{m.plan_type} | {m.option_type}</div>
                                            </td>
                                            <td className="old-name">{m.old_scheme_name}</td>
                                            <td className="new-name">{m.new_scheme_name}</td>
                                            <td>
                                                <div className="confidence-pill" style={{
                                                    backgroundColor: `rgba(0, 255, 0, ${m.confidence_score * 0.2})`,
                                                    border: `1px solid rgba(0, 255, 0, ${m.confidence_score})`
                                                }}>
                                                    {(m.confidence_score * 100).toFixed(0)}%
                                                </div>
                                                <div className="score-details">
                                                    ISIN: {m.metadata.isin_score}% | Weight: {m.metadata.weight_score}%
                                                </div>
                                            </td>
                                            <td>
                                                <div className="action-btns">
                                                    <button className="btn-approve" onClick={() => handleApprove(m.merge_id)}>Approve</button>
                                                    <button className="btn-reject" onClick={() => handleReject(m.merge_id)}>Ignore</button>
                                                </div>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        )}
                    </div>
                )}

                {activeTab === 'alerts' && (
                    <div className="alerts-list">
                        {alerts.map(a => (
                            <div key={a.notification_id} className={`alert-item level-${a.level.toLowerCase()}`}>
                                <div className="alert-meta">
                                    <span className={`alert-badge ${a.level.toLowerCase()}`}>{a.level}</span>
                                    <span className="alert-time">{new Date(a.created_at).toLocaleString()}</span>
                                    <span className="alert-category">{a.category}</span>
                                </div>
                                <div className="alert-content">{a.content}</div>
                            </div>
                        ))}
                    </div>
                )}

                {activeTab === 'data' && (
                    <div className="data-management">
                        <div className="uploader-card">
                            <h3>Ace Equity Ingestor</h3>
                            <p>Upload the latest market cap and classification Excel exported from Ace Equity Next.</p>

                            <div className="upload-zone">
                                <input
                                    type="file"
                                    id="ace-upload"
                                    accept=".xlsx, .xls"
                                    onChange={async (e) => {
                                        const file = e.target.files[0];
                                        if (!file) return;

                                        const formData = new FormData();
                                        formData.append('file', file);

                                        setUploading(true);
                                        setError(null);
                                        setUploadResult(null);

                                        try {
                                            const resp = await fetch(`${API_BASE}/upload-ace-data`, {
                                                method: 'POST',
                                                headers: { 'X-Admin-Secret': password },
                                                body: formData
                                            });
                                            const res = await resp.json();
                                            if (resp.ok) {
                                                setUploadResult(res);
                                            } else {
                                                setError(res.detail || 'Upload failed');
                                            }
                                        } catch (err) {
                                            setError('Connection error');
                                        } finally {
                                            setUploading(false);
                                        }
                                    }}
                                />
                                <label htmlFor="ace-upload" className={uploading ? 'uploading' : ''}>
                                    {uploading ? '⚙️ Processing Data...' : '📂 Choose Excel File'}
                                </label>
                            </div>

                            {uploadResult && (
                                <div className="upload-success">
                                    <h4>✅ Ingestion Complete</h4>
                                    <div className="result-stats">
                                        <div className="stat-row">
                                            <span>Total Rows:</span>
                                            <strong>{uploadResult.processed_count || uploadResult.updates_count + uploadResult.skipped_count}</strong>
                                        </div>
                                        <div className="stat-row">
                                            <span>DB Updates:</span>
                                            <strong className="text-success">{uploadResult.updates_count}</strong>
                                        </div>
                                        <div className="stat-row">
                                            <span>Skipped (no match):</span>
                                            <strong className="text-warning">{uploadResult.skipped_count}</strong>
                                        </div>
                                    </div>
                                </div>
                            )}
                        </div>

                        <div className="uploader-card" style={{ marginTop: '20px' }}>
                            <h3>Automated Shares Sync (yFinance)</h3>
                            <p>Fetch the most recent 'Shares Outstanding' for all Mutual Fund stock holdings directly from live market APIs.</p>

                            <div className="upload-zone">
                                <button
                                    className={`btn-approve ${syncing ? 'uploading' : ''}`}
                                    disabled={syncing}
                                    style={{ padding: '12px 24px', fontSize: '1rem' }}
                                    onClick={async () => {
                                        setSyncing(true);
                                        setError(null);
                                        setSyncResult(null);

                                        try {
                                            const resp = await fetch(`${API_BASE}/sync-shares`, {
                                                method: 'POST',
                                                headers: { 'X-Admin-Secret': password }
                                            });
                                            const res = await resp.json();
                                            if (resp.ok) {
                                                setSyncResult(res);
                                            } else {
                                                setError(res.detail || 'Sync failed');
                                            }
                                        } catch (err) {
                                            setError('Connection error');
                                        } finally {
                                            setSyncing(false);
                                        }
                                    }}
                                >
                                    {syncing ? '🔄 Syncing Live Shares...' : '⚡ Trigger Global Sync'}
                                </button>
                            </div>

                            {syncResult && (
                                <div className="upload-success">
                                    <h4>✅ Live Sync Complete</h4>
                                    <div className="result-stats">
                                        <div className="stat-row">
                                            <span>Total Companies Updated:</span>
                                            <strong className="text-success">{syncResult.updated_count}</strong>
                                        </div>
                                        <div className="stat-row">
                                            <span>Status:</span>
                                            <strong className="text-success">SUCCESS</strong>
                                        </div>
                                    </div>
                                </div>
                            )}
                        </div>

                        <div className="info-card">
                            <h4>Expected Columns</h4>
                            <ul>
                                <li><strong>ISIN</strong> (Mandatory)</li>
                                <li><strong>Market Cap</strong> (Full Value)</li>
                                <li><strong>Classification</strong> (Large/Mid/Small)</li>
                            </ul>
                        </div>
                    </div>
                )}
                {activeTab === 'files' && (
                    <FileManagement adminPassword={password} />
                )}

                {activeTab === 'extraction' && (
                    <div className="data-management">
                        <div className="uploader-card">
                            <h3>🚀 Pipeline Control Panel</h3>
                            <p>Configure and trigger the full data lifecycle: Download → Merge → Extract → Load.</p>

                            {/* Steps Selection */}
                            <div style={{ marginTop: '16px', padding: '12px', background: 'rgba(99,102,241,0.05)', borderRadius: '8px', border: '1px solid rgba(99,102,241,0.2)' }}>
                                <strong style={{ display: 'block', marginBottom: '8px', fontSize: '0.9rem' }}>Pipeline Stages to Run</strong>
                                <div style={{ display: 'flex', gap: '20px' }}>
                                    {[
                                        { id: 'download', label: '📥 Download Raw Files' },
                                        { id: 'merge', label: '🔀 Merge Excels' },
                                        { id: 'extract', label: '⚡ Extract & Load DB' }
                                    ].map(step => (
                                        <label key={step.id} style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontSize: '0.9rem' }}>
                                            <input
                                                type="checkbox"
                                                checked={selectedSteps.includes(step.id)}
                                                onChange={e => {
                                                    if (e.target.checked) setSelectedSteps(p => [...p, step.id]);
                                                    else setSelectedSteps(p => p.filter(s => s !== step.id));
                                                }}
                                            />
                                            {step.label}
                                        </label>
                                    ))}
                                </div>
                            </div>

                            {/* AMC Selection */}
                            <div style={{ marginTop: '16px' }}>
                                <div style={{ display: 'flex', gap: '8px', marginBottom: '10px', alignItems: 'center' }}>
                                    <strong>AMCs ({selectedSlugs.length}/{ALL_SLUGS.length} selected)</strong>
                                    <button className="btn-secondary" style={{ padding: '4px 12px', fontSize: '0.8rem' }}
                                        onClick={() => setSelectedSlugs([...ALL_SLUGS])}>Select All</button>
                                    <button className="btn-secondary" style={{ padding: '4px 12px', fontSize: '0.8rem' }}
                                        onClick={() => setSelectedSlugs([])}>Clear</button>
                                </div>
                                <div style={{
                                    display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(130px, 1fr))',
                                    gap: '6px', maxHeight: '200px', overflowY: 'auto',
                                    border: '1px solid #333', borderRadius: '8px', padding: '10px'
                                }}>
                                    {ALL_SLUGS.map(slug => (
                                        <label key={slug} style={{
                                            display: 'flex', alignItems: 'center', gap: '6px',
                                            cursor: 'pointer', fontSize: '0.82rem',
                                            padding: '4px 6px', borderRadius: '4px',
                                            background: selectedSlugs.includes(slug) ? 'rgba(99,102,241,0.2)' : 'transparent',
                                            border: selectedSlugs.includes(slug) ? '1px solid #6366f1' : '1px solid transparent'
                                        }}>
                                            <input type="checkbox" checked={selectedSlugs.includes(slug)}
                                                onChange={e => {
                                                    if (e.target.checked) setSelectedSlugs(p => [...p, slug]);
                                                    else setSelectedSlugs(p => p.filter(s => s !== slug));
                                                }} />
                                            {slug}
                                        </label>
                                    ))}
                                </div>
                            </div>

                            {/* Period + Options */}
                            <div style={{ display: 'flex', gap: '16px', marginTop: '16px', flexWrap: 'wrap', alignItems: 'flex-end' }}>
                                <div>
                                    <label style={{ fontSize: '0.85rem', display: 'block', marginBottom: '4px' }}>Year</label>
                                    <input type="number" value={extYear} onChange={e => setExtYear(+e.target.value)}
                                        min="2020" max="2030" style={{
                                            padding: '8px 12px', borderRadius: '6px', border: '1px solid #444',
                                            background: '#1a1a2e', color: '#fff', width: '90px'
                                        }} />
                                </div>
                                <div>
                                    <label style={{ fontSize: '0.85rem', display: 'block', marginBottom: '4px' }}>Month</label>
                                    <select value={extMonth} onChange={e => setExtMonth(+e.target.value)}
                                        style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #444', background: '#1a1a2e', color: '#fff' }}>
                                        {['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                                            .map((m, i) => <option key={i + 1} value={i + 1}>{m}</option>)}
                                    </select>
                                </div>
                                <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer' }}>
                                    <input type="checkbox" checked={isDryRun} onChange={e => setIsDryRun(e.target.checked)} />
                                    <span style={{ fontSize: '0.9rem' }}>Dry Run (no DB write)</span>
                                </label>
                                <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer' }}>
                                    <input type="checkbox" checked={isRedo} onChange={e => setIsRedo(e.target.checked)} />
                                    <span style={{ fontSize: '0.9rem' }}>Redo (overwrites)</span>
                                </label>
                            </div>

                            {/* Trigger Button */}
                            <button
                                className="btn-approve"
                                disabled={triggering || selectedSlugs.length === 0 || selectedSteps.length === 0}
                                style={{ marginTop: '16px', padding: '12px 28px', fontSize: '1rem', background: 'linear-gradient(135deg, #6366f1, #a855f7)' }}
                                onClick={async () => {
                                    setTriggering(true);
                                    try {
                                        const resp = await fetch(`${API_BASE}/trigger-pipeline`, {
                                            method: 'POST',
                                            headers: { 'X-Admin-Secret': password, 'Content-Type': 'application/json' },
                                            body: JSON.stringify({
                                                amc_slugs: selectedSlugs,
                                                year: extYear, month: extMonth,
                                                steps: selectedSteps,
                                                dry_run: isDryRun, redo: isRedo
                                            })
                                        });
                                        const data = await resp.json();
                                        if (resp.ok) {
                                            const newJob = {
                                                ...data, status: 'queued', results: {}, done: 0,
                                                total: selectedSlugs.length, amc_slugs: selectedSlugs,
                                                year: extYear, month: extMonth, steps: selectedSteps,
                                                dry_run: isDryRun, redo: isRedo,
                                                started_at: new Date().toISOString()
                                            };
                                            setExtJobs(prev => [newJob, ...prev]);
                                            setActiveJobId(data.job_id);
                                        }
                                    } catch { alert('Failed to trigger'); }
                                    finally { setTriggering(false); }
                                }}
                            >
                                {triggering ? '🔄 Starting...' : `🚀 Run Pipeline (${selectedSlugs.length} AMCs)`}
                            </button>
                        </div>

                        {/* Jobs Panel */}
                        {extJobs.length > 0 && (
                            <div className="uploader-card" style={{ marginTop: '20px' }}>
                                <h3>Job History</h3>
                                {extJobs.map(job => (
                                    <div key={job.job_id} style={{
                                        border: '1px solid #333', borderRadius: '8px',
                                        padding: '12px', marginBottom: '12px'
                                    }}>
                                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                                            <div>
                                                <strong>Job #{job.job_id}</strong>
                                                <span style={{ marginLeft: '10px', fontSize: '0.8rem', color: '#aaa' }}>
                                                    {job.year}-{String(job.month).padStart(2, '0')} • {job.dry_run ? 'Dry Run' : 'Load Mode'} {job.redo ? '• Redo' : ''}
                                                </span>
                                            </div>
                                            <span style={{
                                                padding: '3px 10px', borderRadius: '20px', fontSize: '0.8rem',
                                                background: job.status === 'completed' ? '#16a34a33' : job.status === 'running' ? '#b45309' + '33' : '#6366f133',
                                                color: job.status === 'completed' ? '#4ade80' : job.status === 'running' ? '#fbbf24' : '#a5b4fc'
                                            }}>{job.status.toUpperCase()}</span>
                                        </div>

                                        {/* Progress bar */}
                                        <div style={{ background: '#1e1e2e', borderRadius: '4px', height: '6px', marginBottom: '8px' }}>
                                            <div style={{
                                                background: 'linear-gradient(90deg, #6366f1, #8b5cf6)',
                                                width: `${job.total > 0 ? (job.done / job.total) * 100 : 0}%`,
                                                height: '100%', borderRadius: '4px', transition: 'width 0.3s'
                                            }} />
                                        </div>
                                        <div style={{ fontSize: '0.8rem', color: '#aaa', marginBottom: '8px' }}>
                                            {job.done}/{job.total} AMCs processed
                                        </div>

                                        {/* Results table */}
                                        {Object.keys(job.results).length > 0 && (
                                            <div style={{ marginTop: '10px', overflowX: 'auto' }}>
                                                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
                                                    <thead>
                                                        <tr style={{ borderBottom: '1px solid #333', color: '#888' }}>
                                                            <th style={{ textAlign: 'left', padding: '4px 8px' }}>AMC</th>
                                                            <th style={{ textAlign: 'center', padding: '4px 8px' }}>Steps</th>
                                                            <th style={{ textAlign: 'center', padding: '4px 8px' }}>Read</th>
                                                            <th style={{ textAlign: 'center', padding: '4px 8px' }}>Loaded</th>
                                                            <th style={{ textAlign: 'left', padding: '4px 8px' }}>Notes</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {Object.entries(job.results).map(([slug, res]) => {
                                                            const steps = res.steps || {};
                                                            const extractRes = steps.extract || {};
                                                            const isErr = res.status === 'error' || extractRes.status === 'error';

                                                            const getStepColor = (stepKey) => {
                                                                const s = steps[stepKey];
                                                                if (!s) return '#334155'; // Not requested
                                                                if (s.status === 'success' || s.status === 'completed' || s.status === 'skipped') return '#4ade80';
                                                                if (s.status === 'failed' || s.status === 'error') return '#f87171';
                                                                return '#fbbf24'; // pending/other
                                                            };

                                                            return (
                                                                <tr key={slug} style={{ borderBottom: '1px solid #1e293b' }}>
                                                                    <td style={{ padding: '5px 8px', fontWeight: 600 }}>{slug}</td>
                                                                    <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                                                                        <div style={{ display: 'flex', gap: '4px', justifyContent: 'center' }}>
                                                                            <span title="Download" style={{ width: '8px', height: '8px', borderRadius: '50%', background: getStepColor('download') }} />
                                                                            <span title="Merge" style={{ width: '8px', height: '8px', borderRadius: '50%', background: getStepColor('merge') }} />
                                                                            <span title="Extract" style={{ width: '8px', height: '8px', borderRadius: '50%', background: getStepColor('extract') }} />
                                                                        </div>
                                                                    </td>
                                                                    <td style={{ padding: '5px 8px', textAlign: 'center', color: extractRes.rows_read > 0 ? '#4ade80' : '#555' }}>
                                                                        {extractRes.rows_read ?? '—'}
                                                                    </td>
                                                                    <td style={{ padding: '5px 8px', textAlign: 'center', color: extractRes.rows_inserted > 0 ? '#4ade80' : '#555' }}>
                                                                        {extractRes.rows_inserted ?? '—'}
                                                                    </td>
                                                                    <td style={{ padding: '5px 8px', color: isErr ? '#f87171' : '#666', maxWidth: '200px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                                                        {isErr ? (res.error || extractRes.error || 'Err') : (steps.download?.reason || steps.merge?.error || extractRes.message || '')}
                                                                    </td>
                                                                </tr>
                                                            );
                                                        })}
                                                    </tbody>
                                                </table>
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
};

export default AdminVault;
