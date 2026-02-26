import React, { useState, useEffect } from 'react';
import './AdminVault.css';

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

    const API_BASE = 'http://localhost:8000/api/v1/admin';

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
                <button
                    className={`tab-btn ${activeTab === 'merges' ? 'active' : ''}`}
                    onClick={() => setActiveTab('merges')}
                >
                    Scheme Merges
                </button>
                <button
                    className={`tab-btn ${activeTab === 'alerts' ? 'active' : ''}`}
                    onClick={() => setActiveTab('alerts')}
                >
                    System Alerts
                </button>
                <button
                    className={`tab-btn ${activeTab === 'data' ? 'active' : ''}`}
                    onClick={() => setActiveTab('data')}
                >
                    Data Management
                </button>
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
                                        <th>AMC & Type</th>
                                        <th>Old Scheme</th>
                                        <th>New Name detected</th>
                                        <th>Confidence</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {merges.map(m => (
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
            </div>
        </div>
    );
};

export default AdminVault;
