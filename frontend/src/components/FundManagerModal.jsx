import React, { useEffect, useState } from 'react';
import { X, UserCheck, ShieldCheck, Briefcase, Calendar, Layers, ExternalLink } from 'lucide-react';
import { getFundManagerProfile } from '../api/schemes';
import Loading from './common/Loading';
import ErrorMessage from './common/ErrorMessage';
import './FundManagerModal.css';

export default function FundManagerModal({ managerId, onClose, onSelectScheme }) {
    const [profile, setProfile] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!managerId) return;
        setLoading(true);
        setError(null);
        getFundManagerProfile(managerId)
            .then(data => setProfile(data))
            .catch(err => setError(err.message || "Failed to load manager profile"))
            .finally(() => setLoading(false));
    }, [managerId]);

    // Handle ESC key to close
    useEffect(() => {
        const handleKeyDown = (e) => {
            if (e.key === 'Escape') onClose();
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [onClose]);

    if (!managerId) return null;

    const initials = profile?.name
        ? profile.name.split(' ').map(n => n[0]).slice(0, 2).join('').toUpperCase()
        : 'FM';

    return (
        <div className="fmm-overlay" onClick={onClose} role="dialog" aria-modal="true" aria-label="Fund Manager Profile">
            <div className="fmm-modal" onClick={e => e.stopPropagation()}>
                <div className="fmm-header">
                    <div className="fmm-header-left">
                        <div className="fmm-avatar">{initials}</div>
                        <div className="fmm-title-group">
                            <h3>{profile ? profile.name : "Fund Manager Profile"}</h3>
                            <div className="fmm-subtitle">
                                <ShieldCheck size={13} color="#10b981" />
                                <span>Verified SEBI / AMFI Disclosure</span>
                            </div>
                        </div>
                    </div>
                    <button className="fmm-close-btn" onClick={onClose} aria-label="Close modal">
                        <X size={20} />
                    </button>
                </div>

                <div className="fmm-body">
                    {loading && <Loading message="Loading manager profile and portfolios..." />}
                    {error && <ErrorMessage message={error} />}

                    {profile && !loading && !error && (
                        <>
                            <div className="fmm-kpi-row">
                                <div className="fmm-kpi-card">
                                    <div className="fmm-kpi-label">Active Schemes</div>
                                    <div className="fmm-kpi-value">{profile.active_schemes_count}</div>
                                </div>
                                <div className="fmm-kpi-card">
                                    <div className="fmm-kpi-label">Active Equity AUM</div>
                                    <div className="fmm-kpi-value">
                                        {profile.total_active_aum_cr > 0 ? `₹${profile.total_active_aum_cr.toLocaleString('en-IN')} Cr` : '—'}
                                    </div>
                                </div>
                                <div className="fmm-kpi-card">
                                    <div className="fmm-kpi-label">Historical Mandates</div>
                                    <div className="fmm-kpi-value">{profile.historical_schemes_count}</div>
                                </div>
                            </div>

                            <div className="fmm-section-title">
                                <Layers size={16} />
                                <span>Currently Managed Mutual Fund Schemes ({profile.active_schemes.length})</span>
                            </div>

                            <div className="fmm-schemes-list">
                                {profile.active_schemes.map(scheme => (
                                    <div key={scheme.scheme_id} className="fmm-scheme-item">
                                        <div className="fmm-scheme-info">
                                            <div className="fmm-scheme-name" title={scheme.scheme_name}>
                                                {scheme.scheme_name}
                                            </div>
                                            <div className="fmm-scheme-sub">
                                                <span>{scheme.amc_name}</span>
                                                <span>•</span>
                                                <span>{scheme.plan_type} - {scheme.option_type}</span>
                                                {scheme.tenure_start_date && (
                                                    <>
                                                        <span>•</span>
                                                        <span style={{ color: '#10b981' }}>
                                                            Since {scheme.tenure_start_date}
                                                        </span>
                                                    </>
                                                )}
                                                {scheme.latest_aum_cr && (
                                                    <>
                                                        <span>•</span>
                                                        <span>₹{scheme.latest_aum_cr.toLocaleString('en-IN')} Cr AUM</span>
                                                    </>
                                                )}
                                            </div>
                                        </div>
                                        <button
                                            type="button"
                                            className="fmm-view-btn"
                                            onClick={() => {
                                                if (onSelectScheme) {
                                                    onSelectScheme(scheme.scheme_id);
                                                }
                                                onClose();
                                            }}
                                        >
                                            View Portfolio
                                        </button>
                                    </div>
                                ))}

                                {profile.active_schemes.length === 0 && (
                                    <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-secondary)' }}>
                                        No active schemes currently assigned to this manager identity.
                                    </div>
                                )}
                            </div>
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}
