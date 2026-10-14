import React, { useEffect, useState } from 'react';
import { UserCheck, ShieldCheck, AlertCircle, Calendar, ExternalLink } from 'lucide-react';
import { getSchemeFundManagers } from '../api/schemes';
import FundManagerModal from './FundManagerModal';
import './FundManagerCard.css';

export default function FundManagerCard({ schemeId, onSelectScheme }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [activeManagerId, setActiveManagerId] = useState(null);

    useEffect(() => {
        if (!schemeId) return;
        setLoading(true);
        setError(null);
        getSchemeFundManagers(schemeId)
            .then(res => setData(res))
            .catch(err => {
                // If 404 or missing, gracefully show unassigned rather than crashing
                setData(null);
            })
            .finally(() => setLoading(false));
    }, [schemeId]);

    if (loading) {
        return (
            <div className="fmc-container" style={{ opacity: 0.6 }}>
                <div className="fmc-title">
                    <UserCheck size={14} />
                    <span>Loading Fund Managers...</span>
                </div>
            </div>
        );
    }

    const managers = data?.managers || [];
    const hasManagers = managers.length > 0;

    return (
        <>
            <div className="fmc-container">
                <div className="fmc-header">
                    <div className="fmc-header-left">
                        <span className="fmc-title">
                            <UserCheck size={14} />
                            <span>Fund Manager{managers.length > 1 ? 's' : ''}</span>
                        </span>
                        {hasManagers ? (
                            <span className="fmc-badge fmc-badge-verified" title="Verified against SEBI Scheme Summary Document (SSD) filing">
                                <ShieldCheck size={12} />
                                <span>Verified AMFI</span>
                            </span>
                        ) : (
                            <span className="fmc-badge fmc-badge-unverified" title="Disclosure pending verification">
                                <AlertCircle size={12} />
                                <span>Pending Verification</span>
                            </span>
                        )}
                    </div>
                    {data?.source_url && (
                        <a
                            href={data.source_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            style={{ fontSize: '11px', color: '#3b82f6', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: '4px' }}
                            title="View source scheme summary disclosure"
                        >
                            <span>Source Doc</span>
                            <ExternalLink size={11} />
                        </a>
                    )}
                </div>

                <div className="fmc-managers-grid">
                    {managers.map(mgr => {
                        const initials = mgr.name
                            .split(' ')
                            .map(n => n[0])
                            .slice(0, 2)
                            .join('')
                            .toUpperCase();

                        return (
                            <div
                                key={mgr.manager_id}
                                className="fmc-manager-chip"
                                onClick={() => setActiveManagerId(mgr.manager_id)}
                                title="Click to view manager profile and all managed schemes"
                            >
                                <div className="fmc-avatar-circle">{initials}</div>
                                <div className="fmc-chip-details">
                                    <span className="fmc-manager-name">{mgr.name}</span>
                                    <span className="fmc-manager-tenure">
                                        {mgr.tenure_start_date ? (
                                            <>
                                                <Calendar size={10} />
                                                <span>Since {new Date(mgr.tenure_start_date).toLocaleDateString('en-IN', { month: 'short', year: 'numeric' })}</span>
                                            </>
                                        ) : (
                                            <span>Fund Manager</span>
                                        )}
                                    </span>
                                </div>
                            </div>
                        );
                    })}

                    {!hasManagers && (
                        <div className="fmc-empty-state">
                            <span>No verified manager assignments currently recorded for this scheme variant.</span>
                        </div>
                    )}
                </div>
            </div>

            {activeManagerId && (
                <FundManagerModal
                    managerId={activeManagerId}
                    onClose={() => setActiveManagerId(null)}
                    onSelectScheme={onSelectScheme}
                />
            )}
        </>
    );
}
