import React, { useState, useEffect } from 'react';
import { useSearchParams, useNavigate, Link } from 'react-router-dom';
import { UserPlus, Shield, CheckCircle, AlertCircle, ArrowRight, Loader2 } from 'lucide-react';
import { API_BASE_URL } from '../api/client';
import './RegisterInvitePage.css';

const RegisterInvitePage = () => {
    const [searchParams] = useSearchParams();
    const token = searchParams.get('token');
    const navigate = useNavigate();

    const [loading, setLoading] = useState(true);
    const [submitting, setSubmitting] = useState(false);
    const [inviteData, setInviteData] = useState(null);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(false);

    const [formData, setFormData] = useState({
        username: '',
        password: '',
        confirmPassword: ''
    });

    useEffect(() => {
        if (!token) {
            setError("Missing invite token.");
            setLoading(false);
            return;
        }

        const verifyToken = async () => {
            try {
                const resp = await fetch(`${API_BASE_URL}/admin/public/invites/${token}`);
                if (resp.ok) {
                    setInviteData(await resp.json());
                } else {
                    const err = await resp.json();
                    setError(err.detail || "Invalid or expired invite.");
                }
            } catch (err) {
                setError("Connection failed.");
            } finally {
                setLoading(false);
            }
        };

        verifyToken();
    }, [token]);

    const handleSubmit = async (e) => {
        e.preventDefault();
        if (formData.password !== formData.confirmPassword) {
            setError("Passwords do not match.");
            return;
        }

        setSubmitting(true);
        setError(null);
        
        try {
            const resp = await fetch(`${API_BASE_URL}/admin/public/invites/register`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    token: token,
                    username: formData.username,
                    password: formData.password
                })
            });

            if (resp.ok) {
                setSuccess(true);
            } else {
                const err = await resp.json();
                setError(err.detail || "Registration failed.");
            }
        } catch (err) {
            setError("Connection failed.");
        } finally {
            setSubmitting(false);
        }
    };

    if (loading) {
        return (
            <div className="register-invite-container">
                <div className="loading-box">
                    <Loader2 className="spinner" size={48} />
                    <p>Securing your workspace...</p>
                </div>
            </div>
        );
    }

    if (success) {
        return (
            <div className="register-invite-container">
                <div className="glass-card success-card">
                    <CheckCircle size={64} className="text-success" />
                    <h1>Account Verified</h1>
                    <p>Welcome to MF Analytics. Your secure portal access is now ready.</p>
                    <Link to="/login" className="btn-primary">
                        <span>Sign In Now</span>
                        <ArrowRight size={18} />
                    </Link>
                </div>
            </div>
        );
    }

    return (
        <div className="register-invite-container">
            <div className="glass-card">
                <div className="card-header">
                    <div className="badge-wrapper">
                        <Shield size={24} />
                    </div>
                    <h1>Join the Portal</h1>
                    <p className="subtitle">Secure guest onboarding for <strong>{inviteData?.email}</strong></p>
                </div>

                {error && (
                    <div className="alert-error">
                        <AlertCircle size={18} />
                        <span>{error}</span>
                    </div>
                )}

                <form onSubmit={handleSubmit} className="register-form">
                    <div className="form-group">
                        <label>Choose Username</label>
                        <input
                            type="text"
                            required
                            placeholder="johndoe"
                            value={formData.username}
                            onChange={e => setFormData({ ...formData, username: e.target.value })}
                        />
                    </div>
                    <div className="form-group">
                        <label>Secure Password</label>
                        <input
                            type="password"
                            required
                            placeholder="••••••••"
                            value={formData.password}
                            onChange={e => setFormData({ ...formData, password: e.target.value })}
                        />
                    </div>
                    <div className="form-group">
                        <label>Confirm Password</label>
                        <input
                            type="password"
                            required
                            placeholder="••••••••"
                            value={formData.confirmPassword}
                            onChange={e => setFormData({ ...formData, confirmPassword: e.target.value })}
                        />
                    </div>

                    <button type="submit" className="btn-primary" disabled={submitting}>
                        {submitting ? 'Creating Secure Account...' : 'Complete Registration'}
                        {!submitting && <UserPlus size={18} />}
                    </button>
                    
                    <p className="privacy-note">
                        By registering, you agree to the portal privacy and data usage terms.
                    </p>
                </form>
            </div>
            
            <div className="background-decor">
                <div className="blob blob-primary"></div>
                <div className="blob blob-secondary"></div>
            </div>
        </div>
    );
};

export default RegisterInvitePage;
