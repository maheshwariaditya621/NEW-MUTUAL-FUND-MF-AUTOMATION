import React, { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { apiPostForm, API_BASE_URL } from '../api/client';
import { ShieldCheck, Lock, AlertCircle, Key, ArrowRight } from 'lucide-react';
import './LoginPage.css';

const LoginPage = () => {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [totpCode, setTotpCode] = useState('');
    const [totpRequired, setTotpRequired] = useState(false);
    const [error, setError] = useState('');
    const [isLoading, setIsLoading] = useState(false);

    const { login } = useAuth();
    const navigate = useNavigate();
    const location = useLocation();

    // Redirect path after login
    const from = location.state?.from?.pathname || '/';

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        setIsLoading(true);

        try {
            const formData = new URLSearchParams();
            formData.append('username', username);
            formData.append('password', password);

            // If TOTP is required, append it to the URL as a query param
            let loginUrl = '/auth/login';
            if (totpRequired && totpCode) {
                loginUrl += `?code=${totpCode}`;
            }

            // Using fetch directly for more granular status code handling
            const response = await fetch(`${API_BASE_URL}${loginUrl}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: formData
            });

            const data = await response.json();

            if (response.status === 401 && data.detail === "TOTP_REQUIRED") {
                setTotpRequired(true);
                setIsLoading(false);
                return;
            }

            if (!response.ok) {
                if (response.status === 423) {
                    throw new Error(data.detail); // Brute force lock message
                }
                throw new Error(data.detail || 'Invalid credentials');
            }

            // Successfully logged in
            const userResponse = await fetch(`${API_BASE_URL}/auth/me`, {
                headers: { 'Authorization': `Bearer ${data.access_token}` }
            });

            if (userResponse.ok) {
                const userData = await userResponse.json();
                login(data.access_token, userData);
                navigate(from, { replace: true });
            } else {
                throw new Error('Failed to fetch user details');
            }
        } catch (err) {
            console.error('Login error:', err);
            setError(err.message || 'Authentication failed');
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="login-container">
            <div className="login-card">
                <div className="login-header">
                    <div className="login-icon-wrapper">
                        {totpRequired ? <ShieldCheck size={40} className="totp-icon" /> : <h1 className="login-logo">MF-AUTO</h1>}
                    </div>
                    <h1 className="login-title">{totpRequired ? 'Second Factor' : 'Welcome Back'}</h1>
                    <p className="login-subtitle">
                        {totpRequired ? 'Enter the code from your authenticator app' : 'Secure Portfolio Intelligence'}
                    </p>
                </div>

                {error && (
                    <div className="error-message">
                        <AlertCircle size={16} />
                        <span>{error}</span>
                    </div>
                )}

                <form className="login-form" onSubmit={handleSubmit}>
                    {!totpRequired ? (
                        <>
                            <div className="form-group">
                                <label htmlFor="username">Username</label>
                                <div className="input-wrapper">
                                    <input
                                        id="username"
                                        type="text"
                                        value={username}
                                        onChange={(e) => setUsername(e.target.value)}
                                        placeholder="Enter your username"
                                        required
                                        autoComplete="username"
                                    />
                                </div>
                            </div>

                            <div className="form-group">
                                <div className="label-row">
                                    <label htmlFor="password">Password</label>
                                    <button 
                                        type="button" 
                                        className="forgot-link"
                                        onClick={() => alert("🔑 Password Reset\n\nPlease contact the System Administrator to reset your password. The administrator can override your credentials from the Admin Vault.")}
                                    >
                                        Forgot Password?
                                    </button>
                                </div>
                                <div className="input-wrapper">
                                    <input
                                        id="password"
                                        type="password"
                                        value={password}
                                        onChange={(e) => setPassword(e.target.value)}
                                        placeholder="••••••••"
                                        required
                                        autoComplete="current-password"
                                    />
                                </div>
                            </div>
                        </>
                    ) : (
                        <div className="form-group">
                            <label htmlFor="totp">One-Time Password (2FA)</label>
                            <div className="input-wrapper">
                                <input
                                    id="totp"
                                    type="text"
                                    value={totpCode}
                                    onChange={(e) => setTotpCode(e.target.value)}
                                    placeholder="000000"
                                    required
                                    autoFocus
                                    inputMode="numeric"
                                    pattern="[0-9]*"
                                    maxLength="6"
                                />
                            </div>
                        </div>
                    )}

                    <button
                        type="submit"
                        className={`login-button ${totpRequired ? 'totp-btn' : ''}`}
                        disabled={isLoading}
                    >
                        {isLoading ? (
                            <span className="loading-text">Authenticating...</span>
                        ) : (
                            <div className="btn-content">
                                <span>{totpRequired ? 'Verify & Sign In' : 'Sign In to Dashboard'}</span>
                                <ArrowRight size={18} />
                            </div>
                        )}
                    </button>
                </form>

                <div className="login-footer">
                    <p>&copy; {new Date().getFullYear()} MF Automation Platform</p>
                </div>
            </div>
        </div>
    );
};

export default LoginPage;
