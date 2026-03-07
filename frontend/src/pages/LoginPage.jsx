import React, { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { apiPostForm } from '../api/client';
import './LoginPage.css';

const LoginPage = () => {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
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
            // FastAPI OAuth2PasswordRequestForm expects x-www-form-urlencoded
            const formData = new URLSearchParams();
            formData.append('username', username);
            formData.append('password', password);

            const response = await apiPostForm('/auth/login', formData);

            // response contains { access_token, token_type }
            // Now fetch user details
            const userResponse = await fetch(`${import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'}/api/v1/auth/me`, {
                headers: {
                    'Authorization': `Bearer ${response.access_token}`
                }
            });

            if (userResponse.ok) {
                const userData = await userResponse.json();
                login(response.access_token, userData);
                navigate(from, { replace: true });
            } else {
                throw new Error('Failed to fetch user details');
            }
        } catch (err) {
            console.error('Login error:', err);
            setError(err.message || 'Invalid username or password');
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="login-container">
            <div className="login-card">
                <div className="login-header">
                    <h1 className="login-logo">MF-AUTO</h1>
                    <p className="login-subtitle">Secure Portfolio Intelligence</p>
                </div>

                {error && <div className="error-message">{error}</div>}

                <form className="login-form" onSubmit={handleSubmit}>
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
                        <label htmlFor="password">Password</label>
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

                    <button
                        type="submit"
                        className="login-button"
                        disabled={isLoading}
                    >
                        {isLoading ? (
                            <span className="loading-text">Authenticating...</span>
                        ) : (
                            'Sign In to Dashboard'
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
