import React, { createContext, useState, useContext, useEffect } from 'react';
import { API_BASE_URL } from '../api/client';

const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
    const [user, setUser] = useState(() => {
        try {
            const saved = localStorage.getItem('user');
            return saved ? JSON.parse(saved) : null;
        } catch {
            return null;
        }
    });
    const [token, setToken] = useState(localStorage.getItem('token'));
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        // Validate token and fetch user on load
        const validateToken = async () => {
            if (token) {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 3500);
                try {
                    const response = await fetch(`${API_BASE_URL}/auth/me`, {
                        signal: controller.signal,
                        headers: {
                            'Authorization': `Bearer ${token}`
                        }
                    });
                    clearTimeout(timeoutId);

                    if (response.ok) {
                        const userData = await response.json();
                        setUser(userData);
                        localStorage.setItem('user', JSON.stringify(userData));
                    } else if (response.status === 401) {
                        // Token invalid or expired
                        logout();
                    }
                } catch (error) {
                    clearTimeout(timeoutId);
                    console.error("Auth validation failed:", error);
                    // Don't logout on network error or timeout, preserve cached user
                }
            }
            setLoading(false);
        };

        validateToken();
    }, [token]);

    const login = (newToken, userData) => {
        setToken(newToken);
        setUser(userData);
        localStorage.setItem('token', newToken);
        if (userData) localStorage.setItem('user', JSON.stringify(userData));
    };

    const logout = () => {
        setToken(null);
        setUser(null);
        localStorage.removeItem('token');
        localStorage.removeItem('user');
    };

    /**
     * Check if the current user has a specific permission.
     * Admin role or "all" permission bypasses all specific checks.
     */
    const hasPermission = (permission) => {
        if (!user) return false;
        if (user.role === 'admin') return true;
        const perms = user.permissions || [];
        if (perms.includes('all')) return true;
        return perms.includes(permission);
    };

    return (
        <AuthContext.Provider value={{ 
            user, 
            token, 
            loading, 
            login, 
            logout, 
            hasPermission,
            isAuthenticated: !!user,
            isAdmin: user?.role === 'admin'
        }}>
            {children}
        </AuthContext.Provider>
    );
};

export const useAuth = () => {
    const context = useContext(AuthContext);
    if (!context) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
};
