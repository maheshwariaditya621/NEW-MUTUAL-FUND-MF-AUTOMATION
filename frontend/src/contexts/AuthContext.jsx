import React, { createContext, useState, useContext, useEffect } from 'react';
import { API_BASE_URL } from '../api/client';

const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
    const [user, setUser] = useState(null);
    const [token, setToken] = useState(localStorage.getItem('token'));
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        // Validate token and fetch user on load
        const validateToken = async () => {
            if (token) {
                try {
                    const response = await fetch(`${API_BASE_URL}/auth/me`, {
                        headers: {
                            'Authorization': `Bearer ${token}`
                        }
                    });

                    if (response.ok) {
                        const userData = await response.json();
                        setUser(userData);
                    } else {
                        // Token invalid or expired
                        logout();
                    }
                } catch (error) {
                    console.error("Auth validation failed:", error);
                    // Don't logout on network error, just stop loading
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
    };

    const logout = () => {
        setToken(null);
        setUser(null);
        localStorage.removeItem('token');
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
