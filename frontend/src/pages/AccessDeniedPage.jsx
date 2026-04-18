import React from 'react';
import { Link } from 'react-router-dom';
import { ShieldAlert, Home, Mail } from 'lucide-react';
import './AccessDeniedPage.css';

const AccessDeniedPage = () => {
    return (
        <div className="access-denied-container">
            <div className="glass-card">
                <div className="icon-wrapper">
                    <ShieldAlert size={64} className="error-icon" />
                </div>
                
                <h1>Access Restricted</h1>
                
                <div className="message-section">
                    <p>Your access period has ended or you do not have permission for this feature.</p>
                    <p className="sub-message">Please contact the administrator to renew your access or unlock this feature.</p>
                </div>

                <div className="action-buttons">
                    <Link to="/" className="btn-primary">
                        <Home size={18} />
                        <span>Return Home</span>
                    </Link>
                    <a href="mailto:admin@mf-analytics.com" className="btn-secondary">
                        <Mail size={18} />
                        <span>Contact Admin</span>
                    </a>
                </div>
                
                <div className="footer-note">
                    Secure Multi-User Portal • MF Analytics v2.0
                </div>
            </div>
            
            <div className="background-decor">
                <div className="blob blob-1"></div>
                <div className="blob blob-2"></div>
            </div>
        </div>
    );
};

export default AccessDeniedPage;
