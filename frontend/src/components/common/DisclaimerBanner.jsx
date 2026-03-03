import React, { useState, useEffect } from 'react';
import { X, ShieldAlert } from 'lucide-react';
import './DisclaimerBanner.css';

const STORAGE_KEY = 'mf_disclaimer_dismissed';
const AUTO_DISMISS_MS = 15000;

const DisclaimerBanner = () => {
    const [visible, setVisible] = useState(false);
    const [progress, setProgress] = useState(100);

    useEffect(() => {
        const alreadyDismissed = localStorage.getItem(STORAGE_KEY);
        if (!alreadyDismissed) {
            setVisible(true);
        }
    }, []);

    useEffect(() => {
        if (!visible) return;

        // Countdown progress bar
        const interval = setInterval(() => {
            setProgress(prev => {
                if (prev <= 0) return 0;
                return prev - (100 / (AUTO_DISMISS_MS / 100));
            });
        }, 100);

        // Auto dismiss after 15 seconds
        const timer = setTimeout(() => {
            handleDismiss();
        }, AUTO_DISMISS_MS);

        return () => {
            clearInterval(interval);
            clearTimeout(timer);
        };
    }, [visible]);

    const handleDismiss = () => {
        setVisible(false);
        localStorage.setItem(STORAGE_KEY, 'true');
    };

    if (!visible) return null;

    return (
        <div className="disclaimer-overlay">
            <div className="disclaimer-card">
                {/* Header */}
                <div className="disclaimer-header">
                    <div className="disclaimer-title-row">
                        <ShieldAlert size={20} className="disclaimer-icon" />
                        <span className="disclaimer-title">Data Disclaimer</span>
                    </div>
                    <button className="disclaimer-close" onClick={handleDismiss} aria-label="Close disclaimer">
                        <X size={18} />
                    </button>
                </div>

                {/* Body */}
                <div className="disclaimer-body">
                    <p>
                        The data, information, and statistics presented on this platform have been gathered from sources
                        believed to be highly reliable. All reasonable precautions have been taken to ensure accuracy,
                        however <strong>no representations or warranties</strong> (express or implied) are made as to the
                        reliability, accuracy, or completeness of such information.
                    </p>
                    <p>
                        This platform is intended <strong>for informational purposes only</strong> and does not constitute
                        financial advice or an offer to buy or sell any securities. The platform shall not be held liable
                        for any loss arising directly or indirectly from the use of, or any action taken in reliance on,
                        any information appearing herein.
                    </p>
                </div>

                {/* Auto-dismiss progress bar */}
                <div className="disclaimer-footer">
                    <span className="disclaimer-timer-label">Auto-closing in 15s</span>
                    <button className="disclaimer-understood-btn" onClick={handleDismiss}>
                        I Understand
                    </button>
                </div>
                <div className="disclaimer-progress-track">
                    <div
                        className="disclaimer-progress-bar"
                        style={{ width: `${progress}%` }}
                    />
                </div>
            </div>
        </div>
    );
};

export default DisclaimerBanner;
