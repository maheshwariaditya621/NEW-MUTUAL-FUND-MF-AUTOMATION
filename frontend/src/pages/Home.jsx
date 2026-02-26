import { Link } from 'react-router-dom';
import { TrendingUp, PieChart, Lightbulb, Activity, Database, ShieldCheck } from 'lucide-react';
import './Home.css';

export default function Home() {
    return (
        <div className="home-wrapper">
            {/* Dynamic Animated Background for Hero */}
            <div className="hero-section">
                <div className="hero-bg-shapes">
                    <div className="shape shape-1"></div>
                    <div className="shape shape-2"></div>
                    <div className="shape shape-3"></div>
                </div>

                <div className="container hero-content">
                    <div className="hero-badge">Next-Generation Platform</div>
                    <h1 className="hero-title">
                        Smarter Insights for <br />
                        <span className="hero-title-highlight">Mutual Fund Analytics</span>
                    </h1>
                    <p className="hero-subtitle">
                        Track portfolio changes, analyze fund manager behavior, and make data-driven investment decisions with our advanced search and visualization tools.
                    </p>

                    <div className="hero-stats">
                        <div className="stat-item">
                            <span className="stat-number">40+</span>
                            <span className="stat-label">AMCs Tracked</span>
                        </div>
                        <div className="stat-divider"></div>
                        <div className="stat-item">
                            <span className="stat-number">4,500+</span>
                            <span className="stat-label">Active Schemes</span>
                        </div>
                        <div className="stat-divider"></div>
                        <div className="stat-item">
                            <span className="stat-number">₹60L+ Cr</span>
                            <span className="stat-label">AUM Analyzed</span>
                        </div>
                    </div>
                </div>
            </div>

            {/* Core Features Section */}
            <div className="features-section">
                <div className="container">
                    <div className="section-header">
                        <h2>Powerful Analytical Tools</h2>
                        <p>Everything you need to decode mutual fund strategies</p>
                    </div>

                    <div className="features-grid">
                        <Link to="/stocks" className="feature-card glass-card">
                            <div className="feature-icon-wrapper blue-glow">
                                <TrendingUp size={28} className="feature-icon" />
                            </div>
                            <h3>Stock Holdings Search</h3>
                            <p>
                                Find exactly which mutual fund schemes hold a specific stock. Track their allocation percentages and historical buying or selling trends seamlessly.
                            </p>
                            <span className="feature-link">Explore Tool <span className="arrow">→</span></span>
                        </Link>

                        <Link to="/schemes" className="feature-card glass-card">
                            <div className="feature-icon-wrapper purple-glow">
                                <PieChart size={28} className="feature-icon" />
                            </div>
                            <h3>Scheme Portfolio Tracker</h3>
                            <p>
                                Dive deep into any mutual fund's portfolio. Compare monthly asset holdings, sector allocations, and spot shifting strategies effortlessly.
                            </p>
                            <span className="feature-link">Explore Tool <span className="arrow">→</span></span>
                        </Link>

                        <Link to="/insights" className="feature-card glass-card">
                            <div className="feature-icon-wrapper green-glow">
                                <Lightbulb size={28} className="feature-icon" />
                            </div>
                            <h3>Smart Market Insights</h3>
                            <p>
                                Discover macro trends. See which mid and small-cap stocks are attracting the most fund manager capital across the entire industry.
                            </p>
                            <span className="feature-link">Explore Tool <span className="arrow">→</span></span>
                        </Link>
                    </div>
                </div>
            </div>

            {/* Why Choose Us / Trust Badges */}
            <div className="trust-section">
                <div className="container trust-grid">
                    <div className="trust-item">
                        <Activity className="trust-icon" />
                        <h4>Real-Time Sync</h4>
                        <p>Data is updated instantly as AMCs release their monthly portfolios.</p>
                    </div>
                    <div className="trust-item">
                        <Database className="trust-icon" />
                        <h4>Historical Depth</h4>
                        <p>Compare trends across months to spot long-term strategic shifts.</p>
                    </div>
                    <div className="trust-item">
                        <ShieldCheck className="trust-icon" />
                        <h4>Unbiased Data</h4>
                        <p>Pure mathematical analysis directly from raw regulatory filings.</p>
                    </div>
                </div>
            </div>
        </div>
    );
}
