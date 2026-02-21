import { Link } from 'react-router-dom';
import { TrendingUp, PieChart } from 'lucide-react';
import './Home.css';

export default function Home() {
    return (
        <div className="home">
            <div className="container">
                <div className="hero">
                    <h1 className="hero-title">Mutual Fund Analytics Platform</h1>
                    <p className="hero-subtitle">
                        Analyze mutual fund holdings and track portfolio changes with powerful search and visualization tools.
                    </p>
                </div>

                <div className="features">
                    <Link to="/stocks" className="feature-card">
                        <div className="feature-icon">
                            <TrendingUp size={32} />
                        </div>
                        <h3>Stock Holdings Search</h3>
                        <p>
                            Find which mutual fund schemes hold a specific stock. View holdings, trends, and sector allocation.
                        </p>
                        <span className="feature-link">Explore →</span>
                    </Link>

                    <Link to="/schemes" className="feature-card">
                        <div className="feature-icon">
                            <PieChart size={32} />
                        </div>
                        <h3>Scheme Portfolio Tracker</h3>
                        <p>
                            Track mutual fund portfolios over time. Compare holdings across months and analyze changes.
                        </p>
                        <span className="feature-link">Explore →</span>
                    </Link>
                </div>
            </div>
        </div>
    );
}
