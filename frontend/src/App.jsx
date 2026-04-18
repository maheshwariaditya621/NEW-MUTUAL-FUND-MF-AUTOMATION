import { BrowserRouter, Routes, Route, Link, useNavigate, useLocation } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { LogOut, User, X, ChevronDown, Lock } from 'lucide-react';
import { ThemeProvider } from './contexts/ThemeContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { WatchlistProvider } from './contexts/WatchlistContext';
import ThemeToggle from './components/common/ThemeToggle';
import ScrollToTop from './components/common/ScrollToTop';
import ProtectedRoute from './components/ProtectedRoute';
import Home from './pages/Home';
import StockHoldingsPage from './pages/StockHoldingsPage';
import SchemePortfolioPage from './pages/SchemePortfolioPage';
import InsightsPage from './pages/InsightsPage';
import AMCExplorerPage from './pages/AMCExplorerPage';
import FundsExplorerPage from './pages/FundsExplorerPage';
import SectorExposurePage from './pages/SectorExposurePage';
import AMCSectorExposurePage from './pages/AMCSectorExposurePage';
import AdminVault from './pages/AdminVault';
import LoginPage from './pages/LoginPage';
import HeaderSearch from './components/common/HeaderSearch';
import Chatbot from './components/Chatbot';
import DisclaimerBanner from './components/common/DisclaimerBanner';
import AccessDeniedPage from './pages/AccessDeniedPage';
import RegisterInvitePage from './pages/RegisterInvitePage';
import WatchlistDashboardPage from './pages/WatchlistDashboardPage';
import './App.css';

// Helper component to handle global auth events like 401
const AuthHandler = ({ children }) => {
  const { logout } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    const handleUnauthorized = () => {
      logout();
      navigate('/login');
    };

    window.addEventListener('api-unauthorized', handleUnauthorized);
    return () => window.removeEventListener('api-unauthorized', handleUnauthorized);
  }, [logout, navigate]);

  return children;
};

const SecuritySettingsModal = ({ isOpen, onClose }) => {
  const { user, token } = useAuth();
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);

  if (!isOpen) return null;

  const handleReset = async (e) => {
    e.preventDefault();
    if (newPassword !== confirmPassword) {
      setError("New passwords don't match");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch('/api/v1/auth/change-password', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          current_password: currentPassword,
          new_password: newPassword
        })
      });
      if (resp.ok) {
        setSuccess(true);
        setTimeout(onClose, 2000);
      } else {
        const data = await resp.json();
        setError(data.detail || "Failed to change password");
      }
    } catch (err) {
      setError("Network error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="modal-overlay">
      <div className="modal-content">
        <div className="modal-header">
          <h3>Security Settings</h3>
          <button className="icon-btn" onClick={onClose}><X size={20} /></button>
        </div>
        
        {success ? (
          <div style={{ padding: '2rem', textAlign: 'center' }}>
            <div style={{ color: '#4ade80', fontSize: '3rem', marginBottom: '1rem' }}>✓</div>
            <h4>Password changed!</h4>
            <p style={{ opacity: 0.7 }}>Closing in active...</p>
          </div>
        ) : (
          <form onSubmit={handleReset}>
            {error && <div className="error-message" style={{ color: '#ff6b6b', marginBottom: '1rem', textAlign: 'center' }}>{error}</div>}
            <div className="form-group">
              <label>Current Password</label>
              <input 
                type="password" 
                required 
                className="admin-input"
                value={currentPassword}
                onChange={e => setCurrentPassword(e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>New Password</label>
              <input 
                type="password" 
                required 
                className="admin-input"
                value={newPassword}
                onChange={e => setNewPassword(e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Confirm New Password</label>
              <input 
                type="password" 
                required 
                className="admin-input"
                value={confirmPassword}
                onChange={e => setConfirmPassword(e.target.value)}
              />
            </div>
            <div className="modal-footer">
              <button type="button" className="btn-secondary" onClick={onClose}>Cancel</button>
              <button type="submit" className="btn-approve" disabled={loading}>
                {loading ? 'Processing...' : 'Update Password'}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
};

// Component for the header to handle auth state
const Header = () => {
  const { user, logout, isAuthenticated, hasPermission } = useAuth();
  const [showProfileModal, setShowProfileModal] = useState(false);

  return (
    <header className="app-header">
      <div className="container">
        <div className="header-content">
          <div className="header-left">
            <Link to="/" className="logo">
              <div className="logo-container">
                <span className="logo-mf">MF</span>
                <span className="logo-analytics">Analytics</span>
              </div>
            </Link>

            {isAuthenticated && (
              <nav className="nav">
                {hasPermission('view_stocks') && <Link to="/stocks" className="nav-link">Stock Holdings</Link>}
                {hasPermission('view_portfolio') && <Link to="/schemes" className="nav-link">Scheme Portfolio</Link>}
                {hasPermission('view_insights') && <Link to="/insights" className="nav-link">Insights</Link>}
                {hasPermission('view_watchlist') && <Link to="/watchlist" className="nav-link"><span className="wl-icon">🔖</span> Watchlist</Link>}
                
                {hasPermission('view_tools') && (
                  <div className="nav-dropdown">
                    <span className="nav-link dropdown-toggle">Tools ▾</span>
                    <div className="dropdown-menu">
                      <Link to="/funds-explorer" className="dropdown-item">Funds Explorer</Link>
                      <Link to="/sector-exposure" className="dropdown-item">Fund Sector Exposure</Link>
                      <Link to="/amc-sector-exposure" className="dropdown-item">AMC Sector Exposure</Link>
                      <Link to="/amcs" className="dropdown-item">AMC Analytics</Link>
                    </div>
                  </div>
                )}
              </nav>
            )}
          </div>

          <div className="header-right">
            {isAuthenticated && <HeaderSearch />}
            <ThemeToggle />
            {isAuthenticated && (
              <div className="nav-dropdown user-menu-dropdown">
                <button className="user-menu-btn">
                  <div className="user-avatar">
                    <User size={18} />
                  </div>
                  <span className="user-name-label">{user?.username}</span>
                  <ChevronDown size={14} className="dropdown-arrow" />
                </button>
                <div className="dropdown-menu align-right">
                  <div className="dropdown-header">Account</div>
                  <button 
                    className="dropdown-item" 
                    onClick={() => setShowProfileModal(true)}
                  >
                    <Lock size={16} /> Security Settings
                  </button>
                  <div className="dropdown-divider"></div>
                  <button 
                    className="dropdown-item text-danger" 
                    onClick={logout}
                  >
                    <LogOut size={16} /> Logout
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      <SecuritySettingsModal 
        isOpen={showProfileModal} 
        onClose={() => setShowProfileModal(false)} 
      />
    </header>
  );
};

function AppContent() {
  const { isAuthenticated } = useAuth();
  const location = useLocation();

  // These pages are fully standalone — no header/footer/chatbot regardless of auth state
  const STANDALONE_ROUTES = ['/register-invite', '/access-denied', '/login'];
  const isStandalone = STANDALONE_ROUTES.some(r => location.pathname.startsWith(r));

  // Only show the shell (header/footer/chatbot) when authenticated AND not on a standalone page
  const showShell = isAuthenticated && !isStandalone;

  return (
    <AuthHandler>
      <div className="app">
        {showShell && <Header />}

        <main className={showShell ? "app-main" : "app-main-auth"}>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/register-invite" element={<RegisterInvitePage />} />
            <Route path="/access-denied" element={<AccessDeniedPage />} />

            <Route path="/" element={
              <ProtectedRoute>
                <Home />
              </ProtectedRoute>
            } />
            <Route path="/stocks" element={
              <ProtectedRoute requiredPermission="view_stocks">
                <StockHoldingsPage />
              </ProtectedRoute>
            } />
            <Route path="/schemes" element={
              <ProtectedRoute requiredPermission="view_portfolio">
                <SchemePortfolioPage />
              </ProtectedRoute>
            } />
            <Route path="/amcs" element={
              <ProtectedRoute requiredPermission="view_tools">
                <AMCExplorerPage />
              </ProtectedRoute>
            } />
            <Route path="/funds-explorer" element={
              <ProtectedRoute requiredPermission="view_tools">
                <FundsExplorerPage />
              </ProtectedRoute>
            } />
            <Route path="/insights" element={
              <ProtectedRoute requiredPermission="view_insights">
                <InsightsPage />
              </ProtectedRoute>
            } />
            <Route path="/sector-exposure" element={
              <ProtectedRoute requiredPermission="view_tools">
                <SectorExposurePage />
              </ProtectedRoute>
            } />
            <Route path="/amc-sector-exposure" element={
              <ProtectedRoute requiredPermission="view_tools">
                <AMCSectorExposurePage />
              </ProtectedRoute>
            } />
            <Route path="/admin-vault" element={
              <ProtectedRoute requiredPermission="admin">
                <AdminVault />
              </ProtectedRoute>
            } />
            <Route path="/watchlist" element={
              <ProtectedRoute requiredPermission="view_watchlist">
                <WatchlistDashboardPage />
              </ProtectedRoute>
            } />
          </Routes>
        </main>

        {showShell && (
          <footer className="app-footer">
            <div className="container">
              <div className="footer-content">
                <p className="footer-copyright">
                  Mutual Fund Analytics Platform © 2026
                </p>
                <Link to="/admin-vault" className="admin-link-btn">🔐 Admin Vault</Link>
              </div>
              <p className="footer-disclaimer">
                <strong>Disclaimer:</strong> Data and information presented on this platform have been gathered from sources believed to be reliable.
                No warranties (express or implied) are made as to accuracy or completeness. This platform is for <strong>informational purposes only</strong> and
                does not constitute financial advice. The platform shall not be liable for any loss arising from use of this information.
              </p>
            </div>
          </footer>
        )}

        {showShell && <DisclaimerBanner />}
        {showShell && <Chatbot />}
      </div>
    </AuthHandler>
  );
}

function App() {
  return (
    <ThemeProvider>
      <BrowserRouter>
        <AuthProvider>
          <WatchlistProvider>
            <ScrollToTop />
            <AppContent />
          </WatchlistProvider>
        </AuthProvider>
      </BrowserRouter>
    </ThemeProvider>
  );
}

export default App;
