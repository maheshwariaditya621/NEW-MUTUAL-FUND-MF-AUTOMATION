import { BrowserRouter, Routes, Route, Link, useNavigate } from 'react-router-dom';
import { useEffect } from 'react';
import { ThemeProvider } from './contexts/ThemeContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import ThemeToggle from './components/common/ThemeToggle';
import ScrollToTop from './components/common/ScrollToTop';
import ProtectedRoute from './components/ProtectedRoute';
import Home from './pages/Home';
import StockHoldingsPage from './pages/StockHoldingsPage';
import SchemePortfolioPage from './pages/SchemePortfolioPage';
import InsightsPage from './pages/InsightsPage';
import AMCExplorerPage from './pages/AMCExplorerPage';
import AdminVault from './pages/AdminVault';
import LoginPage from './pages/LoginPage';
import HeaderSearch from './components/common/HeaderSearch';
import Chatbot from './components/Chatbot';
import DisclaimerBanner from './components/common/DisclaimerBanner';
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

// Component for the header to handle auth state
const Header = () => {
  const { user, logout, isAuthenticated } = useAuth();

  return (
    <header className="app-header">
      <div className="container">
        <div className="header-content">
          <div className="header-left">
            <Link to="/" className="logo">
              <h1>MF Analytics</h1>
            </Link>

            {isAuthenticated && (
              <nav className="nav">
                <Link to="/stocks" className="nav-link">Stock Holdings</Link>
                <Link to="/schemes" className="nav-link">Scheme Portfolio</Link>
                <Link to="/amcs" className="nav-link">Funds Explorer</Link>
                <Link to="/insights" className="nav-link">Insights</Link>
              </nav>
            )}
          </div>

          <div className="header-right">
            {isAuthenticated && <HeaderSearch />}
            <ThemeToggle />
            {isAuthenticated && (
              <button onClick={logout} className="logout-btn" title="Logout">
                Logout
              </button>
            )}
          </div>
        </div>
      </div>
    </header>
  );
};

function AppContent() {
  const { isAuthenticated } = useAuth();

  return (
    <AuthHandler>
      <div className="app">
        {isAuthenticated && <Header />}

        <main className={isAuthenticated ? "app-main" : "app-main-auth"}>
          <Routes>
            <Route path="/login" element={<LoginPage />} />

            <Route path="/" element={
              <ProtectedRoute>
                <Home />
              </ProtectedRoute>
            } />
            <Route path="/stocks" element={
              <ProtectedRoute>
                <StockHoldingsPage />
              </ProtectedRoute>
            } />
            <Route path="/schemes" element={
              <ProtectedRoute>
                <SchemePortfolioPage />
              </ProtectedRoute>
            } />
            <Route path="/amcs" element={
              <ProtectedRoute>
                <AMCExplorerPage />
              </ProtectedRoute>
            } />
            <Route path="/insights" element={
              <ProtectedRoute>
                <InsightsPage />
              </ProtectedRoute>
            } />
            <Route path="/admin-vault" element={
              <ProtectedRoute>
                <AdminVault />
              </ProtectedRoute>
            } />
          </Routes>
        </main>

        {isAuthenticated && (
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

        {isAuthenticated && <DisclaimerBanner />}
        {isAuthenticated && <Chatbot />}
      </div>
    </AuthHandler>
  );
}

function App() {
  return (
    <ThemeProvider>
      <BrowserRouter>
        <AuthProvider>
          <ScrollToTop />
          <AppContent />
        </AuthProvider>
      </BrowserRouter>
    </ThemeProvider>
  );
}

export default App;
