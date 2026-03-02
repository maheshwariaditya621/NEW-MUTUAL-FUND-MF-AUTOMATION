import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import { ThemeProvider } from './contexts/ThemeContext';
import ThemeToggle from './components/common/ThemeToggle';
import ScrollToTop from './components/common/ScrollToTop';
import Home from './pages/Home';
import StockHoldingsPage from './pages/StockHoldingsPage';
import SchemePortfolioPage from './pages/SchemePortfolioPage';
import InsightsPage from './pages/InsightsPage';
import AMCExplorerPage from './pages/AMCExplorerPage';
import AdminVault from './pages/AdminVault';
import HeaderSearch from './components/common/HeaderSearch';
import Chatbot from './components/Chatbot';
import './App.css';

function App() {
  return (
    <ThemeProvider>
      <BrowserRouter>
        <ScrollToTop />
        <div className="app">
          <header className="app-header">
            <div className="container">
              <div className="header-content">
                <div className="header-left">
                  <Link to="/" className="logo">
                    <h1>MF Analytics</h1>
                  </Link>

                  <nav className="nav">
                    <Link to="/stocks" className="nav-link">Stock Holdings</Link>
                    <Link to="/schemes" className="nav-link">Scheme Portfolio</Link>
                    <Link to="/amcs" className="nav-link">Funds Explorer</Link>
                    <Link to="/insights" className="nav-link">Insights</Link>
                  </nav>
                </div>

                <div className="header-right">
                  <HeaderSearch />
                  <ThemeToggle />
                </div>
              </div>
            </div>
          </header>

          <main className="app-main">
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/stocks" element={<StockHoldingsPage />} />
              <Route path="/schemes" element={<SchemePortfolioPage />} />
              <Route path="/amcs" element={<AMCExplorerPage />} />
              <Route path="/insights" element={<InsightsPage />} />
              <Route path="/admin-vault" element={<AdminVault />} />
            </Routes>
          </main>

          <footer className="app-footer">
            <div className="container">
              <div className="footer-content">
                <p className="footer-copyright">
                  Mutual Fund Analytics Platform © 2026
                </p>
                <Link to="/admin-vault" className="admin-link-btn">🔐 Admin Vault</Link>
              </div>
            </div>
          </footer>

          {/* AI Chatbot widget — visible on every page */}
          <Chatbot />
        </div>
      </BrowserRouter>
    </ThemeProvider>
  );
}

export default App;
