import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import { ThemeProvider } from './contexts/ThemeContext';
import ThemeToggle from './components/common/ThemeToggle';
import Home from './pages/Home';
import StockHoldingsPage from './pages/StockHoldingsPage';
import SchemePortfolioPage from './pages/SchemePortfolioPage';
import HeaderSearch from './components/common/HeaderSearch';
import './App.css';

function App() {
  return (
    <ThemeProvider>
      <BrowserRouter>
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
            </Routes>
          </main>

          <footer className="app-footer">
            <div className="container">
              <p className="text-muted text-center">
                Mutual Fund Analytics Platform © 2026
              </p>
            </div>
          </footer>
        </div>
      </BrowserRouter>
    </ThemeProvider>
  );
}

export default App;
