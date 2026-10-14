import { BrowserRouter, Routes, Route, Link, NavLink, useNavigate, useLocation } from "react-router-dom";
import { useEffect, useState, useCallback } from "react";
import { LogOut, User, X, ChevronDown, Lock, TrendingUp, PieChart, BarChart3, Bookmark, Bell, Layers, Menu, ChevronRight, Shield } from "lucide-react";
import { ThemeProvider } from "./contexts/ThemeContext";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import { WatchlistProvider } from "./contexts/WatchlistContext";
import ThemeToggle from "./components/common/ThemeToggle";
import ScrollToTop from "./components/common/ScrollToTop";
import ProtectedRoute from "./components/ProtectedRoute";
import Home from "./pages/Home";
import StockHoldingsPage from "./pages/StockHoldingsPage";
import SchemePortfolioPage from "./pages/SchemePortfolioPage";
import InsightsPage from "./pages/InsightsPage";
import AMCExplorerPage from "./pages/AMCExplorerPage";
import FundsExplorerPage from "./pages/FundsExplorerPage";
import SectorExposurePage from "./pages/SectorExposurePage";
import AMCSectorExposurePage from "./pages/AMCSectorExposurePage";
import AdminVault from "./pages/AdminVault";
import LoginPage from "./pages/LoginPage";
import HeaderSearch from "./components/common/HeaderSearch";
import Chatbot from "./components/Chatbot";
import DisclaimerBanner from "./components/common/DisclaimerBanner";
import AccessDeniedPage from "./pages/AccessDeniedPage";
import RegisterInvitePage from "./pages/RegisterInvitePage";
import WatchlistDashboardPage from "./pages/WatchlistDashboardPage";
import AnnouncementsPage from "./pages/AnnouncementsPage";
import TimeMachinePage from "./pages/TimeMachinePage";
import "./App.css";

const AuthHandler = ({ children }) => {
  const { logout } = useAuth();
  const navigate = useNavigate();
  useEffect(() => {
    const handleUnauthorized = () => { logout(); navigate("/login"); };
    window.addEventListener("api-unauthorized", handleUnauthorized);
    return () => window.removeEventListener("api-unauthorized", handleUnauthorized);
  }, [logout, navigate]);
  return children;
};

const SecuritySettingsModal = ({ isOpen, onClose }) => {
  const { token } = useAuth();
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  if (!isOpen) return null;
  const handleReset = async (e) => {
    e.preventDefault();
    if (newPassword !== confirmPassword) { setError("New passwords don't match"); return; }
    setLoading(true); setError(null);
    try {
      const resp = await fetch("/api/v1/auth/change-password", {
        method: "POST",
        headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ current_password: currentPassword, new_password: newPassword })
      });
      if (resp.ok) { setSuccess(true); setTimeout(onClose, 2000); }
      else { const data = await resp.json(); setError(data.detail || "Failed to change password"); }
    } catch { setError("Network error"); } finally { setLoading(false); }
  };
  return (
    <div className="modal-overlay" role="dialog" aria-modal="true" aria-labelledby="security-modal-title">
      <div className="modal-content">
        <div className="modal-header">
          <h3 id="security-modal-title">Security Settings</h3>
          <button className="icon-btn" onClick={onClose} aria-label="Close security settings"><X size={20} /></button>
        </div>
        {success ? (
          <div style={{ padding: "2rem", textAlign: "center" }}>
            <div style={{ color: "#4ade80", fontSize: "3rem", marginBottom: "1rem" }}>?</div>
            <h4>Password changed!</h4>
            <p style={{ opacity: 0.7 }}>Closing...</p>
          </div>
        ) : (
          <form onSubmit={handleReset}>
            {error && <div style={{ color: "#ff6b6b", marginBottom: "1rem", textAlign: "center" }}>{error}</div>}
            <div className="form-group"><label>Current Password</label><input type="password" required className="admin-input" value={currentPassword} onChange={e => setCurrentPassword(e.target.value)} /></div>
            <div className="form-group"><label>New Password</label><input type="password" required className="admin-input" value={newPassword} onChange={e => setNewPassword(e.target.value)} /></div>
            <div className="form-group"><label>Confirm New Password</label><input type="password" required className="admin-input" value={confirmPassword} onChange={e => setConfirmPassword(e.target.value)} /></div>
            <div className="modal-footer">
              <button type="button" className="btn-secondary" onClick={onClose}>Cancel</button>
              <button type="submit" className="btn-approve" disabled={loading}>{loading ? "Processing..." : "Update Password"}</button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
};

const AVLogo = ({ size = 24, gradId = "av-grad" }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M12 2L2 7L12 12L22 7L12 2Z" fill={`url(#${gradId}-1)`}/>
    <path d="M2 17L12 22L22 17" stroke={`url(#${gradId}-2)`} strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"/>
    <path d="M2 12L12 17L22 12" stroke={`url(#${gradId}-2)`} strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"/>
    <defs>
      <linearGradient id={`${gradId}-1`} x1="2" y1="2" x2="22" y2="12" gradientUnits="userSpaceOnUse">
        <stop stopColor="#3b82f6"/><stop offset="1" stopColor="#6366f1"/>
      </linearGradient>
      <linearGradient id={`${gradId}-2`} x1="2" y1="12" x2="22" y2="22" gradientUnits="userSpaceOnUse">
        <stop stopColor="#60a5fa"/><stop offset="1" stopColor="#818cf8"/>
      </linearGradient>
    </defs>
  </svg>
);

const LogoBrand = ({ gradId = "av-grad", iconSize = 24 }) => (
  <div className="logo-container">
    <div className="logo-brand-icon"><AVLogo size={iconSize} gradId={gradId} /></div>
    <div className="logo-text-group">
      <div className="logo-text-main">
        <span className="logo-part-av">AV</span>
        <span className="logo-part-fincorp">FINCORP</span>
      </div>
      <span className="logo-text-sub">INSTITUTIONAL</span>
    </div>
  </div>
);

const MobileDrawer = ({ isOpen, onClose, hasPermission, user, logout, onOpenSecurity, isAnnouncementOnly }) => {
  const location = useLocation();
  const [toolsExpanded, setToolsExpanded] = useState(false);

  useEffect(() => { onClose(); }, [location.pathname, location.search]);

  useEffect(() => {
    if (!isOpen) return;
    const handleKey = (e) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [isOpen, onClose]);

  useEffect(() => {
    document.body.style.overflow = isOpen ? "hidden" : "";
    return () => { document.body.style.overflow = ""; };
  }, [isOpen]);

  const isActive = (path) => location.pathname === path || location.pathname.startsWith(path + "/");

  return (
    <>
      <div className={`drawer-backdrop${isOpen ? " open" : ""}`} onClick={onClose} aria-hidden="true" />
      <div id="mobile-drawer" className={`mobile-drawer${isOpen ? " open" : ""}`} role="dialog" aria-modal="true" aria-label="Navigation menu">
        <div className="drawer-header">
          <Link to={isAnnouncementOnly ? "/announcements" : "/"} className="logo drawer-logo">
            <LogoBrand gradId="av-drawer-grad" iconSize={20} />
          </Link>
          <button className="drawer-close-btn" onClick={onClose} aria-label="Close navigation menu"><X size={20} /></button>
        </div>

        {/* Search inside drawer */}
        {(hasPermission("view_stocks") || hasPermission("view_portfolio") || hasPermission("all")) && (
          <div className="drawer-search-wrap">
            <HeaderSearch />
          </div>
        )}

        <nav className="drawer-nav" aria-label="Main navigation">
          {hasPermission("view_stocks") && (
            <NavLink to="/stocks" className={`drawer-nav-link${isActive("/stocks") ? " active" : ""}`}>
              <TrendingUp size={18} className="drawer-nav-icon" /><span>Stock Holdings</span>
            </NavLink>
          )}
          {hasPermission("view_portfolio") && (
            <NavLink to="/schemes" className={`drawer-nav-link${isActive("/schemes") ? " active" : ""}`}>
              <PieChart size={18} className="drawer-nav-icon" /><span>Scheme Portfolio</span>
            </NavLink>
          )}
          {hasPermission("view_insights") && (
            <NavLink to="/insights" className={`drawer-nav-link${isActive("/insights") ? " active" : ""}`}>
              <BarChart3 size={18} className="drawer-nav-icon" /><span>Insights</span>
            </NavLink>
          )}
          {hasPermission("view_watchlist") && (
            <NavLink to="/watchlist" className={`drawer-nav-link${isActive("/watchlist") ? " active" : ""}`}>
              <Bookmark size={18} className="drawer-nav-icon" /><span>Watchlist</span>
            </NavLink>
          )}
          {hasPermission("view_announcements") && (
            <NavLink to="/announcements" className={`drawer-nav-link${isActive("/announcements") ? " active" : ""}`}>
              <Bell size={18} className="drawer-nav-icon" /><span>Announcements</span>
            </NavLink>
          )}
          {hasPermission("view_tools") && (
            <div className="drawer-nav-section">
              <button
                className={`drawer-nav-link drawer-nav-expandable${toolsExpanded ? " expanded" : ""}`}
                onClick={() => setToolsExpanded(v => !v)}
                aria-expanded={toolsExpanded}
              >
                <Layers size={18} className="drawer-nav-icon" />
                <span>Tools</span>
                <ChevronRight size={16} className="drawer-expand-icon" />
              </button>
              {toolsExpanded && (
                <div className="drawer-sub-nav">
                  <NavLink to="/funds-explorer" className={`drawer-sub-link${isActive("/funds-explorer") ? " active" : ""}`}>Funds Explorer</NavLink>
                  <NavLink to="/time-machine" className={`drawer-sub-link${isActive("/time-machine") ? " active" : ""}`}>Time-Machine Comparison</NavLink>
                  <NavLink to="/sector-exposure" className={`drawer-sub-link${isActive("/sector-exposure") ? " active" : ""}`}>Fund Sector Exposure</NavLink>
                  <NavLink to="/amc-sector-exposure" className={`drawer-sub-link${isActive("/amc-sector-exposure") ? " active" : ""}`}>AMC Sector Exposure</NavLink>
                  <NavLink to="/amcs" className={`drawer-sub-link${isActive("/amcs") ? " active" : ""}`}>AMC Analytics</NavLink>
                </div>
              )}
            </div>
          )}
        </nav>

        <div className="drawer-footer">
          <div className="drawer-user-info">
            <div className="drawer-user-avatar"><User size={18} /></div>
            <div className="drawer-user-details">
              <span className="drawer-username">{user?.username}</span>
              <span className="drawer-user-role">{user?.role || "User"}</span>
            </div>
          </div>
          <div className="drawer-footer-actions">
            <button className="drawer-action-btn" onClick={() => { onClose(); onOpenSecurity(); }}>
              <Lock size={16} /><span>Security Settings</span>
            </button>
            <button className="drawer-action-btn danger" onClick={() => { onClose(); logout(); }}>
              <LogOut size={16} /><span>Logout</span>
            </button>
          </div>
        </div>
      </div>
    </>
  );
};

const Header = () => {
  const { user, logout, isAuthenticated, hasPermission } = useAuth();
  const [showProfileModal, setShowProfileModal] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);

  const isAnnouncementOnly = isAuthenticated && user?.role !== "admin" && !hasPermission("all") && !hasPermission("view_stocks") && !hasPermission("view_portfolio") && hasPermission("view_announcements");
  const openDrawer = useCallback(() => setDrawerOpen(true), []);
  const closeDrawer = useCallback(() => setDrawerOpen(false), []);

  return (
    <header className="app-header">
      <div className="container">
        <div className="header-content">
          <div className="header-left">
            <Link to={isAnnouncementOnly ? "/announcements" : "/"} className="logo">
              <LogoBrand gradId="av-header-grad" iconSize={24} />
            </Link>
            {isAuthenticated && (
              <nav className="nav desktop-nav" aria-label="Main navigation">
                {hasPermission("view_stocks") && (
                  <NavLink to="/stocks" className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
                    <TrendingUp size={14} className="nav-icon" /><span>Stock Holdings</span>
                  </NavLink>
                )}
                {hasPermission("view_portfolio") && (
                  <NavLink to="/schemes" className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
                    <PieChart size={14} className="nav-icon" /><span>Scheme Portfolio</span>
                  </NavLink>
                )}
                {hasPermission("view_insights") && (
                  <NavLink to="/insights" className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
                    <BarChart3 size={14} className="nav-icon" /><span>Insights</span>
                  </NavLink>
                )}
                {hasPermission("view_watchlist") && (
                  <NavLink to="/watchlist" className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
                    <Bookmark size={14} className="nav-icon" /><span>Watchlist</span>
                  </NavLink>
                )}
                {hasPermission("view_announcements") && (
                  <NavLink to="/announcements" className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
                    <Bell size={14} className="nav-icon" /><span>Announcements</span>
                  </NavLink>
                )}
                {hasPermission("view_tools") && (
                  <div className="nav-dropdown">
                    <span className="nav-link dropdown-toggle">
                      <Layers size={14} className="nav-icon" /><span>Tools</span>
                      <ChevronDown size={12} className="dropdown-chevron" />
                    </span>
                    <div className="dropdown-menu">
                      <Link to="/funds-explorer" className="dropdown-item">Funds Explorer</Link>
                      <Link to="/time-machine" className="dropdown-item">Time-Machine Comparison</Link>
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
            {isAuthenticated && (hasPermission("view_stocks") || hasPermission("view_portfolio") || hasPermission("all")) && <HeaderSearch />}
            <ThemeToggle />
            {isAuthenticated && (
              <div className="nav-dropdown user-menu-dropdown desktop-user-menu">
                <button className="user-menu-btn" aria-label="User menu" aria-haspopup="true">
                  <div className="user-avatar"><User size={18} /></div>
                  <span className="user-name-label">{user?.username}</span>
                  <ChevronDown size={14} className="dropdown-arrow" />
                </button>
                <div className="dropdown-menu align-right">
                  <div className="dropdown-header">Account</div>
                  <button className="dropdown-item" onClick={() => setShowProfileModal(true)}><Lock size={16} /> Security Settings</button>
                  <div className="dropdown-divider"></div>
                  <button className="dropdown-item text-danger" onClick={logout}><LogOut size={16} /> Logout</button>
                </div>
              </div>
            )}
            {isAuthenticated && (
              <button className="hamburger-btn" onClick={openDrawer} aria-label="Open navigation menu" aria-expanded={drawerOpen} aria-controls="mobile-drawer">
                <Menu size={22} />
              </button>
            )}
          </div>
        </div>
      </div>
      <SecuritySettingsModal isOpen={showProfileModal} onClose={() => setShowProfileModal(false)} />
      {isAuthenticated && (
        <MobileDrawer
          isOpen={drawerOpen}
          onClose={closeDrawer}
          hasPermission={hasPermission}
          user={user}
          logout={logout}
          onOpenSecurity={() => setShowProfileModal(true)}
          isAnnouncementOnly={isAnnouncementOnly}
        />
      )}
    </header>
  );
};

function AppContent() {
  const { isAuthenticated } = useAuth();
  const location = useLocation();
  const STANDALONE_ROUTES = ["/register-invite", "/access-denied", "/login"];
  const isStandalone = STANDALONE_ROUTES.some(r => location.pathname.startsWith(r));
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
            <Route path="/" element={<ProtectedRoute><Home /></ProtectedRoute>} />
            <Route path="/stocks" element={<ProtectedRoute requiredPermission="view_stocks"><StockHoldingsPage /></ProtectedRoute>} />
            <Route path="/schemes" element={<ProtectedRoute requiredPermission="view_portfolio"><SchemePortfolioPage /></ProtectedRoute>} />
            <Route path="/amcs" element={<ProtectedRoute requiredPermission="view_tools"><AMCExplorerPage /></ProtectedRoute>} />
            <Route path="/funds-explorer" element={<ProtectedRoute requiredPermission="view_tools"><FundsExplorerPage /></ProtectedRoute>} />
            <Route path="/insights" element={<ProtectedRoute requiredPermission="view_insights"><InsightsPage /></ProtectedRoute>} />
            <Route path="/time-machine" element={<ProtectedRoute requiredPermission="view_insights"><TimeMachinePage /></ProtectedRoute>} />
            <Route path="/sector-exposure" element={<ProtectedRoute requiredPermission="view_tools"><SectorExposurePage /></ProtectedRoute>} />
            <Route path="/amc-sector-exposure" element={<ProtectedRoute requiredPermission="view_tools"><AMCSectorExposurePage /></ProtectedRoute>} />
            <Route path="/admin-vault" element={<ProtectedRoute requiredPermission="admin"><AdminVault /></ProtectedRoute>} />
            <Route path="/watchlist" element={<ProtectedRoute requiredPermission="view_watchlist"><WatchlistDashboardPage /></ProtectedRoute>} />
            <Route path="/announcements" element={<ProtectedRoute requiredPermission="view_announcements"><AnnouncementsPage /></ProtectedRoute>} />
          </Routes>
        </main>
        {showShell && (
          <footer className="app-footer">
            <div className="container">
              <div className="footer-content">
                <p className="footer-copyright">AV Fincorp Pvt. Ltd &copy; 2026</p>
                <Link to="/admin-vault" className="admin-link-btn"><Shield size={13} /> Admin Vault</Link>
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
