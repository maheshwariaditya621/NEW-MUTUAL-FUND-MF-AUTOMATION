import { apiGet, apiPost, API_BASE_URL } from './client';

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function apiDelete(endpoint) {
    const url = `${API_BASE_URL}${endpoint}`;
    const token = localStorage.getItem('token');
    const response = await fetch(url, {
        method: 'DELETE',
        headers: { ...(token ? { 'Authorization': `Bearer ${token}` } : {}) },
    });
    if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
    }
    return response.json();
}

async function apiPut(endpoint, data = {}) {
    const url = `${API_BASE_URL}${endpoint}`;
    const token = localStorage.getItem('token');
    const response = await fetch(url, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
            ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(data),
    });
    if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
    }
    return response.json();
}

async function apiDownload(endpoint, params = {}) {
    const url = new URL(`${API_BASE_URL}${endpoint}`, window.location.origin);
    Object.entries(params).forEach(([k, v]) => v !== undefined && url.searchParams.append(k, v));
    const token = localStorage.getItem('token');
    const response = await fetch(url.toString(), {
        headers: { ...(token ? { 'Authorization': `Bearer ${token}` } : {}) },
    });
    if (!response.ok) throw new Error(`Export failed: HTTP ${response.status}`);
    return response.blob();
}

// ─── Watchlist CRUD ───────────────────────────────────────────────────────────

export const getWatchlistItems = () => apiGet('/watchlist/items');
export const addWatchlistItem = (payload) => apiPost('/watchlist/items', payload);
export const removeWatchlistItem = (id) => apiDelete(`/watchlist/items/${id}`);
export const checkWatched = (assetType, identifier) =>
    apiGet('/watchlist/items/check', { asset_type: assetType, identifier });

// ─── Dashboard & Analytics ────────────────────────────────────────────────────

export const getPeriods = () => apiGet('/watchlist/periods');
export const getWatchlistDashboard = (period_id) => apiGet('/watchlist/dashboard', period_id ? { period_id } : {});
export const getStockActivity = (isin, period_id) => apiGet(`/watchlist/stocks/${isin}/activity`, period_id ? { period_id } : {});
export const getSchemeActivity = (schemeId, period_id) => apiGet(`/watchlist/schemes/${schemeId}/activity`, period_id ? { period_id } : {});
export const getActivityFeed = (period_id) => apiGet('/watchlist/activity-feed', period_id ? { period_id } : {});

// ─── Preferences ──────────────────────────────────────────────────────────────

export const getPreferences = () => apiGet('/watchlist/preferences');
export const updatePreferences = (prefs) => apiPut('/watchlist/preferences', prefs);

// ─── Export ───────────────────────────────────────────────────────────────────

export async function exportWatchlist(format = 'csv') {
    const blob = await apiDownload('/watchlist/export', { format });
    const ext = format === 'excel' ? 'xlsx' : format;
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `watchlist_export.${ext}`;
    link.click();
    URL.revokeObjectURL(link.href);
}
