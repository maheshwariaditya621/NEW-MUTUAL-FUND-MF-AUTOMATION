import { apiGet, apiPost, apiDelete, API_BASE_URL } from './client';

/**
 * Fetch paginated canonical corporate announcements with filters.
 */
export async function fetchAnnouncements(params = {}) {
    return await apiGet('/announcements', params);
}

/**
 * Fetch detailed view for a single canonical announcement.
 */
export async function fetchAnnouncementDetails(canonicalId) {
    return await apiGet(`/announcements/${canonicalId}`);
}

/**
 * Fetch available category tags with count.
 */
export async function fetchAnnouncementCategories() {
    return await apiGet('/announcements/categories');
}

/**
 * Fetch active Master Office Watchlist companies.
 */
export async function fetchActiveMasterWatchlist() {
    return await apiGet('/announcements/watchlist/active');
}

/**
 * Add a stock to the Master Office Watchlist.
 */
export async function addToMasterWatchlist(companyId, notes = '') {
    return await apiPost('/announcements/watchlist/add', {
        company_id: companyId,
        notes: notes
    });
}

/**
 * Remove a stock from the Master Office Watchlist.
 */
export async function removeFromMasterWatchlist(companyId) {
    return await apiDelete(`/announcements/watchlist/${companyId}`);
}

/**
 * Search companies to add to the Master Watchlist.
 */
export async function searchCompaniesForWatchlist(query) {
    return await apiGet('/announcements/watchlist/search', { q: query });
}

/**
 * Get collector sync states and health stats.
 */
export async function fetchSyncHealth() {
    return await apiGet('/announcements/health/sync');
}

/**
 * Trigger an immediate poll cycle.
 */
export async function triggerManualSync() {
    return await apiPost('/announcements/sync-now', {});
}

/**
 * Build direct attachment streaming/view URL.
 */
export function getAttachmentViewUrl(attachmentId) {
    const token = localStorage.getItem('token');
    const base = `${API_BASE_URL}/announcements/attachments/${attachmentId}/view`;
    return token ? `${base}?token=${encodeURIComponent(token)}` : base;
}
