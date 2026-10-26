import { apiGet, apiPost } from './client';

/**
 * Fetch paginated earnings calendar events with filters.
 */
export async function fetchEarningsCalendar(params = {}) {
    return await apiGet('/earnings/calendar', params);
}

/**
 * Fetch Today's Results Desk (summary, awaiting results, published results today).
 */
export async function fetchTodayResultsDesk() {
    return await apiGet('/earnings/today');
}

/**
 * Fetch paginated published results filings.
 */
export async function fetchPublishedResults(params = {}) {
    return await apiGet('/earnings/results', params);
}

/**
 * Fetch single company earnings dossier & historical timeline.
 */
export async function fetchCompanyEarningsTimeline(companyId) {
    return await apiGet(`/earnings/company/${companyId}`);
}

/**
 * Fetch detailed view of a single calendar event.
 */
export async function fetchEventDetails(eventId) {
    return await apiGet(`/earnings/event/${eventId}`);
}

/**
 * Fetch operational diagnostics and sync status.
 */
export async function fetchEarningsSyncStatus() {
    return await apiGet('/earnings/sync-status');
}

/**
 * Trigger manual background sync cycle (admin only).
 */
export async function triggerEarningsSyncNow() {
    return await apiPost('/earnings/sync-now');
}
