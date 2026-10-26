import { apiGet, apiPost } from './client';

/**
 * Fetch paginated bulk and block deals with filters and sorting.
 */
export async function fetchDeals(params = {}) {
    return await apiGet('/deals', params);
}

/**
 * Fetch aggregate deals summary statistics and category breakdown.
 */
export async function fetchDealsSummary(params = {}) {
    return await apiGet('/deals/summary', params);
}

/**
 * Fetch operational sync checkpoints, collector health, and recent ingestion runs.
 */
export async function fetchDealsStatus() {
    return await apiGet('/deals/status');
}

/**
 * Manually trigger background sync cycle across NSE & BSE (admin only).
 */
export async function triggerDealsSync(days = 5, dealType = 'ALL') {
    return await apiPost(`/deals/sync-now?days=${days}&deal_type=${dealType}`);
}

/**
 * Upload manual deals CSV for parsing, validation, and optional dry-run preview.
 */
export async function uploadDealsCsv(payload) {
    return await apiPost('/deals/upload-csv', payload);
}
