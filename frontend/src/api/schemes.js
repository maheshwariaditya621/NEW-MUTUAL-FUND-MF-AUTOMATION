import { apiGet } from './client';

/**
 * Search for schemes by name or AMC
 */
export async function searchSchemes(query, limit = 10) {
    return apiGet('/schemes/search', { q: query, limit });
}

export async function getSchemePortfolio(identifier, months = 4, endMonth = null) {
    const params = { q: identifier, months };
    if (endMonth) params.end_month = endMonth;
    return apiGet('/schemes/portfolio', params);
}

/**
 * Fetch verified fund managers for a specific scheme
 */
export async function getSchemeFundManagers(schemeId) {
    return apiGet(`/schemes/${schemeId}/fund-managers`);
}

/**
 * Fetch historical fund managers and transitions for a scheme
 */
export async function getSchemeFundManagersHistory(schemeId) {
    return apiGet(`/schemes/${schemeId}/fund-managers/history`);
}

/**
 * Fetch manager profile and scheme assignments
 */
export async function getFundManagerProfile(managerId) {
    return apiGet(`/fund-managers/${managerId}`);
}

/**
 * List or search fund managers
 */
export async function listFundManagers(params = {}) {
    return apiGet('/fund-managers', params);
}

