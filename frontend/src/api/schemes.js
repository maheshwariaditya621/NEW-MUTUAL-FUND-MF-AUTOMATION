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
