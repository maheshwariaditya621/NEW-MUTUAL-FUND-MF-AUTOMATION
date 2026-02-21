import { apiGet } from './client';

/**
 * Search for schemes by name or AMC
 */
export async function searchSchemes(query, limit = 10) {
    return apiGet('/schemes/search', { q: query, limit });
}

/**
 * Get scheme portfolio by identifier (scheme ID or scheme name)
 */
export async function getSchemePortfolio(identifier, months = 4) {
    return apiGet('/schemes/portfolio', { q: identifier, months });
}
