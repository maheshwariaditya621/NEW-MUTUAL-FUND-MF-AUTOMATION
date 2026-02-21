import { apiGet } from './client';

/**
 * Search for stocks by company name, ISIN, or NSE symbol
 */
export async function searchStocks(query, limit = 10) {
    return apiGet('/stocks/search', { q: query, limit });
}

/**
 * Get stock holdings by identifier (ISIN, company name, or NSE symbol)
 */
export async function getStockHoldings(identifier, months = 4) {
    return apiGet('/stocks/holdings', { q: identifier, months });
}
