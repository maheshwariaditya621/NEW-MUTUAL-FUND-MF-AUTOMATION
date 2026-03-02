import { apiGet, apiPost } from './client';

/**
 * Search for stocks by company name, ISIN, or NSE symbol
 */
export async function searchStocks(query, limit = 10) {
    return apiGet('/stocks/search', { q: query, limit });
}

/**
 * Get stock holdings by identifier (ISIN, company name, or NSE symbol)
 * @param {string} identifier - ISIN, company name, or NSE symbol
 * @param {number} months - Number of months to fetch (default 3)
 * @param {string|null} endMonth - Optional end month label (e.g. "JAN-26") for month picker
 */
export async function getStockHoldings(identifier, months = 3, endMonth = null) {
    const params = { q: identifier, months };
    if (endMonth) params.end_month = endMonth;
    return apiGet('/stocks/holdings', params);
}

/**
 * Get the latest live price (LTP) for a specific stock by ISIN
 */
export const getStockPrice = (isin) => apiGet(`/stocks/${isin}/price`);

export const getBulkPrices = (isins) => apiPost('/stocks/prices', { isins });
