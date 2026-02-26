import { apiGet } from './client';

/**
 * Fetch stock activity insights (Top Buys/Sells).
 * 
 * @param {string} activityType - 'buying' or 'selling'
 * @param {string} mcapCategory - Optional 'Large Cap', 'Mid Cap', or 'Small Cap'
 * @param {number} limit - Number of results (default 50)
 * @returns {Promise<Object>} The activity response with month and results
 */
export const getStockActivity = async (activityType = 'buying', mcapCategory = null, limit = 50) => {
    const params = {
        activity_type: activityType,
        limit
    };

    if (mcapCategory && mcapCategory !== 'All') {
        params.mcap_category = mcapCategory;
    }

    return apiGet('/insights/stock-activity', params);
};
