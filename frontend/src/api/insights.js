import { apiGet } from './client';

/**
 * Fetch stock activity insights (Top Buys/Sells).
 * 
 * @param {string} activityType - 'buying' or 'selling'
 * @param {string} mcapCategory - Optional 'Large Cap', 'Mid Cap', or 'Small Cap'
 * @param {number} limit - Number of results (default 250)
 * @returns {Promise<Object>} The activity response with month and results
 */
export const getStockActivity = async (activityType = 'buying', mcapCategory = null, limit = 250) => {
    const params = {
        activity_type: activityType,
        limit
    };

    if (mcapCategory && mcapCategory !== 'All') {
        params.mcap_category = mcapCategory;
    }

    return apiGet('/insights/stock-activity', params);
};

/**
 * Fetch Time-Machine historical comparison between two periods.
 * 
 * @param {string|null} basePeriod - Origin period (e.g. 'OCT-25')
 * @param {string|null} targetPeriod - Destination period (e.g. 'JAN-26')
 * @param {number} limit - Number of stocks per category
 * @returns {Promise<Object>} Time machine response with summary, buys, sells, entrants, exits, and sector shifts
 */
export const getTimeMachineComparison = async (basePeriod = null, targetPeriod = null, limit = 60) => {
    const params = { limit };
    if (basePeriod) params.base_period = basePeriod;
    if (targetPeriod) params.target_period = targetPeriod;
    return apiGet('/insights/time-machine', params);
};
