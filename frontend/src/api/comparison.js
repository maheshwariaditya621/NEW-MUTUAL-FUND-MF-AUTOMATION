import { apiGet } from './client';

export async function getAvailablePeriods() {
    return apiGet('/comparison/periods');
}

export async function getSectorComparison(schemeIds, periodId = null) {
    const params = { scheme_ids: schemeIds.join(',') };
    if (periodId) params.period_id = periodId;
    return apiGet('/comparison/sector-exposure', params);
}

export async function getSectorCompanies(schemeIds, sectorName, periodId = null) {
    const params = {
        scheme_ids: schemeIds.join(','),
        sector_name: sectorName
    };
    if (periodId) params.period_id = periodId;
    return apiGet('/comparison/sector-companies', params);
}

export async function getAMCSectorComparison(amcIds, periodId = null) {
    const params = { amc_ids: amcIds.join(',') };
    if (periodId) params.period_id = periodId;
    return apiGet('/comparison/amc-sector-exposure', params);
}

export async function getAMCSectorCompanies(amcIds, sectorName, periodId = null) {
    const params = {
        amc_ids: amcIds.join(','),
        sector_name: sectorName
    };
    if (periodId) params.period_id = periodId;
    return apiGet('/comparison/amc-sector-companies', params);
}
